package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/retry"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/errcode"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const (
	// defaultMaxEmptyAnswerRetries is the empty-answer re-prompt budget used
	// when AICHAT_MAX_EMPTY_ANSWER_RETRIES is unset or invalid.
	defaultMaxEmptyAnswerRetries = 3
	// maxEmptyAnswerRetriesCap is the hard ceiling. The env var can never push
	// the budget above this, so a misconfiguration can't make the loop
	// re-prompt excessively.
	maxEmptyAnswerRetriesCap = 5
	// envMaxEmptyAnswerRetries overrides the empty-answer re-prompt budget.
	envMaxEmptyAnswerRetries = "AICHAT_MAX_EMPTY_ANSWER_RETRIES"

	// defaultMaxToolRetries is how many times the model may re-call a SINGLE
	// tool that keeps erroring, per turn, before the loop refuses to invoke it
	// again. Counts retries only: 0 = one attempt / no retry, 3 = up to four
	// attempts. Distinct from a tool's own RetryPolicy (transient HTTP retries
	// inside one invocation) and from the empty-answer guard.
	defaultMaxToolRetries = 3
	// maxToolRetriesCap is the hard ceiling for AICHAT_MAX_TOOL_RETRIES.
	maxToolRetriesCap = 10
	// envMaxToolRetries overrides the per-tool error-retry cap.
	envMaxToolRetries = "AICHAT_MAX_TOOL_RETRIES"
)

// emptyAnswerNudge is injected as a user message to make the model produce a
// final written answer (or retry the failed tool) instead of ending silently.
const emptyAnswerNudge = "You ended your turn without writing any answer for the user. " +
	"If a tool failed, either correct the tool input and call it again, or use the data you already have to answer in plain text. " +
	"You must always provide a final written answer - never end your turn silently."

// toolErrorRecoveryHint is embedded in every surfaced tool_result error so the
// model is told how to recover entirely in CODE — independent of the system
// prompt. The prompt is DB-managed and user-editable; if its recovery guidance
// is removed, recovery must still work because of this hint plus the
// empty-answer guard in the loop.
const toolErrorRecoveryHint = "If you can correct the tool input, call the tool again. " +
	"Otherwise, answer the user in plain text using the data you already have. " +
	"Never end your turn without a written answer."

// policyDenyRecoveryHint guides recovery from a security/policy denial: the
// request must not be retried unchanged.
const policyDenyRecoveryHint = "This request was denied by policy; do not retry it unchanged. " +
	"Work within the allowed scope or explain the limitation to the user in plain text. " +
	"Never end your turn without a written answer."

// toolRetryCapHint is returned in place of a tool result once a tool has hit
// the per-turn retry cap. It steers the model to stop hammering the tool and
// answer with what it has — code-enforced, independent of the system prompt.
const toolRetryCapHint = "This tool has failed too many times this turn and is now disabled. " +
	"Do not call it again. Answer the user in plain text using the information you already have. " +
	"Never end your turn without a written answer."

// chartRecallNudge is injected as a user message when the model's final answer
// references a chart image it did not actually generate this turn (no QuickChart
// call produced it). It forces ONE real tool call so we can patch the produced
// chart into the answer in place. The model is told to call the tool ONLY (not
// rewrite its answer) — the loop preserves the already-shown answer and
// substitutes the real chart URL. Capped at one round per turn.
const chartRecallNudge = "Your last answer references a chart image, but you did not call the QuickChart tool " +
	"this turn, so no chart was actually generated. Call QuickChart now to generate exactly that chart. " +
	"Do not rewrite or resend your answer — just call the tool. Do not invent, guess, modify, or reuse a chart URL."

// envIntClamped reads key as an int, returning def when unset/invalid and
// clamping the parsed value to [0, max].
func envIntClamped(key string, def, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	switch {
	case n < 0:
		return 0
	case n > max:
		return max
	default:
		return n
	}
}

// resolveMaxEmptyAnswerRetries reads AICHAT_MAX_EMPTY_ANSWER_RETRIES
// (default 3, floor 0, hard cap 5).
func resolveMaxEmptyAnswerRetries() int {
	return envIntClamped(envMaxEmptyAnswerRetries, defaultMaxEmptyAnswerRetries, maxEmptyAnswerRetriesCap)
}

// resolveMaxToolRetries reads AICHAT_MAX_TOOL_RETRIES (default 3, floor 0,
// hard cap 10).
func resolveMaxToolRetries() int {
	return envIntClamped(envMaxToolRetries, defaultMaxToolRetries, maxToolRetriesCap)
}

// Process drives the LLM↔tool loop. See spec §6.5 for the full state diagram.
func (a *agentNode) Process(ctx context.Context, in node.AgentInput, sink node.StreamSink) error {
	// 1. Load history.
	var history []node.Message
	if a.mem != nil {
		id, _ := identity.FromContext(ctx)
		h, err := a.mem.Load(ctx, node.MemoryKey{
			WorkflowID: a.workflowID,
			AccountID:  id.AccountID,
			UserID:     id.UserID,
			SessionID:  in.SessionID,
		})
		if err != nil {
			return a.failStream(sink, errcode.MemoryLoadError, err)
		}
		history = h
	}

	// 2. Render system message.
	sys, err := a.env.Render(a.systemPrompt(), in.RenderCtx)
	if err != nil {
		return a.failStream(sink, errcode.RenderError, err)
	}

	// 3. Build initial messages. Attachments become image/document blocks
	// placed BEFORE the text block, mirroring the Anthropic-recommended
	// ordering (visual context first, instruction second).
	userBlocks := make([]node.ContentBlock, 0, len(in.Attachments)+1)
	for _, a := range in.Attachments {
		if blk, ok := attachmentBlock(a); ok {
			userBlocks = append(userBlocks, blk)
		}
	}
	userBlocks = append(userBlocks, node.ContentBlock{Type: node.BlockText, Text: in.Message})
	userMsg := node.Message{Role: node.UserMsg, Content: userBlocks}
	msgs := append([]node.Message{}, history...)
	msgs = append(msgs, userMsg)

	// 4. Build tool specs from connected tools.
	toolSpecs := make([]node.ToolSpec, 0, len(a.tools))
	for _, t := range a.tools {
		toolSpecs = append(toolSpecs, t.ToolSpec())
	}

	// 5. Iteration loop.
	emptyAnswerRetries := 0
	toolErrorCounts := map[string]int{} // per-tool error count this turn (retry cap)
	var producedCharts []chartRef       // charts tools actually rendered this turn (key+url)

	// Forced chart-recall state — all per-Process-call (per-turn) locals, so they
	// reset every turn and cannot leak suppression into a later turn. An early
	// error/terminate return during a recall round simply ends the turn (the
	// terminal frame goes out via sink.Close in completeStream/failStream, which
	// bypass the emit gate), so no explicit cleanup is needed. A very low
	// MaxIterations could exhaust during a recall and surface a max-iterations
	// error instead of the preserved answer — acceptable given the cap of 1.
	chartRecallForced := false  // true once we've forced a chart re-call this turn (cap = 1)
	forcedRecallActive := false // true ONLY during the forced recall round (suppresses client events)
	preservedAnswer := ""       // the answer the user already read, preserved across the recall

	// emit sends an intermediate stream event to the client EXCEPT during a
	// forced chart recall: that round runs invisibly so the answer already on
	// screen stays frozen (chart shown as *(chart)* until the patched complete).
	emit := func(ev node.StreamEvent) {
		if forcedRecallActive {
			return
		}
		_ = sink.Send(ctx, ev)
	}

	for iter := 0; iter < a.cfg.MaxIterations; iter++ {
		if ctx.Err() != nil {
			return a.failStream(sink, errcode.Cancelled, ctx.Err())
		}
		events := make(chan node.LLMEvent, 32)
		go func() {
			defer close(events)

			var llmPolicy *retry.Policy
			if rp := a.env.RetryPolicy(); rp != nil {
				if p, ok := rp.(*retry.Policy); ok {
					llmPolicy = p
				}
			}

			req := node.LLMRequest{
				System:    sys,
				Messages:  msgs,
				Tools:     toolSpecs,
				MaxTokens: 4096,
			}

			llmLabel := fmt.Sprintf("wf=%s agent=%s llm-stream", a.workflowID, a.env.NodeID())
			res := retry.DoWithLogger(ctx, llmPolicy, a.env.Logger(), llmLabel, func(rctx context.Context, _ int) (struct{}, error) {
				attemptEvents := make(chan node.LLMEvent, 32)
				streamRet := make(chan error, 1)
				go func() {
					streamRet <- a.llm.Stream(rctx, req, attemptEvents)
				}()
				emitted := false
				var lastLLMErr error
				for {
					select {
					case ev, ok := <-attemptEvents:
						if !ok {
							// attemptEvents closed by Stream's defer-close.
							serr := <-streamRet
							if serr == nil && lastLLMErr == nil {
								return struct{}{}, nil
							}
							err := serr
							if err == nil {
								err = lastLLMErr
							}
							if emitted {
								// Mid-stream: do NOT retry; force terminal.
								return struct{}{}, &node.ToolError{
									Code:      string(retry.ClassStreamEmitted),
									Cause:     err,
									Retryable: false,
								}
							}
							return struct{}{}, err
						}
						if ev.Kind == node.LLMError {
							// Defer surfacing — may be retryable.
							lastLLMErr = ev.Error
							continue
						}
						emitted = true
						events <- ev
					case <-rctx.Done():
						return struct{}{}, rctx.Err()
					}
				}
			})

			if res.Err != nil {
				select {
				case events <- node.LLMEvent{Kind: node.LLMError, Error: res.Err}:
				case <-ctx.Done():
				}
			}
		}()

		var (
			assistantBlocks []node.ContentBlock
			stopReason      string
			toolCalls       []*node.LLMToolUse
			llmErr          error
		)

		for ev := range events {
			switch ev.Kind {
			case node.LLMTextDelta:
				emit(node.StreamEvent{
					Type: node.StreamDelta,
					Data: mustJSON(map[string]string{"text": ev.Delta}),
				})
				assistantBlocks = appendOrExtendText(assistantBlocks, ev.Delta)
			case node.LLMToolUseStop:
				toolCalls = append(toolCalls, ev.ToolUse)
				a.tracef("iter=%d model tool_call name=%s id=%s inputBytes=%d", iter, ev.ToolUse.Name, ev.ToolUse.ID, len(ev.ToolUse.InputJSON))
				emit(node.StreamEvent{
					Type: node.StreamToolCall,
					Data: mustJSON(map[string]any{
						"toolUseId": ev.ToolUse.ID,
						"name":      ev.ToolUse.Name,
						"input":     json.RawMessage(ev.ToolUse.InputJSON),
					}),
				})
				assistantBlocks = append(assistantBlocks, node.ContentBlock{
					Type: node.BlockToolUse, ToolUseID: ev.ToolUse.ID,
					ToolName: ev.ToolUse.Name, ToolInput: ev.ToolUse.InputJSON,
				})
			case node.LLMMessageStop:
				stopReason = ev.Stop
			case node.LLMError:
				llmErr = ev.Error
			}
		}
		if llmErr != nil {
			code := errcode.LLMError
			switch {
			case errors.Is(llmErr, context.DeadlineExceeded):
				code = errcode.Timeout
			case errors.Is(llmErr, context.Canceled):
				code = errcode.Cancelled
			}
			return a.failStream(sink, code, llmErr)
		}

		msgs = append(msgs, node.Message{Role: node.AssistantMsg, Content: assistantBlocks})

		if stopReason == "end_turn" || (stopReason == "" && len(toolCalls) == 0) {
			// Guard against an empty answer: if the model ends its turn without
			// writing any text (commonly after a tool error it couldn't
			// recover from), re-prompt it for a real answer instead of closing
			// the stream with an empty bubble — which the client renders as a
			// blank, truncated conversation. Bounded by maxEmptyAnswerRetries.
			if !forcedRecallActive && strings.TrimSpace(lastText(msgs[len(msgs)-1])) == "" && emptyAnswerRetries < a.maxEmptyAnswerRetries {
				emptyAnswerRetries++
				if len(assistantBlocks) == 0 {
					// An empty-content assistant message is invalid for the
					// LLM; keep the turn well-formed before appending the nudge.
					msgs[len(msgs)-1].Content = []node.ContentBlock{{Type: node.BlockText, Text: "(no answer)"}}
				}
				msgs = append(msgs, node.Message{
					Role:    node.UserMsg,
					Content: []node.ContentBlock{{Type: node.BlockText, Text: emptyAnswerNudge}},
				})
				continue
			}
			finalMsg := msgs[len(msgs)-1]
			finalText := lastText(finalMsg)
			// If this completion follows a forced chart recall, ignore whatever the
			// model said this round and restore the answer the user already read;
			// only its chart is patched in place below (correctChartRefs substitutes
			// the produced chart URL). Keeps the prose stable across the recall.
			if forcedRecallActive {
				forcedRecallActive = false
				finalText = preservedAnswer
				finalMsg = node.Message{Role: node.AssistantMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: preservedAnswer}}}
			}
			referenced := extractChartKeys(finalText)
			producedKeys := chartKeysOf(producedCharts)
			if len(referenced) > 0 || len(producedCharts) > 0 {
				a.tracef("turn complete: referencedChartKeys=%v producedChartKeys=%v", referenced, producedKeys)
				for _, rk := range referenced {
					if !sliceContains(producedKeys, rk) {
						a.tracef("WARNING model referenced chart key %s NOT produced this turn (possible fabrication or prior-turn chart)", rk)
					}
				}
			}
			// Force ONE chart-only recall: if the model referenced exactly one chart
			// it did NOT produce this turn, store this answer and make the model call
			// QuickChart (chart only). On the next completion we restore this answer
			// and correctChartRefs substitutes the real produced URL in place, so the
			// chart fills in where the user is already reading — no prose change, no
			// bubble collapse. The recall round's client events are suppressed (emit).
			// Multi-chart-partial and "already forced" fall through to the gray box.
			if missing := missingChartKeys(referenced, producedKeys); len(missing) == 1 && !chartRecallForced {
				chartRecallForced = true
				forcedRecallActive = true
				preservedAnswer = finalText
				a.tracef("turn complete: forcing one QuickChart re-call for unproduced chart key %s", missing[0])
				msgs = append(msgs, node.Message{
					Role:    node.UserMsg,
					Content: []node.ContentBlock{{Type: node.BlockText, Text: chartRecallNudge}},
				})
				continue
			}
			// Neutralize fabricated/corrupted chart URLs: rewrite the answer so it
			// can only show charts actually rendered this turn (inline, in place).
			corrected := correctChartRefs(finalText, producedCharts)
			// Then ensure no produced chart is silently dropped: when the model
			// embeds a chart inline and then calls another tool, that chart lands in
			// an intermediate (reasoning) segment rather than the final assistant
			// message, so the user never sees it. Append any produced chart absent
			// from the final answer (in production order).
			withCharts, appended := appendMissingCharts(corrected, producedCharts)
			if appended > 0 {
				a.tracef("turn complete: appended %d produced chart(s) missing from the final answer", appended)
				corrected = withCharts
			}
			if corrected != finalText {
				a.tracef("turn complete: rewrote chart reference(s) in final answer to match rendered charts")
				finalMsg = node.Message{Role: node.AssistantMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: corrected}}}
			}
			return a.completeStream(ctx, sink, in, userMsg, finalMsg)
		}

		// Invoke each tool, build tool_result blocks.
		toolResults := make([]node.ContentBlock, 0, len(toolCalls))
		for _, tc := range toolCalls {
			tool := a.lookupTool(tc.Name)
			if tool == nil {
				toolResults = append(toolResults, node.ContentBlock{
					Type: node.BlockToolResult, ToolUseID: tc.ID,
					IsError:    true,
					ToolResult: mustJSON(map[string]string{"error": "unknown tool: " + tc.Name}),
				})
				continue
			}

			// Per-tool retry cap: once a tool has errored more than
			// maxToolRetries times this turn, stop invoking it and steer the
			// model to answer with what it has. Bounds runaway retry loops
			// (e.g. a chart the model can't fix) independently of the prompt.
			if toolErrorCounts[tc.Name] > a.maxToolRetries {
				capPayload := mustJSON(map[string]any{
					"error":    fmt.Sprintf("tool %q failed %d times this turn; disabled for the rest of this turn (retry cap %d reached)", tc.Name, toolErrorCounts[tc.Name], a.maxToolRetries),
					"code":     "retry-cap-exceeded",
					"recovery": toolRetryCapHint,
				})
				emit(node.StreamEvent{
					Type: node.StreamToolResult,
					Data: mustJSON(map[string]any{"toolUseId": tc.ID, "output": json.RawMessage(capPayload), "isError": true}),
				})
				toolResults = append(toolResults, node.ContentBlock{
					Type: node.BlockToolResult, ToolUseID: tc.ID,
					ToolResult: capPayload, IsError: true,
				})
				continue
			}

			// Per-invoke attachment emitter: writes attachments straight to
			// the sink as the tool emits them. Tools can emit zero, one, or
			// many attachments; each arrives at the client immediately rather
			// than after the tool returns. The emitter binds tc.ID so the
			// client knows which tool call the attachment belongs to.
			toolUseID := tc.ID
			emitter := node.AttachmentEmitterFunc(func(emitCtx context.Context, att node.ToolAttachment) error {
				if forcedRecallActive {
					return nil
				}
				return sink.Send(emitCtx, node.StreamEvent{
					Type: node.StreamAttachment,
					Data: mustJSON(map[string]any{
						"toolUseId": toolUseID,
						"kind":      att.Kind,
						"payload":   json.RawMessage(att.Payload),
					}),
				})
			})
			invokeCtx := node.WithAttachmentEmitter(ctx, emitter)

			var (
				out          json.RawMessage
				err          error
				toolAttempts = 1
				toolErrClass retry.Class
			)
			if cu, ok := tool.(node.ClientUITool); ok && cu.ClientOnly() {
				timeout := time.Duration(a.cfg.HumanResponseTimeoutSeconds) * time.Second
				if timeout <= 0 {
					timeout = 5 * time.Minute
				}
				out, err = a.env.AwaitClientToolResult(invokeCtx, toolUseID, timeout)
				if err != nil {
					err = fmt.Errorf("client-tool %s: %w", tc.Name, err)
				} else {
					// _skipped:true on the result means the user clicked Skip.
					// Convert to a surfaced tool error so the LLM treats it like
					// any other failure and recovers (rephrase/default/move on).
					var probe struct {
						Skipped bool `json:"_skipped"`
					}
					_ = json.Unmarshal(out, &probe)
					if probe.Skipped {
						err = errors.New("user declined to answer")
					}
				}
			} else {
				var policy *retry.Policy
				if rp := tool.RetryPolicy(); rp != nil {
					if p, ok := rp.(*retry.Policy); ok {
						policy = p
					}
				}
				toolLabel := fmt.Sprintf("wf=%s agent=%s tool=%s", a.workflowID, a.env.NodeID(), tc.Name)
				res := retry.DoWithLogger(invokeCtx, policy, a.env.Logger(), toolLabel, func(ctx context.Context, _ int) (json.RawMessage, error) {
					return tool.Invoke(ctx, tc.InputJSON)
				})
				out, err = res.Value, res.Err
				toolAttempts = res.Attempts
				if toolAttempts <= 0 {
					toolAttempts = 1
				}
				toolErrClass = res.LastClass
			}

			isErr := err != nil
			payload := out
			// Bedrock rejects user messages with empty content; a tool that legitimately
			// returns nothing must still produce a non-empty tool_result block.
			if !isErr && isBlankToolPayload(payload) {
				payload = mustJSON("(tool returned no output)")
			}
			if isErr {
				toolErrorCounts[tc.Name]++
				var pde *node.PolicyDeniedError
				if errors.As(err, &pde) {
					payload = mustJSON(map[string]any{
						"error":             err.Error(),
						"code":              errcode.PolicyDeny,
						"allowedIndices":    pde.AllowedIndices,
						"attemptsExhausted": toolAttempts,
						"recovery":          policyDenyRecoveryHint,
					})
				} else {
					code := string(toolErrClass)
					if code == "" {
						code = "validation"
					}
					payload = mustJSON(map[string]any{
						"error":             err.Error(),
						"code":              code,
						"attemptsExhausted": toolAttempts,
						"recovery":          toolErrorRecoveryHint,
					})
				}
				if tool.FailurePolicy() == node.FailureTerminate {
					return a.failStream(sink, errcode.ToolError, err)
				}
			}
			emit(node.StreamEvent{
				Type: node.StreamToolResult,
				Data: mustJSON(map[string]any{
					"toolUseId": tc.ID,
					"output":    json.RawMessage(payload),
					"isError":   isErr,
				}),
			})
			toolResults = append(toolResults, node.ContentBlock{
				Type: node.BlockToolResult, ToolUseID: tc.ID,
				ToolResult: payload, IsError: isErr,
			})
		}
		for _, tr := range toolResults {
			if tr.IsError {
				continue
			}
			for _, ref := range extractChartRefs(string(tr.ToolResult)) {
				if !sliceContains(chartKeysOf(producedCharts), ref.key) {
					producedCharts = append(producedCharts, ref)
					a.tracef("iter=%d produced chart key=%s", iter, ref.key)
				}
			}
		}
		msgs = append(msgs, node.Message{Role: node.UserMsg, Content: toolResults})
	}

	return a.failStream(sink, errcode.MaxIterations,
		fmt.Errorf("ai/agent: reached maxIterations=%d", a.cfg.MaxIterations))
}

func (a *agentNode) completeStream(ctx context.Context, sink node.StreamSink, in node.AgentInput, userMsg, finalAssistantMsg node.Message) error {
	if a.mem != nil {
		id, _ := identity.FromContext(ctx)
		if err := a.mem.Append(ctx, node.MemoryKey{
			WorkflowID: a.workflowID,
			AccountID:  id.AccountID,
			UserID:     id.UserID,
			SessionID:  in.SessionID,
		}, node.Turn{User: messageForMemory(userMsg), Assistant: textOnlyMessage(finalAssistantMsg), Timestamp: time.Now().UTC()}); err != nil {
			return a.failStream(sink, errcode.MemoryAppendError, err)
		}
	}
	return sink.Close(ctx, node.StreamEvent{
		Type: node.StreamComplete,
		Data: mustJSON(map[string]string{"finalText": lastText(finalAssistantMsg), "messageStop": "end_turn"}),
	})
}

func (a *agentNode) failStream(sink node.StreamSink, code string, cause error) error {
	if logger := a.env.Logger(); logger != nil {
		logger.Printf("ai/agent failed: wf=%s node=%s code=%s err=%v", a.workflowID, a.env.NodeID(), code, cause)
	}
	_ = sink.Close(context.Background(), node.StreamEvent{
		Type: node.StreamError,
		Data: mustJSON(map[string]string{"code": code, "message": cause.Error()}),
	})
	return cause
}

// textOnlyMessage strips tool_use/tool_result blocks; used before persisting
// a turn to memory (intermediate tool blocks are noise).
func textOnlyMessage(m node.Message) node.Message {
	out := node.Message{Role: m.Role}
	for _, b := range m.Content {
		if b.Type == node.BlockText {
			out.Content = append(out.Content, b)
		}
	}
	return out
}

// messageForMemory converts a user message for persistence: text blocks are
// kept verbatim; image/document blocks are replaced with a single descriptor
// line ("[attached: name (mediaType, N bytes)]") so memory doesn't carry the
// raw bytes — replaying images on every turn would balloon token cost.
func messageForMemory(m node.Message) node.Message {
	out := node.Message{Role: m.Role}
	var refs []string
	for _, b := range m.Content {
		switch b.Type {
		case node.BlockText:
			out.Content = append(out.Content, b)
		case node.BlockImage, node.BlockDocument:
			refs = append(refs, attachmentRef(b))
		}
	}
	if len(refs) > 0 {
		out.Content = append([]node.ContentBlock{{Type: node.BlockText, Text: strings.Join(refs, "\n")}}, out.Content...)
	}
	return out
}

// attachmentRef returns the text-form descriptor used in persisted memory.
func attachmentRef(b node.ContentBlock) string {
	name := b.Filename
	if name == "" {
		name = "(unnamed)"
	}
	kind := "attached"
	if b.Type == node.BlockImage {
		kind = "attached image"
	} else if b.Type == node.BlockDocument {
		kind = "attached document"
	}
	return fmt.Sprintf("[%s: %s (%s, %d bytes)]", kind, name, b.MediaType, len(b.Data))
}

// attachmentBlock converts a node.Attachment into a ContentBlock by MediaType
// classification. Returns ok=false for unsupported types (caller skips it).
func attachmentBlock(a node.Attachment) (node.ContentBlock, bool) {
	if len(a.Data) == 0 || a.MediaType == "" {
		return node.ContentBlock{}, false
	}
	switch {
	case strings.HasPrefix(a.MediaType, "image/"):
		return node.ContentBlock{
			Type:      node.BlockImage,
			MediaType: a.MediaType,
			Filename:  a.Filename,
			Data:      a.Data,
		}, true
	case a.MediaType == "application/pdf":
		return node.ContentBlock{
			Type:      node.BlockDocument,
			MediaType: a.MediaType,
			Filename:  a.Filename,
			Data:      a.Data,
		}, true
	}
	return node.ContentBlock{}, false
}

// lastText concatenates all text blocks of a message.
func lastText(m node.Message) string {
	var buf string
	for _, b := range m.Content {
		if b.Type == node.BlockText {
			buf += b.Text
		}
	}
	return buf
}

// appendOrExtendText keeps streamed text in a single text block on the
// assistant message we'll commit to history.
func appendOrExtendText(blocks []node.ContentBlock, delta string) []node.ContentBlock {
	if len(blocks) > 0 && blocks[len(blocks)-1].Type == node.BlockText {
		blocks[len(blocks)-1].Text += delta
		return blocks
	}
	return append(blocks, node.ContentBlock{Type: node.BlockText, Text: delta})
}

// chartKeyRe matches the S3 chart object key (the UUID) inside either a tool's
// returned imageUrl or the model's `![chart](...)` text, so we can compare what
// the tool actually produced against what the model claims.
var chartKeyRe = regexp.MustCompile(`chart/([0-9a-fA-F\-]{8,})\.png`)

// extractChartKeys returns the distinct chart object UUIDs referenced in s.
func extractChartKeys(s string) []string {
	matches := chartKeyRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, m := range matches {
		if k := m[1]; !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

func sliceContains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// missingChartKeys returns the referenced chart keys that were not produced
// this turn (referenced minus produced), preserving reference order.
func missingChartKeys(referenced, produced []string) []string {
	var out []string
	for _, k := range referenced {
		if !sliceContains(produced, k) {
			out = append(out, k)
		}
	}
	return out
}

// chartRef pairs a chart object key with the canonical URL a tool produced.
type chartRef struct {
	key string
	url string
}

// chartURLRe captures a full chart URL and its key from tool output or text.
// Scheme-agnostic: matches both the legacy absolute presigned form
// (https://host/.../chart/<uuid>.png?...) and the relative short form
// (aichatviewer/chart/<uuid>.png). Matching is bounded by the stop-chars
// [^\s")]; the optional scheme is not an anchor.
var chartURLRe = regexp.MustCompile(`((?:https?://)?[^\s")]*chart/([0-9a-fA-F\-]{8,})\.png[^\s")]*)`)

// chartImgRe matches a markdown image whose URL points at a chart object
// (absolute or relative short form, same as chartURLRe).
var chartImgRe = regexp.MustCompile(`!\[([^\]]*)\]\(((?:https?://)?[^)\s]*chart/[0-9a-fA-F\-]{8,}\.png[^)\s]*)\)`)

// extractChartRefs returns the distinct chart {key,url} pairs in s (e.g. a
// tool result's imageUrl).
func extractChartRefs(s string) []chartRef {
	matches := chartURLRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var out []chartRef
	for _, m := range matches {
		url, key := unescapeJSONURL(m[1]), m[2]
		if !seen[key] {
			seen[key] = true
			out = append(out, chartRef{key: key, url: url})
		}
	}
	return out
}

// unescapeJSONURL decodes a URL captured from raw JSON bytes (e.g. a tool
// result). Go's json.Marshal defaults to HTML-safe escaping, turning the bytes
// ampersand, less-than and greater-than into their backslash-u-00XX forms so the
// JSON is safe to embed in HTML. A presigned URL scraped straight out of those
// bytes therefore has its query separators (ampersands) as the literal 6-char
// sequence backslash-u-0026; less-than/greater-than would escape too but do not
// occur in URLs. Left undecoded, the browser requests a malformed URL and the
// chart image 404s.
//
// This is NOT ampersand-specific: we decode the captured fragment as a JSON
// string, so every JSON escape (any backslash-u-XXXX plus \n \t \" \\ etc.) is
// resolved in one step. The guard skips work when there is no backslash to
// decode; we fall back to the raw input if the fragment is not a decodable
// JSON string.
func unescapeJSONURL(u string) string {
	if !strings.Contains(u, `\`) {
		return u
	}
	var dec string
	if err := json.Unmarshal([]byte(`"`+u+`"`), &dec); err == nil {
		return dec
	}
	return u
}

func chartKeysOf(refs []chartRef) []string {
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.key
	}
	return out
}

// correctChartRefs rewrites every chart markdown image in text so the user can
// only ever see a chart that was actually rendered this turn:
//   - URL whose key matches a produced chart -> replaced with the canonical URL
//   - URL with no matching key, exactly 1 chart produced -> that chart (unambiguous)
//   - otherwise (fabricated; multi/zero candidates) -> left unchanged so the
//     redirect route renders the gray "Chart unavailable" box in place
//
// It neutralizes model-fabricated/corrupted chart URLs while keeping the image
// inline at the position the model chose. Multi-chart with a fabricated ref is
// left unchanged rather than guessed (the redirect serves a gray box for a
// key with no S3 object).
func correctChartRefs(text string, produced []chartRef) string {
	byKey := make(map[string]string, len(produced))
	for _, c := range produced {
		byKey[c.key] = c.url
	}
	return chartImgRe.ReplaceAllStringFunc(text, func(m string) string {
		sub := chartImgRe.FindStringSubmatch(m)
		if sub == nil {
			return m
		}
		alt, url := sub[1], sub[2]
		key := ""
		if ks := extractChartKeys(url); len(ks) > 0 {
			key = ks[0]
		}
		if canonical, ok := byKey[key]; ok {
			return "![" + alt + "](" + canonical + ")"
		}
		if len(produced) == 1 {
			return "![" + alt + "](" + produced[0].url + ")"
		}
		// Not produced and not unambiguously substitutable: LEAVE the ref
		// unchanged. The chart-redirect route serves a gray "Chart unavailable"
		// box for an id with no S3 object, so the user sees the gray box in the
		// model's chosen position rather than stray "(chart unavailable)" text.
		return m
	})
}

// appendMissingCharts guarantees every chart produced this turn appears in the
// final answer. When the model writes a chart inline and then calls another tool,
// that chart ends up in an intermediate assistant segment (rendered as a reasoning
// row) rather than the final assistant message, so the user never sees it — e.g.
// asking for two charts yields a turn with two QuickChart calls but a final answer
// that only references the last one. Any produced chart whose key is absent from
// text is appended (in production order) so no rendered chart is silently dropped.
// Returns the new text and the number of charts appended.
func appendMissingCharts(text string, produced []chartRef) (string, int) {
	if len(produced) == 0 {
		return text, 0
	}
	present := extractChartKeys(text)
	var add []string
	for _, c := range produced {
		if !sliceContains(present, c.key) {
			add = append(add, "![chart]("+c.url+")")
		}
	}
	if len(add) == 0 {
		return text, 0
	}
	joined := strings.Join(add, "\n\n")
	if strings.TrimSpace(text) == "" {
		return joined, len(add)
	}
	return text + "\n\n" + joined, len(add)
}

// tracef emits a "chart-trace:" diagnostic line. These trace the full chart
// lifecycle (tool call -> tool result/produced key -> what the model references
// in its final answer) so we can prove in production whether a chart the model
// shows was actually rendered. Greppable prefix; safe to remove later.
func (a *agentNode) tracef(format string, args ...any) {
	if lg := a.env.Logger(); lg != nil {
		lg.Printf("chart-trace: "+format, args...)
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// isBlankToolPayload reports whether a successful tool payload would render
// as empty content: no bytes, whitespace, an empty JSON string, or JSON null.
func isBlankToolPayload(p json.RawMessage) bool {
	s := strings.TrimSpace(string(p))
	return s == "" || s == `""` || s == "null"
}
