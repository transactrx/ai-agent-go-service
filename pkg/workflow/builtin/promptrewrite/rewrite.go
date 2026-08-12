package promptrewrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

// defaultNodeID / defaultField are applied when the request omits them.
const (
	defaultNodeID = "agent1"
	defaultField  = "systemMessageFlexible"
)

// requestTimeout bounds one rewrite run (Bedrock invoke + full stream).
const requestTimeout = 5 * time.Minute

// RewriteRequest is the input for the PromptRewrite endpoint.
type RewriteRequest struct {
	WorkflowID   string `json:"workflowId"`
	NodeID       string `json:"nodeId"`
	Field        string `json:"field"`
	Instructions string `json:"instructions"`
	Current      string `json:"current"` // optional; falls back to promptstore.Latest when empty
	UpdatedBy    string `json:"updatedBy"`
}

// validateRequest checks the fields the caller MUST supply. Defaults for
// NodeID/Field are applied by the caller before validation runs.
func validateRequest(r RewriteRequest) error {
	if strings.TrimSpace(r.WorkflowID) == "" {
		return errors.New("workflowId is required")
	}
	if strings.TrimSpace(r.Instructions) == "" {
		return errors.New("instructions is required")
	}
	return nil
}

// buildMessages returns the meta system prompt and the user message sent to
// Bedrock to rewrite the current prompt per instructions. Ported verbatim
// (text unchanged) from the old pkg/admin/handlers_prompt_rewrite.go.
func buildMessages(current, instructions string) (system, user string) {
	system = "You are editing the system prompt of an AI agent. " +
		"The user will give you the CURRENT system prompt and a plain-English update request.\n\n" +
		"Rules:\n" +
		"- PRESERVE everything in the current prompt that the request doesn't touch. Do not rewrite, reorder, or summarize sections that aren't being changed.\n" +
		"- If the request is ambiguous, make the smallest, most conservative interpretation rather than guessing.\n" +
		"- Output ONLY the complete revised system prompt as your final answer. NO preamble (no 'Here is the updated prompt'), NO trailing commentary, NO surrounding markdown code fence. The exact text you output replaces the current system prompt verbatim.\n"

	user = "## CURRENT SYSTEM PROMPT\n```\n" + current + "\n```\n\n" +
		"## REQUESTED UPDATE\n" + strings.TrimSpace(instructions) + "\n\n" +
		"Produce the complete revised system prompt now."
	return system, user
}

// registerEndpoint registers the single streaming "PromptRewrite" endpoint.
func (n *Node) registerEndpoint() error {
	reg := nats_service.EndpointRegistration{
		Path: "PromptRewrite",
		Description: "AI-assisted rewrite of an agent's system prompt: sends the current prompt plus a plain-English instruction to Bedrock and streams back the revised text. " +
			"Does NOT persist the result — review the streamed text and call PromptSave to keep it. " +
			`Example body: {"workflowId":"powerlineSearch","nodeId":"agent1","field":"systemMessageFlexible","instructions":"add a rule about refunds","updatedBy":"jdoe"}`,
		Parameters: []nats_service.ParameterDoc{
			{Name: "workflowId", Description: "Workflow id (see ListWorkflows)", Required: true, Example: "powerlineSearch"},
			{Name: "nodeId", Description: "Node id inside the workflow JSON", Required: false, Example: "agent1"},
			{Name: "field", Description: "Overridable config field of that node", Required: false, Example: "systemMessageFlexible"},
			{Name: "instructions", Description: "Plain-English description of the desired change", Required: true, Example: "add a rule about refunds"},
			{Name: "current", Description: "Text being rewritten; when omitted, the latest saved value for workflowId/nodeId/field is loaded and rewritten instead", Required: false, Example: "Answer using the search tool..."},
			{Name: "updatedBy", Description: "Author, for caller-side bookkeeping (this endpoint does not persist)", Required: false, Example: "jdoe"},
		},
		Response: &nats_service.ResponseDoc{
			Description: "Stream of NATS events on the reply inbox: start → delta* → complete | error. " +
				"Event type is in the _Stream_Event header, ordering in _Stream_Sequence. " +
				"The start event carries _Stream_Cancel_Subject — publish any message on that subject to cancel.",
			ContentType: "application/json",
			Example:     `{"start": {"requestId": "e1f0c9a2-4b7d-4f7e-9c1a-8f2d3e4a5b6c"}, "delta": {"text": "You are a pharmacy claims assistant..."}, "complete": {"messageStop": "end_turn"}, "error": {"code": "bedrock_error", "message": "..."}}`,
		},
		Handler: n.handle,
	}
	return n.nats.AddEndpointWithDocs([]nats_service.EndpointRegistration{reg})
}

// handle is registered with nats-service. It validates the request, opens a
// streaming session, and hands off to run — mirrors the old
// MakeRewriteHandler body.
func (n *Node) handle(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	if msg.OriginalMessage == nil || msg.OriginalMessage.Reply == "" {
		e := nats_service.NewValidationError("rewrite requires a reply subject (streaming)", 400, fmt.Errorf("missing reply"))
		return &e
	}

	var req RewriteRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		e := nats_service.NewValidationError("invalid request body", 400, err)
		return &e
	}
	if req.NodeID == "" {
		req.NodeID = defaultNodeID
	}
	if req.Field == "" {
		req.Field = defaultField
	}
	if err := validateRequest(req); err != nil {
		e := nats_service.NewValidationError(err.Error(), 400, err)
		return &e
	}

	sess, err := natsstream.NewStreamSession(n.nats.GetNatsService(), msg, n.basePath, n.logger)
	if err != nil {
		e := nats_service.NewServerError("failed to create stream session", 500, err)
		return &e
	}

	go n.run(sess, req)

	forwarded := nats_service.NewForwardedError(msg.Path)
	return &forwarded
}

// run drives one rewrite: resolve current text, invoke Bedrock, stream
// deltas, complete. Ported faithfully from the old runRewrite.
func (n *Node) run(sess *natsstream.StreamSession, req RewriteRequest) {
	ctx, cancel := context.WithTimeout(sess.Context(), requestTimeout)
	defer cancel()

	// Send start event.
	_ = sess.Start(ctx, natsstream.StartPayload{
		SessionID:  "",
		WorkflowID: req.WorkflowID,
		RequestID:  sess.StreamID(),
	})

	// Resolve current text: request body wins; else fall back to the store.
	current := req.Current
	if current == "" && n.store != nil {
		if txt, found, err := n.store.Latest(ctx, req.WorkflowID, req.NodeID, req.Field); err == nil && found {
			current = txt
		} else if err != nil {
			n.logger.Printf("[admin/prompt-rewrite] failed to load current prompt for %s/%s/%s: %v — using empty", req.WorkflowID, req.NodeID, req.Field, err)
		}
	}

	metaSystem, userMessage := buildMessages(current, req.Instructions)

	// Build Bedrock request payload.
	payload, _ := json.Marshal(map[string]any{
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens":        16384,
		"system":            metaSystem,
		"messages": []map[string]any{
			{"role": "user", "content": []map[string]string{{"type": "text", "text": userMessage}}},
		},
	})

	// Bedrock client is built once in Init and reused here.
	resp, err := n.bedrock.InvokeModelWithResponseStream(ctx, &bedrockruntime.InvokeModelWithResponseStreamInput{
		ModelId:     aws.String(n.model),
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
		Body:        payload,
	})
	if err != nil {
		n.logger.Printf("[admin/prompt-rewrite] bedrock invoke: %v", err)
		_ = sess.Fail(ctx, natsstream.ErrorPayload{Code: "bedrock_error", Message: err.Error()})
		return
	}
	stream := resp.GetStream()
	defer stream.Close()

	for evt := range stream.Events() {
		if ctx.Err() != nil {
			_ = sess.Fail(ctx, natsstream.ErrorPayload{Code: "cancelled", Message: "request cancelled"})
			return
		}
		switch e := evt.(type) {
		case *types.ResponseStreamMemberChunk:
			// Parse Anthropic chunk to extract text delta.
			var chunk struct {
				Type  string `json:"type"`
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			}
			if err := json.Unmarshal(e.Value.Bytes, &chunk); err != nil {
				continue
			}
			if chunk.Type == "content_block_delta" && chunk.Delta.Type == "text_delta" && chunk.Delta.Text != "" {
				data, _ := json.Marshal(map[string]string{"text": chunk.Delta.Text})
				_ = sess.Send(ctx, natsstream.StreamEvent{Type: natsstream.StreamDelta, Data: data})
			}
		}
	}
	if err := stream.Err(); err != nil {
		n.logger.Printf("[admin/prompt-rewrite] stream error: %v", err)
		_ = sess.Fail(ctx, natsstream.ErrorPayload{Code: "stream_error", Message: err.Error()})
		return
	}

	_ = sess.Complete(ctx, natsstream.CompletePayload{MessageStop: "end_turn"})
}
