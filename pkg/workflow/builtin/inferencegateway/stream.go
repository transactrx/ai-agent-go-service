package inferencegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Stream sends one invokeStream request to the gateway and translates the
// events it publishes on a private inbox into node.LLMEvent values on out.
// Closes out before returning. Every call logs one llm-timing line.
func (g *gatewayLLM) Stream(ctx context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	timing := node.LLMTiming{Provider: nodeType, Start: time.Now()}
	defer func() {
		timing.End = time.Now()
		node.LogLLMTiming(g.logger, g.wfID, g.nodeID, timing)
	}()
	fail := func(err error) error {
		timing.Err = err
		if g.logger != nil {
			g.logger.Printf("ai/inference-gateway wf=%s node=%s subject=%s failed: %v", g.wfID, g.nodeID, g.subject, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}

	wire, err := buildRequest(req, g.cfg)
	if err != nil {
		return fail(err)
	}

	sctx, cancel := context.WithTimeout(ctx, g.streamTimeout)
	defer cancel()

	// Subscribe BEFORE the request so no event can be published into the void.
	inbox := g.nc.NewInbox()
	sub, err := g.nc.SubscribeSync(inbox)
	if err != nil {
		return fail(fmt.Errorf("ai/inference-gateway: subscribe %s: %w", inbox, err))
	}
	defer func() { _ = sub.Unsubscribe() }()
	wire.StreamSubject = inbox

	body, err := json.Marshal(wire)
	if err != nil {
		return fail(err)
	}
	rctx, rcancel := context.WithTimeout(sctx, requestTimeout)
	msg, err := g.nc.RequestWithContext(rctx, g.subject, body)
	rcancel()
	if err != nil {
		return fail(fmt.Errorf("ai/inference-gateway: gateway request %s: %w", g.subject, err))
	}
	ack, err := parseAck(msg.Header.Get(nats_service_common.STATUS), msg.Data)
	if err != nil {
		return fail(fmt.Errorf("ai/inference-gateway: %w", err))
	}
	timing.Model = ack.InvokeID
	if line, ok := policyLine(g.wfID, g.nodeID, g.cfg.Alias, ack.InvokeID, ack); ok && g.logger != nil {
		g.logger.Print(line)
	}

	if err := consumeStream(sctx, sub, g.idleTimeout, out, &timing); err != nil {
		return fail(err)
	}
	return nil
}

// consumeStream reads gateway StreamEvents from sub until done/error,
// emitting LLMEvents. seq must be contiguous from 0. idle bounds the silence
// between two events; ctx bounds the whole stream.
func consumeStream(ctx context.Context, sub *nats.Subscription, idle time.Duration, out chan<- node.LLMEvent, timing *node.LLMTiming) error {
	accum := map[int]*node.LLMToolUse{}
	want := 0
	for {
		ictx, icancel := context.WithTimeout(ctx, idle)
		m, err := sub.NextMsgWithContext(ictx)
		icancel()
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("ai/inference-gateway: stream: %w", ctx.Err())
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("ai/inference-gateway: stream idle for %s without events: %w", idle, err)
			}
			return fmt.Errorf("ai/inference-gateway: stream subscription failed: %w", err)
		}
		var ev streamEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			return fmt.Errorf("ai/inference-gateway: bad stream event: %w", err)
		}
		if ev.Seq != want {
			return fmt.Errorf("ai/inference-gateway: stream seq gap: want %d got %d", want, ev.Seq)
		}
		want++
		if timing.FirstEvent.IsZero() {
			timing.FirstEvent = time.Now()
		}

		switch ev.Type {
		case "delta":
			if ev.Text != "" {
				emit(ctx, out, node.LLMEvent{Kind: node.LLMTextDelta, Delta: ev.Text})
			} else if ev.ToolInputDelta != "" {
				if tu := accum[ev.ContentIndex]; tu != nil {
					tu.InputJSON = append(tu.InputJSON, []byte(ev.ToolInputDelta)...)
					emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseDelta, ToolUse: tu})
				}
			}
		case "toolUseStart":
			tu := &node.LLMToolUse{ID: ev.ToolUseID, Name: ev.ToolName, InputJSON: []byte("")}
			accum[ev.ContentIndex] = tu
			emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseStart, ToolUse: tu})
		case "contentBlockStop":
			if tu := accum[ev.ContentIndex]; tu != nil {
				emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseStop, ToolUse: tu})
				delete(accum, ev.ContentIndex)
			}
		case "messageStop":
			timing.Stop = ev.StopReason
			if ev.StopReason != "" {
				emit(ctx, out, node.LLMEvent{Kind: node.LLMMessageStop, Stop: ev.StopReason})
			}
		case "metadata":
			if ev.Usage != nil {
				timing.HasUsage = true
				timing.InputTokens = int(ev.Usage.InputTokens)
				timing.OutputTokens = int(ev.Usage.OutputTokens)
			}
		case "done":
			return nil
		case "error":
			return errors.New("ai/inference-gateway: gateway stream error: " + ev.Error)
		default:
			// messageStart and unknown types: nothing to emit.
		}
	}
}

func emit(ctx context.Context, out chan<- node.LLMEvent, evt node.LLMEvent) {
	select {
	case out <- evt:
	case <-ctx.Done():
	}
}
