package bedrock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Stream invokes Bedrock with response streaming and translates Anthropic
// chunks to node.LLMEvent values on out. Closes out before returning.
func (b *bedrockLLM) Stream(ctx context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	model := b.currentModel()

	payload, err := buildAnthropicPayload(req, b.cfg)
	if err != nil {
		return err
	}

	input := func(m string) *bedrockruntime.InvokeModelWithResponseStreamInput {
		return &bedrockruntime.InvokeModelWithResponseStreamInput{
			ModelId:     aws.String(m),
			ContentType: aws.String("application/json"),
			Accept:      aws.String("application/json"),
			Body:        payload,
		}
	}
	resp, err := b.client.InvokeModelWithResponseStream(ctx, input(model))
	if err != nil && isModelUnavailable(err) {
		// Request-time fallback: the fresh model broke mid-day (deprecated or
		// removed by AWS after the last check) — retry ONCE with the model
		// displaced by the last upgrade. Only before any event was emitted;
		// mid-stream failures are never retried.
		if lkg := b.lastKnownGoodModel(); lkg != "" && lkg != model {
			if b.logger != nil {
				b.logger.Printf("ai/bedrock wf=%s node=%s model %s failed (%v), retrying with last-known-good %s", b.wfID, b.nodeID, model, err, lkg)
			}
			model = lkg
			resp, err = b.client.InvokeModelWithResponseStream(ctx, input(model))
		}
	}
	if err != nil {
		if b.logger != nil {
			b.logger.Printf("ai/bedrock invoke failed: wf=%s node=%s model=%s region=%s err=%v", b.wfID, b.nodeID, model, b.cfg.Region, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}

	stream := resp.GetStream()
	defer stream.Close()

	// Per-stream tool_use accumulator: contentBlockIdx → *LLMToolUse.
	accum := map[int]*node.LLMToolUse{}

	for evt := range stream.Events() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch e := evt.(type) {
		case *types.ResponseStreamMemberChunk:
			if err := handleAnthropicChunk(ctx, e.Value.Bytes, accum, out); err != nil {
				if b.logger != nil {
					b.logger.Printf("ai/bedrock chunk decode failed: wf=%s node=%s model=%s err=%v", b.wfID, b.nodeID, model, err)
				}
				emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
				return err
			}
		default:
			if b.logger != nil {
				b.logger.Printf("ai/bedrock unexpected stream event: wf=%s node=%s model=%s type=%T", b.wfID, b.nodeID, model, e)
			}
			emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: fmt.Errorf("ai/bedrock: stream error: %T", e)})
			return fmt.Errorf("bedrock stream: %T", e)
		}
	}
	if err := stream.Err(); err != nil {
		if b.logger != nil {
			b.logger.Printf("ai/bedrock stream terminated with error: wf=%s node=%s model=%s err=%v", b.wfID, b.nodeID, model, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}
	return nil
}

// handleAnthropicChunk parses one streamed JSON event from Bedrock's Anthropic
// adapter and emits the corresponding LLMEvent(s).
func handleAnthropicChunk(ctx context.Context, raw []byte, accum map[int]*node.LLMToolUse, out chan<- node.LLMEvent) error {
	type delta struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
	}
	type contentBlockStart struct {
		Type         string `json:"type"`
		Index        int    `json:"index"`
		ContentBlock struct {
			Type  string          `json:"type"`
			ID    string          `json:"id"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content_block"`
	}
	type messageDelta struct {
		Type  string `json:"type"`
		Delta struct {
			StopReason string `json:"stop_reason"`
		} `json:"delta"`
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	var head map[string]json.RawMessage
	if err := dec.Decode(&head); err != nil {
		return err
	}
	t := ""
	_ = json.Unmarshal(head["type"], &t)

	switch t {
	case "content_block_start":
		var v contentBlockStart
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		if v.ContentBlock.Type == "tool_use" {
			tu := &node.LLMToolUse{ID: v.ContentBlock.ID, Name: v.ContentBlock.Name, InputJSON: []byte("")}
			accum[v.Index] = tu
			emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseStart, ToolUse: tu})
		}
	case "content_block_delta":
		var v struct {
			Index int   `json:"index"`
			Delta delta `json:"delta"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		switch v.Delta.Type {
		case "text_delta":
			emit(ctx, out, node.LLMEvent{Kind: node.LLMTextDelta, Delta: v.Delta.Text})
		case "input_json_delta":
			tu := accum[v.Index]
			if tu != nil {
				tu.InputJSON = append(tu.InputJSON, []byte(v.Delta.PartialJSON)...)
				emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseDelta, ToolUse: tu})
			}
		}
	case "content_block_stop":
		var v struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		if tu := accum[v.Index]; tu != nil {
			emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseStop, ToolUse: tu})
			delete(accum, v.Index)
		}
	case "message_delta":
		var v messageDelta
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		if v.Delta.StopReason != "" {
			emit(ctx, out, node.LLMEvent{Kind: node.LLMMessageStop, Stop: v.Delta.StopReason})
		}
	}
	return nil
}

func emit(ctx context.Context, out chan<- node.LLMEvent, evt node.LLMEvent) {
	select {
	case out <- evt:
	case <-ctx.Done():
	}
}

// isModelUnavailable reports invoke errors that indicate the model ID itself
// is bad (deprecated, removed, or unentitled) rather than a transient fault —
// only these justify the last-known-good retry.
func isModelUnavailable(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.ErrorCode() {
	case "ValidationException", "ResourceNotFoundException":
		return true
	}
	return false
}
