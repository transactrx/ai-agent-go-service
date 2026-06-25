// Package executor runs one TriggerEvent through a workflow. Cycle 1 is
// agent-driven: the trigger emits, the executor finds the agent connected
// to the trigger's main output, and calls agent.Process. Future cycles
// generalize to topo-walking (see spec §4.7).
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Uploader is the narrow interface the executor needs from pkg/aws/s3files.
// Declared here so executor can be tested without importing the s3files
// package (and so the engine can pass nil when no uploader is configured,
// in which case attachment uploads are skipped).
type Uploader interface {
	UploadAttachment(ctx context.Context, sessionID, filename, mediaType string, data []byte) (key, url string, err error)
}

// Executor is created per workflow and shared across requests for that workflow.
type Executor struct {
	wf       *Workflow
	logger   *log.Logger
	clock    func() time.Time
	uploader Uploader
}

// New returns an executor for wf. uploader may be nil; if nil, executor will
// not upload attachments to S3 and will pass them through as-is.
func New(wf *Workflow, logger *log.Logger, uploader Uploader) *Executor {
	return &Executor{wf: wf, logger: logger, clock: time.Now, uploader: uploader}
}

// Run handles one TriggerEvent. The trigger's Subscribe wraps incoming events
// with a call to executor.Run.
func (e *Executor) Run(ctx context.Context, evt node.TriggerEvent) error {
	ctx = identity.WithIdentity(ctx, evt.Identity)

	// Pin RenderCtx for the entire request.
	var triggerBody any
	_ = json.Unmarshal(evt.Body, &triggerBody)
	rctx := node.RenderCtx{
		Now:          e.clock(),
		SessionID:    evt.SessionID,
		UserID:       evt.Identity.UserID,
		RequestID:    evt.RequestID,
		TriggerEvent: triggerBody,
		NodeOutputs:  map[string]any{},
		UserName:     evt.Identity.UserName,
		TimeZone:     evt.Identity.TimeZone,
	}
	if mp := e.resolveMappingProvider(); mp != nil {
		if m, err := mp.IndexMapping(ctx); err != nil {
			e.logger.Printf("executor: index mapping unavailable (wf=%s): %v", e.wf.ID, err)
		} else {
			rctx.IndexMapping = m
		}
	}

	agent, err := e.resolveAgent()
	if err != nil {
		return e.fail(ctx, evt, err)
	}

	msg, atts := parseChatBody(evt.Body)
	if e.uploader != nil {
		atts = e.persistAttachments(ctx, evt.SessionID, atts)
	}
	in := node.AgentInput{
		Message:     msg,
		SessionID:   evt.SessionID,
		UserID:      evt.Identity.UserID,
		RequestID:   evt.RequestID,
		Headers:     evt.Headers,
		RenderCtx:   rctx,
		Attachments: atts,
	}

	if err := agent.Process(ctx, in, evt.StreamSink); err != nil {
		return e.fail(ctx, evt, err)
	}
	return nil
}

// persistAttachments uploads every attachment whose bytes the caller supplied,
// overwriting Attachment.URL with the durable S3 presigned URL. The caller's
// original URL (e.g. a WebApp local-disk URL) is intentionally replaced — the
// S3 URL is what the chat message carries forward for future history use.
// Attachments without bytes are passed through untouched (an upstream uploader
// has already taken responsibility for durability).
// Upload failures on a single attachment log a warning and keep the original
// URL/Data; other attachments still flow through.
func (e *Executor) persistAttachments(ctx context.Context, sessionID string, atts []node.Attachment) []node.Attachment {
	out := make([]node.Attachment, 0, len(atts))
	for _, a := range atts {
		if len(a.Data) == 0 {
			out = append(out, a)
			continue
		}
		_, url, err := e.uploader.UploadAttachment(ctx, sessionID, a.Filename, a.MediaType, a.Data)
		if err != nil {
			e.logger.Printf("executor: upload attachment %q failed (keeping original url): %v", a.Filename, err)
			out = append(out, a)
			continue
		}
		a.URL = url
		out = append(out, a)
	}
	return out
}

// resolveAgent walks topo order to find the first node that implements
// node.Agent. Cycle 1 has exactly one agent per workflow.
func (e *Executor) resolveAgent() (node.Agent, error) {
	for _, id := range e.wf.TopoOrder {
		if a, ok := e.wf.Nodes[id].(node.Agent); ok {
			return a, nil
		}
	}
	return nil, fmt.Errorf("workflow %s: no agent node found", e.wf.ID)
}

// resolveMappingProvider returns the first node implementing node.MappingProvider,
// or nil if none. The executor depends only on the interface (never on a concrete
// builtin), preserving the engine's node-agnostic pattern.
func (e *Executor) resolveMappingProvider() node.MappingProvider {
	for _, id := range e.wf.TopoOrder {
		if mp, ok := e.wf.Nodes[id].(node.MappingProvider); ok {
			return mp
		}
	}
	return nil
}

func (e *Executor) fail(_ context.Context, evt node.TriggerEvent, cause error) error {
	if evt.StreamSink != nil {
		_ = evt.StreamSink.Close(context.Background(), node.StreamEvent{
			Type: node.StreamError,
			Data: mustJSON(map[string]string{"code": "executor-failed", "message": cause.Error()}),
		})
	} else if evt.Reply != nil {
		_ = evt.Reply(context.Background(),
			mustJSON(map[string]string{"code": "executor-failed", "message": cause.Error()}),
			map[string]string{"status": "500"})
	}
	return cause
}

// parseChatBody extracts body.message and body.attachments. Returns ("", nil)
// if the body is malformed or empty. Attachment Data on the wire is base64
// (Go []byte unmarshals base64 strings transparently).
func parseChatBody(body json.RawMessage) (string, []node.Attachment) {
	var b struct {
		Message     string `json:"message"`
		Attachments []struct {
			URL       string `json:"url,omitempty"`
			MediaType string `json:"mediaType"`
			Filename  string `json:"filename,omitempty"`
			Size      int64  `json:"size,omitempty"`
			Data      []byte `json:"data,omitempty"`
		} `json:"attachments,omitempty"`
	}
	_ = json.Unmarshal(body, &b)
	if len(b.Attachments) == 0 {
		return b.Message, nil
	}
	out := make([]node.Attachment, 0, len(b.Attachments))
	for _, a := range b.Attachments {
		out = append(out, node.Attachment{
			URL:       a.URL,
			MediaType: a.MediaType,
			Filename:  a.Filename,
			Size:      a.Size,
			Data:      a.Data,
		})
	}
	return b.Message, out
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
