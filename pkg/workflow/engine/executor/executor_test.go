package executor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

type recordingAgent struct{ ctx node.RenderCtx }

func (a *recordingAgent) Spec() node.NodeSpec                        { return node.NodeSpec{Type: "ai/agent", Role: node.RoleAgent} }
func (a *recordingAgent) Init(context.Context, node.NodeEnv) error   { return nil }
func (a *recordingAgent) Close(context.Context) error                { return nil }
func (a *recordingAgent) Process(_ context.Context, in node.AgentInput, _ node.StreamSink) error {
	a.ctx = in.RenderCtx
	return nil
}

type stubMappingProvider struct {
	out string
	err error
}

func (s *stubMappingProvider) Spec() node.NodeSpec                      { return node.NodeSpec{Type: "tool/stub", Role: node.RoleTool} }
func (s *stubMappingProvider) Init(context.Context, node.NodeEnv) error { return nil }
func (s *stubMappingProvider) Close(context.Context) error             { return nil }
func (s *stubMappingProvider) IndexMapping(context.Context) (string, error) {
	return s.out, s.err
}

func wfWith(rec *recordingAgent, mp *stubMappingProvider) *Workflow {
	return &Workflow{
		ID:        "t",
		Nodes:     map[string]node.Node{"agent1": rec, "os1": mp},
		TopoOrder: []string{"agent1", "os1"},
	}
}

func TestRunPinsIdentityAndMappingIntoRenderCtx(t *testing.T) {
	rec := &recordingAgent{}
	wf := wfWith(rec, &stubMappingProvider{out: "MAP"})
	ex := New(wf, log.New(io.Discard, "", 0), nil)
	evt := node.TriggerEvent{
		Body:      []byte(`{"message":"hi"}`),
		SessionID: "s1",
		RequestID: "r1",
		Identity:  identity.Identity{UserID: "u1", AccountID: "a1", UserName: "Ada", TimeZone: "America/New_York"},
	}
	if err := ex.Run(context.Background(), evt); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rec.ctx.UserName != "Ada" || rec.ctx.TimeZone != "America/New_York" {
		t.Errorf("identity not pinned: %+v", rec.ctx)
	}
	if rec.ctx.IndexMapping != "MAP" {
		t.Errorf("mapping not pinned: %q", rec.ctx.IndexMapping)
	}
}

func TestRunSurvivesMappingProviderError(t *testing.T) {
	rec := &recordingAgent{}
	wf := wfWith(rec, &stubMappingProvider{err: fmt.Errorf("boom")})
	ex := New(wf, log.New(io.Discard, "", 0), nil)
	evt := node.TriggerEvent{Body: []byte(`{"message":"hi"}`), SessionID: "s", Identity: identity.Identity{AccountID: "a"}}
	if err := ex.Run(context.Background(), evt); err != nil {
		t.Fatalf("Run should not fail on mapping error: %v", err)
	}
	if rec.ctx.IndexMapping != "" {
		t.Errorf("expected empty mapping on error, got %q", rec.ctx.IndexMapping)
	}
}

func TestParseChatBodyMessageOnly(t *testing.T) {
	body := json.RawMessage(`{"message":"hi","sessionId":"s1"}`)
	msg, atts := parseChatBody(body)
	if msg != "hi" {
		t.Errorf("message = %q", msg)
	}
	if len(atts) != 0 {
		t.Errorf("expected no attachments, got %d", len(atts))
	}
}

func TestParseChatBodyWithAttachments(t *testing.T) {
	raw := []byte{0x89, 0x50, 0x4E, 0x47}
	b64 := base64.StdEncoding.EncodeToString(raw)
	body := json.RawMessage(`{"message":"check this","attachments":[{"url":"/x/y.png","mediaType":"image/png","filename":"claim.png","size":4,"data":"` + b64 + `"}]}`)
	msg, atts := parseChatBody(body)
	if msg != "check this" {
		t.Errorf("message = %q", msg)
	}
	if len(atts) != 1 {
		t.Fatalf("attachments len = %d", len(atts))
	}
	a := atts[0]
	if a.MediaType != "image/png" || a.Filename != "claim.png" || a.URL != "/x/y.png" || a.Size != 4 {
		t.Errorf("attachment fields wrong: %+v", a)
	}
	if string(a.Data) != string(raw) {
		t.Errorf("data not base64-decoded: got %v, want %v", a.Data, raw)
	}
}

func TestParseChatBodyMalformedReturnsEmpty(t *testing.T) {
	msg, atts := parseChatBody(json.RawMessage(`not json`))
	if msg != "" || atts != nil {
		t.Errorf("expected empty, got (%q, %v)", msg, atts)
	}
}

type stubUploader struct {
	calls []struct {
		SessionID, Filename, MediaType string
		Data                           []byte
	}
	url string
	err error
}

func (s *stubUploader) UploadAttachment(_ context.Context, sid, fn, mt string, data []byte) (string, string, error) {
	s.calls = append(s.calls, struct {
		SessionID, Filename, MediaType string
		Data                           []byte
	}{sid, fn, mt, append([]byte(nil), data...)})
	if s.err != nil {
		return "", "", s.err
	}
	return "uploads/" + sid + "/u-" + fn, s.url, nil
}

func newExecutorForTest(uploader Uploader) *Executor {
	return &Executor{logger: log.New(io.Discard, "", 0), uploader: uploader}
}

func TestPersistAttachments_UploadsBytesAndSetsURL(t *testing.T) {
	up := &stubUploader{url: "https://signed.example/uploads/x"}
	exec := newExecutorForTest(up)
	in := []node.Attachment{
		{MediaType: "image/png", Filename: "claim.png", Data: []byte("PNGBYTES")},
	}
	out := exec.persistAttachments(context.Background(), "sess123", in)
	if len(out) != 1 || out[0].URL != up.url {
		t.Fatalf("URL not populated: %+v", out)
	}
	if string(out[0].Data) != "PNGBYTES" {
		t.Errorf("Data must be preserved for Bedrock; got %q", string(out[0].Data))
	}
	if len(up.calls) != 1 || up.calls[0].SessionID != "sess123" || up.calls[0].Filename != "claim.png" {
		t.Errorf("uploader called wrong: %+v", up.calls)
	}
}

func TestPersistAttachments_OverwritesCallerURLWhenDataPresent(t *testing.T) {
	up := &stubUploader{url: "https://signed.example/uploads/durable"}
	exec := newExecutorForTest(up)
	in := []node.Attachment{
		{MediaType: "image/png", Filename: "x.png", URL: "https://caller-local/x", Data: []byte("X")},
	}
	out := exec.persistAttachments(context.Background(), "s", in)
	if out[0].URL != up.url {
		t.Errorf("URL not overwritten with S3 URL: %q", out[0].URL)
	}
	if len(up.calls) != 1 {
		t.Errorf("uploader should fire even when caller supplied a URL: %+v", up.calls)
	}
}

func TestPersistAttachments_SkipWhenDataEmpty(t *testing.T) {
	up := &stubUploader{url: "https://should-not-be-used"}
	exec := newExecutorForTest(up)
	in := []node.Attachment{
		{MediaType: "image/png", Filename: "x.png", URL: "https://upstream-durable/x"},
	}
	out := exec.persistAttachments(context.Background(), "s", in)
	if out[0].URL != "https://upstream-durable/x" {
		t.Errorf("upstream URL must be preserved when no bytes to upload: %q", out[0].URL)
	}
	if len(up.calls) != 0 {
		t.Errorf("uploader called without bytes: %+v", up.calls)
	}
}

func TestPersistAttachments_UploaderErrorKeepsOriginalURL(t *testing.T) {
	up := &stubUploader{err: errors.New("s3 down")}
	exec := newExecutorForTest(up)
	in := []node.Attachment{
		{MediaType: "image/png", Filename: "x.png", URL: "https://caller-local/x", Data: []byte("X")},
	}
	out := exec.persistAttachments(context.Background(), "s", in)
	if len(out) != 1 {
		t.Fatalf("expected attachment to flow through, got %d", len(out))
	}
	if out[0].URL != "https://caller-local/x" {
		t.Errorf("original URL should survive upload failure; got %q", out[0].URL)
	}
	if string(out[0].Data) != "X" {
		t.Errorf("Data must still flow for Bedrock fallback")
	}
}
