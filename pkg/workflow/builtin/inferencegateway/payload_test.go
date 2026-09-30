package inferencegateway

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestBuildRequestFullShape(t *testing.T) {
	temp := 0.2
	req := node.LLMRequest{
		System:         "sys",
		MaxTokens:      512,
		Temperature:    &temp,
		Stop:           []string{"END"},
		Tools:          []node.ToolSpec{{Name: "search", Description: "d", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolChoiceName: "search",
		Messages: []node.Message{
			{Role: node.UserMsg, Content: []node.ContentBlock{
				{Type: node.BlockText, Text: "hello"},
				{Type: node.BlockImage, MediaType: "image/png", Data: []byte{1, 2, 3}},
			}},
			{Role: node.AssistantMsg, Content: []node.ContentBlock{
				{Type: node.BlockToolUse, ToolUseID: "tu1", ToolName: "search"},
			}},
			{Role: node.UserMsg, Content: []node.ContentBlock{
				{Type: node.BlockToolResult, ToolUseID: "tu1", ToolResult: json.RawMessage(`{"hits":3}`), IsError: true},
				{Type: node.BlockToolResult, ToolUseID: "tu2"},
			}},
		},
	}
	got, err := buildRequest(req, Config{Alias: "MAX_MODEL"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	want := `{"alias":"MAX_MODEL","system":"sys","messages":[` +
		`{"role":"user","content":[{"text":"hello"},{"image":{"format":"png","base64":"AQID"}}]},` +
		`{"role":"assistant","content":[{"toolUse":{"toolUseId":"tu1","name":"search","input":{}}}]},` +
		`{"role":"user","content":[{"toolResult":{"toolUseId":"tu1","content":[{"text":"{\"hits\":3}"}],"status":"error"}},` +
		`{"toolResult":{"toolUseId":"tu2","content":[{"text":"\"\""}]}}]}],` +
		`"tools":[{"name":"search","description":"d","inputSchema":{"type":"object"}}],` +
		`"toolChoice":"tool","toolChoiceName":"search","streamSubject":""}`
	if string(b) != want {
		t.Fatalf("body =\n%s\nwant\n%s", b, want)
	}
}

// The gateway alias paramPolicy owns every knob: whatever the LLMRequest
// carries, the body never names a model parameter.
func TestBuildRequestNeverSendsKnobs(t *testing.T) {
	temp := 0.2
	req := node.LLMRequest{
		MaxTokens:   512,
		Temperature: &temp,
		Stop:        []string{"END"},
		Messages:    []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "x"}}}},
	}
	got, err := buildRequest(req, Config{Alias: "POWERLINE_CLAIM_SEARCH_MODEL"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(b, &keys); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"maxTokens", "temperature", "stopSequences", "modelId", "lab", "family"} {
		if _, present := keys[k]; present {
			t.Fatalf("body must not carry %q: %s", k, b)
		}
	}
	for _, k := range []string{"alias", "messages", "streamSubject"} {
		if _, present := keys[k]; !present {
			t.Fatalf("body must carry %q: %s", k, b)
		}
	}
	if got.Alias != "POWERLINE_CLAIM_SEARCH_MODEL" || got.ToolChoice != "" || got.Tools != nil {
		t.Fatalf("body = %+v", got)
	}
}

// Forced tool choice is per-call intent, not a model parameter: still forwarded.
func TestBuildRequestForwardsToolChoice(t *testing.T) {
	req := node.LLMRequest{
		Tools:          []node.ToolSpec{{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolChoiceName: "echo",
		Messages:       []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "x"}}}},
	}
	got, err := buildRequest(req, Config{Alias: "A1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ToolChoice != "tool" || got.ToolChoiceName != "echo" || len(got.Tools) != 1 {
		t.Fatalf("tool choice = %+v", got)
	}
}

func TestBuildRequestRejectsUnsupported(t *testing.T) {
	doc := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockDocument, MediaType: "application/pdf", Data: []byte("x")}}}}}
	if _, err := buildRequest(doc, Config{Alias: "A1"}); err == nil || !strings.Contains(err.Error(), "document blocks are not supported") {
		t.Fatalf("document: err = %v", err)
	}
	bmp := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockImage, MediaType: "image/bmp", Data: []byte("x")}}}}}
	if _, err := buildRequest(bmp, Config{Alias: "A1"}); err == nil || !strings.Contains(err.Error(), "image/bmp") {
		t.Fatalf("bmp: err = %v", err)
	}
	unknown := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: "weird"}}}}}
	if _, err := buildRequest(unknown, Config{Alias: "A1"}); err == nil || !strings.Contains(err.Error(), "unknown block type") {
		t.Fatalf("unknown: err = %v", err)
	}
}

func TestImageFormat(t *testing.T) {
	for mt, want := range map[string]string{"image/png": "png", "image/jpeg": "jpeg", "image/jpg": "jpeg", "image/gif": "gif", "image/webp": "webp"} {
		if got, err := imageFormat(mt); err != nil || got != want {
			t.Fatalf("%s → %q, %v", mt, got, err)
		}
	}
	if _, err := imageFormat("image/tiff"); err == nil {
		t.Fatal("tiff must error")
	}
}
