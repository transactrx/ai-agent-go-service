package bedrock

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// testModelGen4 keeps these envelope expectations on the 4.x generation, whose
// payload must stay byte-identical to pre-v1.3.0.
const testModelGen4 = "us.anthropic.claude-opus-4-7"

// TestBuildPayloadImageBlock verifies image content blocks are emitted as the
// Anthropic-on-Bedrock {type:"image", source:{type:"base64", media_type, data}}
// shape with base64-encoded bytes.
func TestBuildPayloadImageBlock(t *testing.T) {
	cfg := Config{Model: "us.anthropic.claude-opus-4-7", MaxTokens: 4096, AnthropicVersion: "bedrock-2023-05-31"}
	rawBytes := []byte{0xFF, 0xD8, 0xFF, 0xE0} // JPEG SOI marker; arbitrary
	req := node.LLMRequest{
		Messages: []node.Message{{
			Role: node.UserMsg,
			Content: []node.ContentBlock{
				{Type: node.BlockImage, MediaType: "image/jpeg", Filename: "claim.jpg", Data: rawBytes},
				{Type: node.BlockText, Text: "what's in this image?"},
			},
		}},
	}
	body, err := buildAnthropicPayload(req, cfg, testModelGen4)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	msgs := got["messages"].([]any)
	blocks := msgs[0].(map[string]any)["content"].([]any)
	if len(blocks) != 2 {
		t.Fatalf("blocks len = %d, want 2", len(blocks))
	}
	img := blocks[0].(map[string]any)
	if img["type"] != "image" {
		t.Fatalf("block[0] type = %v, want image", img["type"])
	}
	src := img["source"].(map[string]any)
	if src["type"] != "base64" || src["media_type"] != "image/jpeg" {
		t.Fatalf("source mismatch: %v", src)
	}
	want := base64.StdEncoding.EncodeToString(rawBytes)
	if src["data"] != want {
		t.Fatalf("data not base64-encoded: got %v, want %v", src["data"], want)
	}
}

// TestBuildPayloadDocumentBlock verifies pdf documents serialize with title.
func TestBuildPayloadDocumentBlock(t *testing.T) {
	cfg := Config{Model: "us.anthropic.claude-opus-4-7", MaxTokens: 4096, AnthropicVersion: "bedrock-2023-05-31"}
	pdfBytes := []byte("%PDF-1.4 fake")
	req := node.LLMRequest{
		Messages: []node.Message{{
			Role: node.UserMsg,
			Content: []node.ContentBlock{
				{Type: node.BlockDocument, MediaType: "application/pdf", Filename: "report.pdf", Data: pdfBytes},
				{Type: node.BlockText, Text: "summarize"},
			},
		}},
	}
	body, err := buildAnthropicPayload(req, cfg, testModelGen4)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	doc := got["messages"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)
	if doc["type"] != "document" {
		t.Fatalf("type = %v, want document", doc["type"])
	}
	if doc["title"] != "report.pdf" {
		t.Fatalf("title = %v, want report.pdf", doc["title"])
	}
	src := doc["source"].(map[string]any)
	if src["media_type"] != "application/pdf" {
		t.Fatalf("media_type = %v", src["media_type"])
	}
}

func TestBuildPayloadSimpleUserMessage(t *testing.T) {
	cfg := Config{Model: "us.anthropic.claude-opus-4-7", MaxTokens: 4096, AnthropicVersion: "bedrock-2023-05-31"}
	req := node.LLMRequest{
		System: "you are helpful",
		Messages: []node.Message{
			{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "hi"}}},
		},
		MaxTokens: 4096,
	}
	body, err := buildAnthropicPayload(req, cfg, testModelGen4)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["system"] != "you are helpful" {
		t.Fatalf("system: %v", got["system"])
	}
	if got["max_tokens"].(float64) != 4096 {
		t.Fatalf("max_tokens: %v", got["max_tokens"])
	}
	msgs := got["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages len: %d", len(msgs))
	}
	first := msgs[0].(map[string]any)
	if first["role"] != "user" {
		t.Fatalf("role: %v", first["role"])
	}
	contentBlocks := first["content"].([]any)
	if contentBlocks[0].(map[string]any)["text"] != "hi" {
		t.Fatalf("content: %v", contentBlocks)
	}
}

func TestBuildPayloadIncludesTools(t *testing.T) {
	cfg := Config{Model: "x", MaxTokens: 100, AnthropicVersion: "bedrock-2023-05-31"}
	req := node.LLMRequest{
		Tools: []node.ToolSpec{
			{Name: "T", Description: "desc", InputSchema: json.RawMessage(`{"type":"object"}`)},
		},
	}
	body, err := buildAnthropicPayload(req, cfg, testModelGen4)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	tools := got["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools: %v", tools)
	}
	if tools[0].(map[string]any)["name"] != "T" {
		t.Fatalf("tool name: %v", tools[0])
	}
}

// TestBuildAnthropicPayloadToolChoice: ToolChoiceName set → tool_choice
// {"type":"tool","name":...} present; unset → tool_choice key absent
// (production regression guard — must be byte-identical to today).
func TestBuildAnthropicPayloadToolChoice(t *testing.T) {
	cfg := Config{Model: "us.anthropic.claude-opus-4-7", MaxTokens: 128, AnthropicVersion: "bedrock-2023-05-31"}
	base := node.LLMRequest{
		Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "ping"}}}},
	}

	t.Run("set", func(t *testing.T) {
		req := base
		req.ToolChoiceName = "echo"
		body, err := buildAnthropicPayload(req, cfg, testModelGen4)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		tc, ok := got["tool_choice"].(map[string]any)
		if !ok {
			t.Fatalf("tool_choice missing or wrong type: %v", got["tool_choice"])
		}
		if tc["type"] != "tool" || tc["name"] != "echo" {
			t.Fatalf("tool_choice = %v, want {type:tool name:echo}", tc)
		}
	})

	t.Run("unset", func(t *testing.T) {
		req := base
		body, err := buildAnthropicPayload(req, cfg, testModelGen4)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "tool_choice") {
			t.Fatalf("payload must not contain tool_choice when ToolChoiceName is empty: %s", body)
		}
	})
}

func TestBuildPayloadAssistantToolUseAndUserToolResult(t *testing.T) {
	cfg := Config{Model: "x", MaxTokens: 100, AnthropicVersion: "bedrock-2023-05-31"}
	req := node.LLMRequest{
		Messages: []node.Message{
			{Role: node.AssistantMsg, Content: []node.ContentBlock{
				{Type: node.BlockToolUse, ToolUseID: "tu1", ToolName: "T", ToolInput: json.RawMessage(`{"q":"1"}`)},
			}},
			{Role: node.UserMsg, Content: []node.ContentBlock{
				{Type: node.BlockToolResult, ToolUseID: "tu1", ToolResult: json.RawMessage(`{"hits":[]}`)},
			}},
		},
	}
	body, err := buildAnthropicPayload(req, cfg, testModelGen4)
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	if !strings.Contains(s, "tool_use") || !strings.Contains(s, "tool_result") {
		t.Fatalf("payload missing tool blocks: %s", body)
	}
}
