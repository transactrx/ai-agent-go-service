package bedrock

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// buildAnthropicPayload converts an LLMRequest to the Bedrock-Anthropic JSON
// envelope used by InvokeModelWithResponseStream.
func buildAnthropicPayload(req node.LLMRequest, cfg Config) ([]byte, error) {
	type textBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	type toolUseBlock struct {
		Type  string          `json:"type"`
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}
	// Anthropic requires tool_result.content to be a string OR a list of
	// content blocks ([{type:"text", text:"..."}]). Embedding a raw JSON
	// object directly is rejected with a 400 ValidationException. We always
	// emit content as a list with a single text block whose text is the
	// JSON-stringified payload — that round-trips object payloads safely.
	type toolResultBlock struct {
		Type      string `json:"type"`
		ToolUseID string `json:"tool_use_id"`
		Content   []any  `json:"content"`
		IsError   bool   `json:"is_error,omitempty"`
	}
	type base64Source struct {
		Type      string `json:"type"`
		MediaType string `json:"media_type"`
		Data      string `json:"data"`
	}
	type imageBlock struct {
		Type   string       `json:"type"`
		Source base64Source `json:"source"`
	}
	type documentBlock struct {
		Type   string       `json:"type"`
		Source base64Source `json:"source"`
		Title  string       `json:"title,omitempty"`
	}
	type message struct {
		Role    string `json:"role"`
		Content []any  `json:"content"`
	}
	type tool struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		InputSchema json.RawMessage `json:"input_schema"`
	}
	type envelope struct {
		AnthropicVersion string    `json:"anthropic_version"`
		MaxTokens        int       `json:"max_tokens"`
		Temperature      *float64  `json:"temperature,omitempty"`
		System           string    `json:"system,omitempty"`
		Messages         []message `json:"messages"`
		Tools            []tool    `json:"tools,omitempty"`
		StopSequences    []string  `json:"stop_sequences,omitempty"`
	}

	maxTok := cfg.MaxTokens
	if req.MaxTokens > 0 {
		maxTok = req.MaxTokens
	}

	env := envelope{
		AnthropicVersion: cfg.AnthropicVersion,
		MaxTokens:        maxTok,
		Temperature:      req.Temperature,
		System:           req.System,
		StopSequences:    req.Stop,
	}
	for _, m := range req.Messages {
		blocks := make([]any, 0, len(m.Content))
		for _, b := range m.Content {
			switch b.Type {
			case node.BlockText:
				blocks = append(blocks, textBlock{Type: "text", Text: b.Text})
			case node.BlockToolUse:
				input := b.ToolInput
				if len(input) == 0 {
					input = json.RawMessage(`{}`)
				}
				blocks = append(blocks, toolUseBlock{
					Type: "tool_use", ID: b.ToolUseID, Name: b.ToolName, Input: input,
				})
			case node.BlockToolResult:
				// Stringify the raw payload (object/array/string/number/null)
				// so it can ride inside the {type:"text", text:"..."} block.
				text := string(b.ToolResult)
				if text == "" {
					text = `""`
				}
				blocks = append(blocks, toolResultBlock{
					Type:      "tool_result",
					ToolUseID: b.ToolUseID,
					Content:   []any{textBlock{Type: "text", Text: text}},
					IsError:   b.IsError,
				})
			case node.BlockImage:
				blocks = append(blocks, imageBlock{
					Type: "image",
					Source: base64Source{
						Type:      "base64",
						MediaType: b.MediaType,
						Data:      base64.StdEncoding.EncodeToString(b.Data),
					},
				})
			case node.BlockDocument:
				blocks = append(blocks, documentBlock{
					Type: "document",
					Source: base64Source{
						Type:      "base64",
						MediaType: b.MediaType,
						Data:      base64.StdEncoding.EncodeToString(b.Data),
					},
					Title: b.Filename,
				})
			default:
				return nil, fmt.Errorf("ai/bedrock: unknown block type %q", b.Type)
			}
		}
		env.Messages = append(env.Messages, message{Role: string(m.Role), Content: blocks})
	}
	for _, ts := range req.Tools {
		env.Tools = append(env.Tools, tool{Name: ts.Name, Description: ts.Description, InputSchema: ts.InputSchema})
	}
	return json.Marshal(env)
}
