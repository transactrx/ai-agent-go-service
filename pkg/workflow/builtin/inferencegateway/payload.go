package inferencegateway

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Wire types mirror the gateway's models.InvokeStreamRequest (repo
// transactrx/inferenceGateway, pkg/models). Kept local so this library does
// not import the gateway module.

type wireImage struct {
	Format string `json:"format"`
	Base64 string `json:"base64"`
}

type wireToolUse struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}

type wireToolResultContent struct {
	Text string `json:"text,omitempty"`
}

type wireToolResult struct {
	ToolUseID string                  `json:"toolUseId"`
	Content   []wireToolResultContent `json:"content"`
	Status    string                  `json:"status,omitempty"`
}

type wireBlock struct {
	Text       string          `json:"text,omitempty"`
	Image      *wireImage      `json:"image,omitempty"`
	ToolUse    *wireToolUse    `json:"toolUse,omitempty"`
	ToolResult *wireToolResult `json:"toolResult,omitempty"`
}

type wireMessage struct {
	Role    string      `json:"role"`
	Content []wireBlock `json:"content"`
}

type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// invokeStreamRequest is the body sent to <base>.invokeStream. It carries no
// inference parameter on purpose: the gateway alias paramPolicy owns them.
type invokeStreamRequest struct {
	Alias string `json:"alias"`

	System   string        `json:"system,omitempty"`
	Messages []wireMessage `json:"messages"`

	Tools          []wireTool `json:"tools,omitempty"`
	ToolChoice     string     `json:"toolChoice,omitempty"`
	ToolChoiceName string     `json:"toolChoiceName,omitempty"`

	StreamSubject string `json:"streamSubject"`
}

// imageFormat maps an IANA media type to the gateway's image format enum.
func imageFormat(mediaType string) (string, error) {
	switch mediaType {
	case "image/png":
		return "png", nil
	case "image/jpeg", "image/jpg":
		return "jpeg", nil
	case "image/gif":
		return "gif", nil
	case "image/webp":
		return "webp", nil
	}
	return "", fmt.Errorf("ai/inference-gateway: unsupported image media type %q", mediaType)
}

// buildRequest converts the provider-agnostic request into the gateway body:
// alias, system, messages, tools and forced tool choice. MaxTokens,
// Temperature and Stop on req are ignored here — the gateway alias
// paramPolicy decides them (spec 2026-09-29 §4.2). Only the stream subject
// is left for Stream to fill.
func buildRequest(req node.LLMRequest, cfg Config) (*invokeStreamRequest, error) {
	out := &invokeStreamRequest{
		Alias:  cfg.Alias,
		System: req.System,
	}
	for _, m := range req.Messages {
		wm := wireMessage{Role: string(m.Role), Content: make([]wireBlock, 0, len(m.Content))}
		for _, b := range m.Content {
			switch b.Type {
			case node.BlockText:
				wm.Content = append(wm.Content, wireBlock{Text: b.Text})
			case node.BlockToolUse:
				input := b.ToolInput
				if len(input) == 0 {
					input = json.RawMessage(`{}`)
				}
				wm.Content = append(wm.Content, wireBlock{ToolUse: &wireToolUse{ToolUseID: b.ToolUseID, Name: b.ToolName, Input: input}})
			case node.BlockToolResult:
				// Same stringify rule as ai/bedrock: the raw JSON rides inside
				// one text content so object payloads round-trip safely.
				text := string(b.ToolResult)
				if text == "" {
					text = `""`
				}
				tr := &wireToolResult{ToolUseID: b.ToolUseID, Content: []wireToolResultContent{{Text: text}}}
				if b.IsError {
					tr.Status = "error"
				}
				wm.Content = append(wm.Content, wireBlock{ToolResult: tr})
			case node.BlockImage:
				format, err := imageFormat(b.MediaType)
				if err != nil {
					return nil, err
				}
				wm.Content = append(wm.Content, wireBlock{Image: &wireImage{Format: format, Base64: base64.StdEncoding.EncodeToString(b.Data)}})
			case node.BlockDocument:
				return nil, fmt.Errorf("ai/inference-gateway: document blocks are not supported by the gateway (file %q)", b.Filename)
			default:
				return nil, fmt.Errorf("ai/inference-gateway: unknown block type %q", b.Type)
			}
		}
		out.Messages = append(out.Messages, wm)
	}
	for _, ts := range req.Tools {
		out.Tools = append(out.Tools, wireTool{Name: ts.Name, Description: ts.Description, InputSchema: ts.InputSchema})
	}
	if req.ToolChoiceName != "" {
		out.ToolChoice = "tool"
		out.ToolChoiceName = req.ToolChoiceName
	}
	return out, nil
}
