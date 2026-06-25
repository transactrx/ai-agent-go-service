package node

import (
	"context"
	"encoding/json"
	"time"
)

// Memory persists a conversation per (workflowId, accountId, userId, sessionId).
// Cycle 1 ships memory/postgres; future cycles add dsql/redis/etc.
type Memory interface {
	Node
	Load(ctx context.Context, key MemoryKey) ([]Message, error)
	Append(ctx context.Context, key MemoryKey, t Turn) error
}

// MemoryKey is the composite identifier for a session's history. AccountID
// partitions sessions so a sessionId leaked across accounts cannot replay
// another account's chat history. UserID is the per-user partition within an
// account, ensuring two users in the same account cannot read each other's
// chats even if a sessionId collides. Empty AccountID/UserID is allowed for
// workflows without an identity layer (none in cycle 1).
type MemoryKey struct {
	WorkflowID string
	AccountID  string
	UserID     string
	SessionID  string
}

// Message is one Anthropic-style message (role + content blocks).
type Message struct {
	Role    MessageRole
	Content []ContentBlock
}

// MessageRole is the sender of a message.
type MessageRole string

const (
	UserMsg      MessageRole = "user"
	AssistantMsg MessageRole = "assistant"
	SystemMsg    MessageRole = "system"
)

// ContentBlock is one block within a message — text, tool_use, tool_result,
// image, or document. For image/document blocks: MediaType holds the IANA type
// (e.g. "image/jpeg", "application/pdf"), Filename is shown to the model for
// documents, and Data carries the raw bytes (base64-encoded only at the wire
// layer when sent to the LLM provider).
type ContentBlock struct {
	Type       BlockType
	Text       string
	ToolUseID  string
	ToolName   string
	ToolInput  json.RawMessage
	ToolResult json.RawMessage
	IsError    bool
	MediaType  string
	Filename   string
	Data       []byte
}

// BlockType enumerates the content-block variants.
type BlockType string

const (
	BlockText       BlockType = "text"
	BlockToolUse    BlockType = "tool_use"
	BlockToolResult BlockType = "tool_result"
	BlockImage      BlockType = "image"
	BlockDocument   BlockType = "document"
)

// Turn is a user/assistant pair stored in memory after each successful run.
// Intermediate tool_use/tool_result blocks are NOT persisted (spec §6.5).
type Turn struct {
	User      Message
	Assistant Message
	Timestamp time.Time
}
