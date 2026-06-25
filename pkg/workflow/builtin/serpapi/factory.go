// Package serpapi implements tool/serpapi — the N8N-parity web search tool.
// Backed by https://serpapi.com. Use for context not in our OpenSearch data.
package serpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const (
	defaultToolName       = "WebSearchSerpAPI"
	defaultEngine         = "google"
	defaultMaxResults     = 5
	defaultRequestTimeout = 15
)

// Config validated by Factory.
type Config struct {
	Host                  string `json:"host,omitempty"`
	APIKeyEnv             string `json:"apiKeyEnv"`
	DefaultEngine         string `json:"defaultEngine,omitempty"`
	MaxResults            int    `json:"maxResults,omitempty"`
	RequestTimeoutSeconds int    `json:"requestTimeoutSeconds,omitempty"`
	FailurePolicyValue    string `json:"failurePolicy,omitempty"`
	ToolName              string `json:"toolName,omitempty"`
	ToolDescription       string `json:"toolDescription"`
}

// Factory builds a serpapi tool node.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("tool/serpapi: parse config: %w", err)
	}
	if cfg.APIKeyEnv == "" {
		return nil, fmt.Errorf("tool/serpapi: apiKeyEnv is required")
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/serpapi: toolDescription is required")
	}
	if cfg.Host == "" {
		cfg.Host = "https://serpapi.com"
	}
	if cfg.DefaultEngine == "" {
		cfg.DefaultEngine = defaultEngine
	}
	if cfg.MaxResults == 0 {
		cfg.MaxResults = defaultMaxResults
	}
	if cfg.RequestTimeoutSeconds == 0 {
		cfg.RequestTimeoutSeconds = defaultRequestTimeout
	}
	if cfg.FailurePolicyValue == "" {
		cfg.FailurePolicyValue = string(node.FailureSurfaceToLLM)
	}
	if cfg.ToolName == "" {
		cfg.ToolName = defaultToolName
	}
	return &serpapiTool{cfg: cfg}, nil
})

type serpapiTool struct {
	node.BaseTool
	cfg    Config
	apiKey secret.String
	http   *http.Client
	logger *log.Logger
	nodeID string
	wfID   string
}

func (t *serpapiTool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "tool/serpapi",
		Role: node.RoleTool,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

func (t *serpapiTool) FailurePolicy() node.FailurePolicy {
	return node.FailurePolicy(t.cfg.FailurePolicyValue)
}

func (t *serpapiTool) Init(_ context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	key, err := env.Secret(t.cfg.APIKeyEnv)
	if err != nil {
		return err
	}
	t.apiKey = key
	t.http = &http.Client{Timeout: time.Duration(t.cfg.RequestTimeoutSeconds) * time.Second}
	t.logger = env.Logger()
	t.nodeID = env.NodeID()
	t.wfID = env.WorkflowID()
	return nil
}

func (t *serpapiTool) Close(_ context.Context) error { return nil }

func (t *serpapiTool) ToolSpec() node.ToolSpec {
	return node.ToolSpec{
		Name:        t.cfg.ToolName,
		Description: t.cfg.ToolDescription,
		InputSchema: json.RawMessage(`{
		  "type": "object",
		  "required": ["query"],
		  "additionalProperties": false,
		  "properties": {
		    "query": {"type": "string", "minLength": 1, "maxLength": 500},
		    "num":   {"type": "integer", "minimum": 1, "maximum": 10},
		    "hl":    {"type": "string"},
		    "gl":    {"type": "string"}
		  }
		}`),
	}
}
