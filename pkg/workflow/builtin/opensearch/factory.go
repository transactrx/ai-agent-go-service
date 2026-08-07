// Package opensearch implements tool/opensearch — a security-policy-gated tool
// that runs OpenSearch DSL queries against a configured cluster. The tool's
// LLM input schema is intentionally narrow (must / must_not / filter only);
// the server constructs the final query and merges policy clauses.
package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config validated by Factory.
type Config struct {
	Host                  string `json:"host"`
	UserEnv               string `json:"userEnv"`
	PasswordEnv           string `json:"passwordEnv"`
	ToolName              string `json:"toolName,omitempty"`
	ToolDescription       string `json:"toolDescription"`
	RequestTimeoutSeconds int    `json:"requestTimeoutSeconds,omitempty"`
	FailurePolicyValue    string `json:"failurePolicy,omitempty"`
	MaxResultSize         int    `json:"maxResultSize,omitempty"`
	AllowedIndexPattern   string `json:"allowedIndexPattern"`
}

const (
	defaultToolName       = "OpenSearchQuery"
	defaultRequestTimeout = 30
	defaultMaxResultSize  = 500
)

// Factory builds an opensearch tool node.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("tool/opensearch: parse config: %w", err)
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("tool/opensearch: host is required")
	}
	if cfg.UserEnv == "" || cfg.PasswordEnv == "" {
		return nil, fmt.Errorf("tool/opensearch: userEnv and passwordEnv are required")
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/opensearch: toolDescription is required")
	}
	if cfg.AllowedIndexPattern == "" {
		return nil, fmt.Errorf("tool/opensearch: allowedIndexPattern is required")
	}
	if cfg.ToolName == "" {
		cfg.ToolName = defaultToolName
	}
	if cfg.RequestTimeoutSeconds == 0 {
		cfg.RequestTimeoutSeconds = defaultRequestTimeout
	}
	if cfg.MaxResultSize == 0 {
		cfg.MaxResultSize = defaultMaxResultSize
	}
	if cfg.FailurePolicyValue == "" {
		cfg.FailurePolicyValue = string(node.FailureSurfaceToLLM)
	}
	return &opensearchTool{cfg: cfg}, nil
})

type opensearchTool struct {
	node.BaseTool
	cfg           Config
	user          secret.String
	pass          secret.String
	http          *http.Client
	policy        node.Policy
	logger        *log.Logger
	nodeID        string
	wfID          string
	allowedRegexp *regexp.Regexp
	mappingCache  mappingCache
}

// Spec returns the node's metadata. Note the REQUIRED policy input port.
func (t *opensearchTool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "tool/opensearch",
		Role: node.RoleTool,
		InputPorts: []node.PortSpec{
			{Name: node.PortPolicy, Direction: node.PortIn, Cardinality: node.CardOne, Required: true, PeerRole: node.RolePolicy},
		},
		OutputPorts: []node.PortSpec{
			{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

// FailurePolicy returns the configured policy as a typed value.
func (t *opensearchTool) FailurePolicy() node.FailurePolicy {
	return node.FailurePolicy(t.cfg.FailurePolicyValue)
}

// Init resolves credentials, builds the HTTP client, and stores the policy peer.
func (t *opensearchTool) Init(ctx context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	var err error
	if t.user, err = env.Secret(t.cfg.UserEnv); err != nil {
		return err
	}
	if t.pass, err = env.Secret(t.cfg.PasswordEnv); err != nil {
		return err
	}
	t.http = &http.Client{Timeout: time.Duration(t.cfg.RequestTimeoutSeconds) * time.Second}
	t.logger = env.Logger()
	t.nodeID = env.NodeID()
	t.wfID = env.WorkflowID()

	re, err := compileIndexPattern(t.cfg.AllowedIndexPattern)
	if err != nil {
		return fmt.Errorf("tool/opensearch: %w", err)
	}
	t.allowedRegexp = re

	peers, err := env.Peer(node.PortPolicy)
	if err != nil {
		return err
	}
	if len(peers) != 1 {
		return fmt.Errorf("tool/opensearch: requires exactly one policy peer, got %d", len(peers))
	}
	policy, ok := peers[0].(node.Policy)
	if !ok {
		return fmt.Errorf("tool/opensearch: peer is not a Policy")
	}
	t.policy = policy
	return nil
}

func (t *opensearchTool) Close(_ context.Context) error { return nil }

// ToolSpec — the LLM-facing contract.
func (t *opensearchTool) ToolSpec() node.ToolSpec {
	return node.ToolSpec{
		Name:        t.cfg.ToolName,
		Description: t.cfg.ToolDescription,
		InputSchema: json.RawMessage(`{
		  "type": "object",
		  "required": ["indexPath", "queryBody"],
		  "additionalProperties": false,
		  "properties": {
		    "indexPath": {
		      "type": "string",
		      "description": "Comma-separated index list, e.g. \"prod.cpe-2026-04-30,prod.cpe-2026-04-29\". Must be in the user's allowed-index set; the server will reject otherwise."
		    },
		    "queryBody": {
		      "type": "object",
		      "description": "OpenSearch bool clause WITHOUT a top-level wrapping. Use must/must_not/filter as needed. The server wraps your clause in a top-level bool, injects security constraints into must, and adds size/sort/aggs. Do NOT include size, _source, sort, aggregations, or top-level should-only constructs.",
		      "properties": {
		        "must":     {"type": "array"},
		        "must_not": {"type": "array"},
		        "filter":   {"type": "array"}
		      },
		      "additionalProperties": false
		    },
		    "size": {"type": "integer", "minimum": 0, "maximum": 10000},
		    "aggregations": {"type": "object"},
		    "sort": {"type": "array"}
		  }
		}`),
	}
}
