// Package inferencegateway implements the ai/inference-gateway LLMProvider:
// model calls go through the org inferenceGateway NATS service
// (<base>.invokeStream) and the model is chosen by alias (tier), model id, or
// lab+family. Spec: docs/superpowers/specs/2026-09-25-inference-gateway-llm-design.md
package inferencegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const (
	nodeType             = "ai/inference-gateway"
	defaultMaxTokens     = 4096
	defaultStreamTimeout = 600 * time.Second
	defaultIdleTimeout   = 120 * time.Second
)

// Config is the per-instance node config. Exactly one selector: alias,
// modelId, or lab+family. The gateway resolves it on every call.
type Config struct {
	Alias   string `json:"alias,omitempty"`
	ModelID string `json:"modelId,omitempty"`
	Lab     string `json:"lab,omitempty"`
	Family  string `json:"family,omitempty"`

	MaxTokens   int      `json:"maxTokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`

	StreamTimeoutSeconds int `json:"streamTimeoutSeconds,omitempty"`
	IdleTimeoutSeconds   int `json:"idleTimeoutSeconds,omitempty"`

	// BasePath overrides INFERENCE_GATEWAY_BASE_PATH for this node.
	BasePath string `json:"basePath,omitempty"`
}

func (c Config) validate() error {
	if (c.Lab == "") != (c.Family == "") {
		return errors.New("ai/inference-gateway: lab and family must be set together")
	}
	selectors := 0
	if c.Alias != "" {
		selectors++
	}
	if c.ModelID != "" {
		selectors++
	}
	if c.Lab != "" {
		selectors++
	}
	if selectors != 1 {
		return errors.New("ai/inference-gateway: exactly one of alias, modelId, lab+family is required")
	}
	if c.MaxTokens < 0 {
		return errors.New("ai/inference-gateway: maxTokens must be positive")
	}
	if c.StreamTimeoutSeconds < 0 || c.IdleTimeoutSeconds < 0 {
		return errors.New("ai/inference-gateway: timeouts must be positive")
	}
	return nil
}

// Factory builds an ai/inference-gateway node from rawConfig.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("ai/inference-gateway: parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newLLM(cfg), nil
})

// gatewayLLM is the node instance.
type gatewayLLM struct {
	cfg Config

	nc            *nats.Conn
	subject       string // <base>.invokeStream, fixed at Init
	streamTimeout time.Duration
	idleTimeout   time.Duration

	logger *log.Logger
	nodeID string
	wfID   string
}

// newLLM applies config defaults.
func newLLM(cfg Config) *gatewayLLM {
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = defaultMaxTokens
	}
	g := &gatewayLLM{cfg: cfg, streamTimeout: defaultStreamTimeout, idleTimeout: defaultIdleTimeout}
	if cfg.StreamTimeoutSeconds > 0 {
		g.streamTimeout = time.Duration(cfg.StreamTimeoutSeconds) * time.Second
	}
	if cfg.IdleTimeoutSeconds > 0 {
		g.idleTimeout = time.Duration(cfg.IdleTimeoutSeconds) * time.Second
	}
	return g
}

// Spec returns immutable metadata; same output port as ai/bedrock so the
// agent wiring is identical.
func (g *gatewayLLM) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: nodeType,
		Role: node.RoleLLM,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAILanguageModel, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

// Close has nothing to release: the NATS connection is shared and owned by
// the host.
func (g *gatewayLLM) Close(_ context.Context) error { return nil }

// Init is a temporary stub for Task 6.
func (g *gatewayLLM) Init(_ context.Context, _ node.NodeEnv) error { return nil }

// Stream is a temporary stub for Task 7.
func (g *gatewayLLM) Stream(_ context.Context, _ node.LLMRequest, out chan<- node.LLMEvent) error {
	close(out)
	return errors.New("ai/inference-gateway: not implemented")
}
