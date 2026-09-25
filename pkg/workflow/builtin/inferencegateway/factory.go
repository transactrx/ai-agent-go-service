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
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
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
		return errors.New("ai/inference-gateway: maxTokens must not be negative")
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

// hostLookup is the slice of node.NodeEnv natsConn needs (narrowed for tests).
type hostLookup interface {
	Host(kind string) (any, bool)
}

// natsConn returns the shared NATS connection from the hosts map, or nil
// when the host is missing, mistyped, or not connected.
func natsConn(env hostLookup) *nats.Conn {
	host, ok := env.Host("nats")
	if !ok {
		return nil
	}
	ns, ok := host.(*nats_service.NatService)
	if !ok {
		return nil
	}
	return ns.GetNatsService()
}

// Init binds the node to the shared NATS connection and fixes the
// invokeStream subject. No connection means the workflow cannot call any
// model, so it fails loudly at load time.
func (g *gatewayLLM) Init(_ context.Context, env node.NodeEnv) error {
	g.logger = env.Logger()
	g.nodeID = env.NodeID()
	g.wfID = env.WorkflowID()
	g.nc = natsConn(env)
	if g.nc == nil {
		return errors.New("ai/inference-gateway: nats host not registered or not connected")
	}
	g.subject = invokeSubject(g.cfg.BasePath)
	g.logger.Printf("ai/inference-gateway wf=%s node=%s ready (alias=%q modelId=%q lab=%q family=%q subject=%s)",
		g.wfID, g.nodeID, g.cfg.Alias, g.cfg.ModelID, g.cfg.Lab, g.cfg.Family, g.subject)
	return nil
}
