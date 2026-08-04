// Package bedrock implements the ai/bedrock LLMProvider over AWS Bedrock
// Runtime InvokeModelWithResponseStream for Anthropic Claude models.
package bedrock

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config is the per-instance bedrock node config. Validated by Factory.
type Config struct {
	Model            string   `json:"model"`
	Region           string   `json:"region,omitempty"`
	Temperature      *float64 `json:"temperature,omitempty"`
	MaxTokens        int      `json:"maxTokens,omitempty"`
	AnthropicVersion string   `json:"anthropicVersion,omitempty"`
	// AutoUpdate enables the daily latest-model check (spec
	// 2026-06-05-bedrock-model-autoupdate). Absent → enabled.
	AutoUpdate *bool `json:"autoUpdate,omitempty"`
}

// autoUpdateEnabled: absent defaults to true; explicit false disables.
func (c Config) autoUpdateEnabled() bool {
	return c.AutoUpdate == nil || *c.AutoUpdate
}

const (
	defaultRegion           = "us-east-1"
	defaultMaxTokens        = 4096
	defaultAnthropicVersion = "bedrock-2023-05-31"
)

// Factory builds a bedrock node from rawConfig.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("ai/bedrock: parse config: %w", err)
	}
	if cfg.Model == "" {
		return nil, fmt.Errorf("ai/bedrock: model is required")
	}
	if cfg.Region == "" {
		cfg.Region = defaultRegion
	}
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = defaultMaxTokens
	}
	if cfg.AnthropicVersion == "" {
		cfg.AnthropicVersion = defaultAnthropicVersion
	}
	return &bedrockLLM{cfg: cfg, model: cfg.Model}, nil
})

// bedrockLLM is the node instance.
type bedrockLLM struct {
	cfg    Config
	client *bedrockruntime.Client
	logger *log.Logger
	nodeID string
	wfID   string

	// model is the current effective model ID. cfg.Model is the configured
	// starting model; the auto-updater (gateway-first, catalog-scan fallback)
	// may swap model to a different validated release.
	modelMu sync.RWMutex
	model   string

	// lastKnownGood is the model displaced by the last validated upgrade —
	// offered as a request-time fallback in Stream if the fresh model breaks
	// mid-day. In-memory only; reset on restart (startup runOnce re-resolves).
	lastKnownGood string

	cancelUpdater context.CancelFunc
}

// currentModel returns the effective model. Read once per request so
// in-flight requests keep the model they started with.
func (b *bedrockLLM) currentModel() string {
	b.modelMu.RLock()
	defer b.modelMu.RUnlock()
	return b.model
}

// setModel hot-swaps the effective model. Auto-updater is the only caller.
func (b *bedrockLLM) setModel(m string) {
	b.modelMu.Lock()
	b.model = m
	b.modelMu.Unlock()
}

// swapModel promotes m after a VALIDATED upgrade, keeping the displaced
// model as last-known-good for the request-time fallback.
func (b *bedrockLLM) swapModel(m string) {
	b.modelMu.Lock()
	if b.model != m {
		b.lastKnownGood = b.model
	}
	b.model = m
	b.modelMu.Unlock()
}

// recoverModel promotes m after the current model FAILED its health check.
// The broken model is never recorded as fallback; a stale fallback equal to
// m is cleared (it is current again, not a fallback).
func (b *bedrockLLM) recoverModel(m string) {
	b.modelMu.Lock()
	if b.lastKnownGood == m {
		b.lastKnownGood = ""
	}
	b.model = m
	b.modelMu.Unlock()
}

// lastKnownGoodModel returns the request-time fallback model ("" when none).
func (b *bedrockLLM) lastKnownGoodModel() string {
	b.modelMu.RLock()
	defer b.modelMu.RUnlock()
	return b.lastKnownGood
}

// Spec returns immutable metadata.
func (b *bedrockLLM) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "ai/bedrock",
		Role: node.RoleLLM,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAILanguageModel, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

// Init opens the AWS Bedrock client and, when enabled, starts the daily
// model auto-update check.
func (b *bedrockLLM) Init(ctx context.Context, env node.NodeEnv) error {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(b.cfg.Region))
	if err != nil {
		return fmt.Errorf("ai/bedrock: load AWS config: %w", err)
	}
	b.client = bedrockruntime.NewFromConfig(awsCfg)
	b.logger = env.Logger()
	b.nodeID = env.NodeID()
	b.wfID = env.WorkflowID()
	if b.cfg.autoUpdateEnabled() {
		b.startAutoUpdate(env, awsCfg)
	}
	return nil
}

// Close stops the auto-update goroutine, if running.
func (b *bedrockLLM) Close(_ context.Context) error {
	if b.cancelUpdater != nil {
		b.cancelUpdater()
	}
	return nil
}
