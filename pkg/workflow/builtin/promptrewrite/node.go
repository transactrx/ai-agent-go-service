// Package promptrewrite implements admin/prompt-rewrite: a portless node
// that registers a streaming PromptRewrite NATS endpoint for AI-assisted
// prompt editing. It never touches the graph (no input/output ports) — it
// is wired purely through hosts (nats, nats_basePath, promptstore). It does
// NOT persist the rewritten text; the client reviews the streamed result and
// calls PromptSave itself.
package promptrewrite

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/promptstore"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// NodeType is the workflow-JSON "type" key for this node.
const NodeType = "admin/prompt-rewrite"

// Config is decoded from the workflow-JSON config block.
type Config struct {
	ModelEnv  string `json:"modelEnv,omitempty"`  // env var name holding the Bedrock model id; default "BEDROCK_REWRITE_MODEL"
	RegionEnv string `json:"regionEnv,omitempty"` // env var name holding the Bedrock region; default "BEDROCK_REWRITE_REGION"
}

const (
	defaultModelEnv  = "BEDROCK_REWRITE_MODEL"
	defaultRegionEnv = "BEDROCK_REWRITE_REGION"
	defaultModel     = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
	defaultRegion    = "us-east-1"
)

// bedrockStreamClient is the Bedrock surface run() uses. *bedrockruntime.Client
// satisfies it; tests substitute a fake.
type bedrockStreamClient interface {
	InvokeModelWithResponseStream(ctx context.Context, in *bedrockruntime.InvokeModelWithResponseStreamInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelWithResponseStreamOutput, error)
}

// Node implements the admin/prompt-rewrite node.
type Node struct {
	cfg      Config
	nats     *nats_service.NatService
	basePath string
	store    *promptstore.Store // nil when promptstore disabled
	logger   *log.Logger
	model    string
	region   string
	bedrock  bedrockStreamClient // built once in Init, reused per request
}

// Spec returns the node's metadata. No input/output ports: this node is
// wired purely through hosts, never through the graph, so the loader's
// port-connection validation never sees it.
func (n *Node) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type:        NodeType,
		Role:        node.RolePolicy,
		Description: "Registers the streaming PromptRewrite admin endpoint (AI-assisted prompt editing).",
	}
}

// Init wires the NATS host + optional promptstore host and registers the
// PromptRewrite endpoint.
func (n *Node) Init(ctx context.Context, env node.NodeEnv) error {
	n.logger = env.Logger()

	host, ok := env.Host("nats")
	if !ok {
		return errors.New("admin/prompt-rewrite: nats host not registered")
	}
	n.nats, ok = host.(*nats_service.NatService)
	if !ok {
		return errors.New("admin/prompt-rewrite: nats host has wrong type")
	}

	if bp, ok := env.Host("nats_basePath"); ok {
		n.basePath, _ = bp.(string)
	}
	if n.basePath == "" {
		return errors.New("admin/prompt-rewrite: nats_basePath missing")
	}

	if ps, ok := env.Host("promptstore"); ok {
		n.store, _ = ps.(*promptstore.Store)
	}

	n.model = envOr(defStr(n.cfg.ModelEnv, defaultModelEnv), defaultModel)
	n.region = envOr(defStr(n.cfg.RegionEnv, defaultRegionEnv), defaultRegion)

	// Build the Bedrock client once here rather than per request. Tests
	// pre-set n.bedrock and skip this.
	if n.bedrock == nil {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(n.region))
		if err != nil {
			return fmt.Errorf("admin/prompt-rewrite: aws config: %w", err)
		}
		n.bedrock = bedrockruntime.NewFromConfig(awsCfg)
	}

	return n.registerEndpoint()
}

// Close is a no-op: this node holds no resources beyond the shared NATS host.
func (n *Node) Close(_ context.Context) error { return nil }

// defStr returns v, or def when v is empty. Used to default a config field
// that itself names an env var.
func defStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// envOr reads envVarName from the environment, falling back to def when
// unset or empty.
func envOr(envVarName, def string) string {
	if v := os.Getenv(envVarName); v != "" {
		return v
	}
	return def
}

var _ node.Node = (*Node)(nil)
