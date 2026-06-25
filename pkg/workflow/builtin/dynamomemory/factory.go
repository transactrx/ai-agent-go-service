package dynamomemory

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config is validated by Factory.
type Config struct {
	TableName string `json:"tableName"`
	Region    string `json:"region,omitempty"`
	MaxTurns  int    `json:"maxTurns,omitempty"`
	TTLDays   int    `json:"ttlDays,omitempty"`
}

const (
	defaultRegion   = "us-east-1"
	defaultMaxTurns = 30
	defaultTTLDays  = 90
	ttlAttribute    = "expires_at"
)

// tableNamePattern follows the AWS DynamoDB rule (3-255 chars, alnum + _ - .).
var tableNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,255}$`)

// Factory builds a dynamomemory node.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("memory/dynamodb: parse config: %w", err)
	}
	if cfg.TableName == "" {
		return nil, fmt.Errorf("memory/dynamodb: tableName is required")
	}
	if !tableNamePattern.MatchString(cfg.TableName) {
		return nil, fmt.Errorf("memory/dynamodb: tableName %q invalid (must be 3-255 chars of [a-zA-Z0-9_.-])", cfg.TableName)
	}
	if cfg.TTLDays < 0 {
		return nil, fmt.Errorf("memory/dynamodb: ttlDays %d invalid (must be >= 0)", cfg.TTLDays)
	}
	if cfg.Region == "" {
		cfg.Region = defaultRegion
	}
	if cfg.MaxTurns == 0 {
		cfg.MaxTurns = defaultMaxTurns
	}
	if cfg.TTLDays == 0 {
		cfg.TTLDays = defaultTTLDays
	}
	return &dynamoMemory{cfg: cfg}, nil
})

type dynamoMemory struct {
	cfg    Config
	client dynamoAPI
}

func (m *dynamoMemory) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "memory/dynamodb",
		Role: node.RoleMemory,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAIMemory, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

// Init wires the real *dynamodb.Client when client is nil (tests inject before Init).
func (m *dynamoMemory) Init(ctx context.Context, env node.NodeEnv) error {
	if m.client == nil {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(m.cfg.Region))
		if err != nil {
			return fmt.Errorf("memory/dynamodb: load aws config: %w", err)
		}
		m.client = dynamodb.NewFromConfig(awsCfg)
	}
	return m.provision(ctx, env.Logger())
}

func (m *dynamoMemory) Close(_ context.Context) error { return nil }
