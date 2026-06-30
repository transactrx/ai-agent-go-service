// Package pgmemory implements memory/postgres for chat history storage.
package pgmemory

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config validated by Factory.
type Config struct {
	DSNEnv      string `json:"dsnEnv"`
	TablePrefix string `json:"tablePrefix,omitempty"`
	MaxTurns    int    `json:"maxTurns,omitempty"`
}

const (
	defaultPrefix   = "chat_"
	defaultMaxTurns = 30
)

var prefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Factory builds a pgmemory node.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("memory/postgres: parse config: %w", err)
	}
	if cfg.DSNEnv == "" {
		return nil, fmt.Errorf("memory/postgres: dsnEnv is required")
	}
	if cfg.TablePrefix == "" {
		cfg.TablePrefix = defaultPrefix
	}
	if !prefixPattern.MatchString(cfg.TablePrefix) {
		return nil, fmt.Errorf("memory/postgres: tablePrefix %q must match %s", cfg.TablePrefix, prefixPattern)
	}
	if cfg.MaxTurns == 0 {
		cfg.MaxTurns = defaultMaxTurns
	}
	return &pgMemory{cfg: cfg}, nil
})

type pgMemory struct {
	cfg  Config
	pool *pgxpool.Pool
	dsn  secret.String
}

func (m *pgMemory) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "memory/postgres",
		Role: node.RoleMemory,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAIMemory, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

func (m *pgMemory) Init(ctx context.Context, env node.NodeEnv) error {
	dsn, err := env.Secret(m.cfg.DSNEnv)
	if err != nil {
		return err
	}
	m.dsn = dsn
	pool, err := pgxpool.New(ctx, dsn.Reveal())
	if err != nil {
		return fmt.Errorf("memory/postgres: open pool: %w", err)
	}
	m.pool = pool
	if err := m.pool.Ping(ctx); err != nil {
		return fmt.Errorf("memory/postgres: ping: %w", err)
	}
	return m.applyMigrations(ctx)
}

func (m *pgMemory) Close(_ context.Context) error {
	if m.pool != nil {
		m.pool.Close()
	}
	return nil
}

func (m *pgMemory) table() string { return m.cfg.TablePrefix + "messages" }
