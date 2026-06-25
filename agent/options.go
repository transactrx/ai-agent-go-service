// Package agent is the high-level facade for the agent half of ai-agent-go-service.
// A consuming service creates a Service with sensible defaults, optionally overrides
// node factories / hosts, and calls Run to boot the NATS-fronted workflow engine.
package agent

import (
	"log"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Service holds the configuration for one agent service instance. Build it with
// NewService and the With* options, then call Run.
type Service struct {
	workflowsDir   string
	extraFactories map[string]node.Factory // type key -> factory; applied AFTER defaults (override-capable)
	extraHosts     map[string]any          // merged into the engine hosts map
	logger         *log.Logger             // nil => Run builds the default region-prefixed logger
}

// Option mutates a Service during NewService.
type Option func(*Service)

// NewService returns a Service with defaults (workflowsDir "./workflows") and applies opts.
func NewService(opts ...Option) *Service {
	s := &Service{
		workflowsDir:   "./workflows",
		extraFactories: map[string]node.Factory{},
		extraHosts:     map[string]any{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// WithWorkflowsDir overrides the directory the engine loads *.json workflows from.
func WithWorkflowsDir(dir string) Option {
	return func(s *Service) {
		if dir != "" {
			s.workflowsDir = dir
		}
	}
}

// WithNode registers (or overrides) a node-type factory beyond the library defaults.
// Use it to add tenant-specific node types, e.g. WithNode("policy/powerline-scope", powerlinescope.Factory).
func WithNode(typeKey string, f node.Factory) Option {
	return func(s *Service) { s.extraFactories[typeKey] = f }
}

// WithHost injects an extra entry into the engine hosts map (advanced).
func WithHost(key string, h any) Option {
	return func(s *Service) { s.extraHosts[key] = h }
}

// WithLogger overrides the default logger.
func WithLogger(l *log.Logger) Option {
	return func(s *Service) { s.logger = l }
}
