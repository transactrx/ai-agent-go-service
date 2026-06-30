package agent

import (
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// buildRegistry creates a registry with the library defaults, then applies any
// consumer-supplied factories. Replace (not Register) is used so a consumer may
// override a default node type by reusing its type key without a duplicate-key error.
func (s *Service) buildRegistry() (*node.Registry, error) {
	reg := node.NewRegistry()
	if err := builtin.RegisterDefaults(reg); err != nil {
		return nil, err
	}
	for typeKey, f := range s.extraFactories {
		reg.Replace(typeKey, f)
	}
	return reg, nil
}
