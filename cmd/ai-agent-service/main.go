// Command ai-agent-service is the generic reference agent service. It loads workflows
// from ./workflows and serves them over NATS using only the library default node types.
// Services that need tenant-specific node types should write their own main using
// agent.NewService(agent.WithNode(...)).
package main

import (
	"context"
	"log"

	"github.com/transactrx/ai-agent-go-service/agent"
)

var version = "dev" // overridden via -ldflags "-X main.version=..."

func main() {
	if err := agent.NewService().Run(context.Background()); err != nil {
		log.Fatalf("ai-agent-service: %v", err)
	}
}
