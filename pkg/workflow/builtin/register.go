// Package builtin wires every generic node-type factory into a node.Registry via
// RegisterDefaults. Tenant-specific node types (e.g. policy/powerline-scope) are
// registered by the consuming service, not here. Adding a new generic node type =
// one new subpackage + one line here.
package builtin

import (
	"errors"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/agent"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/bedrock"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/dynamomemory"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/mongoquery"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/natschat"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/opensearch"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/pgmemory"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/postgresquery"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/promptrewrite"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/quickchart"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/rabbitmqmanagement"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/serpapi"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/webfetch"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// RegisterDefaults registers every generic node-type factory shipped with the
// library. It deliberately omits tenant-specific node types.
func RegisterDefaults(reg *node.Registry) error {
	return errors.Join(
		reg.Register("trigger/nats-chat", natschat.Factory),
		reg.Register("ai/agent", agent.Factory),
		reg.Register("ai/bedrock", bedrock.Factory),
		reg.Register("memory/postgres", pgmemory.Factory),
		reg.Register("memory/dynamodb", dynamomemory.Factory),
		reg.Register("tool/opensearch", opensearch.Factory),
		reg.Register("tool/serpapi", serpapi.Factory),
		reg.Register("tool/quickchart", quickchart.Factory),
		reg.Register("tool/ui-confirm", clientui.ConfirmFactory),
		reg.Register("tool/ui-pick-one", clientui.PickOneFactory),
		reg.Register("tool/ui-human-input", clientui.HumanInputFactory),
		reg.Register("tool/ui-pick-many", clientui.PickManyFactory),
		reg.Register("tool/ui-ask-date", clientui.AskDateFactory),
		reg.Register("tool/ui-ask-form", clientui.AskFormFactory),
		reg.Register("tool/ui-pick-row", clientui.PickRowFactory),
		reg.Register("tool/ui-ask-number", clientui.AskNumberFactory),
		reg.Register("tool/ui-ask-long-text", clientui.AskLongTextFactory),
		reg.Register("tool/postgres-query", postgresquery.Factory),
		reg.Register("tool/mongo-query", mongoquery.Factory),
		reg.Register("tool/rabbitmq-management", rabbitmqmanagement.Factory),
		reg.Register("tool/web-fetch", webfetch.Factory),
		reg.Register("admin/prompt-rewrite", promptrewrite.Factory),
	)
}
