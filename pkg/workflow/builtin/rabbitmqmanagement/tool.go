// Package rabbitmqmanagement is the tool/rabbitmq-management node: queries
// RabbitMQ's Management HTTP API (read-only). No publish/purge/delete; no
// message-body retrieval.
package rabbitmqmanagement

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// ConnConfig holds per-connection settings. BaseURL/Username/Password are
// wired from env vars in Init, NOT from the workflow-JSON config block.
type ConnConfig struct {
	BaseURL          string `json:"-"`
	Username         string `json:"-"`
	Password         string `json:"-"`
	Vhost            string `json:"vhost"`
	MaxItemsDefault  int    `json:"maxItemsDefault"`
	RequestTimeoutMs int    `json:"requestTimeoutMs"`
}

// Config is decoded from the workflow-JSON config block.
type Config struct {
	Connections     map[string]ConnConfig `json:"connections"`
	ToolName        string                `json:"toolName,omitempty"`      // default "rabbitmq_inspect"
	ToolDescription string                `json:"toolDescription"`         // required
	FailurePolicy   string                `json:"failurePolicy,omitempty"` // default "surface-to-llm"
	Disabled        bool                  `json:"-"`                       // set true in Init when DISABLE_RABBITMQ=true
}

type inspectInput struct {
	Connection string         `json:"connection"`
	Op         string         `json:"op"`
	Args       map[string]any `json:"args,omitempty"`
}

var opToPath = map[string]string{
	"overview":         "/api/overview",
	"list_queues":      "/api/queues",
	"queue_stats":      "/api/queues/%s/%s", // vhost, name
	"list_exchanges":   "/api/exchanges",
	"list_bindings":    "/api/bindings",
	"list_consumers":   "/api/consumers",
	"list_connections": "/api/connections",
	"list_channels":    "/api/channels",
	"dlq_summary":      "/api/queues", // filtered after fetch
}

// Tool implements the tool/rabbitmq-management node.
type Tool struct {
	node.BaseTool
	cfg    Config
	client *http.Client
}

func (t *Tool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type:        NodeType,
		Role:        node.RoleTool,
		Description: "Read-only RabbitMQ Management API inspection (queues, exchanges, DLQ summaries).",
		OutputPorts: []node.PortSpec{{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne}},
	}
}

// Init resolves connection credentials from env vars and builds the HTTP
// client. Test seam: if a case pre-sets t.client (non-nil) and/or a
// connection's BaseURL/Username/Password, those values are preserved and
// env resolution is skipped for the already-set fields.
func (t *Tool) Init(_ context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	if t.client == nil {
		// Safety-net ceiling only; the effective per-request bound is the
		// connection's requestTimeoutMs (see Invoke), default 10s.
		t.client = &http.Client{Timeout: 60 * time.Second}
	}
	if v := os.Getenv("DISABLE_RABBITMQ"); v == "true" || v == "1" {
		t.cfg.Disabled = true
	}
	for name, c := range t.cfg.Connections {
		if c.Vhost == "" {
			c.Vhost = "/"
		}
		if c.BaseURL == "" {
			scheme := envOr("RABBITMQ_MGMT_SCHEME", "https")
			port := envOr("RABBITMQ_MGMT_PORT", "15671")
			if host := os.Getenv("RABBITMQ_HOST"); host != "" {
				c.BaseURL = fmt.Sprintf("%s://%s:%s", scheme, host, port)
			}
		}
		if c.Username == "" {
			c.Username = os.Getenv("RABBITMQ_USER")
		}
		if c.Password == "" {
			if pw, err := env.Secret("RABBITMQ_PASSWORD"); err == nil && pw.IsSet() {
				c.Password = pw.Reveal()
			}
		}
		t.cfg.Connections[name] = c
	}
	return nil
}

// envOr returns the env var's value, or def if unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (t *Tool) Close(_ context.Context) error { return nil }

// ToolSpec returns the LLM-facing contract for this tool.
func (t *Tool) ToolSpec() node.ToolSpec {
	name := t.cfg.ToolName
	if name == "" {
		name = "rabbitmq_inspect"
	}
	desc := t.cfg.ToolDescription
	if desc == "" {
		desc = "Read-only RabbitMQ Management API. Primary exchanges in the batch processing cluster follow X12BatchProcessing.<Event> and routing keys follow {BATCH_TYPE}.{CONTENT_TYPE}.{SENDER}. Use this for queue-depth, DLQ-count, and exchange-binding questions."
	}
	return node.ToolSpec{
		Name:        name,
		Description: desc,
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"connection":{"type":"string"},
				"op":{"type":"string","enum":["overview","list_queues","queue_stats","list_exchanges","list_bindings","list_consumers","list_connections","list_channels","dlq_summary"]},
				"args":{"type":"object"}
			},
			"required":["connection","op"]
		}`),
	}
}

// FailurePolicy returns the configured policy, defaulting to surface-to-llm.
func (t *Tool) FailurePolicy() node.FailurePolicy {
	if t.cfg.FailurePolicy == "" {
		return node.FailureSurfaceToLLM
	}
	return node.FailurePolicy(t.cfg.FailurePolicy)
}

// Invoke runs the requested read-only Management API operation.
func (t *Tool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	if t.cfg.Disabled {
		return nil, fmt.Errorf("rabbitmq-management: disabled (DISABLE_RABBITMQ=true)")
	}
	var in inspectInput
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}
	path, ok := opToPath[in.Op]
	if !ok {
		return nil, fmt.Errorf("op %q not allowed (read-only allowlist)", in.Op)
	}
	cc, ok := t.cfg.Connections[in.Connection]
	if !ok {
		return nil, fmt.Errorf("unknown connection %q", in.Connection)
	}

	switch in.Op {
	case "queue_stats":
		name, _ := in.Args["name"].(string)
		if name == "" {
			return nil, fmt.Errorf("queue_stats requires args.name")
		}
		vhost := cc.Vhost
		if vhost == "" {
			vhost = "/"
		}
		path = fmt.Sprintf(path, url.PathEscape(vhost), url.PathEscape(name))
	}

	// Enforce the connection's declared request timeout (default 10s).
	timeoutMs := cc.RequestTimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 10000
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	u := strings.TrimRight(cc.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	if cc.Username != "" {
		req.SetBasicAuth(cc.Username, cc.Password)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mgmt api: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("mgmt api %d: %s", resp.StatusCode, string(body))
	}

	if in.Op == "dlq_summary" {
		return filterDLQs(body)
	}
	return body, nil
}

func filterDLQs(body []byte) ([]byte, error) {
	var queues []map[string]any
	if err := json.Unmarshal(body, &queues); err != nil {
		return nil, err
	}
	dlqs := make([]map[string]any, 0)
	for _, q := range queues {
		name, _ := q["name"].(string)
		lname := strings.ToLower(name)
		if strings.HasSuffix(lname, ".dlq") || strings.Contains(lname, "deadletter") || strings.HasSuffix(lname, "-dlx") {
			dlqs = append(dlqs, q)
		}
	}
	return json.Marshal(dlqs)
}

// Compile-time check that Tool satisfies the node.Tool interface.
var _ node.Tool = (*Tool)(nil)
