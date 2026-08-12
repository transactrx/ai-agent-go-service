// Package postgresquery is the tool/postgres-query node: read-only
// SELECT against named, pre-configured connections.
package postgresquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Pool is the subset of pgxpool.Pool the tool uses. *pgxpool.Pool
// satisfies it; pgxmock.PgxPoolIface also satisfies it.
type Pool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// validSSLModes is the libpq sslmode allowlist for ConnConfig.SSLMode.
var validSSLModes = map[string]bool{
	"disable": true, "allow": true, "prefer": true,
	"require": true, "verify-ca": true, "verify-full": true,
}

// buildDSN assembles the pool DSN for one connection. Port defaults to
// "5433" and sslMode to "prefer" — the libpq default and the TransactRx
// platform convention (powerlineClaimSearchApi): TLS is negotiated with
// hosts that offer it and falls back to plaintext for localhost pgbouncer
// sidecars, which have no TLS listener. An sslMode outside the libpq set is
// a configuration error surfaced at Init.
func buildDSN(cc ConnConfig, password string) (string, error) {
	port := cc.Port
	if port == "" {
		port = "5433"
	}
	sslMode := cc.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	if !validSSLModes[sslMode] {
		return "", fmt.Errorf("invalid sslMode %q (allowed: disable, allow, prefer, require, verify-ca, verify-full)", sslMode)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		url.QueryEscape(cc.User), url.QueryEscape(password), cc.Host, port, cc.Database, sslMode), nil
}

// ConnConfig holds per-connection settings.
type ConnConfig struct {
	Host               string `json:"host"`
	Port               string `json:"port,omitempty"` // default "5433"
	Database           string `json:"database"`
	User               string `json:"user"`
	PasswordEnv        string `json:"passwordEnv"` // env VAR NAME, resolved via env.Secret
	MaxRowsDefault     int    `json:"maxRowsDefault,omitempty"`
	StatementTimeoutMs int    `json:"statementTimeoutMs,omitempty"`
	// SSLMode sets libpq sslmode for the connection (default "prefer", the
	// libpq default: negotiates TLS when the server offers it, plaintext
	// otherwise — e.g. a localhost pgbouncer sidecar, which has no TLS
	// listener and holds its own upstream TLS). Set "require" or stricter
	// when the connection leaves the task.
	SSLMode string `json:"sslMode,omitempty"`
}

// Config is decoded from the workflow-JSON config block.
type Config struct {
	Connections     map[string]ConnConfig `json:"connections"`
	ToolName        string                `json:"toolName,omitempty"`      // default "postgres_query"
	ToolDescription string                `json:"toolDescription"`         // required
	FailurePolicy   string                `json:"failurePolicy,omitempty"` // default "surface-to-llm"
}

type queryInput struct {
	Database string `json:"database"`
	SQL      string `json:"sql"`
	Params   []any  `json:"params,omitempty"`
}

type queryOutput struct {
	Database string           `json:"database"`
	RowCount int              `json:"rowCount"`
	Rows     []map[string]any `json:"rows"`
	// Truncated marks that maxRows bit — more rows matched than were returned,
	// so the LLM should add a tighter filter/LIMIT rather than treat rowCount
	// as the true total.
	Truncated bool `json:"truncated,omitempty"`
}

// Tool implements the tool/postgres-query node.
type Tool struct {
	node.BaseTool
	cfg   Config
	pools map[string]Pool
	owned []*pgxpool.Pool // closed in Close()
}

func (t *Tool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type:        NodeType,
		Role:        node.RoleTool,
		Description: "Read-only SELECT against named Postgres connections.",
		OutputPorts: []node.PortSpec{{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne}},
	}
}

func (t *Tool) Init(ctx context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	if t.pools != nil { // test path: pools pre-injected
		return nil
	}
	t.pools = map[string]Pool{}
	for name, cc := range t.cfg.Connections {
		pw, err := env.Secret(cc.PasswordEnv)
		if err != nil {
			return fmt.Errorf("postgres-query %s: %w", name, err)
		}
		dsn, err := buildDSN(cc, pw.Reveal())
		if err != nil {
			return fmt.Errorf("postgres-query %s: %w", name, err)
		}
		pcfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return fmt.Errorf("postgres-query %s: parse config: %w", name, err)
		}
		// Server-side statement timeout — the config field is the contract the
		// workflow declares; without this a pg_sleep() or unindexed scan holds
		// a read-replica connection for the whole agent turn.
		//
		// Applied via SET after connect, NOT as a startup parameter
		// (RuntimeParams): the platform's pgbouncer sidecars only allow
		// extra_float_digits through ignore_startup_parameters and reject any
		// other startup parameter with "FATAL: unsupported startup parameter".
		// The sidecars run pool_mode=session with server_reset_query=DISCARD ALL,
		// so a per-connection SET is safe and is reset on release.
		if cc.StatementTimeoutMs > 0 {
			stmt := fmt.Sprintf("SET statement_timeout = %d", cc.StatementTimeoutMs)
			pcfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
				_, err := conn.Exec(ctx, stmt)
				return err
			}
		}
		pool, err := pgxpool.NewWithConfig(ctx, pcfg)
		if err != nil {
			return fmt.Errorf("postgres-query %s: pool: %w", name, err)
		}
		t.pools[name] = pool
		t.owned = append(t.owned, pool)
	}
	return nil
}

func (t *Tool) Close(_ context.Context) error {
	for _, p := range t.owned {
		p.Close()
	}
	return nil
}

// ToolSpec returns the LLM-facing contract for this tool.
func (t *Tool) ToolSpec() node.ToolSpec {
	name := t.cfg.ToolName
	if name == "" {
		name = "postgres_query"
	}
	return node.ToolSpec{Name: name, Description: t.cfg.ToolDescription,
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"database":{"type":"string","description":"Connection name (e.g. 'clearinghouse' or 'prod')."},
				"sql":{"type":"string","description":"SELECT statement; placeholders use $1, $2, etc."},
				"params":{"type":"array","items":{},"description":"Positional parameters for the SQL placeholders."}
			},
			"required":["database","sql"]
		}`)}
}

// FailurePolicy returns the configured policy, defaulting to surface-to-llm.
func (t *Tool) FailurePolicy() node.FailurePolicy {
	if t.cfg.FailurePolicy == "" {
		return node.FailureSurfaceToLLM
	}
	return node.FailurePolicy(t.cfg.FailurePolicy)
}

// Invoke runs the SQL query and returns the rows as JSON.
func (t *Tool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var in queryInput
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}
	if !isSelect(in.SQL) {
		return nil, fmt.Errorf("postgres-query refuses non-SELECT statements (this is read-only): only SELECT or WITH...SELECT allowed")
	}
	cc, ok := t.cfg.Connections[in.Database]
	if !ok {
		return nil, fmt.Errorf("unknown connection %q (allowed: %v)", in.Database, mapKeys(t.cfg.Connections))
	}
	pool, ok := t.pools[in.Database]
	if !ok {
		return nil, fmt.Errorf("connection %q has no pool wired in main.go", in.Database)
	}
	maxRows := cc.MaxRowsDefault
	if maxRows <= 0 {
		maxRows = 200
	}

	// Client-side bound mirroring the server statement_timeout (plus grace for
	// network/rows transfer), so a hung connection can't eat the turn budget.
	if cc.StatementTimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cc.StatementTimeoutMs)*time.Millisecond+5*time.Second)
		defer cancel()
	}

	normalizedParams := normalizeParams(in.Params)
	rows, err := pool.Query(ctx, in.SQL, normalizedParams...)
	if err != nil {
		return nil, classifyPgError(err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	out := queryOutput{Database: in.Database, Rows: []map[string]any{}}
	for rows.Next() {
		if out.RowCount >= maxRows {
			out.Truncated = true // more rows matched than the cap allows
			break
		}
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[string(c.Name)] = vals[i]
		}
		out.Rows = append(out.Rows, m)
		out.RowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, classifyPgError(err)
	}
	return json.Marshal(out)
}

// classifyPgError wraps a query error as a node.ToolError so the engine's
// retry helper can act on it without string-parsing. A timeout /
// context-deadline is transient (a retry with a narrower query may succeed);
// everything else surfaces to the LLM unclassified.
func classifyPgError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &node.ToolError{Code: "timeout", Retryable: false,
			Cause: fmt.Errorf("query timed out (statement_timeout) — narrow the filter or add a tighter LIMIT: %w", err)}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "57014" { // query_canceled (statement_timeout)
		return &node.ToolError{Code: "timeout", Retryable: false,
			Cause: fmt.Errorf("query canceled by statement_timeout — narrow the filter or add a tighter LIMIT: %w", err)}
	}
	return fmt.Errorf("query: %w", err)
}

// isSelect returns true iff the statement is a SELECT or WITH...SELECT.
// False positives won't cause safety issues since the RO pool also rejects
// writes, but they keep the LLM honest about intent.
func isSelect(sql string) bool {
	s := strings.ToLower(strings.TrimSpace(sql))
	if strings.HasPrefix(s, "select") {
		return true
	}
	if strings.HasPrefix(s, "with ") && strings.Contains(s, "select") {
		return true
	}
	return false
}

// normalizeParams converts JSON-decoded float64 whole numbers to int64 so the
// pgx driver sends the correct OID and pgxmock expectations match. Fractional
// floats are left as float64.
func normalizeParams(params []any) []any {
	out := make([]any, len(params))
	for i, p := range params {
		if f, ok := p.(float64); ok && f == float64(int64(f)) {
			out[i] = int64(f)
		} else {
			out[i] = p
		}
	}
	return out
}

func mapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Compile-time check that Tool satisfies the node.Tool interface.
var _ node.Tool = (*Tool)(nil)
