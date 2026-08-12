package postgresquery

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestPostgresQuery_SelectReturnsRows(t *testing.T) {
	mock, _ := pgxmock.NewPool()
	defer mock.Close()
	mock.ExpectQuery(`SELECT name FROM payers WHERE id = \$1`).
		WithArgs(int64(42)).
		WillReturnRows(pgxmock.NewRows([]string{"name"}).AddRow("CareMark"))

	pools := map[string]Pool{"clearinghouse": mock}
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"clearinghouse": {MaxRowsDefault: 200}}}, pools)

	args, _ := json.Marshal(map[string]any{
		"database": "clearinghouse",
		"sql":      "SELECT name FROM payers WHERE id = $1",
		"params":   []any{42},
	})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	rows := resp["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["name"] != "CareMark" {
		t.Errorf("rows = %v", rows)
	}
	if got := resp["rowCount"]; got != float64(1) {
		t.Errorf("rowCount = %v", got)
	}
}

func TestPostgresQuery_NonSelectRejected(t *testing.T) {
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {MaxRowsDefault: 100}}}, nil)
	args, _ := json.Marshal(map[string]any{
		"database": "prod",
		"sql":      "UPDATE accounts SET active=false WHERE id=1",
	})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "select") {
		t.Fatalf("expected non-select rejection, got %v", err)
	}
}

func TestPostgresQuery_WithSelectAllowed(t *testing.T) {
	mock, _ := pgxmock.NewPool()
	defer mock.Close()
	mock.ExpectQuery(`WITH active AS .* SELECT \*`).
		WillReturnRows(pgxmock.NewRows([]string{"x"}).AddRow(1))

	pools := map[string]Pool{"prod": mock}
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {MaxRowsDefault: 100}}}, pools)
	args, _ := json.Marshal(map[string]any{
		"database": "prod",
		"sql":      "WITH active AS (SELECT * FROM accounts WHERE active=true) SELECT * FROM active",
	})
	if _, err := tool.Invoke(context.Background(), args); err != nil {
		t.Fatalf("WITH...SELECT should be allowed, got %v", err)
	}
}

func TestPostgresQuery_UnknownConnectionRejected(t *testing.T) {
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {}}}, nil)
	args, _ := json.Marshal(map[string]any{"database": "nope", "sql": "SELECT 1"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected unknown-connection rejection, got %v", err)
	}
}

func TestPostgresQuery_MaxRowsCap(t *testing.T) {
	mock, _ := pgxmock.NewPool()
	defer mock.Close()
	rows := pgxmock.NewRows([]string{"x"})
	for i := 0; i < 10; i++ {
		rows.AddRow(i)
	}
	mock.ExpectQuery(`SELECT x FROM t`).WillReturnRows(rows)

	pools := map[string]Pool{"prod": mock}
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {MaxRowsDefault: 3}}}, pools)
	args, _ := json.Marshal(map[string]any{"database": "prod", "sql": "SELECT x FROM t"})
	out, _ := tool.Invoke(context.Background(), args)
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if got := len(resp["rows"].([]any)); got > 3 {
		t.Errorf("got %d rows, want ≤3", got)
	}
}

func TestPostgresQuery_ParamNormalization(t *testing.T) {
	mock, _ := pgxmock.NewPool()
	defer mock.Close()
	// JSON-decoded whole-number float64 params must be normalized to int64
	// before hitting the driver, or pgxmock's WithArgs won't match.
	mock.ExpectQuery(`SELECT name FROM payers WHERE id = \$1 AND active = \$2`).
		WithArgs(int64(7), true).
		WillReturnRows(pgxmock.NewRows([]string{"name"}).AddRow("Acme"))

	pools := map[string]Pool{"prod": mock}
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {MaxRowsDefault: 100}}}, pools)
	args, _ := json.Marshal(map[string]any{
		"database": "prod",
		"sql":      "SELECT name FROM payers WHERE id = $1 AND active = $2",
		"params":   []any{7, true},
	})
	if _, err := tool.Invoke(context.Background(), args); err != nil {
		t.Fatalf("param normalization should let query match, got %v", err)
	}
}

func TestFactoryRequiresToolDescription(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{"connections":{"prod":{"host":"h","user":"u","passwordEnv":"DBPASSWORD","database":"prod"}}}`))
	if err == nil {
		t.Fatal("expected error when toolDescription missing")
	}
}

func TestToolSpecUsesConfiguredName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolName":"postgres_query","toolDescription":"d","connections":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "postgres_query" {
		t.Fatalf("ToolSpec().Name = %q", got)
	}
}

// Helper: bypass Init to inject pools directly.
func newToolForTest(cfg Config, pools map[string]Pool) *Tool {
	return &Tool{cfg: cfg, pools: pools}
}

func TestBuildDSNDefaultsToPrefer(t *testing.T) {
	dsn, err := buildDSN(ConnConfig{Host: "db.example.com", User: "u", Database: "prod"}, "pw")
	if err != nil {
		t.Fatal(err)
	}
	want := "postgres://u:pw@db.example.com:5433/prod?sslmode=prefer"
	if dsn != want {
		t.Fatalf("dsn = %q, want %q", dsn, want)
	}
}

func TestBuildDSNSSLModeDisableForSidecar(t *testing.T) {
	dsn, err := buildDSN(ConnConfig{Host: "localhost", Port: "5433", User: "u", Database: "prod", SSLMode: "disable"}, "pw")
	if err != nil {
		t.Fatal(err)
	}
	want := "postgres://u:pw@localhost:5433/prod?sslmode=disable"
	if dsn != want {
		t.Fatalf("dsn = %q, want %q", dsn, want)
	}
}

func TestBuildDSNRejectsInvalidSSLMode(t *testing.T) {
	if _, err := buildDSN(ConnConfig{Host: "h", User: "u", Database: "d", SSLMode: "yes-please"}, "pw"); err == nil {
		t.Fatal("expected error for invalid sslMode")
	}
}
