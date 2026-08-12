package postgresquery

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// TestPostgresQuery_TruncationFlagged proves rowCount capping sets truncated.
func TestPostgresQuery_TruncationFlagged(t *testing.T) {
	mock, _ := pgxmock.NewPool()
	defer mock.Close()
	rows := pgxmock.NewRows([]string{"id"})
	for i := 0; i < 6; i++ {
		rows.AddRow(int64(i))
	}
	mock.ExpectQuery(`SELECT id FROM t`).WillReturnRows(rows)

	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {MaxRowsDefault: 3}}},
		map[string]Pool{"prod": mock})
	args, _ := json.Marshal(map[string]any{"database": "prod", "sql": "SELECT id FROM t"})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if got := resp["rowCount"]; got != float64(3) {
		t.Fatalf("rowCount = %v, want capped 3", got)
	}
	if resp["truncated"] != true {
		t.Fatalf("truncated flag missing: %v", resp)
	}
}

// TestPostgresQuery_StatementTimeoutClassified proves a 57014 (statement_timeout
// cancel) surfaces as a typed timeout ToolError, not a bare string error.
func TestPostgresQuery_StatementTimeoutClassified(t *testing.T) {
	mock, _ := pgxmock.NewPool()
	defer mock.Close()
	mock.ExpectQuery(`SELECT pg_sleep`).
		WillReturnError(&pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"})

	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"prod": {MaxRowsDefault: 100}}},
		map[string]Pool{"prod": mock})
	args, _ := json.Marshal(map[string]any{"database": "prod", "sql": "SELECT pg_sleep(600)"})
	_, err := tool.Invoke(context.Background(), args)

	var te *node.ToolError
	if !errors.As(err, &te) || te.Code != "timeout" {
		t.Fatalf("want timeout ToolError, got %v", err)
	}
}
