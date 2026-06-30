package pgmemory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const maxAppendRetries = 5

// buildLoadQuery returns the SELECT used by Load. Extracted for testability.
// ORDER: turn_index ASC orders turns chronologically; role DESC puts 'user'
// before 'assistant' inside a turn ('u' > 'a' alphabetically), preserving the
// natural conversational order the LLM expects to see.
func buildLoadQuery(table string) string {
	return fmt.Sprintf(`
        SELECT role, content
        FROM %s
        WHERE workflow_id = $1 AND account_id = $2 AND user_id = $3 AND session_id = $4
        ORDER BY turn_index ASC, role DESC
        LIMIT $5
    `, table)
}

// Load returns up to MaxTurns*2 most-recent messages for the key, ordered
// chronologically (user then assistant within each turn).
func (m *pgMemory) Load(ctx context.Context, key node.MemoryKey) ([]node.Message, error) {
	rows, err := m.pool.Query(ctx, buildLoadQuery(m.table()), key.WorkflowID, key.AccountID, key.UserID, key.SessionID, m.cfg.MaxTurns*2)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []node.Message
	for rows.Next() {
		var role string
		var content []byte
		if err := rows.Scan(&role, &content); err != nil {
			return nil, err
		}
		var blocks []node.ContentBlock
		if err := json.Unmarshal(content, &blocks); err != nil {
			return nil, err
		}
		out = append(out, node.Message{Role: node.MessageRole(role), Content: blocks})
	}
	return out, rows.Err()
}

// Append inserts the user/assistant pair atomically. Retries on
// UniqueViolation up to maxAppendRetries (handles concurrent writers).
func (m *pgMemory) Append(ctx context.Context, key node.MemoryKey, t node.Turn) error {
	for attempt := 0; attempt < maxAppendRetries; attempt++ {
		err := m.appendOnce(ctx, key, t)
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			continue
		}
		return err
	}
	return fmt.Errorf("memory/postgres: append failed after %d retries", maxAppendRetries)
}

func (m *pgMemory) appendOnce(ctx context.Context, key node.MemoryKey, t node.Turn) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var nextIdx int
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
        SELECT COALESCE(MAX(turn_index)+1, 0) FROM %s
        WHERE workflow_id = $1 AND account_id = $2 AND user_id = $3 AND session_id = $4
    `, m.table()), key.WorkflowID, key.AccountID, key.UserID, key.SessionID).Scan(&nextIdx); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	for _, msg := range []node.Message{t.User, t.Assistant} {
		content, _ := json.Marshal(msg.Content)
		id := uuid.NewString()
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
            INSERT INTO %s (id, workflow_id, account_id, user_id, session_id, turn_index, role, content)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, m.table()), id, key.WorkflowID, key.AccountID, key.UserID, key.SessionID, nextIdx, msg.Role, content); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
