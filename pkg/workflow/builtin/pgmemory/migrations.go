package pgmemory

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// migration is one schema version's SQL. The {prefix} placeholder is replaced
// with the configured tablePrefix. Append-only across cycles.
type migration struct {
	Version int
	SQL     string
}

var pgMemoryMigrations = []migration{
	{
		Version: 1,
		SQL: `
CREATE TABLE IF NOT EXISTS {prefix}messages (
    id          TEXT         PRIMARY KEY,
    workflow_id TEXT         NOT NULL,
    session_id  TEXT         NOT NULL,
    turn_index  INTEGER      NOT NULL,
    role        TEXT         NOT NULL CHECK (role IN ('user','assistant','system')),
    content     JSONB        NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (workflow_id, session_id, turn_index, role)
);
CREATE INDEX IF NOT EXISTS idx_{prefix}messages_session
    ON {prefix}messages (workflow_id, session_id, turn_index);
CREATE INDEX IF NOT EXISTS idx_{prefix}messages_created
    ON {prefix}messages (created_at);
`,
	},
	{
		Version: 2,
		SQL: `
ALTER TABLE {prefix}messages
    ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT '';

ALTER TABLE {prefix}messages
    DROP CONSTRAINT IF EXISTS {prefix}messages_workflow_id_session_id_turn_index_role_key;

ALTER TABLE {prefix}messages
    ADD CONSTRAINT {prefix}messages_acct_turn_uniq
        UNIQUE (workflow_id, account_id, session_id, turn_index, role);

DROP INDEX IF EXISTS idx_{prefix}messages_session;
CREATE INDEX IF NOT EXISTS idx_{prefix}messages_acct_session
    ON {prefix}messages (workflow_id, account_id, session_id, turn_index);
`,
	},
	{
		Version: 3,
		SQL: `
ALTER TABLE {prefix}messages
    ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT '';

ALTER TABLE {prefix}messages
    DROP CONSTRAINT IF EXISTS {prefix}messages_acct_turn_uniq;

ALTER TABLE {prefix}messages
    ADD CONSTRAINT {prefix}messages_acct_user_turn_uniq
        UNIQUE (workflow_id, account_id, user_id, session_id, turn_index, role);

DROP INDEX IF EXISTS idx_{prefix}messages_acct_session;
CREATE INDEX IF NOT EXISTS idx_{prefix}messages_acct_user_session
    ON {prefix}messages (workflow_id, account_id, user_id, session_id, turn_index);
`,
	},
}

// applyMigrations runs all migrations newer than the current version in one
// transaction.
func (m *pgMemory) applyMigrations(ctx context.Context) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %sschema_versions (
            component  TEXT PRIMARY KEY,
            version    INTEGER NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`, m.cfg.TablePrefix)); err != nil {
		return err
	}

	var current int
	row := tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT version FROM %sschema_versions WHERE component = $1`, m.cfg.TablePrefix),
		"memory/postgres",
	)
	if err := row.Scan(&current); errors.Is(err, pgx.ErrNoRows) {
		current = 0
	} else if err != nil {
		return err
	}

	for _, mig := range pgMemoryMigrations {
		if mig.Version <= current {
			continue
		}
		sql := strings.ReplaceAll(mig.SQL, "{prefix}", m.cfg.TablePrefix)
		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("memory/postgres: apply migration v%d: %w", mig.Version, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
            INSERT INTO %sschema_versions (component, version)
            VALUES ($1, $2)
            ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now()
        `, m.cfg.TablePrefix), "memory/postgres", mig.Version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
