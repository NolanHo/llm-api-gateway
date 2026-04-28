package sqlitestore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db                    *sql.DB
	activeSessionWindow   time.Duration
	inactiveSessionRetain time.Duration
}

func Open(ctx context.Context, path string, activeWindow, inactiveRetain time.Duration) (*Store, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on&_busy_timeout=5000", path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	store := &Store{db: db, activeSessionWindow: activeWindow, inactiveSessionRetain: inactiveRetain}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) Close(_ context.Context) error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			account_id TEXT PRIMARY KEY,
			provider_kind TEXT NOT NULL,
			display_name TEXT NOT NULL,
			downstream_host TEXT NOT NULL,
			downstream_port INTEGER NOT NULL,
			enabled INTEGER NOT NULL,
			state TEXT NOT NULL,
			cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
			cooldown_reason TEXT NOT NULL DEFAULT '',
			model_allowlist_json TEXT NOT NULL,
			weight INTEGER NOT NULL DEFAULT 1,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_accounts_host_port ON accounts(downstream_host, downstream_port);`,
		`CREATE TABLE IF NOT EXISTS lineage_bindings (
			lineage_session_id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			downstream_host TEXT NOT NULL,
			downstream_port INTEGER NOT NULL,
			status TEXT NOT NULL,
			first_seen_at_ms INTEGER NOT NULL,
			last_seen_at_ms INTEGER NOT NULL,
			retained_until_ms INTEGER NOT NULL,
			first_turn_id TEXT NOT NULL,
			last_turn_id TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_lineage_bindings_account_status ON lineage_bindings(account_id, status, last_seen_at_ms);`,
		`CREATE INDEX IF NOT EXISTS idx_lineage_bindings_retained_until ON lineage_bindings(retained_until_ms);`,
		`CREATE TABLE IF NOT EXISTS carrier_index (
			carrier_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
			lineage_session_id TEXT NOT NULL,
			carrier_kind TEXT NOT NULL,
			carrier_id_hmac TEXT,
			carrier_blob_hmac TEXT,
			owner_account_id TEXT NOT NULL,
			owner_host TEXT NOT NULL,
			owner_port INTEGER NOT NULL,
			first_seen_turn_id TEXT NOT NULL,
			last_seen_turn_id TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			last_seen_at_ms INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_carrier_index_id ON carrier_index(carrier_kind, carrier_id_hmac);`,
		`CREATE INDEX IF NOT EXISTS idx_carrier_index_blob ON carrier_index(carrier_kind, carrier_blob_hmac);`,
		`CREATE INDEX IF NOT EXISTS idx_carrier_index_owner ON carrier_index(owner_account_id, owner_host, owner_port);`,
		`CREATE INDEX IF NOT EXISTS idx_carrier_index_lineage ON carrier_index(lineage_session_id, last_seen_at_ms);`,
		`CREATE TABLE IF NOT EXISTS turns_meta (
			turn_id TEXT PRIMARY KEY,
			parent_turn_id TEXT,
			replay_parent_turn_id TEXT,
			lineage_session_id TEXT NOT NULL,
			lineage_generation INTEGER NOT NULL DEFAULT 0,
			route_mode TEXT NOT NULL,
			surface TEXT NOT NULL,
			model TEXT,
			account_id TEXT,
			downstream_host TEXT,
			downstream_port INTEGER,
			has_real_carrier INTEGER NOT NULL DEFAULT 0,
			carrier_kinds TEXT,
			carrier_removed INTEGER NOT NULL DEFAULT 0,
			removed_carrier_kinds TEXT,
			removed_carrier_count INTEGER NOT NULL DEFAULT 0,
			weak_history_fingerprint TEXT,
			request_status_code INTEGER,
			error_code TEXT,
			error_message TEXT,
			duckdb_turn_pk TEXT,
			otel_trace_id TEXT,
			otel_span_id TEXT,
			created_at_ms INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_turns_meta_lineage ON turns_meta(lineage_session_id, created_at_ms);`,
		`CREATE INDEX IF NOT EXISTS idx_turns_meta_parent ON turns_meta(parent_turn_id, replay_parent_turn_id);`,
		`CREATE INDEX IF NOT EXISTS idx_turns_meta_fingerprint ON turns_meta(weak_history_fingerprint);`,
		`CREATE INDEX IF NOT EXISTS idx_turns_meta_account_status ON turns_meta(account_id, request_status_code, created_at_ms);`,
		`CREATE TABLE IF NOT EXISTS replay_events (
			replay_event_id TEXT PRIMARY KEY,
			from_turn_id TEXT NOT NULL,
			to_turn_id TEXT NOT NULL,
			lineage_session_id TEXT NOT NULL,
			old_account_id TEXT,
			new_account_id TEXT,
			replay_reason TEXT NOT NULL,
			removed_carrier_kinds TEXT,
			removed_carrier_count INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_replay_events_lineage ON replay_events(lineage_session_id, created_at_ms);`,
		`CREATE TABLE IF NOT EXISTS routing_failures (
			failure_id TEXT PRIMARY KEY,
			turn_id TEXT NOT NULL,
			lineage_session_id TEXT NOT NULL,
			account_id TEXT,
			reason_code TEXT NOT NULL,
			reason_detail TEXT,
			http_status INTEGER,
			created_at_ms INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_routing_failures_lineage ON routing_failures(lineage_session_id, created_at_ms);`,
		`CREATE INDEX IF NOT EXISTS idx_routing_failures_reason ON routing_failures(reason_code, created_at_ms);`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec migration: %w", err)
		}
	}
	if err := s.ensureColumn(ctx, "accounts", "cooldown_until_ms", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "accounts", "cooldown_reason", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_accounts_state ON accounts(enabled, state, cooldown_until_ms)`); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+definition)
	return err
}
