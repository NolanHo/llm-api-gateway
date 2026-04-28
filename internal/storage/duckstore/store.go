package duckstore

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
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
		`CREATE TABLE IF NOT EXISTS turns (
			turn_pk VARCHAR,
			turn_id VARCHAR,
			parent_turn_id VARCHAR,
			replay_parent_turn_id VARCHAR,
			lineage_session_id VARCHAR,
			lineage_generation INTEGER,
			route_mode VARCHAR,
			surface VARCHAR,
			model VARCHAR,
			account_id VARCHAR,
			downstream_host VARCHAR,
			downstream_port INTEGER,
			has_real_carrier BOOLEAN,
			carrier_kinds VARCHAR,
			carrier_removed BOOLEAN,
			removed_carrier_kinds VARCHAR,
			removed_carrier_count INTEGER,
			stream_state VARCHAR,
			finish_reason VARCHAR,
			request_token_est INTEGER,
			response_token_est INTEGER,
			created_at TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS turn_documents (
			turn_pk VARCHAR,
			turn_id VARCHAR,
			lineage_session_id VARCHAR,
			route_mode VARCHAR,
			effective_request_items_json JSON,
			response_items_json JSON,
			effective_conversation_text JSON,
			created_at TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS turn_items (
			turn_pk VARCHAR,
			turn_id VARCHAR,
			lineage_session_id VARCHAR,
			item_seq INTEGER,
			phase VARCHAR,
			role VARCHAR,
			item_kind VARCHAR,
			text_content VARCHAR,
			tool_name VARCHAR,
			tool_args_json JSON,
			tool_result_json JSON,
			item_json JSON,
			created_at TIMESTAMP
		);`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec migration: %w", err)
		}
	}
	return nil
}
