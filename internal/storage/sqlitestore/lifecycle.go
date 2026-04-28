package sqlitestore

import (
	"context"
	"time"
)

func (s *Store) RefreshLineageStatuses(ctx context.Context, now time.Time) error {
	activeCutoff := now.Add(-s.activeSessionWindow).UnixMilli()
	nowMS := now.UnixMilli()
	if _, err := s.db.ExecContext(ctx, `UPDATE lineage_bindings SET status = CASE WHEN last_seen_at_ms >= ? THEN 'active' ELSE 'inactive' END`, activeCutoff); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM lineage_bindings WHERE retained_until_ms < ?`, nowMS)
	return err
}

func (s *Store) UpsertLineageBinding(ctx context.Context, lineageSessionID string, account Account, turnID string, now time.Time) error {
	nowMS := now.UnixMilli()
	status := "active"
	retainedUntil := now.Add(s.inactiveSessionRetain).UnixMilli()
	_, err := s.db.ExecContext(ctx, `INSERT INTO lineage_bindings (
		lineage_session_id, account_id, downstream_host, downstream_port, status,
		first_seen_at_ms, last_seen_at_ms, retained_until_ms, first_turn_id, last_turn_id
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(lineage_session_id) DO UPDATE SET
		account_id = excluded.account_id,
		downstream_host = excluded.downstream_host,
		downstream_port = excluded.downstream_port,
		status = excluded.status,
		last_seen_at_ms = excluded.last_seen_at_ms,
		retained_until_ms = excluded.retained_until_ms,
		last_turn_id = excluded.last_turn_id`,
		lineageSessionID, account.AccountID, account.DownstreamHost, account.DownstreamPort, status,
		nowMS, nowMS, retainedUntil, turnID, turnID,
	)
	return err
}
