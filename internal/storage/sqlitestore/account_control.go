package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (s *Store) RefreshAccountCooldowns(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE accounts SET state = 'running', cooldown_until_ms = 0, cooldown_reason = '', updated_at_ms = ? WHERE enabled = 1 AND state = 'cooldown' AND cooldown_until_ms <= ?`, now.UnixMilli(), now.UnixMilli())
	return err
}

func (s *Store) SetAccountEnabled(ctx context.Context, accountID string, enabled bool, now time.Time) error {
	state := "disabled"
	cooldownUntil := int64(0)
	cooldownReason := "manual_disable"
	if enabled {
		state = "running"
		cooldownReason = ""
	}
	res, err := s.db.ExecContext(ctx, `UPDATE accounts SET enabled = ?, state = ?, cooldown_until_ms = ?, cooldown_reason = ?, updated_at_ms = ? WHERE account_id = ?`, boolToInt(enabled), state, cooldownUntil, cooldownReason, now.UnixMilli(), accountID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) CooldownAccount(ctx context.Context, accountID, reason string, until time.Time, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE accounts SET state = 'cooldown', cooldown_until_ms = ?, cooldown_reason = ?, updated_at_ms = ? WHERE account_id = ? AND enabled = 1`, until.UnixMilli(), reason, now.UnixMilli(), accountID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) GetAccount(ctx context.Context, accountID string) (Account, error) {
	var a Account
	var enabled int
	var allowlistJSON string
	if err := s.db.QueryRowContext(ctx, `SELECT account_id, provider_kind, display_name, downstream_host, downstream_port, enabled, state, cooldown_until_ms, cooldown_reason, model_allowlist_json, weight FROM accounts WHERE account_id = ?`, accountID).Scan(
		&a.AccountID, &a.ProviderKind, &a.DisplayName, &a.DownstreamHost, &a.DownstreamPort, &enabled, &a.State, &a.CooldownUntilMS, &a.CooldownReason, &allowlistJSON, &a.Weight,
	); err != nil {
		return Account{}, err
	}
	a.Enabled = enabled == 1
	_ = json.Unmarshal([]byte(allowlistJSON), &a.ModelAllowlist)
	return a, nil
}
