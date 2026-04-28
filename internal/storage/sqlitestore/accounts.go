package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type Account struct {
	AccountID      string   `json:"account_id"`
	ProviderKind   string   `json:"provider_kind"`
	DisplayName    string   `json:"display_name"`
	DownstreamHost string   `json:"downstream_host"`
	DownstreamPort int      `json:"downstream_port"`
	Enabled        bool     `json:"enabled"`
	State          string   `json:"state"`
	ModelAllowlist []string `json:"model_allowlist"`
	Weight         int      `json:"weight"`
}

func (s *Store) UpsertAccounts(ctx context.Context, accounts []Account, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt := `INSERT INTO accounts (
		account_id, provider_kind, display_name, downstream_host, downstream_port,
		enabled, state, model_allowlist_json, weight, created_at_ms, updated_at_ms
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(account_id) DO UPDATE SET
		provider_kind = excluded.provider_kind,
		display_name = excluded.display_name,
		downstream_host = excluded.downstream_host,
		downstream_port = excluded.downstream_port,
		enabled = excluded.enabled,
		state = excluded.state,
		model_allowlist_json = excluded.model_allowlist_json,
		weight = excluded.weight,
		updated_at_ms = excluded.updated_at_ms`
	for _, account := range accounts {
		allowlist, err := json.Marshal(account.ModelAllowlist)
		if err != nil {
			return fmt.Errorf("marshal model allowlist: %w", err)
		}
		weight := account.Weight
		if weight <= 0 {
			weight = 1
		}
		if _, err := tx.ExecContext(ctx, stmt,
			account.AccountID,
			defaultIfEmpty(account.ProviderKind, "copilot-api"),
			defaultIfEmpty(account.DisplayName, account.AccountID),
			account.DownstreamHost,
			account.DownstreamPort,
			boolToInt(account.Enabled),
			defaultIfEmpty(account.State, "running"),
			string(allowlist),
			weight,
			now.UnixMilli(),
			now.UnixMilli(),
		); err != nil {
			return fmt.Errorf("upsert account %s: %w", account.AccountID, err)
		}
	}
	return tx.Commit()
}

func (s *Store) SelectLeastActiveAccount(ctx context.Context, model string, now time.Time) (Account, error) {
	if err := s.RefreshLineageStatuses(ctx, now); err != nil {
		return Account{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT account_id, provider_kind, display_name, downstream_host, downstream_port, enabled, state, model_allowlist_json, weight FROM accounts WHERE enabled = 1 AND state = 'running'`)
	if err != nil {
		return Account{}, err
	}
	defer rows.Close()
	counts, err := s.activeSessionCounts(ctx, now)
	if err != nil {
		return Account{}, err
	}
	candidates := make([]struct {
		account Account
		count   int64
	}, 0)
	for rows.Next() {
		var a Account
		var enabled int
		var allowlistJSON string
		if err := rows.Scan(&a.AccountID, &a.ProviderKind, &a.DisplayName, &a.DownstreamHost, &a.DownstreamPort, &enabled, &a.State, &allowlistJSON, &a.Weight); err != nil {
			return Account{}, err
		}
		a.Enabled = enabled == 1
		if err := json.Unmarshal([]byte(allowlistJSON), &a.ModelAllowlist); err != nil {
			return Account{}, fmt.Errorf("decode model allowlist: %w", err)
		}
		if len(a.ModelAllowlist) > 0 && model != "" && !contains(a.ModelAllowlist, model) {
			continue
		}
		candidates = append(candidates, struct {
			account Account
			count   int64
		}{account: a, count: counts[a.AccountID]})
	}
	if err := rows.Err(); err != nil {
		return Account{}, err
	}
	if len(candidates) == 0 {
		return Account{}, sql.ErrNoRows
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].count != candidates[j].count {
			return candidates[i].count < candidates[j].count
		}
		return candidates[i].account.AccountID < candidates[j].account.AccountID
	})
	return candidates[0].account, nil
}

func (s *Store) activeSessionCounts(ctx context.Context, now time.Time) (map[string]int64, error) {
	cutoff := now.Add(-s.activeSessionWindow).UnixMilli()
	rows, err := s.db.QueryContext(ctx, `SELECT account_id, COUNT(DISTINCT lineage_session_id) FROM lineage_bindings WHERE last_seen_at_ms >= ? AND retained_until_ms >= ? GROUP BY account_id`, cutoff, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64)
	for rows.Next() {
		var accountID string
		var count int64
		if err := rows.Scan(&accountID, &count); err != nil {
			return nil, err
		}
		counts[accountID] = count
	}
	return counts, rows.Err()
}

func contains(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}

func defaultIfEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
