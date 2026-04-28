package sqlitestore

import (
	"context"
	"time"
)

type MonitoringSnapshot struct {
	CapturedAtMS          int64                     `json:"captured_at_ms"`
	ActiveSessionWindowMS int64                     `json:"active_session_window_ms"`
	ReplayLookbackMS      int64                     `json:"replay_lookback_ms"`
	Global                GlobalMonitoringMetrics   `json:"global"`
	Accounts              []AccountMonitoringMetric `json:"accounts"`
	RecentFailures        []LabelCount              `json:"recent_failures"`
}

type GlobalMonitoringMetrics struct {
	EnabledAccounts       int64   `json:"enabled_accounts"`
	RetainedLineages      int64   `json:"retained_lineages"`
	ActiveLineages        int64   `json:"active_lineages"`
	InactiveLineages      int64   `json:"inactive_lineages"`
	ActiveSessions        int64   `json:"active_sessions"`
	ActiveCarriers        int64   `json:"active_carriers"`
	RecentReplays         int64   `json:"recent_replays"`
	RecentTurns           int64   `json:"recent_turns"`
	RecentRoutingFailures int64   `json:"recent_routing_failures"`
	Recent30mTurns        int64   `json:"recent_30m_turns"`
	Recent30mReplays      int64   `json:"recent_30m_replays"`
	ReplayRate30m         float64 `json:"replay_rate_30m"`
	RecentSuccesses       int64   `json:"recent_successes"`
	RecentFailures        int64   `json:"recent_failures"`
	FailureRate           float64 `json:"failure_rate"`
}

type AccountMonitoringMetric struct {
	AccountID        string       `json:"account_id"`
	DisplayName      string       `json:"display_name"`
	DownstreamHost   string       `json:"downstream_host"`
	DownstreamPort   int          `json:"downstream_port"`
	Enabled          bool         `json:"enabled"`
	State            string       `json:"state"`
	CooldownUntilMS  int64        `json:"cooldown_until_ms"`
	CooldownReason   string       `json:"cooldown_reason"`
	LineageStatuses  []LabelCount `json:"lineage_statuses"`
	ActiveSessions   int64        `json:"active_sessions"`
	ActiveCarriers   int64        `json:"active_carriers"`
	CarrierKinds     []LabelCount `json:"carrier_kinds"`
	RecentReplays    int64        `json:"recent_replays"`
	ReplayReasons    []LabelCount `json:"replay_reasons"`
	RecentTurns      int64        `json:"recent_turns"`
	RouteModes       []LabelCount `json:"route_modes"`
	RecentFailures   int64        `json:"recent_failures"`
	FailureReasons   []LabelCount `json:"failure_reasons"`
	RecentSuccesses  int64        `json:"recent_successes"`
	FailureRate      float64      `json:"failure_rate"`
	StatusCodes      []LabelCount `json:"status_codes"`
	Recent30mTurns   int64        `json:"recent_30m_turns"`
	Recent30mReplays int64        `json:"recent_30m_replays"`
	ReplayRate30m    float64      `json:"replay_rate_30m"`
}

type LabelCount struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

func (s *Store) MonitoringSnapshot(ctx context.Context, now time.Time, replayLookback time.Duration) (MonitoringSnapshot, error) {
	if err := s.RefreshAccountCooldowns(ctx, now); err != nil {
		return MonitoringSnapshot{}, err
	}
	if err := s.RefreshLineageStatuses(ctx, now); err != nil {
		return MonitoringSnapshot{}, err
	}
	nowMS := now.UnixMilli()
	activeCutoff := now.Add(-s.activeSessionWindow).UnixMilli()
	replayCutoff := now.Add(-replayLookback).UnixMilli()
	statsCutoff := now.Add(-30 * time.Minute).UnixMilli()
	snapshot := MonitoringSnapshot{
		CapturedAtMS:          nowMS,
		ActiveSessionWindowMS: s.activeSessionWindow.Milliseconds(),
		ReplayLookbackMS:      replayLookback.Milliseconds(),
	}
	accounts, err := s.monitoringAccounts(ctx)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	snapshot.Accounts = accounts
	byAccount := make(map[string]*AccountMonitoringMetric, len(snapshot.Accounts))
	for i := range snapshot.Accounts {
		byAccount[snapshot.Accounts[i].AccountID] = &snapshot.Accounts[i]
	}

	if snapshot.Global.EnabledAccounts, err = s.countInt64(ctx, `SELECT COUNT(*) FROM accounts WHERE enabled = 1`); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.RetainedLineages, err = s.countInt64(ctx, `SELECT COUNT(*) FROM lineage_bindings WHERE retained_until_ms >= ?`, nowMS); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.ActiveSessions, err = s.countInt64(ctx, `SELECT COUNT(DISTINCT lineage_session_id) FROM lineage_bindings WHERE last_seen_at_ms >= ? AND retained_until_ms >= ?`, activeCutoff, nowMS); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.ActiveCarriers, err = s.countInt64(ctx, `SELECT COUNT(*) FROM carrier_index WHERE last_seen_at_ms >= ?`, activeCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.RecentReplays, err = s.countInt64(ctx, `SELECT COUNT(*) FROM replay_events WHERE created_at_ms >= ?`, replayCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.RecentTurns, err = s.countInt64(ctx, `SELECT COUNT(*) FROM turns_meta WHERE created_at_ms >= ?`, replayCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.RecentRoutingFailures, err = s.countInt64(ctx, `SELECT COUNT(*) FROM routing_failures WHERE created_at_ms >= ?`, replayCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.Recent30mTurns, err = s.countInt64(ctx, `SELECT COUNT(*) FROM turns_meta WHERE created_at_ms >= ?`, statsCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.Recent30mReplays, err = s.countInt64(ctx, `SELECT COUNT(*) FROM replay_events WHERE created_at_ms >= ?`, statsCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	snapshot.Global.ReplayRate30m = ratio(snapshot.Global.Recent30mReplays, snapshot.Global.Recent30mTurns)
	if snapshot.Global.RecentSuccesses, err = s.countInt64(ctx, `SELECT COUNT(*) FROM turns_meta WHERE created_at_ms >= ? AND request_status_code BETWEEN 200 AND 399`, statsCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	if snapshot.Global.RecentFailures, err = s.countInt64(ctx, `SELECT COUNT(*) FROM turns_meta WHERE created_at_ms >= ? AND (request_status_code >= 400 OR error_code IS NOT NULL)`, statsCutoff); err != nil {
		return MonitoringSnapshot{}, err
	}
	snapshot.Global.FailureRate = ratio(snapshot.Global.RecentFailures, snapshot.Global.RecentSuccesses+snapshot.Global.RecentFailures)

	lineageStatuses, err := s.groupedCounts(ctx, `SELECT account_id, status, COUNT(*) FROM lineage_bindings WHERE retained_until_ms >= ? GROUP BY account_id, status`, nowMS)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, xs := range lineageStatuses {
		for _, x := range xs {
			switch x.Label {
			case "active":
				snapshot.Global.ActiveLineages += x.Count
			case "inactive":
				snapshot.Global.InactiveLineages += x.Count
			}
		}
		if a := byAccount[accountID]; a != nil {
			a.LineageStatuses = xs
		}
	}

	activeSessions, err := s.singleGroupedCounts(ctx, `SELECT account_id, COUNT(DISTINCT lineage_session_id) FROM lineage_bindings WHERE last_seen_at_ms >= ? AND retained_until_ms >= ? GROUP BY account_id`, activeCutoff, nowMS)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, count := range activeSessions {
		if a := byAccount[accountID]; a != nil {
			a.ActiveSessions = count
		}
	}

	carrierKinds, err := s.groupedCounts(ctx, `SELECT owner_account_id, carrier_kind, COUNT(*) FROM carrier_index WHERE last_seen_at_ms >= ? GROUP BY owner_account_id, carrier_kind`, activeCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, xs := range carrierKinds {
		if a := byAccount[accountID]; a != nil {
			a.CarrierKinds = xs
			for _, x := range xs {
				a.ActiveCarriers += x.Count
			}
		}
	}

	replayReasons, err := s.groupedCounts(ctx, `SELECT COALESCE(new_account_id,''), replay_reason, COUNT(*) FROM replay_events WHERE created_at_ms >= ? GROUP BY COALESCE(new_account_id,''), replay_reason`, replayCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, xs := range replayReasons {
		if a := byAccount[accountID]; a != nil {
			a.ReplayReasons = xs
			for _, x := range xs {
				a.RecentReplays += x.Count
			}
		}
	}

	routeModes, err := s.groupedCounts(ctx, `SELECT COALESCE(account_id,''), route_mode, COUNT(*) FROM turns_meta WHERE created_at_ms >= ? GROUP BY COALESCE(account_id,''), route_mode`, replayCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, xs := range routeModes {
		if a := byAccount[accountID]; a != nil {
			a.RouteModes = xs
			for _, x := range xs {
				a.RecentTurns += x.Count
			}
		}
	}

	statusCodes, err := s.groupedCounts(ctx, `SELECT COALESCE(account_id,''), COALESCE(CAST(request_status_code AS TEXT),'0'), COUNT(*) FROM turns_meta WHERE created_at_ms >= ? GROUP BY COALESCE(account_id,''), COALESCE(CAST(request_status_code AS TEXT),'0')`, statsCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, xs := range statusCodes {
		if a := byAccount[accountID]; a != nil {
			a.StatusCodes = xs
		}
	}

	recentSuccesses, err := s.singleGroupedCounts(ctx, `SELECT COALESCE(account_id,''), COUNT(*) FROM turns_meta WHERE created_at_ms >= ? AND request_status_code BETWEEN 200 AND 399 GROUP BY COALESCE(account_id,'')`, statsCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, count := range recentSuccesses {
		if a := byAccount[accountID]; a != nil {
			a.RecentSuccesses = count
		}
	}
	recentFailures, err := s.singleGroupedCounts(ctx, `SELECT COALESCE(account_id,''), COUNT(*) FROM turns_meta WHERE created_at_ms >= ? AND (request_status_code >= 400 OR error_code IS NOT NULL) GROUP BY COALESCE(account_id,'')`, statsCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, count := range recentFailures {
		if a := byAccount[accountID]; a != nil {
			a.RecentFailures += count
		}
	}
	recent30Turns, err := s.singleGroupedCounts(ctx, `SELECT COALESCE(account_id,''), COUNT(*) FROM turns_meta WHERE created_at_ms >= ? GROUP BY COALESCE(account_id,'')`, statsCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, count := range recent30Turns {
		if a := byAccount[accountID]; a != nil {
			a.Recent30mTurns = count
		}
	}
	recent30Replays, err := s.singleGroupedCounts(ctx, `SELECT COALESCE(new_account_id,''), COUNT(*) FROM replay_events WHERE created_at_ms >= ? GROUP BY COALESCE(new_account_id,'')`, statsCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, count := range recent30Replays {
		if a := byAccount[accountID]; a != nil {
			a.Recent30mReplays = count
		}
	}
	for i := range snapshot.Accounts {
		a := &snapshot.Accounts[i]
		a.FailureRate = ratio(a.RecentFailures, a.RecentSuccesses+a.RecentFailures)
		a.ReplayRate30m = ratio(a.Recent30mReplays, a.Recent30mTurns)
	}

	failureReasons, err := s.groupedCounts(ctx, `SELECT COALESCE(account_id,''), reason_code, COUNT(*) FROM routing_failures WHERE created_at_ms >= ? GROUP BY COALESCE(account_id,''), reason_code`, replayCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	for accountID, xs := range failureReasons {
		if a := byAccount[accountID]; a != nil {
			a.FailureReasons = xs
		}
	}

	snapshot.RecentFailures, err = s.labelCounts(ctx, `SELECT reason_code, COUNT(*) FROM routing_failures WHERE created_at_ms >= ? GROUP BY reason_code ORDER BY reason_code`, replayCutoff)
	if err != nil {
		return MonitoringSnapshot{}, err
	}
	return snapshot, nil
}

func (s *Store) monitoringAccounts(ctx context.Context) ([]AccountMonitoringMetric, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT account_id, display_name, downstream_host, downstream_port, enabled, state, cooldown_until_ms, cooldown_reason FROM accounts ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AccountMonitoringMetric, 0)
	for rows.Next() {
		var row AccountMonitoringMetric
		var enabled int
		if err := rows.Scan(&row.AccountID, &row.DisplayName, &row.DownstreamHost, &row.DownstreamPort, &enabled, &row.State, &row.CooldownUntilMS, &row.CooldownReason); err != nil {
			return nil, err
		}
		row.Enabled = enabled == 1
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) singleGroupedCounts(ctx context.Context, query string, args ...any) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		out[key] = count
	}
	return out, rows.Err()
}

func (s *Store) groupedCounts(ctx context.Context, query string, args ...any) (map[string][]LabelCount, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]LabelCount)
	for rows.Next() {
		var key string
		var label string
		var count int64
		if err := rows.Scan(&key, &label, &count); err != nil {
			return nil, err
		}
		out[key] = append(out[key], LabelCount{Label: label, Count: count})
	}
	return out, rows.Err()
}

func ratio(n, d int64) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / float64(d)
}

func (s *Store) labelCounts(ctx context.Context, query string, args ...any) ([]LabelCount, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]LabelCount, 0)
	for rows.Next() {
		var row LabelCount
		if err := rows.Scan(&row.Label, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
