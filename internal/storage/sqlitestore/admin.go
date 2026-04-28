package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type AccountOverview struct {
	AccountID          string        `json:"account_id"`
	DisplayName        string        `json:"display_name"`
	DownstreamHost     string        `json:"downstream_host"`
	DownstreamPort     int           `json:"downstream_port"`
	ActiveSessionCount int64         `json:"active_session_count"`
	ActiveCarrierCount int64         `json:"active_carrier_count"`
	RecentReplayCount  int64         `json:"recent_replay_count"`
	RecentTurns        []TurnSummary `json:"recent_turns,omitempty"`
}

type TurnSummary struct {
	TurnID              string `json:"turn_id"`
	LineageSessionID    string `json:"lineage_session_id"`
	RouteMode           string `json:"route_mode"`
	ReasonCode          string `json:"reason_code,omitempty"`
	CarrierKinds        string `json:"carrier_kinds,omitempty"`
	RemovedCarrierKinds string `json:"removed_carrier_kinds,omitempty"`
	RequestStatusCode   int    `json:"request_status_code,omitempty"`
	CreatedAtMS         int64  `json:"created_at_ms"`
}

type LineageDetail struct {
	LineageSessionID string               `json:"lineage_session_id"`
	Binding          *LineageBindingView  `json:"binding,omitempty"`
	Carriers         []CarrierView        `json:"carriers"`
	Turns            []TurnSummary        `json:"turns"`
	ReplayEvents     []ReplayEventView    `json:"replay_events"`
	Failures         []RoutingFailureView `json:"failures"`
}

type LineageBindingView struct {
	LineageSessionID string `json:"lineage_session_id"`
	AccountID        string `json:"account_id"`
	DownstreamHost   string `json:"downstream_host"`
	DownstreamPort   int    `json:"downstream_port"`
	Status           string `json:"status"`
	FirstSeenAtMS    int64  `json:"first_seen_at_ms"`
	LastSeenAtMS     int64  `json:"last_seen_at_ms"`
	RetainedUntilMS  int64  `json:"retained_until_ms"`
	FirstTurnID      string `json:"first_turn_id"`
	LastTurnID       string `json:"last_turn_id"`
}

type CarrierView struct {
	CarrierKind     string `json:"carrier_kind"`
	OwnerAccountID  string `json:"owner_account_id"`
	OwnerHost       string `json:"owner_host"`
	OwnerPort       int    `json:"owner_port"`
	FirstSeenTurnID string `json:"first_seen_turn_id"`
	LastSeenTurnID  string `json:"last_seen_turn_id"`
	LastSeenAtMS    int64  `json:"last_seen_at_ms"`
}

type ReplayEventView struct {
	ReplayEventID       string `json:"replay_event_id"`
	FromTurnID          string `json:"from_turn_id"`
	ToTurnID            string `json:"to_turn_id"`
	LineageSessionID    string `json:"lineage_session_id"`
	OldAccountID        string `json:"old_account_id,omitempty"`
	NewAccountID        string `json:"new_account_id,omitempty"`
	ReplayReason        string `json:"replay_reason"`
	RemovedCarrierKinds string `json:"removed_carrier_kinds,omitempty"`
	RemovedCarrierCount int    `json:"removed_carrier_count"`
	CreatedAtMS         int64  `json:"created_at_ms"`
}

type RoutingFailureView struct {
	FailureID        string `json:"failure_id"`
	TurnID           string `json:"turn_id"`
	LineageSessionID string `json:"lineage_session_id"`
	AccountID        string `json:"account_id,omitempty"`
	ReasonCode       string `json:"reason_code"`
	ReasonDetail     string `json:"reason_detail,omitempty"`
	HTTPStatus       int    `json:"http_status,omitempty"`
	CreatedAtMS      int64  `json:"created_at_ms"`
}

func (s *Store) ListAccountOverviews(ctx context.Context, now time.Time, replayLookback time.Duration) ([]AccountOverview, error) {
	if err := s.RefreshLineageStatuses(ctx, now); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT account_id, display_name, downstream_host, downstream_port FROM accounts WHERE enabled = 1 ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AccountOverview, 0)
	for rows.Next() {
		var overview AccountOverview
		if err := rows.Scan(&overview.AccountID, &overview.DisplayName, &overview.DownstreamHost, &overview.DownstreamPort); err != nil {
			return nil, err
		}
		out = append(out, overview)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	activeCutoff := now.Add(-s.activeSessionWindow).UnixMilli()
	replayCutoff := now.Add(-replayLookback).UnixMilli()
	for i := range out {
		out[i].ActiveSessionCount, _ = s.countInt64(ctx, `SELECT COUNT(DISTINCT lineage_session_id) FROM lineage_bindings WHERE account_id = ? AND last_seen_at_ms >= ? AND retained_until_ms >= ?`, out[i].AccountID, activeCutoff, now.UnixMilli())
		out[i].ActiveCarrierCount, _ = s.countInt64(ctx, `SELECT COUNT(*) FROM carrier_index WHERE owner_account_id = ? AND last_seen_at_ms >= ?`, out[i].AccountID, activeCutoff)
		out[i].RecentReplayCount, _ = s.countInt64(ctx, `SELECT COUNT(*) FROM replay_events WHERE new_account_id = ? AND created_at_ms >= ?`, out[i].AccountID, replayCutoff)
	}
	return out, nil
}

func (s *Store) GetAccountOverview(ctx context.Context, accountID string, now time.Time, replayLookback time.Duration, recentTurnsLimit int) (AccountOverview, error) {
	var overview AccountOverview
	if err := s.RefreshLineageStatuses(ctx, now); err != nil {
		return overview, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT account_id, display_name, downstream_host, downstream_port FROM accounts WHERE account_id = ?`, accountID).Scan(&overview.AccountID, &overview.DisplayName, &overview.DownstreamHost, &overview.DownstreamPort); err != nil {
		return overview, err
	}
	activeCutoff := now.Add(-s.activeSessionWindow).UnixMilli()
	replayCutoff := now.Add(-replayLookback).UnixMilli()
	overview.ActiveSessionCount, _ = s.countInt64(ctx, `SELECT COUNT(DISTINCT lineage_session_id) FROM lineage_bindings WHERE account_id = ? AND last_seen_at_ms >= ? AND retained_until_ms >= ?`, accountID, activeCutoff, now.UnixMilli())
	overview.ActiveCarrierCount, _ = s.countInt64(ctx, `SELECT COUNT(*) FROM carrier_index WHERE owner_account_id = ? AND last_seen_at_ms >= ?`, accountID, activeCutoff)
	overview.RecentReplayCount, _ = s.countInt64(ctx, `SELECT COUNT(*) FROM replay_events WHERE new_account_id = ? AND created_at_ms >= ?`, accountID, replayCutoff)
	turns, err := s.listTurnsByQuery(ctx, `SELECT turn_id, lineage_session_id, route_mode, error_code, carrier_kinds, removed_carrier_kinds, request_status_code, created_at_ms FROM turns_meta WHERE account_id = ? ORDER BY created_at_ms DESC LIMIT ?`, accountID, recentTurnsLimit)
	if err != nil {
		return overview, err
	}
	overview.RecentTurns = turns
	return overview, nil
}

func (s *Store) GetLineageDetail(ctx context.Context, lineageSessionID string, limit int) (LineageDetail, error) {
	detail := LineageDetail{LineageSessionID: lineageSessionID}
	binding, err := s.lookupLineageBinding(ctx, lineageSessionID)
	if err != nil && err != sql.ErrNoRows {
		return detail, err
	}
	if err == nil {
		detail.Binding = binding
	}
	carriers, err := s.listCarriers(ctx, lineageSessionID)
	if err != nil {
		return detail, err
	}
	detail.Carriers = carriers
	turns, err := s.listTurnsByQuery(ctx, `SELECT turn_id, lineage_session_id, route_mode, error_code, carrier_kinds, removed_carrier_kinds, request_status_code, created_at_ms FROM turns_meta WHERE lineage_session_id = ? ORDER BY created_at_ms ASC LIMIT ?`, lineageSessionID, limit)
	if err != nil {
		return detail, err
	}
	detail.Turns = turns
	replays, err := s.listReplayEvents(ctx, lineageSessionID, limit)
	if err != nil {
		return detail, err
	}
	detail.ReplayEvents = replays
	failures, err := s.ListRoutingFailures(ctx, lineageSessionID, "", limit)
	if err != nil {
		return detail, err
	}
	detail.Failures = failures
	return detail, nil
}

func (s *Store) ListRoutingFailures(ctx context.Context, lineageSessionID string, reasonCode string, limit int) ([]RoutingFailureView, error) {
	query := `SELECT failure_id, turn_id, lineage_session_id, COALESCE(account_id,''), reason_code, COALESCE(reason_detail,''), COALESCE(http_status,0), created_at_ms FROM routing_failures`
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if lineageSessionID != "" {
		clauses = append(clauses, `lineage_session_id = ?`)
		args = append(args, lineageSessionID)
	}
	if reasonCode != "" {
		clauses = append(clauses, `reason_code = ?`)
		args = append(args, reasonCode)
	}
	if len(clauses) > 0 {
		query += ` WHERE ` + stringsJoin(clauses, ` AND `)
	}
	query += ` ORDER BY created_at_ms DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]RoutingFailureView, 0)
	for rows.Next() {
		var view RoutingFailureView
		if err := rows.Scan(&view.FailureID, &view.TurnID, &view.LineageSessionID, &view.AccountID, &view.ReasonCode, &view.ReasonDetail, &view.HTTPStatus, &view.CreatedAtMS); err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, rows.Err()
}

func (s *Store) lookupLineageBinding(ctx context.Context, lineageSessionID string) (*LineageBindingView, error) {
	var view LineageBindingView
	if err := s.db.QueryRowContext(ctx, `SELECT lineage_session_id, account_id, downstream_host, downstream_port, status, first_seen_at_ms, last_seen_at_ms, retained_until_ms, first_turn_id, last_turn_id FROM lineage_bindings WHERE lineage_session_id = ?`, lineageSessionID).Scan(
		&view.LineageSessionID, &view.AccountID, &view.DownstreamHost, &view.DownstreamPort, &view.Status, &view.FirstSeenAtMS, &view.LastSeenAtMS, &view.RetainedUntilMS, &view.FirstTurnID, &view.LastTurnID,
	); err != nil {
		return nil, err
	}
	return &view, nil
}

func (s *Store) listCarriers(ctx context.Context, lineageSessionID string) ([]CarrierView, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT carrier_kind, owner_account_id, owner_host, owner_port, first_seen_turn_id, last_seen_turn_id, last_seen_at_ms FROM carrier_index WHERE lineage_session_id = ? ORDER BY last_seen_at_ms DESC`, lineageSessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]CarrierView, 0)
	for rows.Next() {
		var view CarrierView
		if err := rows.Scan(&view.CarrierKind, &view.OwnerAccountID, &view.OwnerHost, &view.OwnerPort, &view.FirstSeenTurnID, &view.LastSeenTurnID, &view.LastSeenAtMS); err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, rows.Err()
}

func (s *Store) listReplayEvents(ctx context.Context, lineageSessionID string, limit int) ([]ReplayEventView, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT replay_event_id, from_turn_id, to_turn_id, lineage_session_id, COALESCE(old_account_id,''), COALESCE(new_account_id,''), replay_reason, COALESCE(removed_carrier_kinds,''), removed_carrier_count, created_at_ms FROM replay_events WHERE lineage_session_id = ? ORDER BY created_at_ms DESC LIMIT ?`, lineageSessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ReplayEventView, 0)
	for rows.Next() {
		var view ReplayEventView
		if err := rows.Scan(&view.ReplayEventID, &view.FromTurnID, &view.ToTurnID, &view.LineageSessionID, &view.OldAccountID, &view.NewAccountID, &view.ReplayReason, &view.RemovedCarrierKinds, &view.RemovedCarrierCount, &view.CreatedAtMS); err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, rows.Err()
}

func (s *Store) listTurnsByQuery(ctx context.Context, query string, args ...any) ([]TurnSummary, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]TurnSummary, 0)
	for rows.Next() {
		var view TurnSummary
		if err := rows.Scan(&view.TurnID, &view.LineageSessionID, &view.RouteMode, &view.ReasonCode, &view.CarrierKinds, &view.RemovedCarrierKinds, &view.RequestStatusCode, &view.CreatedAtMS); err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, rows.Err()
}

func (s *Store) countInt64(ctx context.Context, query string, args ...any) (int64, error) {
	var value int64
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func stringsJoin(xs []string, sep string) string {
	body, _ := json.Marshal(xs)
	_ = body
	if len(xs) == 0 {
		return ""
	}
	out := xs[0]
	for _, x := range xs[1:] {
		out += sep + x
	}
	return out
}
