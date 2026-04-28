package sqlitestore

import (
	"context"
	"time"
)

type TurnMeta struct {
	TurnID                 string
	ParentTurnID           string
	ReplayParentTurnID     string
	LineageSessionID       string
	LineageGeneration      int
	RouteMode              string
	Surface                string
	Model                  string
	AccountID              string
	DownstreamHost         string
	DownstreamPort         int
	HasRealCarrier         bool
	CarrierKinds           string
	CarrierRemoved         bool
	RemovedCarrierKinds    string
	RemovedCarrierCount    int
	WeakHistoryFingerprint string
	RequestStatusCode      int
	ErrorCode              string
	ErrorMessage           string
	DuckDBTurnPK           string
	OTelTraceID            string
	OTelSpanID             string
	CreatedAt              time.Time
}

func (s *Store) InsertTurnMeta(ctx context.Context, turn TurnMeta) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO turns_meta (
		turn_id, parent_turn_id, replay_parent_turn_id, lineage_session_id, lineage_generation,
		route_mode, surface, model, account_id, downstream_host, downstream_port,
		has_real_carrier, carrier_kinds, carrier_removed, removed_carrier_kinds, removed_carrier_count,
		weak_history_fingerprint, request_status_code, error_code, error_message,
		duckdb_turn_pk, otel_trace_id, otel_span_id, created_at_ms
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		turn.TurnID, nullableString(turn.ParentTurnID), nullableString(turn.ReplayParentTurnID), turn.LineageSessionID, turn.LineageGeneration,
		turn.RouteMode, turn.Surface, nullableString(turn.Model), nullableString(turn.AccountID), nullableString(turn.DownstreamHost), turn.DownstreamPort,
		boolToInt(turn.HasRealCarrier), nullableString(turn.CarrierKinds), boolToInt(turn.CarrierRemoved), nullableString(turn.RemovedCarrierKinds), turn.RemovedCarrierCount,
		nullableString(turn.WeakHistoryFingerprint), zeroToNullInt(turn.RequestStatusCode), nullableString(turn.ErrorCode), nullableString(turn.ErrorMessage),
		nullableString(turn.DuckDBTurnPK), nullableString(turn.OTelTraceID), nullableString(turn.OTelSpanID), turn.CreatedAt.UnixMilli(),
	)
	return err
}

func (s *Store) InsertReplayEvent(ctx context.Context, replayEventID, fromTurnID, toTurnID, lineageSessionID, oldAccountID, newAccountID, replayReason, removedCarrierKinds string, removedCarrierCount int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO replay_events (
		replay_event_id, from_turn_id, to_turn_id, lineage_session_id, old_account_id, new_account_id,
		replay_reason, removed_carrier_kinds, removed_carrier_count, created_at_ms
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, replayEventID, fromTurnID, toTurnID, lineageSessionID, nullableString(oldAccountID), nullableString(newAccountID), replayReason, nullableString(removedCarrierKinds), removedCarrierCount, now.UnixMilli())
	return err
}

func (s *Store) InsertRoutingFailure(ctx context.Context, failureID, turnID, lineageSessionID, accountID, reasonCode, reasonDetail string, httpStatus int, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO routing_failures (
		failure_id, turn_id, lineage_session_id, account_id, reason_code, reason_detail, http_status, created_at_ms
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, failureID, turnID, lineageSessionID, nullableString(accountID), reasonCode, nullableString(reasonDetail), zeroToNullInt(httpStatus), now.UnixMilli())
	return err
}

func zeroToNullInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
