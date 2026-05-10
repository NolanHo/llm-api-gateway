package sqlitestore

import "context"

func (s *Store) UpdateTurnRoute(ctx context.Context, turn TurnMeta) error {
	_, err := s.db.ExecContext(ctx, `UPDATE turns_meta SET
		lineage_session_id = ?,
		lineage_generation = ?,
		route_mode = ?,
		model = ?,
		account_id = ?,
		downstream_host = ?,
		downstream_port = ?,
		has_real_carrier = ?,
		carrier_kinds = ?,
		carrier_removed = ?,
		removed_carrier_kinds = ?,
		removed_carrier_count = ?
	WHERE turn_id = ?`,
		turn.LineageSessionID,
		turn.LineageGeneration,
		turn.RouteMode,
		nullableString(turn.Model),
		nullableString(turn.AccountID),
		nullableString(turn.DownstreamHost),
		turn.DownstreamPort,
		boolToInt(turn.HasRealCarrier),
		nullableString(turn.CarrierKinds),
		boolToInt(turn.CarrierRemoved),
		nullableString(turn.RemovedCarrierKinds),
		turn.RemovedCarrierCount,
		turn.TurnID,
	)
	return err
}

func (s *Store) UpdateTurnResult(ctx context.Context, turnID string, statusCode int, errorCode, errorMessage, duckdbTurnPK string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE turns_meta SET request_status_code = ?, error_code = ?, error_message = ?, duckdb_turn_pk = ? WHERE turn_id = ?`, zeroToNullInt(statusCode), nullableString(errorCode), nullableString(errorMessage), nullableString(duckdbTurnPK), turnID)
	return err
}
