package sqlitestore

import "context"

func (s *Store) UpdateTurnResult(ctx context.Context, turnID string, statusCode int, errorCode, errorMessage, duckdbTurnPK string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE turns_meta SET request_status_code = ?, error_code = ?, error_message = ?, duckdb_turn_pk = ? WHERE turn_id = ?`, zeroToNullInt(statusCode), nullableString(errorCode), nullableString(errorMessage), nullableString(duckdbTurnPK), turnID)
	return err
}
