package sqlitestore

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

type HashedCarrier struct {
	Kind     string
	IDHMAC   string
	BlobHMAC string
}

type CarrierBinding struct {
	LineageSessionID string
	AccountID        string
	OwnerHost        string
	OwnerPort        int
	CarrierKind      string
	IDHMAC           string
	BlobHMAC         string
	LastSeenAtMS     int64
}

type CarrierLookupResult struct {
	Bindings []CarrierBinding
}

func (r CarrierLookupResult) UniqueOwner() (CarrierBinding, bool) {
	if len(r.Bindings) == 0 {
		return CarrierBinding{}, false
	}
	owner := r.Bindings[0]
	for _, binding := range r.Bindings[1:] {
		if binding.AccountID != owner.AccountID || binding.OwnerHost != owner.OwnerHost || binding.OwnerPort != owner.OwnerPort {
			return CarrierBinding{}, false
		}
	}
	return owner, true
}

func (s *Store) LookupCarrierBindings(ctx context.Context, carriers []HashedCarrier) (CarrierLookupResult, error) {
	rowsByKey := make(map[string]CarrierBinding)
	for _, carrier := range carriers {
		rows, err := s.lookupOneCarrier(ctx, carrier)
		if err != nil {
			return CarrierLookupResult{}, err
		}
		for _, row := range rows {
			key := fmt.Sprintf("%s|%s|%s|%d|%s", row.LineageSessionID, row.AccountID, row.OwnerHost, row.OwnerPort, row.CarrierKind)
			existing, ok := rowsByKey[key]
			if !ok || row.LastSeenAtMS > existing.LastSeenAtMS {
				rowsByKey[key] = row
			}
		}
	}
	bindings := make([]CarrierBinding, 0, len(rowsByKey))
	for _, binding := range rowsByKey {
		bindings = append(bindings, binding)
	}
	sort.SliceStable(bindings, func(i, j int) bool {
		if bindings[i].LastSeenAtMS != bindings[j].LastSeenAtMS {
			return bindings[i].LastSeenAtMS > bindings[j].LastSeenAtMS
		}
		if bindings[i].AccountID != bindings[j].AccountID {
			return bindings[i].AccountID < bindings[j].AccountID
		}
		return bindings[i].LineageSessionID < bindings[j].LineageSessionID
	})
	return CarrierLookupResult{Bindings: bindings}, nil
}

func (s *Store) lookupOneCarrier(ctx context.Context, carrier HashedCarrier) ([]CarrierBinding, error) {
	var args []any
	query := `SELECT lineage_session_id, owner_account_id, owner_host, owner_port, carrier_kind, carrier_id_hmac, carrier_blob_hmac, last_seen_at_ms FROM carrier_index WHERE carrier_kind = ?`
	args = append(args, carrier.Kind)
	clauses := make([]string, 0, 2)
	if carrier.IDHMAC != "" {
		clauses = append(clauses, "carrier_id_hmac = ?")
		args = append(args, carrier.IDHMAC)
	}
	if carrier.BlobHMAC != "" {
		clauses = append(clauses, "carrier_blob_hmac = ?")
		args = append(args, carrier.BlobHMAC)
	}
	if len(clauses) == 0 {
		return nil, nil
	}
	query += " AND (" + clauses[0]
	for _, clause := range clauses[1:] {
		query += " OR " + clause
	}
	query += ")"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]CarrierBinding, 0)
	for rows.Next() {
		var row CarrierBinding
		if err := rows.Scan(&row.LineageSessionID, &row.AccountID, &row.OwnerHost, &row.OwnerPort, &row.CarrierKind, &row.IDHMAC, &row.BlobHMAC, &row.LastSeenAtMS); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) UpsertCarrierBindings(ctx context.Context, lineageSessionID, turnID string, account Account, carriers []HashedCarrier, now time.Time) error {
	nowMS := now.UnixMilli()
	for _, carrier := range carriers {
		var existingID int64
		var existingTurn string
		err := s.db.QueryRowContext(ctx, `SELECT carrier_row_id, first_seen_turn_id FROM carrier_index WHERE carrier_kind = ? AND ((carrier_id_hmac = ? AND ? <> '') OR (carrier_blob_hmac = ? AND ? <> '')) ORDER BY last_seen_at_ms DESC LIMIT 1`, carrier.Kind, carrier.IDHMAC, carrier.IDHMAC, carrier.BlobHMAC, carrier.BlobHMAC).Scan(&existingID, &existingTurn)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == sql.ErrNoRows {
			_, err = s.db.ExecContext(ctx, `INSERT INTO carrier_index (
				lineage_session_id, carrier_kind, carrier_id_hmac, carrier_blob_hmac,
				owner_account_id, owner_host, owner_port,
				first_seen_turn_id, last_seen_turn_id, created_at_ms, last_seen_at_ms
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				lineageSessionID, carrier.Kind, nullableString(carrier.IDHMAC), nullableString(carrier.BlobHMAC),
				account.AccountID, account.DownstreamHost, account.DownstreamPort,
				turnID, turnID, nowMS, nowMS,
			)
			if err != nil {
				return err
			}
			continue
		}
		_, err = s.db.ExecContext(ctx, `UPDATE carrier_index SET
			lineage_session_id = ?,
			owner_account_id = ?,
			owner_host = ?,
			owner_port = ?,
			last_seen_turn_id = ?,
			last_seen_at_ms = ?
		WHERE carrier_row_id = ?`,
			lineageSessionID, account.AccountID, account.DownstreamHost, account.DownstreamPort, turnID, nowMS, existingID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
