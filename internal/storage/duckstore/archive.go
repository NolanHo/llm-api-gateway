package duckstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type TurnRecord struct {
	TurnPK              string
	TurnID              string
	ParentTurnID        string
	ReplayParentTurnID  string
	LineageSessionID    string
	LineageGeneration   int
	RouteMode           string
	Surface             string
	Model               string
	AccountID           string
	DownstreamHost      string
	DownstreamPort      int
	HasRealCarrier      bool
	CarrierKinds        string
	CarrierRemoved      bool
	RemovedCarrierKinds string
	RemovedCarrierCount int
	StreamState         string
	FinishReason        string
	RequestTokenEst     int
	ResponseTokenEst    int
	CreatedAt           time.Time
}

type TurnDocument struct {
	TurnPK                string
	TurnID                string
	LineageSessionID      string
	RouteMode             string
	EffectiveRequestItems any
	ResponseItems         any
	EffectiveConversation any
	CreatedAt             time.Time
}

type TurnItem struct {
	TurnPK           string
	TurnID           string
	LineageSessionID string
	ItemSeq          int
	Phase            string
	Role             string
	ItemKind         string
	TextContent      string
	ToolName         string
	ToolArgs         any
	ToolResult       any
	ItemJSON         any
	CreatedAt        time.Time
}

func (s *Store) ArchiveTurn(ctx context.Context, turn TurnRecord, doc TurnDocument, items []TurnItem) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO turns (
		turn_pk, turn_id, parent_turn_id, replay_parent_turn_id, lineage_session_id, lineage_generation,
		route_mode, surface, model, account_id, downstream_host, downstream_port,
		has_real_carrier, carrier_kinds, carrier_removed, removed_carrier_kinds, removed_carrier_count,
		stream_state, finish_reason, request_token_est, response_token_est, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		turn.TurnPK, turn.TurnID, nullable(turn.ParentTurnID), nullable(turn.ReplayParentTurnID), turn.LineageSessionID, turn.LineageGeneration,
		turn.RouteMode, turn.Surface, nullable(turn.Model), nullable(turn.AccountID), nullable(turn.DownstreamHost), turn.DownstreamPort,
		turn.HasRealCarrier, nullable(turn.CarrierKinds), turn.CarrierRemoved, nullable(turn.RemovedCarrierKinds), turn.RemovedCarrierCount,
		nullable(turn.StreamState), nullable(turn.FinishReason), nullableInt(turn.RequestTokenEst), nullableInt(turn.ResponseTokenEst), turn.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert turn: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO turn_documents (
		turn_pk, turn_id, lineage_session_id, route_mode,
		effective_request_items_json, response_items_json, effective_conversation_text, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		doc.TurnPK, doc.TurnID, doc.LineageSessionID, doc.RouteMode,
		marshalJSON(doc.EffectiveRequestItems), marshalJSON(doc.ResponseItems), marshalJSON(doc.EffectiveConversation), doc.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert turn document: %w", err)
	}
	for _, item := range items {
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_items (
			turn_pk, turn_id, lineage_session_id, item_seq, phase, role, item_kind,
			text_content, tool_name, tool_args_json, tool_result_json, item_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			item.TurnPK, item.TurnID, item.LineageSessionID, item.ItemSeq, item.Phase, nullable(item.Role), item.ItemKind,
			nullable(item.TextContent), nullable(item.ToolName), marshalJSON(item.ToolArgs), marshalJSON(item.ToolResult), marshalJSON(item.ItemJSON), item.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert turn item: %w", err)
		}
	}
	return tx.Commit()
}

func marshalJSON(v any) any {
	if v == nil {
		return nil
	}
	body, _ := json.Marshal(v)
	return string(body)
}

func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullableInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
