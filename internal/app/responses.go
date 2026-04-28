package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nolanho/llm-api-gateway/internal/ids"
	"github.com/nolanho/llm-api-gateway/internal/logging"
	"github.com/nolanho/llm-api-gateway/internal/responses"
	"github.com/nolanho/llm-api-gateway/internal/storage/duckstore"
	"github.com/nolanho/llm-api-gateway/internal/storage/sqlitestore"
)

type routePlan struct {
	Mode                string
	ReasonCode          string
	ReasonDetail        string
	Account             sqlitestore.Account
	LineageSessionID    string
	LineageGeneration   int
	HasRealCarrier      bool
	CarrierKinds        []string
	RemovedCarrierKinds []string
	RemovedCarrierCount int
	ForwardBody         []byte
	ForwardMap          map[string]any
}

func (a *App) handleResponses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	now := time.Now().UTC()
	turnID := ids.New("turn")
	turnPK := turnID
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.writeRoutingError(w, r, turnID, ids.New("lineage"), "invalid_request_body", err.Error(), http.StatusBadRequest, now)
		return
	}
	parsed, err := responses.ParseRequest(body)
	if err != nil {
		a.writeRoutingError(w, r, turnID, ids.New("lineage"), "invalid_request_body", err.Error(), http.StatusBadRequest, now)
		return
	}
	plan, err := a.planRoute(r.Context(), parsed, now)
	if err != nil {
		a.writeRoutingError(w, r, turnID, ids.New("lineage"), "route_plan_failed", err.Error(), http.StatusInternalServerError, now)
		return
	}
	if plan.ReasonCode == "carrier_owner_not_found" || plan.ReasonCode == "carrier_owner_conflict" {
		if !a.cfg.DefaultReplayEnabled {
			status := http.StatusConflict
			if plan.ReasonCode == "carrier_owner_not_found" {
				status = http.StatusGone
			}
			a.writeRoutingError(w, r, turnID, plan.LineageSessionID, plan.ReasonCode, plan.ReasonDetail, status, now)
			return
		}
	}
	if err := a.sqlite.UpsertLineageBinding(r.Context(), plan.LineageSessionID, plan.Account, turnID, now); err != nil {
		a.writeRoutingError(w, r, turnID, plan.LineageSessionID, "sqlite_lookup_error", err.Error(), http.StatusInternalServerError, now)
		return
	}
	if err := a.sqlite.InsertTurnMeta(r.Context(), sqlitestore.TurnMeta{
		TurnID:                 turnID,
		LineageSessionID:       plan.LineageSessionID,
		LineageGeneration:      plan.LineageGeneration,
		RouteMode:              plan.Mode,
		Surface:                "responses",
		Model:                  stringValue(parsed.Raw, "model"),
		AccountID:              plan.Account.AccountID,
		DownstreamHost:         plan.Account.DownstreamHost,
		DownstreamPort:         plan.Account.DownstreamPort,
		HasRealCarrier:         plan.HasRealCarrier,
		CarrierKinds:           strings.Join(plan.CarrierKinds, ","),
		CarrierRemoved:         plan.RemovedCarrierCount > 0,
		RemovedCarrierKinds:    strings.Join(plan.RemovedCarrierKinds, ","),
		RemovedCarrierCount:    plan.RemovedCarrierCount,
		WeakHistoryFingerprint: weakHistoryFingerprint(parsed.Raw),
		CreatedAt:              now,
	}); err != nil {
		a.logger.Error("insert turn meta", logging.Err(err), logging.String("turn_id", turnID))
	}
	if plan.Mode == "replay" {
		a.logger.Info("replay started",
			logging.String("turn_id", turnID),
			logging.String("lineage_session_id", plan.LineageSessionID),
			logging.String("reason_code", plan.ReasonCode),
			logging.String("removed_carrier_kinds", strings.Join(plan.RemovedCarrierKinds, ",")),
		)
	}
	result, err := a.forwardResponses(w, r, turnID, turnPK, plan, parsed, now)
	if err != nil {
		a.logger.Error("forward response", logging.Err(err), logging.String("turn_id", turnID))
	}
	if updateErr := a.sqlite.UpdateTurnResult(r.Context(), turnID, result.StatusCode, result.ErrorCode, result.ErrorMessage, turnPK); updateErr != nil {
		a.logger.Error("update turn result", logging.Err(updateErr), logging.String("turn_id", turnID))
	}
	if archiveErr := a.archiveTurn(r.Context(), turnPK, turnID, plan, result, now); archiveErr != nil {
		a.logger.Error("archive turn", logging.Err(archiveErr), logging.String("turn_id", turnID))
	}
}

type forwardResult struct {
	StatusCode    int
	ErrorCode     string
	ErrorMessage  string
	ResponseItems []map[string]any
	StreamState   string
	FinishReason  string
}

func (a *App) planRoute(ctx context.Context, parsed responses.Request, now time.Time) (routePlan, error) {
	carriers := responses.ExtractRealCarriers(parsed.Raw)
	if len(carriers) == 0 {
		account, err := a.sqlite.SelectLeastActiveAccount(ctx, stringValue(parsed.Raw, "model"), now)
		if err != nil {
			return routePlan{}, err
		}
		return routePlan{
			Mode:             "replay",
			ReasonCode:       "new_request_no_carrier",
			Account:          account,
			LineageSessionID: ids.New("lineage"),
			ForwardBody:      mustJSON(parsed.Raw),
			ForwardMap:       parsed.Raw,
		}, nil
	}
	hashed := make([]sqlitestore.HashedCarrier, 0, len(carriers))
	for _, carrier := range carriers {
		hashed = append(hashed, sqlitestore.HashedCarrier{
			Kind:     carrier.Kind,
			IDHMAC:   a.hasher.Sum(carrier.RealID),
			BlobHMAC: a.hasher.Sum(carrier.EncryptedContent),
		})
	}
	lookup, err := a.sqlite.LookupCarrierBindings(ctx, hashed)
	if err != nil {
		return routePlan{}, err
	}
	if owner, ok := lookup.UniqueOwner(); ok {
		return routePlan{
			Mode:       "strict",
			ReasonCode: "carrier_owner_hit",
			Account: sqlitestore.Account{
				AccountID:      owner.AccountID,
				DownstreamHost: owner.OwnerHost,
				DownstreamPort: owner.OwnerPort,
			},
			LineageSessionID: owner.LineageSessionID,
			HasRealCarrier:   true,
			CarrierKinds:     responses.CarrierKinds(carriers),
			ForwardBody:      mustJSON(parsed.Raw),
			ForwardMap:       parsed.Raw,
		}, nil
	}
	stripped, removedKinds, removedCount := responses.StripCarriers(parsed.Raw)
	account, selectErr := a.sqlite.SelectLeastActiveAccount(ctx, stringValue(parsed.Raw, "model"), now)
	if selectErr != nil {
		return routePlan{}, selectErr
	}
	reasonCode := "carrier_owner_not_found"
	reasonDetail := "real carrier owner not found"
	if len(lookup.Bindings) > 1 {
		reasonCode = "carrier_owner_conflict"
		reasonDetail = "real carriers map to multiple owners"
	}
	return routePlan{
		Mode:                "replay",
		ReasonCode:          reasonCode,
		ReasonDetail:        reasonDetail,
		Account:             account,
		LineageSessionID:    ids.New("lineage"),
		HasRealCarrier:      true,
		CarrierKinds:        responses.CarrierKinds(carriers),
		RemovedCarrierKinds: removedKinds,
		RemovedCarrierCount: removedCount,
		ForwardBody:         mustJSON(stripped),
		ForwardMap:          stripped,
	}, nil
}

func (a *App) forwardResponses(w http.ResponseWriter, r *http.Request, turnID, turnPK string, plan routePlan, parsed responses.Request, now time.Time) (forwardResult, error) {
	upstreamURL := fmt.Sprintf("http://%s:%d/v1/responses", plan.Account.DownstreamHost, plan.Account.DownstreamPort)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(plan.ForwardBody))
	if err != nil {
		return forwardResult{}, err
	}
	copyHeaders(req.Header, r.Header)
	resp, err := a.client.Do(req)
	if err != nil {
		_ = a.sqlite.InsertRoutingFailure(r.Context(), ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, "upstream_request_failed", err.Error(), http.StatusBadGateway, now)
		writeJSONError(w, http.StatusBadGateway, "upstream_request_failed", err.Error())
		return forwardResult{StatusCode: http.StatusBadGateway, ErrorCode: "upstream_request_failed", ErrorMessage: err.Error()}, nil
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if isStreamingRequest(plan.ForwardMap) {
		return a.forwardStreamingResponses(w, resp, r.Context(), turnID, plan, now)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return forwardResult{}, err
	}
	_, _ = w.Write(body)
	if resp.StatusCode >= 400 {
		_ = a.sqlite.InsertRoutingFailure(r.Context(), ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, "upstream_rejected", string(body), resp.StatusCode, now)
		return forwardResult{StatusCode: resp.StatusCode, ErrorCode: "upstream_rejected", ErrorMessage: string(body)}, nil
	}
	responseItems := responses.ResponseItems(mustParseMap(body))
	if err := a.upsertResponseCarriers(r.Context(), turnID, plan, now, extractResponseCarriers(body)); err != nil {
		a.logger.Error("upsert carrier bindings", logging.Err(err), logging.String("turn_id", turnID))
	}
	if plan.Mode == "replay" {
		_ = a.sqlite.InsertReplayEvent(r.Context(), ids.New("replay"), turnID, turnID, plan.LineageSessionID, "", plan.Account.AccountID, plan.ReasonCode, strings.Join(plan.RemovedCarrierKinds, ","), plan.RemovedCarrierCount, now)
	}
	return forwardResult{StatusCode: resp.StatusCode, ResponseItems: responseItems, StreamState: "completed"}, nil
}

func (a *App) forwardStreamingResponses(w http.ResponseWriter, resp *http.Response, ctx context.Context, turnID string, plan routePlan, now time.Time) (forwardResult, error) {
	flusher, _ := w.(http.Flusher)
	reader := bufio.NewReader(resp.Body)
	collector := responses.NewStreamCollector()
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			_, _ = io.WriteString(w, line)
			collector.ObserveEventLine(strings.TrimRight(line, "\r\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			collector.MarkInterrupted()
			return forwardResult{StatusCode: resp.StatusCode, ErrorCode: "stream_read_failed", ErrorMessage: err.Error(), ResponseItems: collector.ResponseItems(), StreamState: collector.StreamState(), FinishReason: collector.FinishReason()}, nil
		}
		select {
		case <-ctx.Done():
			collector.MarkInterrupted()
			return forwardResult{StatusCode: resp.StatusCode, ErrorCode: "client_cancelled", ErrorMessage: ctx.Err().Error(), ResponseItems: collector.ResponseItems(), StreamState: collector.StreamState(), FinishReason: collector.FinishReason()}, nil
		default:
		}
	}
	if resp.StatusCode >= 400 {
		return forwardResult{StatusCode: resp.StatusCode, ErrorCode: "upstream_rejected", ResponseItems: collector.ResponseItems(), StreamState: collector.StreamState(), FinishReason: collector.FinishReason()}, nil
	}
	if err := a.upsertResponseCarriers(ctx, turnID, plan, now, collector.Carriers()); err != nil {
		a.logger.Error("upsert stream carrier bindings", logging.Err(err), logging.String("turn_id", turnID))
	}
	if plan.Mode == "replay" {
		_ = a.sqlite.InsertReplayEvent(ctx, ids.New("replay"), turnID, turnID, plan.LineageSessionID, "", plan.Account.AccountID, plan.ReasonCode, strings.Join(plan.RemovedCarrierKinds, ","), plan.RemovedCarrierCount, now)
	}
	return forwardResult{StatusCode: resp.StatusCode, ResponseItems: collector.ResponseItems(), StreamState: collector.StreamState(), FinishReason: collector.FinishReason()}, nil
}

func (a *App) upsertResponseCarriers(ctx context.Context, turnID string, plan routePlan, now time.Time, carriers []responses.Carrier) error {
	if len(carriers) == 0 {
		return nil
	}
	hashed := make([]sqlitestore.HashedCarrier, 0, len(carriers))
	for _, carrier := range carriers {
		hashed = append(hashed, sqlitestore.HashedCarrier{Kind: carrier.Kind, IDHMAC: a.hasher.Sum(carrier.RealID), BlobHMAC: a.hasher.Sum(carrier.EncryptedContent)})
	}
	return a.sqlite.UpsertCarrierBindings(ctx, plan.LineageSessionID, turnID, plan.Account, hashed, now)
}

func (a *App) archiveTurn(ctx context.Context, turnPK, turnID string, plan routePlan, result forwardResult, now time.Time) error {
	reqItems := responses.RequestItems(plan.ForwardMap)
	resItems := responses.MustItems(result.ResponseItems)
	return a.duck.ArchiveTurn(ctx,
		duckstore.TurnRecord{
			TurnPK:              turnPK,
			TurnID:              turnID,
			LineageSessionID:    plan.LineageSessionID,
			LineageGeneration:   plan.LineageGeneration,
			RouteMode:           plan.Mode,
			Surface:             "responses",
			Model:               stringValue(plan.ForwardMap, "model"),
			AccountID:           plan.Account.AccountID,
			DownstreamHost:      plan.Account.DownstreamHost,
			DownstreamPort:      plan.Account.DownstreamPort,
			HasRealCarrier:      plan.HasRealCarrier,
			CarrierKinds:        strings.Join(plan.CarrierKinds, ","),
			CarrierRemoved:      plan.RemovedCarrierCount > 0,
			RemovedCarrierKinds: strings.Join(plan.RemovedCarrierKinds, ","),
			RemovedCarrierCount: plan.RemovedCarrierCount,
			StreamState:         result.StreamState,
			FinishReason:        result.FinishReason,
			CreatedAt:           now,
		},
		duckstore.TurnDocument{
			TurnPK:                turnPK,
			TurnID:                turnID,
			LineageSessionID:      plan.LineageSessionID,
			RouteMode:             plan.Mode,
			EffectiveRequestItems: reqItems,
			ResponseItems:         resItems,
			EffectiveConversation: responses.EffectiveConversationText(reqItems, resItems),
			CreatedAt:             now,
		},
		responses.FlattenItems(turnPK, turnID, plan.LineageSessionID, reqItems, resItems),
	)
}

func extractResponseCarriers(body []byte) []responses.Carrier {
	return responses.ExtractRealCarriers(map[string]any{"input": mustParseMap(body)["output"]})
}

func isStreamingRequest(raw map[string]any) bool {
	v, ok := raw["stream"].(bool)
	return ok && v
}

func mustParseMap(body []byte) map[string]any {
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	return raw
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		switch http.CanonicalHeaderKey(k) {
		case "Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade", "Host", "Content-Length":
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func writeJSONError(w http.ResponseWriter, status int, typ, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"type": typ, "message": message, "reason_code": typ}})
}

func (a *App) writeRoutingError(w http.ResponseWriter, r *http.Request, turnID, lineageID, reasonCode, detail string, status int, now time.Time) {
	_ = a.sqlite.InsertRoutingFailure(r.Context(), ids.New("failure"), turnID, lineageID, "", reasonCode, detail, status, now)
	writeJSONError(w, status, reasonCode, detail)
}

func weakHistoryFingerprint(raw map[string]any) string {
	clone, _, _ := responses.StripCarriers(raw)
	body, _ := json.Marshal(clone)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func mustJSON(raw map[string]any) []byte {
	body, _ := json.Marshal(raw)
	return body
}

func stringValue(raw map[string]any, key string) string {
	v, _ := raw[key].(string)
	return v
}
