package app

import (
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
}

func (a *App) handleResponses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	now := time.Now().UTC()
	turnID := ids.New("turn")
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
	if err := a.forwardResponses(w, r, turnID, plan, parsed, now); err != nil {
		a.logger.Error("forward response", logging.Err(err), logging.String("turn_id", turnID))
	}
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
	}, nil
}

func (a *App) forwardResponses(w http.ResponseWriter, r *http.Request, turnID string, plan routePlan, parsed responses.Request, now time.Time) error {
	upstreamURL := fmt.Sprintf("http://%s:%d/v1/responses", plan.Account.DownstreamHost, plan.Account.DownstreamPort)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(plan.ForwardBody))
	if err != nil {
		return err
	}
	copyHeaders(req.Header, r.Header)
	resp, err := a.client.Do(req)
	if err != nil {
		_ = a.sqlite.InsertRoutingFailure(r.Context(), ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, "upstream_request_failed", err.Error(), http.StatusBadGateway, now)
		writeJSONError(w, http.StatusBadGateway, "upstream_request_failed", err.Error())
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)

	if resp.StatusCode >= 400 {
		_ = a.sqlite.InsertRoutingFailure(r.Context(), ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, "upstream_rejected", string(body), resp.StatusCode, now)
		return nil
	}
	responseCarriers := extractResponseCarriers(body)
	if len(responseCarriers) > 0 {
		hashed := make([]sqlitestore.HashedCarrier, 0, len(responseCarriers))
		for _, carrier := range responseCarriers {
			hashed = append(hashed, sqlitestore.HashedCarrier{Kind: carrier.Kind, IDHMAC: a.hasher.Sum(carrier.RealID), BlobHMAC: a.hasher.Sum(carrier.EncryptedContent)})
		}
		if err := a.sqlite.UpsertCarrierBindings(r.Context(), plan.LineageSessionID, turnID, plan.Account, hashed, now); err != nil {
			a.logger.Error("upsert carrier bindings", logging.Err(err), logging.String("turn_id", turnID))
		}
	}
	if plan.Mode == "replay" {
		_ = a.sqlite.InsertReplayEvent(r.Context(), ids.New("replay"), turnID, turnID, plan.LineageSessionID, "", plan.Account.AccountID, plan.ReasonCode, strings.Join(plan.RemovedCarrierKinds, ","), plan.RemovedCarrierCount, now)
	}
	return nil
}

func extractResponseCarriers(body []byte) []responses.Carrier {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	items, _ := raw["output"].([]any)
	out := make([]responses.Carrier, 0)
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := m["type"].(string)
		if kind != "reasoning" && kind != "compaction" {
			continue
		}
		realID, _ := m["id"].(string)
		encrypted, _ := m["encrypted_content"].(string)
		if realID == "" && encrypted == "" {
			continue
		}
		out = append(out, responses.Carrier{Kind: kind, RealID: realID, EncryptedContent: encrypted})
	}
	return out
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
