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
	"github.com/nolanho/llm-api-gateway/internal/observability"
	"github.com/nolanho/llm-api-gateway/internal/responses"
	"github.com/nolanho/llm-api-gateway/internal/storage/duckstore"
	"github.com/nolanho/llm-api-gateway/internal/storage/sqlitestore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := a.telemetry.Tracer.Start(r.Context(), "gateway.request")
	defer span.End()
	start := time.Now()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		span.SetStatus(codes.Error, "method not allowed")
		return
	}
	now := time.Now().UTC()
	turnID := ids.New("turn")
	turnPK := turnID
	baseLogger := a.logger.With(
		logging.String("turn_id", turnID),
		logging.String("surface", "responses"),
	)
	span.SetAttributes(attribute.String("gateway.turn_id", turnID), attribute.String("gateway.api.surface", "responses"))
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.writeRoutingError(w, r.WithContext(ctx), turnID, ids.New("lineage"), "invalid_request_body", err.Error(), http.StatusBadRequest, now)
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid request body")
		return
	}
	parsed, err := responses.ParseRequest(body)
	if err != nil {
		a.writeRoutingError(w, r.WithContext(ctx), turnID, ids.New("lineage"), "invalid_request_body", err.Error(), http.StatusBadRequest, now)
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid request body")
		return
	}
	plan, err := a.planRoute(ctx, parsed, now)
	if err != nil {
		a.writeRoutingError(w, r.WithContext(ctx), turnID, ids.New("lineage"), "route_plan_failed", err.Error(), http.StatusInternalServerError, now)
		span.RecordError(err)
		span.SetStatus(codes.Error, "route planning failed")
		return
	}
	planLogger := baseLogger.With(
		logging.String("lineage_session_id", plan.LineageSessionID),
		logging.String("route_mode", plan.Mode),
		logging.String("account_id", plan.Account.AccountID),
		logging.String("reason_code", plan.ReasonCode),
		logging.String("removed_carrier_kinds", strings.Join(plan.RemovedCarrierKinds, ",")),
	)
	span.SetAttributes(
		attribute.String("gateway.lineage_session_id", plan.LineageSessionID),
		attribute.String("gateway.route.mode", plan.Mode),
		attribute.String("gateway.route.reason_code", plan.ReasonCode),
		attribute.String("gateway.account.id", plan.Account.AccountID),
		attribute.String("gateway.replay.removed_carrier_kinds", strings.Join(plan.RemovedCarrierKinds, ",")),
		attribute.Int("gateway.replay.removed_carrier_count", plan.RemovedCarrierCount),
		attribute.Bool("gateway.continuation.has_real_carrier_id", plan.HasRealCarrier),
		attribute.String("llm.model.requested", stringValue(parsed.Raw, "model")),
	)
	if plan.ReasonCode == "carrier_owner_not_found" || plan.ReasonCode == "carrier_owner_conflict" {
		if !a.cfg.DefaultReplayEnabled {
			status := http.StatusConflict
			if plan.ReasonCode == "carrier_owner_not_found" {
				status = http.StatusGone
			}
			a.writeRoutingError(w, r.WithContext(ctx), turnID, plan.LineageSessionID, plan.ReasonCode, plan.ReasonDetail, status, now)
			span.SetStatus(codes.Error, plan.ReasonCode)
			return
		}
	}
	if err := a.sqlite.UpsertLineageBinding(ctx, plan.LineageSessionID, plan.Account, turnID, now); err != nil {
		a.writeRoutingError(w, r.WithContext(ctx), turnID, plan.LineageSessionID, "sqlite_lookup_error", err.Error(), http.StatusInternalServerError, now)
		span.RecordError(err)
		span.SetStatus(codes.Error, "sqlite lookup error")
		return
	}
	if err := a.sqlite.InsertTurnMeta(ctx, sqlitestore.TurnMeta{
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
		planLogger.Error("insert turn meta", logging.Err(err))
	}
	if plan.Mode == "replay" {
		planLogger.Info("replay started")
		a.telemetry.Metrics.ReplayTotal.Add(ctx, 1, observability.AddAttrs(
			attribute.String("gateway.route.reason_code", plan.ReasonCode),
			attribute.String("gateway.account.id", plan.Account.AccountID),
		))
	}
	result, err := a.forwardResponses(w, r.WithContext(ctx), turnID, turnPK, plan, now)
	if err != nil {
		planLogger.Error("forward response", logging.Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "forward response failed")
	}
	if updateErr := a.sqlite.UpdateTurnResult(ctx, turnID, result.StatusCode, result.ErrorCode, result.ErrorMessage, turnPK); updateErr != nil {
		planLogger.Error("update turn result", logging.Err(updateErr))
	}
	archiveStart := time.Now()
	if archiveErr := a.archiveTurn(ctx, turnPK, turnID, plan, result, now); archiveErr != nil {
		planLogger.Error("archive turn", logging.Err(archiveErr))
		a.telemetry.Metrics.ArchiveFailures.Add(ctx, 1)
		span.AddEvent("duckdb.archive_failed", eventAttrs(turnID, plan.LineageSessionID, plan.Account.AccountID, result.StatusCode, archiveErr.Error()))
	}
	a.telemetry.Metrics.ArchiveDuration.Record(ctx, observability.MsSince(archiveStart))
	span.SetStatus(codes.Ok, "ok")
	span.SetAttributes(attribute.Int("http.status_code", result.StatusCode))
	a.telemetry.Metrics.UpstreamDuration.Record(ctx, observability.MsSince(start), observability.RecordAttrs(
		attribute.String("gateway.route.mode", plan.Mode),
		attribute.String("gateway.account.id", plan.Account.AccountID),
	))
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
	ctx, span := a.telemetry.Tracer.Start(ctx, "gateway.route.resolve")
	defer span.End()
	carriers := responses.ExtractRealCarriers(parsed.Raw)
	if len(carriers) == 0 {
		account, err := a.sqlite.SelectLeastActiveAccount(ctx, stringValue(parsed.Raw, "model"), now)
		if err != nil {
			span.RecordError(err)
			return routePlan{}, err
		}
		span.SetAttributes(attribute.String("gateway.route.reason_code", "new_request_no_carrier"), attribute.String("gateway.account.id", account.AccountID))
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
		hashed = append(hashed, sqlitestore.HashedCarrier{Kind: carrier.Kind, IDHMAC: a.hasher.Sum(carrier.RealID), BlobHMAC: a.hasher.Sum(carrier.EncryptedContent)})
	}
	lookup, err := a.sqlite.LookupCarrierBindings(ctx, hashed)
	if err != nil {
		span.RecordError(err)
		return routePlan{}, err
	}
	if owner, ok := lookup.UniqueOwner(); ok {
		span.SetAttributes(attribute.String("gateway.route.reason_code", "carrier_owner_hit"), attribute.String("gateway.account.id", owner.AccountID))
		return routePlan{
			Mode:             "strict",
			ReasonCode:       "carrier_owner_hit",
			Account:          sqlitestore.Account{AccountID: owner.AccountID, DownstreamHost: owner.OwnerHost, DownstreamPort: owner.OwnerPort},
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
		span.RecordError(selectErr)
		return routePlan{}, selectErr
	}
	reasonCode := "carrier_owner_not_found"
	reasonDetail := "real carrier owner not found"
	if len(lookup.Bindings) > 1 {
		reasonCode = "carrier_owner_conflict"
		reasonDetail = "real carriers map to multiple owners"
	}
	span.SetAttributes(attribute.String("gateway.route.reason_code", reasonCode), attribute.String("gateway.account.id", account.AccountID))
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

func (a *App) forwardResponses(w http.ResponseWriter, r *http.Request, turnID, turnPK string, plan routePlan, now time.Time) (forwardResult, error) {
	ctx, span := a.telemetry.Tracer.Start(r.Context(), "gateway.provider.invoke")
	defer span.End()
	upstreamURL := fmt.Sprintf("http://%s:%d/v1/responses", plan.Account.DownstreamHost, plan.Account.DownstreamPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(plan.ForwardBody))
	if err != nil {
		return forwardResult{}, err
	}
	copyHeaders(req.Header, r.Header)
	a.telemetry.Metrics.UpstreamRequests.Add(ctx, 1, observability.AddAttrs(attribute.String("gateway.account.id", plan.Account.AccountID), attribute.String("gateway.route.mode", plan.Mode)))
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		span.RecordError(err)
		span.SetStatus(codes.Error, "upstream request failed")
		_ = a.sqlite.InsertRoutingFailure(ctx, ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, "upstream_request_failed", err.Error(), http.StatusBadGateway, now)
		writeJSONError(w, http.StatusBadGateway, "upstream_request_failed", err.Error())
		return forwardResult{StatusCode: http.StatusBadGateway, ErrorCode: "upstream_request_failed", ErrorMessage: err.Error()}, nil
	}
	defer resp.Body.Close()
	defer a.telemetry.Metrics.UpstreamDuration.Record(ctx, observability.MsSince(start), observability.RecordAttrs(attribute.String("gateway.account.id", plan.Account.AccountID), attribute.String("gateway.route.mode", plan.Mode)))

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if isStreamingRequest(plan.ForwardMap) {
		res, err := a.forwardStreamingResponses(w, resp, ctx, turnID, plan, now)
		if res.StatusCode >= 400 {
			a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		}
		return res, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return forwardResult{}, err
	}
	_, _ = w.Write(body)
	if resp.StatusCode >= 400 {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		_ = a.sqlite.InsertRoutingFailure(ctx, ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, "upstream_rejected", string(body), resp.StatusCode, now)
		return forwardResult{StatusCode: resp.StatusCode, ErrorCode: "upstream_rejected", ErrorMessage: string(body)}, nil
	}
	responseItems := responses.ResponseItems(mustParseMap(body))
	if err := a.upsertResponseCarriers(ctx, turnID, plan, now, extractResponseCarriers(body)); err != nil {
		a.logger.Error("upsert carrier bindings", logging.Err(err), logging.String("turn_id", turnID))
	}
	if plan.Mode == "replay" {
		_ = a.sqlite.InsertReplayEvent(ctx, ids.New("replay"), turnID, turnID, plan.LineageSessionID, "", plan.Account.AccountID, plan.ReasonCode, strings.Join(plan.RemovedCarrierKinds, ","), plan.RemovedCarrierCount, now)
		span.AddEvent("replay.completed", eventAttrs(turnID, plan.LineageSessionID, plan.Account.AccountID, resp.StatusCode, plan.ReasonCode))
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
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
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
	a.telemetry.Metrics.CarrierWrites.Add(ctx, int64(len(hashed)), observability.AddAttrs(attribute.String("gateway.account.id", plan.Account.AccountID)))
	return a.sqlite.UpsertCarrierBindings(ctx, plan.LineageSessionID, turnID, plan.Account, hashed, now)
}

func (a *App) archiveTurn(ctx context.Context, turnPK, turnID string, plan routePlan, result forwardResult, now time.Time) error {
	reqItems := responses.RequestItems(plan.ForwardMap)
	resItems := responses.MustItems(result.ResponseItems)
	return a.duck.ArchiveTurn(ctx,
		duckstore.TurnRecord{TurnPK: turnPK, TurnID: turnID, LineageSessionID: plan.LineageSessionID, LineageGeneration: plan.LineageGeneration, RouteMode: plan.Mode, Surface: "responses", Model: stringValue(plan.ForwardMap, "model"), AccountID: plan.Account.AccountID, DownstreamHost: plan.Account.DownstreamHost, DownstreamPort: plan.Account.DownstreamPort, HasRealCarrier: plan.HasRealCarrier, CarrierKinds: strings.Join(plan.CarrierKinds, ","), CarrierRemoved: plan.RemovedCarrierCount > 0, RemovedCarrierKinds: strings.Join(plan.RemovedCarrierKinds, ","), RemovedCarrierCount: plan.RemovedCarrierCount, StreamState: result.StreamState, FinishReason: result.FinishReason, CreatedAt: now},
		duckstore.TurnDocument{TurnPK: turnPK, TurnID: turnID, LineageSessionID: plan.LineageSessionID, RouteMode: plan.Mode, EffectiveRequestItems: reqItems, ResponseItems: resItems, EffectiveConversation: responses.EffectiveConversationText(reqItems, resItems), CreatedAt: now},
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

func eventAttrs(turnID, lineageID, accountID string, statusCode int, detail string) trace.EventOption {
	attrs := []attribute.KeyValue{
		attribute.String("gateway.turn_id", turnID),
		attribute.String("gateway.lineage_session_id", lineageID),
	}
	if accountID != "" {
		attrs = append(attrs, attribute.String("gateway.account.id", accountID))
	}
	if statusCode > 0 {
		attrs = append(attrs, attribute.Int("http.status_code", statusCode))
	}
	if detail != "" {
		attrs = append(attrs, attribute.String("gateway.route.reason_detail", detail))
	}
	return trace.WithAttributes(attrs...)
}
