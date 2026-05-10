package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
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

type routeError struct {
	Code   string
	Detail string
	Status int
}

func (e routeError) Error() string { return e.Detail }

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
		re := routeError{Code: "route_plan_failed", Detail: err.Error(), Status: http.StatusInternalServerError}
		if errors.As(err, &re) {
			// Use the typed route error fields below.
		}
		a.writeRoutingError(w, r.WithContext(ctx), turnID, ids.New("lineage"), re.Code, re.Detail, re.Status, now)
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
	result, finalPlan, err := a.forwardResponses(w, r.WithContext(ctx), turnID, turnPK, plan, now)
	plan = finalPlan
	if err != nil {
		planLogger.Error("forward response", logging.Err(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "forward response failed")
	}
	if updateErr := a.sqlite.UpdateTurnRoute(ctx, turnMetaFromPlan(turnID, plan)); updateErr != nil {
		planLogger.Error("update turn route", logging.Err(updateErr))
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

type upstreamFailure struct {
	StatusCode   int
	ErrorCode    string
	ErrorMessage string
	Header       http.Header
	Body         []byte
	Retryable    bool
	PassThrough  bool
}

type upstreamSuccess struct {
	StatusCode    int
	Header        http.Header
	Body          []byte
	Raw           map[string]any
	ResponseItems []map[string]any
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
		account, err := a.sqlite.GetAccount(ctx, owner.AccountID)
		if err == nil && account.Enabled && account.State == "running" {
			span.SetAttributes(attribute.String("gateway.route.reason_code", "carrier_owner_hit"), attribute.String("gateway.account.id", owner.AccountID))
			return routePlan{
				Mode:             "strict",
				ReasonCode:       "carrier_owner_hit",
				Account:          account,
				LineageSessionID: owner.LineageSessionID,
				HasRealCarrier:   true,
				CarrierKinds:     responses.CarrierKinds(carriers),
				ForwardBody:      mustJSON(parsed.Raw),
				ForwardMap:       parsed.Raw,
			}, nil
		}
		if !a.cfg.StrictReplayRetry || !a.cfg.DefaultReplayEnabled {
			return routePlan{}, routeError{Code: "carrier_owner_unavailable", Detail: fmt.Sprintf("carrier owner %s is unavailable", owner.AccountID), Status: http.StatusServiceUnavailable}
		}
		stripped, removedKinds, removedCount := responses.StripCarriers(parsed.Raw)
		fallback, selectErr := a.sqlite.SelectLeastActiveAccount(ctx, stringValue(parsed.Raw, "model"), now)
		if selectErr != nil {
			span.RecordError(selectErr)
			return routePlan{}, selectErr
		}
		span.SetAttributes(attribute.String("gateway.route.reason_code", "carrier_owner_unavailable"), attribute.String("gateway.account.id", fallback.AccountID))
		return routePlan{
			Mode:                "replay",
			ReasonCode:          "carrier_owner_unavailable",
			ReasonDetail:        fmt.Sprintf("carrier owner %s is unavailable", owner.AccountID),
			Account:             fallback,
			LineageSessionID:    ids.New("lineage"),
			HasRealCarrier:      true,
			CarrierKinds:        responses.CarrierKinds(carriers),
			RemovedCarrierKinds: removedKinds,
			RemovedCarrierCount: removedCount,
			ForwardBody:         mustJSON(stripped),
			ForwardMap:          stripped,
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

func (a *App) forwardResponses(w http.ResponseWriter, r *http.Request, turnID, turnPK string, plan routePlan, now time.Time) (forwardResult, routePlan, error) {
	if isStreamingRequest(plan.ForwardMap) {
		return a.forwardStreamingWithRetry(w, r, turnID, plan, now)
	}
	return a.forwardNonStreamingWithRetry(w, r, turnID, plan, now)
}

func (a *App) forwardNonStreamingWithRetry(w http.ResponseWriter, r *http.Request, turnID string, plan routePlan, now time.Time) (forwardResult, routePlan, error) {
	ctx, span := a.telemetry.Tracer.Start(r.Context(), "gateway.provider.invoke")
	defer span.End()
	maxAttempts := a.retryMaxAttempts()
	excluded := map[string]struct{}{}
	var lastFailure upstreamFailure
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		success, failure, err := a.forwardNonStreamingAttempt(ctx, r, turnID, plan, now)
		if err != nil {
			return forwardResult{}, plan, err
		}
		if failure.ErrorCode == "" {
			copyHeaders(w.Header(), success.Header)
			w.WriteHeader(success.StatusCode)
			_, _ = w.Write(success.Body)
			if err := a.upsertResponseCarriers(ctx, turnID, plan, now, extractResponseCarriers(success.Body)); err != nil {
				a.logger.Error("upsert carrier bindings", logging.Err(err), logging.String("turn_id", turnID))
			}
			if plan.Mode == "replay" {
				_ = a.sqlite.InsertReplayEvent(ctx, ids.New("replay"), turnID, turnID, plan.LineageSessionID, "", plan.Account.AccountID, plan.ReasonCode, strings.Join(plan.RemovedCarrierKinds, ","), plan.RemovedCarrierCount, now)
				span.AddEvent("replay.completed", eventAttrs(turnID, plan.LineageSessionID, plan.Account.AccountID, success.StatusCode, plan.ReasonCode))
			}
			return forwardResult{StatusCode: success.StatusCode, ResponseItems: success.ResponseItems, StreamState: "completed"}, plan, nil
		}
		lastFailure = failure
		excluded[plan.Account.AccountID] = struct{}{}
		retryable := failure.Retryable && attempt < maxAttempts
		var nextPlan routePlan
		if retryable {
			var ok bool
			var err error
			nextPlan, ok, err = a.nextRetryPlan(ctx, plan, excluded, failure, now)
			if err != nil {
				return forwardResult{}, plan, err
			}
			retryable = ok
		}
		nextAccountID := ""
		if retryable {
			nextAccountID = nextPlan.Account.AccountID
		}
		a.recordRetryFailure(ctx, turnID, plan, attempt, maxAttempts, failure, retryable, nextAccountID, now)
		if !retryable {
			a.writeUpstreamFailure(w, failure)
			return forwardResult{StatusCode: failure.StatusCode, ErrorCode: failure.ErrorCode, ErrorMessage: failure.ErrorMessage, StreamState: "error"}, plan, nil
		}
		span.AddEvent("retry.scheduled", eventAttrs(turnID, plan.LineageSessionID, plan.Account.AccountID, failure.StatusCode, failure.ErrorCode))
		if a.cfg.RetryBackoff > 0 {
			select {
			case <-time.After(a.cfg.RetryBackoff):
			case <-ctx.Done():
				cancelFailure := upstreamFailure{StatusCode: http.StatusBadGateway, ErrorCode: "client_cancelled", ErrorMessage: ctx.Err().Error(), Retryable: false}
				a.writeUpstreamFailure(w, cancelFailure)
				return forwardResult{StatusCode: cancelFailure.StatusCode, ErrorCode: cancelFailure.ErrorCode, ErrorMessage: cancelFailure.ErrorMessage, StreamState: "error"}, plan, nil
			}
		}
		plan = nextPlan
		if err := a.sqlite.UpsertLineageBinding(ctx, plan.LineageSessionID, plan.Account, turnID, time.Now().UTC()); err != nil {
			return forwardResult{}, plan, err
		}
	}
	a.writeUpstreamFailure(w, lastFailure)
	return forwardResult{StatusCode: lastFailure.StatusCode, ErrorCode: lastFailure.ErrorCode, ErrorMessage: lastFailure.ErrorMessage, StreamState: "error"}, plan, nil
}

func (a *App) forwardNonStreamingAttempt(ctx context.Context, r *http.Request, turnID string, plan routePlan, now time.Time) (upstreamSuccess, upstreamFailure, error) {
	ctx, span := a.telemetry.Tracer.Start(r.Context(), "gateway.provider.invoke")
	defer span.End()
	upstreamURL := fmt.Sprintf("http://%s:%d/v1/responses", plan.Account.DownstreamHost, plan.Account.DownstreamPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(plan.ForwardBody))
	if err != nil {
		return upstreamSuccess{}, upstreamFailure{}, err
	}
	copyHeaders(req.Header, r.Header)
	a.telemetry.Metrics.UpstreamRequests.Add(ctx, 1, observability.AddAttrs(attribute.String("gateway.account.id", plan.Account.AccountID), attribute.String("gateway.route.mode", plan.Mode)))
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		a.cooldownForStatus(ctx, plan.Account.AccountID, http.StatusBadGateway, "upstream_request_failed", now)
		span.RecordError(err)
		span.SetStatus(codes.Error, "upstream request failed")
		return upstreamSuccess{}, upstreamFailure{StatusCode: http.StatusBadGateway, ErrorCode: "upstream_request_failed", ErrorMessage: err.Error(), Retryable: true}, nil
	}
	defer resp.Body.Close()
	defer a.telemetry.Metrics.UpstreamDuration.Record(ctx, observability.MsSince(start), observability.RecordAttrs(attribute.String("gateway.account.id", plan.Account.AccountID), attribute.String("gateway.route.mode", plan.Mode)))

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		a.cooldownForStatus(ctx, plan.Account.AccountID, http.StatusBadGateway, "upstream_read_failed", now)
		return upstreamSuccess{}, upstreamFailure{StatusCode: http.StatusBadGateway, ErrorCode: "upstream_read_failed", ErrorMessage: err.Error(), Retryable: true}, nil
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		a.cooldownForStatus(ctx, plan.Account.AccountID, http.StatusBadGateway, "invalid_upstream_json", now)
		return upstreamSuccess{}, upstreamFailure{StatusCode: http.StatusBadGateway, ErrorCode: "invalid_upstream_json", ErrorMessage: bodySnippet(body), Body: body, Header: resp.Header.Clone(), Retryable: true}, nil
	}
	if resp.StatusCode >= 400 {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		a.cooldownForStatus(ctx, plan.Account.AccountID, resp.StatusCode, "upstream_rejected", now)
		return upstreamSuccess{}, upstreamFailure{StatusCode: resp.StatusCode, ErrorCode: "upstream_rejected", ErrorMessage: bodySnippet(body), Header: resp.Header.Clone(), Body: body, Retryable: isRetryableStatus(resp.StatusCode), PassThrough: true}, nil
	}
	return upstreamSuccess{StatusCode: resp.StatusCode, Header: resp.Header.Clone(), Body: body, Raw: raw, ResponseItems: responses.ResponseItems(raw)}, upstreamFailure{}, nil
}

func (a *App) forwardStreamingWithRetry(w http.ResponseWriter, r *http.Request, turnID string, plan routePlan, now time.Time) (forwardResult, routePlan, error) {
	ctx := r.Context()
	maxAttempts := a.retryMaxAttempts()
	excluded := map[string]struct{}{}
	var lastFailure upstreamFailure
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, failure, wrote, err := a.forwardStreamingAttempt(w, r, turnID, plan, now)
		if err != nil {
			return forwardResult{}, plan, err
		}
		if failure.ErrorCode == "" {
			return result, plan, nil
		}
		lastFailure = failure
		excluded[plan.Account.AccountID] = struct{}{}
		retryable := !wrote && failure.Retryable && attempt < maxAttempts
		var nextPlan routePlan
		if retryable {
			var ok bool
			var err error
			nextPlan, ok, err = a.nextRetryPlan(ctx, plan, excluded, failure, now)
			if err != nil {
				return forwardResult{}, plan, err
			}
			retryable = ok
		}
		nextAccountID := ""
		if retryable {
			nextAccountID = nextPlan.Account.AccountID
		}
		a.recordRetryFailure(ctx, turnID, plan, attempt, maxAttempts, failure, retryable, nextAccountID, now)
		if !retryable {
			if !wrote {
				a.writeUpstreamFailure(w, failure)
			}
			return forwardResult{StatusCode: failure.StatusCode, ErrorCode: failure.ErrorCode, ErrorMessage: failure.ErrorMessage, StreamState: "error"}, plan, nil
		}
		if a.cfg.RetryBackoff > 0 {
			select {
			case <-time.After(a.cfg.RetryBackoff):
			case <-ctx.Done():
				cancelFailure := upstreamFailure{StatusCode: http.StatusBadGateway, ErrorCode: "client_cancelled", ErrorMessage: ctx.Err().Error(), Retryable: false}
				a.writeUpstreamFailure(w, cancelFailure)
				return forwardResult{StatusCode: cancelFailure.StatusCode, ErrorCode: cancelFailure.ErrorCode, ErrorMessage: cancelFailure.ErrorMessage, StreamState: "error"}, plan, nil
			}
		}
		plan = nextPlan
		if err := a.sqlite.UpsertLineageBinding(ctx, plan.LineageSessionID, plan.Account, turnID, time.Now().UTC()); err != nil {
			return forwardResult{}, plan, err
		}
	}
	a.writeUpstreamFailure(w, lastFailure)
	return forwardResult{StatusCode: lastFailure.StatusCode, ErrorCode: lastFailure.ErrorCode, ErrorMessage: lastFailure.ErrorMessage, StreamState: "error"}, plan, nil
}

func (a *App) forwardStreamingAttempt(w http.ResponseWriter, r *http.Request, turnID string, plan routePlan, now time.Time) (forwardResult, upstreamFailure, bool, error) {
	ctx, span := a.telemetry.Tracer.Start(r.Context(), "gateway.provider.invoke")
	defer span.End()
	upstreamURL := fmt.Sprintf("http://%s:%d/v1/responses", plan.Account.DownstreamHost, plan.Account.DownstreamPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(plan.ForwardBody))
	if err != nil {
		return forwardResult{}, upstreamFailure{}, false, err
	}
	copyHeaders(req.Header, r.Header)
	a.telemetry.Metrics.UpstreamRequests.Add(ctx, 1, observability.AddAttrs(attribute.String("gateway.account.id", plan.Account.AccountID), attribute.String("gateway.route.mode", plan.Mode)))
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		a.cooldownForStatus(ctx, plan.Account.AccountID, http.StatusBadGateway, "upstream_request_failed", now)
		span.RecordError(err)
		span.SetStatus(codes.Error, "upstream request failed")
		return forwardResult{}, upstreamFailure{StatusCode: http.StatusBadGateway, ErrorCode: "upstream_request_failed", ErrorMessage: err.Error(), Retryable: true}, false, nil
	}
	defer resp.Body.Close()
	defer a.telemetry.Metrics.UpstreamDuration.Record(ctx, observability.MsSince(start), observability.RecordAttrs(attribute.String("gateway.account.id", plan.Account.AccountID), attribute.String("gateway.route.mode", plan.Mode)))
	if isRetryableStatus(resp.StatusCode) {
		body, _ := io.ReadAll(resp.Body)
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
		a.cooldownForStatus(ctx, plan.Account.AccountID, resp.StatusCode, "upstream_rejected", now)
		return forwardResult{}, upstreamFailure{StatusCode: resp.StatusCode, ErrorCode: "upstream_rejected", ErrorMessage: bodySnippet(body), Header: resp.Header.Clone(), Body: body, Retryable: true, PassThrough: true}, false, nil
	}
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	result, err := a.forwardStreamingResponses(w, resp, ctx, turnID, plan, now)
	if result.StatusCode >= 400 {
		a.telemetry.Metrics.UpstreamFailures.Add(ctx, 1)
	}
	if result.ErrorCode != "" {
		return result, upstreamFailure{StatusCode: result.StatusCode, ErrorCode: result.ErrorCode, ErrorMessage: result.ErrorMessage, Retryable: false}, true, err
	}
	return result, upstreamFailure{}, true, err
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
		a.cooldownForStatus(ctx, plan.Account.AccountID, resp.StatusCode, "upstream_rejected", now)
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

func (a *App) cooldownForStatus(ctx context.Context, accountID string, status int, reason string, now time.Time) {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		until := now.Add(5 * time.Minute)
		if err := a.sqlite.CooldownAccount(ctx, accountID, fmt.Sprintf("%s_%d", reason, status), until, now); err != nil {
			a.logger.Error("cooldown account", logging.Err(err), logging.String("account_id", accountID), logging.Int64("http_status", int64(status)))
		}
	}
}

func (a *App) retryMaxAttempts() int {
	if a.cfg.RetryMaxAttempts < 1 {
		return 1
	}
	return a.cfg.RetryMaxAttempts
}

func (a *App) nextRetryPlan(ctx context.Context, plan routePlan, excluded map[string]struct{}, failure upstreamFailure, now time.Time) (routePlan, bool, error) {
	if plan.Mode == "strict" {
		if !a.cfg.StrictReplayRetry || !a.cfg.DefaultReplayEnabled {
			return routePlan{}, false, nil
		}
		stripped, removedKinds, removedCount := responses.StripCarriers(plan.ForwardMap)
		account, err := a.sqlite.SelectLeastActiveAccountExcluding(ctx, stringValue(plan.ForwardMap, "model"), now, excluded)
		if errors.Is(err, sql.ErrNoRows) {
			return routePlan{}, false, nil
		}
		if err != nil {
			return routePlan{}, false, err
		}
		return routePlan{
			Mode:                "replay",
			ReasonCode:          "strict_owner_retry_replay",
			ReasonDetail:        failure.ErrorCode,
			Account:             account,
			LineageSessionID:    ids.New("lineage"),
			HasRealCarrier:      true,
			CarrierKinds:        plan.CarrierKinds,
			RemovedCarrierKinds: removedKinds,
			RemovedCarrierCount: removedCount,
			ForwardBody:         mustJSON(stripped),
			ForwardMap:          stripped,
		}, true, nil
	}
	account, err := a.sqlite.SelectLeastActiveAccountExcluding(ctx, stringValue(plan.ForwardMap, "model"), now, excluded)
	if errors.Is(err, sql.ErrNoRows) {
		return routePlan{}, false, nil
	}
	if err != nil {
		return routePlan{}, false, err
	}
	next := plan
	next.Account = account
	next.ForwardBody = mustJSON(plan.ForwardMap)
	return next, true, nil
}

func (a *App) recordRetryFailure(ctx context.Context, turnID string, plan routePlan, attempt, maxAttempts int, failure upstreamFailure, retryable bool, nextAccountID string, now time.Time) {
	_ = a.sqlite.InsertRoutingFailure(ctx, ids.New("failure"), turnID, plan.LineageSessionID, plan.Account.AccountID, failure.ErrorCode, failure.ErrorMessage, failure.StatusCode, now)
	_ = a.sqlite.InsertRetryAttempt(ctx, sqlitestore.RetryAttempt{
		RetryAttemptID:   ids.New("retry"),
		TurnID:           turnID,
		LineageSessionID: plan.LineageSessionID,
		Attempt:          attempt,
		MaxAttempts:      maxAttempts,
		AccountID:        plan.Account.AccountID,
		RouteMode:        plan.Mode,
		ReasonCode:       failure.ErrorCode,
		ReasonDetail:     failure.ErrorMessage,
		HTTPStatus:       failure.StatusCode,
		Retryable:        retryable,
		NextAccountID:    nextAccountID,
		CreatedAt:        now,
	})
	a.telemetry.Metrics.RetryAttempts.Add(ctx, 1, observability.AddAttrs(
		attribute.String("gateway.account.id", plan.Account.AccountID),
		attribute.String("gateway.route.mode", plan.Mode),
		attribute.String("gateway.retry.reason_code", failure.ErrorCode),
		attribute.Bool("gateway.retry.retryable", retryable),
	))
	a.logger.Info("upstream attempt failed",
		logging.String("turn_id", turnID),
		logging.String("lineage_session_id", plan.LineageSessionID),
		logging.String("account_id", plan.Account.AccountID),
		logging.String("reason_code", failure.ErrorCode),
		logging.Int64("http_status", int64(failure.StatusCode)),
		logging.Int64("attempt", int64(attempt)),
		logging.Int64("max_attempts", int64(maxAttempts)),
		logging.Bool("retryable", retryable),
		logging.String("next_account_id", nextAccountID),
	)
}

func (a *App) writeUpstreamFailure(w http.ResponseWriter, failure upstreamFailure) {
	if failure.PassThrough {
		copyHeaders(w.Header(), failure.Header)
		w.WriteHeader(failure.StatusCode)
		_, _ = w.Write(failure.Body)
		return
	}
	status := failure.StatusCode
	if status == 0 {
		status = http.StatusBadGateway
	}
	writeJSONError(w, status, failure.ErrorCode, failure.ErrorMessage)
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func bodySnippet(body []byte) string {
	const max = 2048
	s := strings.TrimSpace(string(body))
	if len(s) > max {
		return s[:max]
	}
	return s
}

func turnMetaFromPlan(turnID string, plan routePlan) sqlitestore.TurnMeta {
	return sqlitestore.TurnMeta{
		TurnID:              turnID,
		LineageSessionID:    plan.LineageSessionID,
		LineageGeneration:   plan.LineageGeneration,
		RouteMode:           plan.Mode,
		Model:               stringValue(plan.ForwardMap, "model"),
		AccountID:           plan.Account.AccountID,
		DownstreamHost:      plan.Account.DownstreamHost,
		DownstreamPort:      plan.Account.DownstreamPort,
		HasRealCarrier:      plan.HasRealCarrier,
		CarrierKinds:        strings.Join(plan.CarrierKinds, ","),
		CarrierRemoved:      plan.RemovedCarrierCount > 0,
		RemovedCarrierKinds: strings.Join(plan.RemovedCarrierKinds, ","),
		RemovedCarrierCount: plan.RemovedCarrierCount,
	}
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
		case "Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade", "Host", "Content-Length", "Authorization", "Cookie", "X-Llm-Gateway-Token":
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
