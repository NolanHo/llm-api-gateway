package observability

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	stdouttrace "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Telemetry struct {
	Tracer  trace.Tracer
	Meter   metric.Meter
	Metrics *Metrics
	Handler http.Handler
	closeFn func(context.Context) error
}

type Metrics struct {
	UpstreamRequests      metric.Int64Counter
	UpstreamFailures      metric.Int64Counter
	ReplayTotal           metric.Int64Counter
	CarrierWrites         metric.Int64Counter
	ArchiveFailures       metric.Int64Counter
	UpstreamDuration      metric.Float64Histogram
	ArchiveDuration       metric.Float64Histogram
	EnabledAccounts       metric.Int64ObservableGauge
	RetainedLineages      metric.Int64ObservableGauge
	LineageBindings       metric.Int64ObservableGauge
	ActiveSessions        metric.Int64ObservableGauge
	ActiveCarriers        metric.Int64ObservableGauge
	RecentReplays         metric.Int64ObservableGauge
	RecentTurns           metric.Int64ObservableGauge
	RecentRoutingFailures metric.Int64ObservableGauge
	AccountEnabled        metric.Int64ObservableGauge
	AccountCooldownUntil  metric.Int64ObservableGauge
	RecentSuccesses       metric.Int64ObservableGauge
	RecentFailures        metric.Int64ObservableGauge
	RecentStatusCodes     metric.Int64ObservableGauge
	Recent30mReplays      metric.Int64ObservableGauge
	Recent30mTurns        metric.Int64ObservableGauge
	FailureRate           metric.Float64ObservableGauge
	ReplayRate30m         metric.Float64ObservableGauge
	runtimeGaugeCallback  metric.Registration
}

func (m *Metrics) RegisterRuntimeGauges(meter metric.Meter, callback metric.Callback) error {
	registration, err := meter.RegisterCallback(callback,
		m.EnabledAccounts,
		m.RetainedLineages,
		m.LineageBindings,
		m.ActiveSessions,
		m.ActiveCarriers,
		m.RecentReplays,
		m.RecentTurns,
		m.RecentRoutingFailures,
		m.AccountEnabled,
		m.AccountCooldownUntil,
		m.RecentSuccesses,
		m.RecentFailures,
		m.RecentStatusCodes,
		m.Recent30mReplays,
		m.Recent30mTurns,
		m.FailureRate,
		m.ReplayRate30m,
	)
	if err != nil {
		return err
	}
	m.runtimeGaugeCallback = registration
	return nil
}

func (m *Metrics) Close() error {
	if m == nil || m.runtimeGaugeCallback == nil {
		return nil
	}
	return m.runtimeGaugeCallback.Unregister()
}

func Init(ctx context.Context, serviceName string, traceStdout bool) (*Telemetry, error) {
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		"",
		attribute.String("service.name", serviceName),
	))
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	prom, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(prom), sdkmetric.WithResource(res))
	otel.SetMeterProvider(meterProvider)

	var traceProvider *tracesdk.TracerProvider
	if traceStdout {
		exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdout trace exporter: %w", err)
		}
		traceProvider = tracesdk.NewTracerProvider(tracesdk.WithBatcher(exporter), tracesdk.WithResource(res))
	} else {
		traceProvider = tracesdk.NewTracerProvider(tracesdk.WithResource(res))
	}
	otel.SetTracerProvider(traceProvider)

	meter := meterProvider.Meter(serviceName)
	metrics, err := newMetrics(meter)
	if err != nil {
		return nil, err
	}
	return &Telemetry{
		Tracer:  traceProvider.Tracer(serviceName),
		Meter:   meter,
		Metrics: metrics,
		Handler: promhttp.Handler(),
		closeFn: func(ctx context.Context) error {
			if err := traceProvider.Shutdown(ctx); err != nil {
				return err
			}
			return meterProvider.Shutdown(ctx)
		},
	}, nil
}

func newMetrics(meter metric.Meter) (*Metrics, error) {
	upstreamRequests, err := meter.Int64Counter("gateway_upstream_requests_total")
	if err != nil {
		return nil, err
	}
	upstreamFailures, err := meter.Int64Counter("gateway_upstream_failures_total")
	if err != nil {
		return nil, err
	}
	replayTotal, err := meter.Int64Counter("gateway_replay_total")
	if err != nil {
		return nil, err
	}
	carrierWrites, err := meter.Int64Counter("gateway_carrier_writes_total")
	if err != nil {
		return nil, err
	}
	archiveFailures, err := meter.Int64Counter("gateway_archive_failures_total")
	if err != nil {
		return nil, err
	}
	upstreamDuration, err := meter.Float64Histogram("gateway_upstream_duration_ms")
	if err != nil {
		return nil, err
	}
	archiveDuration, err := meter.Float64Histogram("gateway_archive_duration_ms")
	if err != nil {
		return nil, err
	}
	enabledAccounts, err := meter.Int64ObservableGauge("gateway_enabled_accounts")
	if err != nil {
		return nil, err
	}
	retainedLineages, err := meter.Int64ObservableGauge("gateway_retained_lineages")
	if err != nil {
		return nil, err
	}
	lineageBindings, err := meter.Int64ObservableGauge("gateway_lineage_bindings")
	if err != nil {
		return nil, err
	}
	activeSessions, err := meter.Int64ObservableGauge("gateway_active_sessions")
	if err != nil {
		return nil, err
	}
	activeCarriers, err := meter.Int64ObservableGauge("gateway_active_carriers")
	if err != nil {
		return nil, err
	}
	recentReplays, err := meter.Int64ObservableGauge("gateway_recent_replays")
	if err != nil {
		return nil, err
	}
	recentTurns, err := meter.Int64ObservableGauge("gateway_recent_turns")
	if err != nil {
		return nil, err
	}
	recentRoutingFailures, err := meter.Int64ObservableGauge("gateway_recent_routing_failures")
	if err != nil {
		return nil, err
	}
	accountEnabled, err := meter.Int64ObservableGauge("gateway_account_enabled")
	if err != nil {
		return nil, err
	}
	accountCooldownUntil, err := meter.Int64ObservableGauge("gateway_account_cooldown_until_ms")
	if err != nil {
		return nil, err
	}
	recentSuccesses, err := meter.Int64ObservableGauge("gateway_recent_30m_successes")
	if err != nil {
		return nil, err
	}
	recentFailures, err := meter.Int64ObservableGauge("gateway_recent_30m_failures")
	if err != nil {
		return nil, err
	}
	recentStatusCodes, err := meter.Int64ObservableGauge("gateway_recent_30m_status_codes")
	if err != nil {
		return nil, err
	}
	recent30mReplays, err := meter.Int64ObservableGauge("gateway_recent_30m_replays")
	if err != nil {
		return nil, err
	}
	recent30mTurns, err := meter.Int64ObservableGauge("gateway_recent_30m_turns")
	if err != nil {
		return nil, err
	}
	failureRate, err := meter.Float64ObservableGauge("gateway_recent_30m_failure_rate")
	if err != nil {
		return nil, err
	}
	replayRate30m, err := meter.Float64ObservableGauge("gateway_recent_30m_replay_rate")
	if err != nil {
		return nil, err
	}
	return &Metrics{
		UpstreamRequests:      upstreamRequests,
		UpstreamFailures:      upstreamFailures,
		ReplayTotal:           replayTotal,
		CarrierWrites:         carrierWrites,
		ArchiveFailures:       archiveFailures,
		UpstreamDuration:      upstreamDuration,
		ArchiveDuration:       archiveDuration,
		EnabledAccounts:       enabledAccounts,
		RetainedLineages:      retainedLineages,
		LineageBindings:       lineageBindings,
		ActiveSessions:        activeSessions,
		ActiveCarriers:        activeCarriers,
		RecentReplays:         recentReplays,
		RecentTurns:           recentTurns,
		RecentRoutingFailures: recentRoutingFailures,
		AccountEnabled:        accountEnabled,
		AccountCooldownUntil:  accountCooldownUntil,
		RecentSuccesses:       recentSuccesses,
		RecentFailures:        recentFailures,
		RecentStatusCodes:     recentStatusCodes,
		Recent30mReplays:      recent30mReplays,
		Recent30mTurns:        recent30mTurns,
		FailureRate:           failureRate,
		ReplayRate30m:         replayRate30m,
	}, nil
}

func (t *Telemetry) Close(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.Metrics != nil {
		if err := t.Metrics.Close(); err != nil {
			return err
		}
	}
	if t.closeFn == nil {
		return nil
	}
	return t.closeFn(ctx)
}

func AddAttrs(kv ...attribute.KeyValue) metric.AddOption {
	return metric.WithAttributes(kv...)
}

func RecordAttrs(kv ...attribute.KeyValue) metric.RecordOption {
	return metric.WithAttributes(kv...)
}

func ObserveAttrs(kv ...attribute.KeyValue) metric.ObserveOption {
	return metric.WithAttributes(kv...)
}

func MsSince(start time.Time) float64 { return float64(time.Since(start).Milliseconds()) }
