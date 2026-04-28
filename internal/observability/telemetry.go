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
	UpstreamRequests metric.Int64Counter
	UpstreamFailures metric.Int64Counter
	ReplayTotal      metric.Int64Counter
	CarrierWrites    metric.Int64Counter
	ArchiveFailures  metric.Int64Counter
	UpstreamDuration metric.Float64Histogram
	ArchiveDuration  metric.Float64Histogram
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
	return &Metrics{upstreamRequests, upstreamFailures, replayTotal, carrierWrites, archiveFailures, upstreamDuration, archiveDuration}, nil
}

func (t *Telemetry) Close(ctx context.Context) error {
	if t == nil || t.closeFn == nil {
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

func MsSince(start time.Time) float64 { return float64(time.Since(start).Milliseconds()) }
