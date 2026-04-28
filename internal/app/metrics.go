package app

import (
	"context"
	"time"

	"github.com/nolanho/llm-api-gateway/internal/observability"
	"github.com/nolanho/llm-api-gateway/internal/storage/sqlitestore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const metricsLookback = 24 * time.Hour

func (a *App) registerRuntimeMetrics() error {
	m := a.telemetry.Metrics
	return m.RegisterRuntimeGauges(a.telemetry.Meter, func(ctx context.Context, o metric.Observer) error {
		snapshot, err := a.sqlite.MonitoringSnapshot(ctx, time.Now().UTC(), metricsLookback)
		if err != nil {
			return err
		}
		observeGlobalMetrics(o, m, snapshot)
		for _, account := range snapshot.Accounts {
			observeAccountMetrics(o, m, account)
		}
		for _, failure := range snapshot.RecentFailures {
			o.ObserveInt64(m.RecentRoutingFailures, failure.Count, observability.ObserveAttrs(attribute.String("reason_code", failure.Label)))
		}
		return nil
	})
}

func observeGlobalMetrics(o metric.Observer, m *observability.Metrics, snapshot sqlitestore.MonitoringSnapshot) {
	o.ObserveInt64(m.EnabledAccounts, snapshot.Global.EnabledAccounts)
	o.ObserveInt64(m.RetainedLineages, snapshot.Global.RetainedLineages)
	o.ObserveInt64(m.LineageBindings, snapshot.Global.ActiveLineages, observability.ObserveAttrs(attribute.String("status", "active")))
	o.ObserveInt64(m.LineageBindings, snapshot.Global.InactiveLineages, observability.ObserveAttrs(attribute.String("status", "inactive")))
	o.ObserveInt64(m.ActiveSessions, snapshot.Global.ActiveSessions)
	o.ObserveInt64(m.ActiveCarriers, snapshot.Global.ActiveCarriers)
	o.ObserveInt64(m.RecentReplays, snapshot.Global.RecentReplays)
	o.ObserveInt64(m.RecentTurns, snapshot.Global.RecentTurns)
	o.ObserveInt64(m.RecentRoutingFailures, snapshot.Global.RecentRoutingFailures)
	o.ObserveInt64(m.RecentSuccesses, snapshot.Global.RecentSuccesses)
	o.ObserveInt64(m.RecentFailures, snapshot.Global.RecentFailures)
	o.ObserveInt64(m.Recent30mTurns, snapshot.Global.Recent30mTurns)
	o.ObserveInt64(m.Recent30mReplays, snapshot.Global.Recent30mReplays)
	o.ObserveFloat64(m.FailureRate, snapshot.Global.FailureRate)
	o.ObserveFloat64(m.ReplayRate30m, snapshot.Global.ReplayRate30m)
}

func observeAccountMetrics(o metric.Observer, m *observability.Metrics, account sqlitestore.AccountMonitoringMetric) {
	attrs := observability.ObserveAttrs(
		attribute.String("account_id", account.AccountID),
		attribute.String("downstream_host", account.DownstreamHost),
		attribute.Int("downstream_port", account.DownstreamPort),
	)
	enabled := int64(0)
	if account.Enabled {
		enabled = 1
	}
	o.ObserveInt64(m.AccountEnabled, enabled, attrs)
	o.ObserveInt64(m.AccountCooldownUntil, account.CooldownUntilMS, attrs)
	o.ObserveInt64(m.ActiveSessions, account.ActiveSessions, attrs)
	o.ObserveInt64(m.ActiveCarriers, account.ActiveCarriers, attrs)
	o.ObserveInt64(m.RecentReplays, account.RecentReplays, attrs)
	o.ObserveInt64(m.RecentTurns, account.RecentTurns, attrs)
	o.ObserveInt64(m.RecentRoutingFailures, account.RecentFailures, attrs)
	o.ObserveInt64(m.RecentSuccesses, account.RecentSuccesses, attrs)
	o.ObserveInt64(m.RecentFailures, account.RecentFailures, attrs)
	o.ObserveInt64(m.Recent30mTurns, account.Recent30mTurns, attrs)
	o.ObserveInt64(m.Recent30mReplays, account.Recent30mReplays, attrs)
	o.ObserveFloat64(m.FailureRate, account.FailureRate, attrs)
	o.ObserveFloat64(m.ReplayRate30m, account.ReplayRate30m, attrs)

	for _, x := range account.StatusCodes {
		o.ObserveInt64(m.RecentStatusCodes, x.Count, observability.ObserveAttrs(
			attribute.String("account_id", account.AccountID),
			attribute.String("downstream_host", account.DownstreamHost),
			attribute.Int("downstream_port", account.DownstreamPort),
			attribute.String("status_code", x.Label),
		))
	}

	for _, x := range account.LineageStatuses {
		o.ObserveInt64(m.LineageBindings, x.Count, observability.ObserveAttrs(
			attribute.String("account_id", account.AccountID),
			attribute.String("downstream_host", account.DownstreamHost),
			attribute.Int("downstream_port", account.DownstreamPort),
			attribute.String("status", x.Label),
		))
	}
	for _, x := range account.CarrierKinds {
		o.ObserveInt64(m.ActiveCarriers, x.Count, observability.ObserveAttrs(
			attribute.String("account_id", account.AccountID),
			attribute.String("downstream_host", account.DownstreamHost),
			attribute.Int("downstream_port", account.DownstreamPort),
			attribute.String("carrier_kind", x.Label),
		))
	}
	for _, x := range account.ReplayReasons {
		o.ObserveInt64(m.RecentReplays, x.Count, observability.ObserveAttrs(
			attribute.String("account_id", account.AccountID),
			attribute.String("downstream_host", account.DownstreamHost),
			attribute.Int("downstream_port", account.DownstreamPort),
			attribute.String("replay_reason", x.Label),
		))
	}
	for _, x := range account.RouteModes {
		o.ObserveInt64(m.RecentTurns, x.Count, observability.ObserveAttrs(
			attribute.String("account_id", account.AccountID),
			attribute.String("downstream_host", account.DownstreamHost),
			attribute.Int("downstream_port", account.DownstreamPort),
			attribute.String("route_mode", x.Label),
		))
	}
	for _, x := range account.FailureReasons {
		o.ObserveInt64(m.RecentRoutingFailures, x.Count, observability.ObserveAttrs(
			attribute.String("account_id", account.AccountID),
			attribute.String("downstream_host", account.DownstreamHost),
			attribute.Int("downstream_port", account.DownstreamPort),
			attribute.String("reason_code", x.Label),
		))
	}
}
