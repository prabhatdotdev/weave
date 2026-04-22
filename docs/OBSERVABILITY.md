# Observability Guide

This guide documents the current observability surface in Weave and shows how to wire it into production logging, metrics, and tracing systems.

## Surface Area

Weave exposes six optional observability and health integrations on `Config`:

```go
type Config struct {
    Logger         weave.EventLogger
    EventHook      weave.EventHook
    Metrics        weave.MetricsHook
    Tracing        weave.TracingHook
    HealthReporter weave.HealthReporter
    HealthHook     weave.HealthHook
}
```

Use them this way:

- `Logger`: send structured events to your log sink.
- `EventHook`: lightweight callback for side effects or custom routing.
- `Metrics`: bridge counters and durations into your metrics backend.
- `Tracing`: create spans around high-level `Client` and `Server` operations.
- `HealthReporter`: send lifecycle health snapshots to a reporting sink.
- `HealthHook`: receive runtime health snapshots when client/server lifecycle state changes.

## Structured Events

The runtime and transport layers emit structured events with:

- `Level`
- `Name`
- `Backend`
- `Component`
- `Operation`
- `Destination`
- `CorrelationID`
- `Err`
- `Fields`

Standard event names include:

- `weave.EventConnect`
- `weave.EventDisconnect`
- `weave.EventPublishFailed`
- `weave.EventSubscribeFailed`
- `weave.EventTimeout`

## Metrics Conventions

Weave emits counters and durations with stable names and labels. Current names include:

- `weave.runtime.connect.success`
- `weave.runtime.connect.failures`
- `weave.runtime.disconnect.events`
- `weave.runtime.subscribe.failures`
- `weave.runtime.call.timeouts`
- `weave.runtime.call.duration`
- `weave.transport.connect.success`
- `weave.transport.connect.failures`
- `weave.transport.connect.duration`
- `weave.transport.disconnect.events`
- `weave.transport.publish.failures`
- `weave.transport.subscribe.failures`
- `weave.transport.call.timeouts`
- `weave.transport.call.duration`

Common labels include:

- `backend`
- `role`
- `destination`
- `outcome`
- `reason`
- `stage`

Recommended dashboard slices:

- success vs failure by `backend`
- RPC latency by `destination`
- timeout count by `destination`
- subscribe failure count by `destination`
- disconnect events by `backend` and `reason`

## Tracing Spans

The tracing hook currently wraps high-level runtime operations:

- `weave.runtime.client.connect`
- `weave.runtime.client.publish`
- `weave.runtime.client.call`
- `weave.runtime.client.close`
- `weave.runtime.server.start`
- `weave.runtime.server.publish`
- `weave.runtime.server.call`
- `weave.runtime.server.stop`

Each span includes the operation metadata Weave already knows, such as:

- backend
- component
- operation
- destination when applicable
- correlation ID when present

## Example: `log/slog` Logger

```go
type SlogLogger struct {
    logger *slog.Logger
}

func (l SlogLogger) LogEvent(ctx context.Context, event weave.Event) {
    attrs := []any{
        "timestamp", event.Timestamp,
        "name", event.Name,
        "backend", event.Backend,
        "component", event.Component,
        "operation", event.Operation,
        "destination", event.Destination,
        "correlation_id", event.CorrelationID,
    }
    if event.Err != nil {
        attrs = append(attrs, "error", event.Err.Error())
    }
    for key, value := range event.Fields {
        attrs = append(attrs, key, value)
    }
    l.logger.Log(ctx, slogLevel(event.Level), "weave event", attrs...)
}
```

## Example: Prometheus Metrics Adapter

```go
type PromMetrics struct {
    counters  *prometheus.CounterVec
    durations *prometheus.HistogramVec
}

func (m *PromMetrics) IncrementCounter(name string, value float64, labels map[string]string) {
    m.counters.WithLabelValues(
        name,
        labels["backend"],
        labels["role"],
        labels["destination"],
        labels["outcome"],
        labels["reason"],
        labels["stage"],
    ).Add(value)
}

func (m *PromMetrics) ObserveDuration(name string, value time.Duration, labels map[string]string) {
    m.durations.WithLabelValues(
        name,
        labels["backend"],
        labels["role"],
        labels["destination"],
        labels["outcome"],
    ).Observe(value.Seconds())
}
```

Recommended Prometheus recording rules:

- alert on sustained `weave.runtime.call.timeouts`
- alert on spikes in `weave.transport.connect.failures`
- graph `weave.runtime.call.duration` p50/p95/p99 by destination

## Example: OpenTelemetry Tracing Adapter

```go
type OTelTracing struct {
    tracer trace.Tracer
}

func (o OTelTracing) StartSpan(ctx context.Context, start weave.TraceSpanStart) (context.Context, weave.TraceSpan) {
    attrs := []attribute.KeyValue{
        attribute.String("messaging.system", start.Backend),
        attribute.String("weave.component", start.Component),
        attribute.String("weave.operation", start.Operation),
    }
    if start.Destination != "" {
        attrs = append(attrs, attribute.String("messaging.destination", start.Destination))
    }
    if start.CorrelationID != "" {
        attrs = append(attrs, attribute.String("messaging.conversation_id", start.CorrelationID))
    }

    ctx, span := o.tracer.Start(ctx, start.Name, trace.WithAttributes(attrs...))
    return ctx, otelSpan{span: span}
}

type otelSpan struct {
    span trace.Span
}

func (s otelSpan) Finish(finish weave.TraceSpanFinish) {
    if finish.Err != nil {
        s.span.RecordError(finish.Err)
        s.span.SetStatus(codes.Error, finish.Err.Error())
    }
    for key, value := range finish.Attributes {
        s.span.SetAttributes(attribute.String("weave."+key, value))
    }
    s.span.End()
}
```

## Example Configuration

```go
config := weave.DefaultConfig()
config.Logger = SlogLogger{logger: slog.Default()}
config.Metrics = &PromMetrics{
    counters:  weaveCounters,
    durations: weaveDurations,
}
config.Tracing = OTelTracing{
    tracer: otel.Tracer("github.com/prabhatdotdev/weave"),
}
config.HealthHook = func(ctx context.Context, report weave.HealthReport) {
    slog.InfoContext(ctx, "weave health",
        "status", report.Status,
        "backend", report.Backend,
        "component", report.Component,
        "connected", report.Connected,
        "started", report.Started,
    )
}
```

## Health Snapshots

`runtime.Client` and `runtime.Server` expose `HealthReport()` helpers for pull-based health checks, and lifecycle operations emit `HealthReport` snapshots to `Config.HealthReporter` / `Config.HealthHook`.

Recommended usage:

- Use `client.HealthReport()` or `server.HealthReport()` inside HTTP `/health` handlers.
- Forward `HealthHook` snapshots to your service registry or readiness monitor.
- Treat `healthy` as connected and ready, `degraded` as partially available, and `unhealthy` as not ready.

## Operational Guidance

For production rollouts, make these checks explicit:

1. Emit logs, metrics, and spans from the same process so `CorrelationID` can tie them together.
2. Add alerts for timeout spikes, subscribe failures, and repeated disconnects.
3. Break down dashboards by `backend` so AMQP and Kafka behavior stay distinguishable.
4. Track latency percentiles for high-value RPC destinations.
5. Include `destination` and `correlation_id` in incident-response log queries.
