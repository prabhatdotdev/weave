package core

import (
	"context"
	"time"
)

// EventLevel indicates the severity of an emitted event.
type EventLevel string

const (
	EventLevelDebug EventLevel = "debug"
	EventLevelInfo  EventLevel = "info"
	EventLevelWarn  EventLevel = "warn"
	EventLevelError EventLevel = "error"
)

const (
	EventConnect         = "connect"
	EventDisconnect      = "disconnect"
	EventPublishFailed   = "publish_failed"
	EventSubscribeFailed = "subscribe_failed"
	EventTimeout         = "timeout"
)

// Event is a structured signal emitted by runtime and transport layers.
type Event struct {
	Timestamp     time.Time
	Level         EventLevel
	Name          string
	Backend       string
	Component     string
	Operation     string
	Destination   string
	CorrelationID string
	Err           error
	Fields        map[string]any
}

// EventLogger receives structured events for logging.
type EventLogger interface {
	LogEvent(ctx context.Context, event Event)
}

// EventHook receives structured events as a lightweight callback.
type EventHook func(ctx context.Context, event Event)

// MetricsHook is an extension point for counters and duration measurements.
type MetricsHook interface {
	IncrementCounter(name string, value float64, labels map[string]string)
	ObserveDuration(name string, value time.Duration, labels map[string]string)
}

// TraceSpanStart describes a span that is about to begin.
type TraceSpanStart struct {
	Name          string
	Backend       string
	Component     string
	Operation     string
	Destination   string
	CorrelationID string
	Attributes    map[string]string
}

// TraceSpanFinish describes how a span completed.
type TraceSpanFinish struct {
	Err        error
	Attributes map[string]string
}

// TraceSpan represents an in-flight span.
type TraceSpan interface {
	Finish(finish TraceSpanFinish)
}

// TracingHook is an extension point for tracing span lifecycle callbacks.
type TracingHook interface {
	StartSpan(ctx context.Context, span TraceSpanStart) (context.Context, TraceSpan)
}

type noopTraceSpan struct{}

func (noopTraceSpan) Finish(TraceSpanFinish) {}

type clonedTraceSpan struct {
	span TraceSpan
}

func (s clonedTraceSpan) Finish(finish TraceSpanFinish) {
	finish.Attributes = cloneLabels(finish.Attributes)
	s.span.Finish(finish)
}

// EmitEvent sends an event to all configured observability sinks.
func (c *Config) EmitEvent(ctx context.Context, event Event) {
	if c == nil {
		return
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	event.Fields = cloneFields(event.Fields)

	if c.Logger != nil {
		c.Logger.LogEvent(ctx, event)
	}
	if c.EventHook != nil {
		c.EventHook(ctx, event)
	}
}

// EmitCounter records a counter metric if a metrics hook is configured.
func (c *Config) EmitCounter(name string, value float64, labels map[string]string) {
	if c == nil || c.Metrics == nil {
		return
	}
	c.Metrics.IncrementCounter(name, value, cloneLabels(labels))
}

// EmitDuration records a duration metric if a metrics hook is configured.
func (c *Config) EmitDuration(name string, value time.Duration, labels map[string]string) {
	if c == nil || c.Metrics == nil {
		return
	}
	c.Metrics.ObserveDuration(name, value, cloneLabels(labels))
}

// StartSpan starts a trace span if tracing is configured.
func (c *Config) StartSpan(ctx context.Context, span TraceSpanStart) (context.Context, TraceSpan) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.Tracing == nil {
		return ctx, noopTraceSpan{}
	}

	span.Attributes = cloneLabels(span.Attributes)
	nextCtx, traceSpan := c.Tracing.StartSpan(ctx, span)
	if nextCtx == nil {
		nextCtx = ctx
	}
	if traceSpan == nil {
		traceSpan = noopTraceSpan{}
	}
	return nextCtx, clonedTraceSpan{span: traceSpan}
}

func cloneFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(fields))
	for k, v := range fields {
		cloned[k] = v
	}
	return cloned
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for k, v := range labels {
		cloned[k] = v
	}
	return cloned
}
