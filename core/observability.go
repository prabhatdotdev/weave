package core

import (
	"context"
	"maps"
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
	EventConnect            = "connect"
	EventDisconnect         = "disconnect"
	EventPublishFailed      = "publish_failed"
	EventSubscribeFailed    = "subscribe_failed"
	EventTimeout            = "timeout"
	EventPublishSucceeded   = "publish_succeeded"
	EventSubscribeSucceeded = "subscribe_succeeded"
	EventHandlerStarted     = "handler_started"
	EventHandlerFinished    = "handler_finished"
	EventCallStarted        = "call_started"
	EventCallFinished       = "call_finished"
	EventConsumerAssigned   = "consumer_assigned"
	EventConsumerRevoked    = "consumer_revoked"
	EventRebalanceStarted   = "rebalance_started"
	EventRebalanceCompleted = "rebalance_completed"
)

// Event is a structured transport signal.
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
	// Duration measures the completed operation; start events have no duration.
	Duration time.Duration
	// Counts are broker-local snapshots at emission, not queue depth or consumer lag.
	ActiveHandlers int64
	PendingRPCs    int64
}

// EventHook receives structured transport events.
// Calls are synchronous, may be concurrent, and run outside broker locks.
// Hooks must return promptly and must not panic. Concurrent events may arrive out of order.
type EventHook func(context.Context, Event)

// EmitEvent sends an event to the configured hook.
func (c *Config) EmitEvent(ctx context.Context, event Event) {
	if c == nil || c.EventHook == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	event.Fields = maps.Clone(event.Fields)
	c.EventHook(ctx, event)
}
