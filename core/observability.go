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
	EventConnect         = "connect"
	EventDisconnect      = "disconnect"
	EventPublishFailed   = "publish_failed"
	EventSubscribeFailed = "subscribe_failed"
	EventTimeout         = "timeout"
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
}

// EventHook receives structured transport events.
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
