// Package observability tracks broker-local operational metrics.
package observability

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

// Metrics has one instance per broker. Its zero value is ready to use.
type Metrics struct {
	activeHandlers atomic.Int64
	pendingRPCs    atomic.Int64
}

// Snapshot annotates an event without holding a lock while its hook runs.
func (m *Metrics) Snapshot(event *core.Event) {
	event.ActiveHandlers = m.activeHandlers.Load()
	event.PendingRPCs = m.pendingRPCs.Load()
}

// WrapHandler observes user callbacks, including failed and panicking handlers.
func (m *Metrics) WrapHandler(handler core.Handler, destination string, emit core.EventHook) core.Handler {
	if handler == nil {
		return nil
	}
	return func(ctx context.Context, msg *core.Message) (err error) {
		started := time.Now()
		m.activeHandlers.Add(1)
		event := core.Event{Level: core.EventLevelInfo, Operation: "handler", Destination: destination, CorrelationID: msg.CorrelationID}
		returned := false
		defer func() {
			m.activeHandlers.Add(-1)
			event.Name, event.Duration, event.Err = core.EventHandlerFinished, time.Since(started), err
			if !returned {
				event.Err = errors.New("handler did not return normally")
			}
			if event.Err != nil {
				event.Level = core.EventLevelError
			}
			emit(ctx, event)
		}()
		event.Name = core.EventHandlerStarted
		emit(ctx, event)
		err = handler(ctx, msg)
		returned = true
		return err
	}
}

// StartCall tracks a registered RPC until its Call method returns.
func (m *Metrics) StartCall(ctx context.Context, destination, correlationID string, started time.Time, emit core.EventHook) func(error) {
	m.pendingRPCs.Add(1)
	event := core.Event{Level: core.EventLevelInfo, Name: core.EventCallStarted, Operation: "call", Destination: destination, CorrelationID: correlationID}
	emit(ctx, event)
	return func(err error) {
		m.pendingRPCs.Add(-1)
		event.Name, event.Duration, event.Err = core.EventCallFinished, time.Since(started), err
		if err != nil {
			event.Level = core.EventLevelError
		}
		emit(ctx, event)
	}
}
