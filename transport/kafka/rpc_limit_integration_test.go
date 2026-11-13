package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestPendingCallLimitWithLiveKafka(t *testing.T) {
	events := make(chan core.Event, 64)
	broker, destinations, ctx := newLiveKafkaBrokerWithConfig(t, "rpc-limit", 2, 1, func(config *core.Config) {
		config.MaxPendingCalls = 1
		config.EventHook = func(_ context.Context, event core.Event) { events <- event }
	})

	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	first := make(chan error, 1)
	go func() {
		_, err := broker.Call(firstCtx, destinations[0], core.NewTextMessage("unanswered").WithCorrelationID("first"))
		first <- err
	}()
	waitEvent := func(name, correlationID string) {
		t.Helper()
		for {
			select {
			case event := <-events:
				if event.PendingRPCs > 1 {
					t.Fatalf("pending limit exceeded: %+v", event)
				}
				if event.Name == name && event.CorrelationID == correlationID {
					return
				}
			case <-ctx.Done():
				t.Fatalf("missing %s event for %s", name, correlationID)
			}
		}
	}
	waitEvent(core.EventPublishSucceeded, "first")
	_, err := broker.Call(ctx, destinations[1], core.NewTextMessage("must not publish").WithCorrelationID("queued"), core.WithTimeout(50*time.Millisecond))
	if !core.IsTimeout(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued RPC=%v", err)
	}
	broker.pendingMu.RLock()
	count := len(broker.pending)
	broker.pendingMu.RUnlock()
	if count != 1 {
		t.Fatalf("pending calls=%d, want 1", count)
	}
	for len(events) > 0 {
		event := <-events
		if event.CorrelationID == "queued" {
			t.Fatalf("queued call reached the broker: %+v", event)
		}
	}
	cancelFirst()
	if err := awaitLimitedCall(t, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("first call=%v", err)
	}
	next := make(chan error, 1)
	go func() {
		_, err := broker.Call(ctx, destinations[1], core.NewTextMessage("capacity reused").WithCorrelationID("next"), core.WithTimeout(500*time.Millisecond))
		next <- err
	}()
	waitEvent(core.EventPublishSucceeded, "next")
	if err := awaitLimitedCall(t, next); !core.IsTimeout(err) {
		t.Fatalf("unanswered next call=%v", err)
	}
	waitEvent(core.EventCallFinished, "next")
	broker.pendingMu.RLock()
	count = len(broker.pending)
	broker.pendingMu.RUnlock()
	if count != 0 {
		t.Fatalf("timed-out calls leaked pending entries: %d", count)
	}
}
