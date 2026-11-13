package amqp

import (
	"context"
	"errors"
	"fmt"
	"github.com/prabhatdotdev/weave/core"
	"os"
	"testing"
	"time"
)

func TestPendingCallLimitWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_AMQP_E2E") != "1" {
		t.Skip("set WEAVE_AMQP_E2E=1 to run the live RabbitMQ test")
	}
	conn := dialLiveRabbitMQ(t)
	t.Cleanup(func() { _ = conn.Close() })
	admin, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	source := fmt.Sprintf("weave-rpc-limit-%d", time.Now().UnixNano())
	destinations := []string{source, source + ".other"}
	for _, destination := range destinations {
		if _, err := admin.QueueDeclare(destination, true, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = admin.QueueDelete(destination, false, false, false) })
	}
	events := make(chan core.Event, 64)
	config := core.DefaultConfig()
	config.MaxPendingCalls = 1
	config.EventHook = func(_ context.Context, event core.Event) { events <- event }
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}

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
	_, err = broker.Call(ctx, destinations[1], core.NewTextMessage("must not publish").WithCorrelationID("queued"), core.WithTimeout(50*time.Millisecond))
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
