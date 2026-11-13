package testkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestReadinessEventsAndReplacement(t *testing.T) {
	broker := NewMockBroker()
	t.Cleanup(func() { _ = broker.Close() })
	events := make(chan core.Event, 16)
	broker.SetEventHook(func(ctx context.Context, event core.Event) {
		_ = broker.IsReady()
		if event.Name == core.EventConsumerAssigned && broker.IsReady() {
			if err := broker.WaitReady(ctx); err != nil {
				t.Error(err)
			}
		}
		events <- event
	})
	if broker.IsReady() || !core.IsNotConnected(broker.WaitReady(context.Background())) {
		t.Fatal("unconnected mock reports ready")
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldCtx, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	for _, ctx := range []context.Context{oldCtx, context.Background()} {
		if err := broker.Subscribe(ctx, "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	cancelOld()
	if !broker.IsReady() {
		t.Fatal("old cancellation invalidated replacement")
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	if broker.IsReady() || !errors.Is(broker.WaitReady(context.Background()), core.ErrClosed) {
		t.Fatal("closed mock reports ready")
	}
	assigned, revoked := 0, 0
	for len(events) > 0 {
		event := <-events
		if event.Backend != "mock" || event.Timestamp.IsZero() {
			t.Fatalf("missing event metadata: %+v", event)
		}
		switch event.Name {
		case core.EventConsumerAssigned:
			assigned++
		case core.EventConsumerRevoked:
			revoked++
		default:
			t.Fatalf("unexpected Kafka-specific mock event: %+v", event)
		}
	}
	if assigned != 2 || revoked != 2 {
		t.Fatalf("assigned=%d revoked=%d", assigned, revoked)
	}
}

func TestReadinessHookCanCloseMock(t *testing.T) {
	broker := NewMockBroker()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	broker.SetEventHook(func(_ context.Context, event core.Event) {
		if event.Name == core.EventConsumerAssigned {
			_ = broker.Close()
		}
	})
	done := make(chan error, 1)
	go func() {
		done <- broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil })
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hook deadlocked on mock locks")
	}
}
