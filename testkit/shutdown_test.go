package testkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestMockShutdownDrainsSimulatedHandlers(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	if err := broker.Subscribe(context.Background(), "orders", func(ctx context.Context, _ *core.Message) error {
		close(started)
		select {
		case <-release:
			return broker.Publish(ctx, "replies", core.NewTextMessage("completed"))
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	handled := make(chan error, 1)
	go func() { handled <- broker.SimulateMessage(context.Background(), "orders", core.NewMessage(nil)) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	if broker.IsConnected() {
		t.Fatal("draining mock still reports readiness")
	}
	if err := broker.Publish(context.Background(), "orders", core.NewMessage(nil)); !errors.Is(err, core.ErrShuttingDown) {
		t.Fatalf("new publish = %v", err)
	}
	close(release)
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	broker.AssertPublishCount(t, "replies", 1)
	if err := broker.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated shutdown = %v", err)
	}
}
