package amqp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestCallCleanupPreservesReplacementAfterDisconnect(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstCall atomic.Bool
	config := core.DefaultConfig()
	config.EventHook = func(ctx context.Context, event core.Event) {
		if event.Name == core.EventCallStarted && firstCall.CompareAndSwap(false, true) {
			// Hold the old call after registration while a disconnect and retry occur.
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
			}
		}
	}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	channel := &limitedChannel{fakeAMQPChannel: newFakeAMQPChannel(), sent: make(chan string, 2)}
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		return &fakeAMQPConnection{channel: channel}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	first := startLimitedCall(broker, ctx, "shared-id")
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal("first call did not register")
	}
	broker.cancelAllPending()
	second := startLimitedCall(broker, ctx, "shared-id")
	awaitLimitedPublish(t, channel.sent, "shared-id")
	broker.pendingMu.RLock()
	replacement := broker.pending["shared-id"]
	broker.pendingMu.RUnlock()
	if replacement == nil {
		t.Fatal("replacement call did not register")
	}

	close(releaseFirst)
	var connectionLost *core.ErrConnectionLost
	if err := awaitLimitedCall(t, first); !errors.As(err, &connectionLost) {
		t.Fatalf("old call error = %v, want ErrConnectionLost", err)
	}
	broker.pendingMu.RLock()
	retained := broker.pending["shared-id"] == replacement
	broker.pendingMu.RUnlock()
	if !retained {
		t.Fatal("old call cleanup removed the replacement registration")
	}
	completeLimitedCall(t, broker, "shared-id")
	if err := awaitLimitedCall(t, second); err != nil {
		t.Fatalf("replacement call failed: %v", err)
	}
	broker.pendingMu.RLock()
	remaining := len(broker.pending)
	broker.pendingMu.RUnlock()
	if remaining != 0 {
		t.Fatalf("completed calls left %d pending entries", remaining)
	}
}
