package amqp

import (
	"context"
	"errors"
	"strings"
	"sync"
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

func TestEndpointCallDoesNotPublishAfterItsReplyConnectionChanges(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	releaseCall := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseCall)
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672"}}
	config.EventHook = func(ctx context.Context, event core.Event) {
		if event.Name == core.EventCallStarted {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	first := &fakeAMQPConnection{channel: newFakeAMQPChannel()}
	secondChannel := newFakeAMQPChannel()
	broker.dial = func(address string, _ amqplib.Config) (amqpConnection, error) {
		if strings.Contains(address, "first:5672") {
			return first, nil
		}
		return &fakeAMQPConnection{channel: secondChannel}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.Cleanup(func() { _ = broker.Close() })
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := broker.Call(ctx, "orders", core.NewTextMessage("work")); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("call did not finish reply setup")
	}
	if !broker.handleConnectionLoss(first, nil) {
		t.Fatal("did not lose the initial connection")
	}
	broker.reconnectLoop()
	releaseCall()
	select {
	case err := <-done:
		if !core.IsConnectionLost(err) {
			t.Fatalf("old Call returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("old call did not fail promptly")
	}
	secondChannel.mu.Lock()
	defer secondChannel.mu.Unlock()
	if len(secondChannel.published) != 0 {
		t.Fatalf("old call migrated to the new connection with reply queue %q", secondChannel.published[0].ReplyTo)
	}
}

func TestEndpointCallPublishesOnlyOnItsReplyChannel(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672"}}
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	first := &fakeAMQPConnection{channel: newFakeAMQPChannel()}
	secondChannel := newFakeAMQPChannel()
	broker.dial = func(address string, _ amqplib.Config) (amqpConnection, error) {
		if strings.Contains(address, "first:5672") {
			return first, nil
		}
		return &fakeAMQPConnection{channel: secondChannel}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	var applications int
	changeConnection := func(*core.PublishOptions) {
		applications++
		// Call has captured its reply address when Publish applies the options again.
		if applications == 2 {
			broker.handleConnectionLoss(first, nil)
			broker.reconnectLoop()
		}
	}
	_, err := broker.Call(ctx, "orders", core.NewTextMessage("work"), changeConnection)
	if !core.IsConnectionLost(err) {
		t.Fatalf("old Call returned %v", err)
	}
	secondChannel.mu.Lock()
	defer secondChannel.mu.Unlock()
	if len(secondChannel.published) != 0 {
		t.Fatalf("old call used a replacement channel with stale reply queue %q", secondChannel.published[0].ReplyTo)
	}
}
