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

func TestCallRepairsReplyQueueLostWithoutConsumerNotification(t *testing.T) {
	broker, sent := limitedTestBroker(t, 2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := startLimitedCall(broker, ctx, "first")
	awaitLimitedPublish(t, sent, "first")
	queue := broker.currentReplyQueue()
	channel := broker.channel.(*limitedChannel)
	channel.mu.Lock()
	oldStream := channel.consumerStreams[queue]
	delete(channel.consumerStreams, queue)
	for tag, stream := range channel.consumerTags {
		if stream == oldStream {
			delete(channel.consumerTags, tag)
		}
	}
	// The cluster lost the queue, but the client never received basic.cancel.
	channel.mu.Unlock()
	second := startLimitedCall(broker, ctx, "second")
	awaitLimitedPublish(t, sent, "second")
	if err := awaitLimitedCall(t, first); !core.IsConnectionLost(err) {
		t.Fatalf("call using the lost reply queue returned %v", err)
	}
	if broker.currentReplyQueue() == queue {
		t.Fatal("reply-queue repair reused an address that can receive late responses")
	}
	channel.mu.Lock()
	stream := channel.consumerStreams[broker.currentReplyQueue()]
	consumers := len(channel.consumerTags)
	_, oldQueueExists := channel.consumerStreams[queue]
	channel.mu.Unlock()
	if oldQueueExists {
		t.Fatal("reply-queue repair left the stale queue behind")
	}
	if stream == nil || stream == oldStream || consumers != 1 {
		t.Fatalf("reply consumer was not repaired: stream=%p old=%p consumers=%d", stream, oldStream, consumers)
	}
	select {
	case stream <- amqplib.Delivery{CorrelationId: "second", Body: []byte("recovered")}:
	case <-ctx.Done():
		t.Fatal("replacement reply consumer did not receive a response")
	}
	if err := awaitLimitedCall(t, second); err != nil {
		t.Fatalf("call after reply-queue repair failed: %v", err)
	}
	if !broker.IsConnected() || broker.IsRecovering() {
		t.Fatal("reply-queue repair unnecessarily replaced the healthy connection")
	}
}

func TestReplyConsumerClosureFailsPendingCalls(t *testing.T) {
	broker, sent := limitedTestBroker(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	call := startLimitedCall(broker, ctx, "first")
	awaitLimitedPublish(t, sent, "first")
	channel := broker.channel.(*limitedChannel)
	channel.mu.Lock()
	close(channel.consumerStreams[broker.currentReplyQueue()])
	channel.mu.Unlock()
	if err := awaitLimitedCall(t, call); !core.IsConnectionLost(err) {
		t.Fatalf("lost reply consumer left a pending call: %v", err)
	}
	if queue := broker.currentReplyQueue(); queue != "" {
		t.Fatalf("closed reply consumer retained queue %q", queue)
	}
}

func TestOldReplyConsumerCannotInvalidateReplacement(t *testing.T) {
	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	oldStop, currentStop := make(chan struct{}), make(chan struct{})
	broker.replyQueue, broker.replyConsumerStop = "replacement", currentStop
	pending := newPendingCall()
	broker.pending["replacement-call"] = pending
	oldStream := make(chan amqplib.Delivery)
	close(oldStream)
	broker.handleResponses(oldStream, oldStop)
	if broker.currentReplyQueue() != "replacement" || broker.pending["replacement-call"] != pending {
		t.Fatal("old reply consumer invalidated the replacement generation")
	}
	select {
	case <-pending.response:
		t.Fatal("old reply consumer failed the replacement call")
	default:
	}
}

func TestHealthyReplyConsumerIsReused(t *testing.T) {
	broker, sent := limitedTestBroker(t, 2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, id := range []string{"first", "second"} {
		call := startLimitedCall(broker, ctx, id)
		awaitLimitedPublish(t, sent, id)
		completeLimitedCall(t, broker, id)
		if err := awaitLimitedCall(t, call); err != nil {
			t.Fatal(err)
		}
	}
	channel := broker.channel.(*limitedChannel)
	channel.mu.Lock()
	defer channel.mu.Unlock()
	if len(channel.consumers) != 1 || len(channel.consumerTags) != 1 {
		t.Fatalf("healthy calls recreated reply consumers: registrations=%d active=%d", len(channel.consumers), len(channel.consumerTags))
	}
}

func TestOldReplyConsumerCannotRouteToReplacement(t *testing.T) {
	brokerAny, err := NewBroker(nil)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	broker.replyQueue, broker.replyConsumerStop = "replacement", make(chan struct{})
	pending := newPendingCall()
	broker.pending["reused-id"] = pending
	oldStream := make(chan amqplib.Delivery, 1)
	oldStream <- amqplib.Delivery{CorrelationId: "reused-id", Body: []byte("stale response")}
	close(oldStream)
	broker.handleResponses(oldStream, make(chan struct{}))
	select {
	case response := <-pending.response:
		t.Fatalf("stale reply consumer completed the replacement call: %+v", response)
	default:
	}
}
