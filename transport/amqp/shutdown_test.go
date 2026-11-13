package amqp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

func shutdownTestBroker(t *testing.T) (*Broker, *fakeAMQPChannel) {
	t.Helper()
	brokerAny, err := NewBroker(nil)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	channel := newFakeAMQPChannel()
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		return &fakeAMQPConnection{channel: channel}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return broker, channel
}

func TestShutdownDrainsHandlerRepliesAndCancelsConsumer(t *testing.T) {
	broker, channel := shutdownTestBroker(t)
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	if err := broker.Subscribe(context.Background(), "orders", func(ctx context.Context, _ *core.Message) error {
		started <- ctx
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return broker.Publish(ctx, "replies", core.NewTextMessage("completed"))
	}, core.WithWorkerCount(1), core.WithConsumerTag("order-worker")); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	stream := channel.consumerStreams["orders"]
	channel.mu.Unlock()
	ack := &ackRecorder{}
	stream <- amqplib.Delivery{Acknowledger: ack, DeliveryTag: 1}
	handlerCtx := <-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	for _, err := range []error{
		broker.Connect(context.Background()),
		broker.Publish(context.Background(), "orders", core.NewMessage(nil)),
		broker.Subscribe(handlerCtx, "more", func(context.Context, *core.Message) error { return nil }),
	} {
		if !errors.Is(err, core.ErrShuttingDown) {
			t.Fatalf("new work during shutdown = %v", err)
		}
	}
	if _, err := broker.Call(context.Background(), "orders", core.NewMessage(nil)); !errors.Is(err, core.ErrShuttingDown) {
		t.Fatalf("new Call during shutdown = %v", err)
	}
	if broker.IsConnected() || handlerCtx.Err() != nil {
		t.Fatal("draining broker remained ready or interrupted its active handler")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ack.ackCalls != 1 || ack.nackCalls != 0 {
		t.Fatalf("completed work: ack=%d nack=%d", ack.ackCalls, ack.nackCalls)
	}
	channel.mu.Lock()
	defer channel.mu.Unlock()
	if len(channel.canceled) != 1 || channel.canceled[0] != "order-worker" || len(channel.published) != 1 {
		t.Fatalf("consumer cancellations=%v replies=%d", channel.canceled, len(channel.published))
	}
}

type shutdownAck struct {
	ackRecorder
	done chan struct{}
}

func (a *shutdownAck) Nack(tag uint64, multiple, requeue bool) error {
	err := a.ackRecorder.Nack(tag, multiple, requeue)
	close(a.done)
	return err
}

type delayedCancelChannel struct {
	*fakeAMQPChannel
	entered chan struct{}
	release <-chan struct{}
}

func (c *delayedCancelChannel) Cancel(tag string, noWait bool) error {
	close(c.entered)
	<-c.release
	return c.fakeAMQPChannel.Cancel(tag, noWait)
}

func TestSubscriptionCancellationRequeuesBufferedDeliveries(t *testing.T) {
	broker, channel := shutdownTestBroker(t)
	cancelRelease := make(chan struct{})
	defer func() {
		select {
		case <-cancelRelease:
		default:
			close(cancelRelease)
		}
	}()
	cancelChannel := &delayedCancelChannel{fakeAMQPChannel: channel, entered: make(chan struct{}), release: cancelRelease}
	broker.closeMu.Lock()
	broker.channel = cancelChannel
	broker.closeMu.Unlock()
	channel.consumerStreams["orders"] = make(chan amqplib.Delivery, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var calls atomic.Int32
	if err := broker.Subscribe(ctx, "orders", func(ctx context.Context, _ *core.Message) error {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, core.WithWorkerCount(1), core.WithPrefetchCount(2)); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	stream := channel.consumerStreams["orders"]
	channel.mu.Unlock()
	first, second := &shutdownAck{done: make(chan struct{})}, &shutdownAck{done: make(chan struct{})}
	stream <- amqplib.Delivery{Acknowledger: first, DeliveryTag: 1}
	<-started
	stream <- amqplib.Delivery{Acknowledger: second, DeliveryTag: 2}
	cancel()
	select {
	case <-cancelChannel.entered:
	case <-time.After(time.Second):
		t.Fatal("consumer cancellation was not sent")
	}
	select {
	case <-second.done:
		t.Fatal("buffered delivery was requeued before consumer cancellation completed")
	case <-time.After(10 * time.Millisecond):
	}
	close(cancelRelease)
	for _, ack := range []*shutdownAck{first, second} {
		select {
		case <-ack.done:
			if ack.nackCalls != 1 || !ack.lastRequeue {
				t.Fatal("canceled delivery was not requeued")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not release a buffered delivery")
		}
	}
	if calls.Load() != 1 || !broker.IsConnected() {
		t.Fatalf("handler calls=%d connected=%v", calls.Load(), broker.IsConnected())
	}
	if err := broker.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	defer channel.mu.Unlock()
	if len(channel.canceled) != 1 || channel.canceled[0] == "" {
		t.Fatalf("generated consumer tag was not canceled: %v", channel.canceled)
	}
}

func TestShutdownDeadlineRequeuesInterruptedHandler(t *testing.T) {
	broker, channel := shutdownTestBroker(t)
	started := make(chan struct{})
	if err := broker.Subscribe(context.Background(), "orders", func(ctx context.Context, _ *core.Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, core.WithWorkerCount(1)); err != nil {
		t.Fatal(err)
	}
	channel.mu.Lock()
	stream := channel.consumerStreams["orders"]
	channel.mu.Unlock()
	ack := &shutdownAck{done: make(chan struct{})}
	stream <- amqplib.Delivery{Acknowledger: ack, DeliveryTag: 1}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := broker.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() = %v, want deadline", err)
	}
	select {
	case <-ack.done:
		if !ack.lastRequeue {
			t.Fatal("deadline discarded unfinished work")
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not interrupt handler")
	}
}

func TestForcedClosePreservesUnfinishedWork(t *testing.T) {
	for _, panicHandler := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicHandler], func(t *testing.T) {
			broker := &Broker{}
			broker.work.Close()
			ack := &ackRecorder{}
			broker.handleMessage(context.Background(), amqplib.Delivery{Acknowledger: ack, DeliveryTag: 1}, func(context.Context, *core.Message) error {
				if panicHandler {
					panic("work interrupted by closure")
				}
				return errors.New("work interrupted by closure")
			}, false, core.HandlerErrorNoRetry)
			if ack.ackCalls != 0 || ack.nackCalls != 1 || !ack.lastRequeue {
				t.Fatalf("forced closure discarded work: ack=%d nack=%d requeue=%v", ack.ackCalls, ack.nackCalls, ack.lastRequeue)
			}
		})
	}
}

func TestShutdownDrainsPendingCall(t *testing.T) {
	broker, _ := shutdownTestBroker(t)
	channel := &signaledPublishChannel{fakeAMQPChannel: newFakeAMQPChannel(), sent: make(chan struct{})}
	broker.closeMu.Lock()
	broker.channel = channel
	broker.closeMu.Unlock()
	callDone := make(chan error, 1)
	go func() {
		response, err := broker.Call(context.Background(), "orders", core.NewMessage(nil))
		if err == nil && response.BodyString() != "response" {
			err = errors.New("incorrect response")
		}
		callDone <- err
	}()
	<-channel.sent
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	channel.mu.Lock()
	request := channel.published[0]
	replies := channel.consumerStreams[broker.currentReplyQueue()]
	channel.mu.Unlock()
	replies <- amqplib.Delivery{CorrelationId: request.CorrelationId, Body: []byte("response")}
	if err := <-callDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type flushingTestChannel struct {
	*fakeAMQPChannel
	publisher *confirmedPublisher
}

func (c *flushingTestChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqplib.Publishing) error {
	return c.publisher.publish(ctx, exchange, key, mandatory, immediate, msg)
}
func (c *flushingTestChannel) Flush(ctx context.Context) error { return c.publisher.flush(ctx) }

func TestShutdownWaitsForLatePublishConfirmation(t *testing.T) {
	broker, channel := shutdownTestBroker(t)
	wire := &fakeConfirmChannel{writes: make(chan amqplib.Publishing, 1)}
	publisher, err := newConfirmedPublisher(wire)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(wire.closed) })
	broker.closeMu.Lock()
	broker.channel = &flushingTestChannel{fakeAMQPChannel: channel, publisher: publisher}
	broker.closeMu.Unlock()
	published := make(chan error, 1)
	go func() {
		published <- broker.Publish(context.Background(), "orders", core.NewMessage(nil), core.WithTimeout(20*time.Millisecond))
	}()
	<-wire.writes
	if err := <-published; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish() = %v, want timeout", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	select {
	case err := <-done:
		t.Fatalf("shutdown discarded an outstanding confirmation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	wire.confirms <- amqplib.Confirmation{Ack: true}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
