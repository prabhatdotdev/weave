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

func readinessAMQPBroker(t *testing.T) (*Broker, []*fakeAMQPConnection, <-chan core.Event) {
	t.Helper()
	var broker *Broker
	events := make(chan core.Event, 32)
	config := core.DefaultConfig()
	config.EventHook = func(_ context.Context, event core.Event) {
		_ = broker.IsReady()
		if event.Name == core.EventConsumerAssigned || event.Name == core.EventConsumerRevoked {
			events <- event
		}
	}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker = brokerAny.(*Broker)
	clients := []*fakeAMQPConnection{{channel: newFakeAMQPChannel()}, {channel: newFakeAMQPChannel()}}
	var next atomic.Int32
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) { return clients[int(next.Add(1))-1], nil }
	t.Cleanup(func() { _ = broker.Close() })
	return broker, clients, events
}

func awaitAMQPReadinessEvent(t *testing.T, events <-chan core.Event, name, destination string) core.Event {
	t.Helper()
	for {
		select {
		case event := <-events:
			if event.Name == name && event.Destination == destination {
				return event
			}
		case <-time.After(time.Second):
			t.Fatalf("missing %s for %s", name, destination)
		}
	}
}

func TestConsumerReadinessTracksSetupLossAndRecovery(t *testing.T) {
	broker, clients, events := readinessAMQPBroker(t)
	if broker.IsReady() || !core.IsNotConnected(broker.WaitReady(context.Background())) {
		t.Fatal("disconnected broker reports ready")
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, queue := range []string{"orders", "other"} {
		if err := broker.Subscribe(ctx, queue, func(context.Context, *core.Message) error { return nil }, core.WithConsumerTag(queue)); err != nil {
			t.Fatal(err)
		}
	}
	event := awaitAMQPReadinessEvent(t, events, core.EventConsumerAssigned, "orders")
	if event.Fields["consumer_generation"] != uint64(1) || event.Fields["consumer_tag"] != "orders" {
		t.Fatalf("assignment metadata = %+v", event)
	}
	if !broker.IsReady() {
		t.Fatal("accepted consumers are not ready")
	}
	if !broker.handleConnectionLoss(clients[0], nil) || broker.IsReady() {
		t.Fatal("connection loss did not revoke readiness")
	}
	waitCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- broker.WaitReady(waitCtx) }()
	go broker.reconnectLoop()
	if err := <-done; err != nil || !broker.IsReady() {
		t.Fatalf("restored consumer readiness = %v", err)
	}
	event = awaitAMQPReadinessEvent(t, events, core.EventConsumerAssigned, "orders")
	if event.Fields["consumer_generation"] != uint64(2) {
		t.Fatalf("restored assignment = %+v", event)
	}
	if err := clients[1].channel.Cancel("orders", false); err != nil {
		t.Fatal(err)
	}
	awaitAMQPReadinessEvent(t, events, core.EventConsumerRevoked, "orders")
	if broker.IsReady() || !broker.IsConnected() {
		t.Fatal("consumer loss should affect readiness while preserving connectivity")
	}
	deadline, end := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer end()
	if err := broker.WaitReady(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unready wait = %v", err)
	}
	cancel()
	if err := broker.WaitReady(waitCtx); err != nil {
		t.Fatalf("canceled subscriptions blocked readiness: %v", err)
	}
}

func TestReadinessWaitDoesNotDelayShutdown(t *testing.T) {
	broker, clients, events := readinessAMQPBroker(t)
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }, core.WithConsumerTag("orders")); err != nil {
		t.Fatal(err)
	}
	if err := clients[0].channel.Cancel("orders", false); err != nil {
		t.Fatal(err)
	}
	awaitAMQPReadinessEvent(t, events, core.EventConsumerRevoked, "orders")
	_, release, err := broker.work.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	wait := make(chan error, 1)
	go func() { wait <- broker.WaitReady(context.Background()) }()
	shutdown := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { shutdown <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	select {
	case err := <-wait:
		if !errors.Is(err, core.ErrShuttingDown) {
			t.Fatalf("wait error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("readiness waiter blocked shutdown")
	}
	release()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}

func TestConsumerEventsCanCloseBroker(t *testing.T) {
	for _, name := range []string{core.EventConsumerAssigned, core.EventConsumerRevoked} {
		t.Run(name, func(t *testing.T) {
			var broker *Broker
			config := core.DefaultConfig()
			config.EventHook = func(_ context.Context, event core.Event) {
				if event.Name == name {
					_ = broker.Close()
				}
			}
			brokerAny, _ := NewBroker(config)
			broker = brokerAny.(*Broker)
			broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
				return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
			}
			if err := broker.Connect(context.Background()); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				_ = broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil })
				_ = broker.Close()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("consumer event hook deadlocked on broker locks")
			}
		})
	}
}

type pendingConsumerChannel struct {
	*fakeAMQPChannel
	entered, release chan struct{}
}

func (c *pendingConsumerChannel) Consume(queue, tag string, autoAck, exclusive, noLocal, noWait bool, args amqplib.Table) (<-chan amqplib.Delivery, error) {
	close(c.entered)
	<-c.release
	return c.fakeAMQPChannel.Consume(queue, tag, autoAck, exclusive, noLocal, noWait, args)
}

func TestShutdownDuringConsumerSetupPreservesLifecycleError(t *testing.T) {
	brokerAny, err := NewBroker(nil)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	channel := &pendingConsumerChannel{fakeAMQPChannel: newFakeAMQPChannel(), entered: make(chan struct{}), release: make(chan struct{})}
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		return &fakeAMQPConnection{channel: channel}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	subscribe := make(chan error, 1)
	go func() {
		subscribe <- broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil })
	}()
	<-channel.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	close(channel.release)
	if err := <-subscribe; !errors.Is(err, core.ErrShuttingDown) {
		t.Fatalf("setup error = %v, want ErrShuttingDown", err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}
