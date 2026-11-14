package amqp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

type blockedReplySetupChannel struct {
	*fakeAMQPChannel
	entered chan struct{}
	release chan struct{}
	started sync.Once
}

func (c *blockedReplySetupChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error) {
	c.started.Do(func() { close(c.entered) })
	<-c.release
	return c.fakeAMQPChannel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func TestCallTimeoutIncludesReplyQueueSetup(t *testing.T) {
	brokerAny, _ := NewBroker(nil)
	broker := brokerAny.(*Broker)
	channel := &blockedReplySetupChannel{fakeAMQPChannel: newFakeAMQPChannel(), entered: make(chan struct{}), release: make(chan struct{})}
	broker.channel, broker.conn, broker.connected = channel, &fakeAMQPConnection{channel: channel}, true
	done := make(chan error, 1)
	go func() {
		_, err := broker.Call(context.Background(), "orders", core.NewTextMessage("order"), core.WithTimeout(30*time.Millisecond))
		done <- err
	}()
	select {
	case <-channel.entered:
	case <-time.After(time.Second):
		t.Fatal("reply queue setup did not begin")
	}
	select {
	case err := <-done:
		if !core.IsTimeout(err) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call() = %v, want deadline timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reply queue setup exceeded the Call timeout")
	}
	if len(broker.pending) != 0 || len(channel.published) != 0 {
		t.Fatal("timed-out setup registered or published an RPC request")
	}
	close(channel.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := broker.ensureReplyQueue(ctx, channel); err != nil {
		t.Fatalf("reply setup did not finish cleanly: %v", err)
	}
	_ = broker.Close()
}

type signaledPublishChannel struct {
	*fakeAMQPChannel
	sent chan struct{}
}

func (c *signaledPublishChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, message amqplib.Publishing) error {
	err := c.fakeAMQPChannel.PublishWithContext(ctx, exchange, key, mandatory, immediate, message)
	close(c.sent)
	return err
}

func TestCallCancellationIsNotReportedAsTimeout(t *testing.T) {
	brokerAny, _ := NewBroker(nil)
	broker := brokerAny.(*Broker)
	channel := &signaledPublishChannel{fakeAMQPChannel: newFakeAMQPChannel(), sent: make(chan struct{})}
	broker.channel, broker.conn, broker.connected = channel, &fakeAMQPConnection{channel: channel}, true
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := broker.Call(ctx, "orders", core.NewTextMessage("order")); done <- err }()
	<-channel.sent
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || core.IsTimeout(err) {
			t.Fatalf("Call() cancellation returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Call() ignored cancellation")
	}
	if len(broker.pending) != 0 {
		t.Fatal("canceled Call retained its pending correlation ID")
	}
}
