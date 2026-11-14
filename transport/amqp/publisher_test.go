package amqp

import (
	"context"
	"errors"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

type fakeConfirmChannel struct {
	confirms chan amqplib.Confirmation
	returns  chan amqplib.Return
	closed   chan *amqplib.Error
	writes   chan amqplib.Publishing
	release  <-chan struct{}
}

func (c *fakeConfirmChannel) Confirm(bool) error { return nil }
func (c *fakeConfirmChannel) NotifyPublish(ch chan amqplib.Confirmation) chan amqplib.Confirmation {
	c.confirms = ch
	return ch
}
func (c *fakeConfirmChannel) NotifyReturn(ch chan amqplib.Return) chan amqplib.Return {
	c.returns = ch
	return ch
}
func (c *fakeConfirmChannel) NotifyClose(ch chan *amqplib.Error) chan *amqplib.Error {
	c.closed = ch
	return ch
}
func (c *fakeConfirmChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, message amqplib.Publishing) error {
	c.writes <- message
	if c.release != nil {
		<-c.release
	}
	return nil
}

func publisherTestBroker(t *testing.T) (*Broker, *fakeConfirmChannel) {
	t.Helper()
	channel := &fakeConfirmChannel{writes: make(chan amqplib.Publishing, 8)}
	publisher, err := newConfirmedPublisher(channel)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(channel.closed) })
	return &Broker{
		config: core.DefaultConfig(), amqpConfig: core.DefaultAMQPConfig(), connected: true, closeChan: make(chan struct{}),
		channel: &amqpChannelAdapter{publisher: publisher},
	}, channel
}

func TestPublishRequiresConfirmationAndReportsReturns(t *testing.T) {
	for _, outcome := range []string{"ack", "nack", "unroutable", "connection lost"} {
		t.Run(outcome, func(t *testing.T) {
			broker, channel := publisherTestBroker(t)
			done := make(chan error, 1)
			go func() {
				done <- broker.Publish(context.Background(), "orders", core.NewTextMessage("order"), core.WithMandatory())
			}()
			<-channel.writes
			select {
			case err := <-done:
				t.Fatalf("publish returned without confirmation: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			var want error
			switch outcome {
			case "ack":
				channel.confirms <- amqplib.Confirmation{Ack: true}
			case "nack":
				want = core.ErrPublishRejected
				channel.confirms <- amqplib.Confirmation{Ack: false}
			case "unroutable":
				want = core.ErrUnroutable
				channel.returns <- amqplib.Return{ReplyCode: 312, ReplyText: "NO_ROUTE"}
				channel.confirms <- amqplib.Confirmation{Ack: true}
			case "connection lost":
				channel.closed <- &amqplib.Error{Code: 320}
			}
			err := <-done
			if outcome == "connection lost" {
				if !core.IsConnectionLost(err) {
					t.Fatalf("Publish() = %v, want ErrConnectionLost", err)
				}
			} else if !errors.Is(err, want) {
				t.Fatalf("Publish() = %v, want %v", err, want)
			}
		})
	}
}

func TestPublishTimeoutDoesNotReuseLateConfirmation(t *testing.T) {
	broker, channel := publisherTestBroker(t)
	request := core.NewTextMessage("original").WithHeader("source", "original")
	err := broker.Publish(context.Background(), "orders", request, core.WithTimeout(20*time.Millisecond))
	if !core.IsTimeout(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish() = %v, want timeout", err)
	}
	written := <-channel.writes
	request.Body[0] = 'X'
	request.Headers["source"] = "changed"
	if string(written.Body) != "original" || written.Headers["source"] != "original" {
		t.Fatal("timed-out publish retained caller-owned data")
	}
	if err := broker.Publish(context.Background(), "orders", core.NewTextMessage("queued"), core.WithTimeout(20*time.Millisecond)); !core.IsTimeout(err) {
		t.Fatalf("queued Publish() = %v, want timeout", err)
	}
	select {
	case <-channel.writes:
		t.Fatal("another publish bypassed an outstanding confirmation")
	default:
	}
	channel.confirms <- amqplib.Confirmation{Ack: true}
	done := make(chan error, 1)
	go func() {
		done <- broker.Publish(context.Background(), "orders", core.NewTextMessage("next"), core.WithTimeout(time.Second))
	}()
	select {
	case <-channel.writes:
	case <-time.After(time.Second):
		t.Fatal("late acknowledgment did not release the publish gate")
	}
	select {
	case err := <-done:
		t.Fatalf("late acknowledgment was attributed to the next publish: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	channel.confirms <- amqplib.Confirmation{Ack: false}
	if err := <-done; !errors.Is(err, core.ErrPublishRejected) {
		t.Fatalf("next Publish() = %v, want its own negative acknowledgment", err)
	}
}

func TestPublishTimeoutWhileTopologyOwnsChannelDoesNotWriteLater(t *testing.T) {
	broker, channel := publisherTestBroker(t)
	publisher := broker.channel.(*amqpChannelAdapter).publisher
	publisher.wireMu.Lock()
	locked := true
	defer func() {
		if locked {
			publisher.wireMu.Unlock()
		}
	}()
	err := broker.Publish(context.Background(), "orders", core.NewTextMessage("expired"), core.WithTimeout(20*time.Millisecond))
	if !core.IsTimeout(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish blocked by topology returned %v", err)
	}
	publisher.wireMu.Unlock()
	locked = false
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := publisher.flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-channel.writes:
		t.Fatal("expired publish wrote after topology released the channel")
	default:
	}
}

func TestPublishTimeoutBoundsBlockedSocketWrite(t *testing.T) {
	broker, channel := publisherTestBroker(t)
	release := make(chan struct{})
	channel.release = release
	defer close(release)
	done := make(chan error, 1)
	go func() {
		done <- broker.Publish(context.Background(), "orders", core.NewTextMessage("order"), core.WithTimeout(20*time.Millisecond))
	}()
	<-channel.writes
	select {
	case err := <-done:
		if !core.IsTimeout(err) {
			t.Fatalf("Publish() = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked write prevented timeout from returning")
	}
}
