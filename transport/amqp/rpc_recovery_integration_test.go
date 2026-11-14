package amqp

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestReplyQueueRecreationWithLiveRabbitMQ(t *testing.T) {
	responder, deliveries, destination := newLiveRabbitMQResponder(t)
	brokerAny, err := NewBroker(nil)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	broker.closeMu.RLock()
	connection := broker.conn
	broker.closeMu.RUnlock()
	type result struct {
		message *core.Message
		err     error
	}
	start := func(id string) <-chan result {
		done := make(chan result, 1)
		go func() {
			message, err := broker.Call(ctx, destination, core.NewTextMessage("request").WithCorrelationID(id))
			done <- result{message, err}
		}()
		return done
	}
	receive := func() amqplib.Delivery {
		select {
		case request := <-deliveries:
			return request
		case <-ctx.Done():
			t.Fatal("RPC request did not arrive")
			return amqplib.Delivery{}
		}
	}
	lost := start("lost-reply-queue")
	request := receive()
	originalReply := request.ReplyTo
	broker.closeMu.RLock()
	owner := broker.channel.(*amqpChannelAdapter).channel
	broker.closeMu.RUnlock()
	if _, err := owner.QueueDelete(originalReply, false, false, false); err != nil {
		t.Fatal(err)
	}
	select {
	case completed := <-lost:
		if !core.IsConnectionLost(completed.err) {
			t.Fatalf("deleted reply queue returned %v", completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("deleted reply consumer stranded its pending call")
	}

	var replacement string
	for i := range 2 {
		call := start(fmt.Sprintf("recovered-%d", i))
		request = receive()
		if request.ReplyTo == "" || request.ReplyTo == originalReply {
			t.Fatalf("RPC reused deleted reply queue %q", request.ReplyTo)
		}
		if replacement != "" && request.ReplyTo != replacement {
			t.Fatal("healthy call replaced the reply queue unnecessarily")
		}
		replacement = request.ReplyTo
		if err := responder.PublishWithContext(ctx, "", request.ReplyTo, false, false, amqplib.Publishing{
			CorrelationId: request.CorrelationId, Body: []byte("recovered"),
		}); err != nil {
			t.Fatal(err)
		}
		select {
		case completed := <-call:
			if completed.err != nil || completed.message == nil || completed.message.BodyString() != "recovered" {
				t.Fatalf("RPC recovery result=%+v", completed)
			}
		case <-ctx.Done():
			t.Fatal("RPC did not recover after reply-queue deletion")
		}
	}
	broker.closeMu.RLock()
	sameConnection := broker.conn == connection
	broker.closeMu.RUnlock()
	if !sameConnection || !broker.IsConnected() {
		t.Fatal("reply consumer recovery replaced the healthy connection")
	}
}

func TestReplyQueueVerificationDuringPublishWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_AMQP_E2E") != "1" {
		t.Skip("set WEAVE_AMQP_E2E=1 to run the live RabbitMQ test")
	}
	conn := dialLiveRabbitMQ(t)
	t.Cleanup(func() { _ = conn.Close() })
	admin, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := admin.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	brokerAny, err := NewBroker(nil)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	channel, err := broker.currentChannel(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.ensureReplyQueue(ctx, channel); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		for range 1024 {
			if err := broker.Publish(ctx, queue.Name, core.NewMessage(make([]byte, 1024))); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	go func() {
		<-start
		for range 4096 {
			if err := broker.ensureReplyQueue(ctx, channel); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	close(start)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("reply verification interrupted publishing: %v", err)
		}
	}
}
