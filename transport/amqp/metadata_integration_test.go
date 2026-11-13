package amqp

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestRPCMetadataWithLiveRabbitMQ(t *testing.T) {
	responder, deliveries, destination := newLiveRabbitMQResponder(t)
	broker, err := NewBroker(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	type result struct {
		msg *core.Message
		err error
	}
	done := make(chan result, 1)
	request := core.NewTextMessage("request").WithCorrelationID("metadata-correlation").WithHeader("trace-id", "request-trace")
	request.MessageID = "request-id"
	original := request.Clone()
	go func() { msg, err := broker.Call(ctx, destination, request); done <- result{msg, err} }()
	var delivery amqplib.Delivery
	select {
	case delivery = <-deliveries:
	case <-ctx.Done():
		t.Fatal("request did not arrive")
	}
	if delivery.MessageId != "request-id" || delivery.Headers["trace-id"] != "request-trace" {
		t.Fatalf("request metadata lost: %+v", delivery)
	}
	timestamp := time.Now().UTC().Truncate(time.Second)
	if err := responder.PublishWithContext(ctx, "", delivery.ReplyTo, false, false, amqplib.Publishing{
		Body: []byte("reply"), CorrelationId: delivery.CorrelationId, ReplyTo: "server.reply", MessageId: "reply-id",
		Headers: amqplib.Table{"trace-id": "reply-trace"}, ContentType: "text/plain", Timestamp: timestamp,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		msg := got.msg
		if msg.BodyString() != "reply" || msg.CorrelationID != delivery.CorrelationId || msg.ReplyTo != "server.reply" || msg.MessageID != "reply-id" || msg.GetHeader("trace-id") != "reply-trace" || msg.ContentType != "text/plain" || !msg.Timestamp.Equal(timestamp) {
			t.Fatalf("reply metadata lost: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("RPC did not complete")
	}
	if !reflect.DeepEqual(request, original) {
		t.Fatal("Call mutated caller request")
	}
}
