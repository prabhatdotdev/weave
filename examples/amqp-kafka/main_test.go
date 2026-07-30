package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/prabhatdotdev/weave"
	"github.com/prabhatdotdev/weave/testkit"
)

func TestNewConfig(t *testing.T) {
	t.Run("AMQP environment", func(t *testing.T) {
		t.Setenv("AMQP_HOST", "rabbit.example")
		t.Setenv("AMQP_PORT", "5678")

		config, err := newConfig("amqp", "server", "run")
		if err != nil {
			t.Fatal(err)
		}
		if config.Backend != "amqp" || config.AMQP.Host != "rabbit.example" || config.AMQP.Port != 5678 {
			t.Fatalf("unexpected AMQP config: %#v", config)
		}
		if !config.AMQP.QueueAutoDelete || !config.AMQP.QueueExclusive {
			t.Fatalf("example queue must be exclusive and auto-delete: %#v", config.AMQP)
		}
	})

	t.Run("Kafka environment", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")

		config, err := newConfig("kafka", "client", "run")
		if err != nil {
			t.Fatal(err)
		}
		if config.Backend != "kafka" || len(config.Kafka.Brokers) != 2 {
			t.Fatalf("unexpected Kafka config: %#v", config)
		}
		if config.Kafka.ClientID != "weave-example-client-run" || config.Kafka.ConsumerGroup != config.Kafka.ClientID {
			t.Fatalf("unexpected Kafka identity: %#v", config.Kafka)
		}
	})

	t.Run("invalid backend", func(t *testing.T) {
		if _, err := newConfig("other", "client", "run"); err == nil {
			t.Fatal("expected invalid backend error")
		}
	})
}

func TestOrderHandlerRetriesEvent(t *testing.T) {
	events := make(chan receivedEvent, 1)
	var attempts atomic.Int32
	handler := newOrderHandler(testkit.NewMockBroker(), &attempts, events)

	msg, err := weave.MarshalMessage(weave.JSON, order{ID: "order-1", Status: "created"})
	if err != nil {
		t.Fatal(err)
	}
	msg.
		WithHeader("example-kind", "event").
		WithHeader("source", "amqp-kafka-example").
		WithCorrelationID("event-1")

	if err := handler(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	select {
	case event := <-events:
		if event.Order.ID != "order-1" || event.Message.CorrelationID != "event-1" {
			t.Fatalf("unexpected event: %#v", event)
		}
	default:
		t.Fatal("event was not delivered")
	}
}

func TestNewDestinations(t *testing.T) {
	destinations, err := newDestinations("team.orders")
	if err != nil {
		t.Fatal(err)
	}
	if destinations.orders != "team.orders.orders" ||
		destinations.native != "team.orders.native" ||
		destinations.missing != "team.orders.missing" {
		t.Fatalf("unexpected destinations: %#v", destinations)
	}

	for _, namespace := range []string{"", "contains space", "bad/topic"} {
		if _, err := newDestinations(namespace); err == nil {
			t.Fatalf("newDestinations(%q) should fail", namespace)
		}
	}
}

func TestOrderHandlerPublishesCorrelatedReply(t *testing.T) {
	broker := testkit.NewMockBroker()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	handler := newOrderHandler(broker, &attempts, make(chan receivedEvent, 1))
	request, err := weave.MarshalMessage(weave.JSON, getOrderRequest{ID: "order-7"})
	if err != nil {
		t.Fatal(err)
	}
	request.
		WithHeader("example-kind", "rpc").
		WithReplyTo("orders.reply").
		WithCorrelationID("corr-7")

	if err := handler(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	published := broker.PublishedTo("orders.reply")
	if len(published) != 1 {
		t.Fatalf("published replies = %d, want 1", len(published))
	}
	reply := published[0].Message
	if reply.CorrelationID != "corr-7" || reply.ContentType != weave.JSON.ContentType() {
		t.Fatalf("unexpected reply metadata: %#v", reply)
	}
	var response getOrderResponse
	if err := weave.UnmarshalMessage(weave.JSON, reply, &response); err != nil {
		t.Fatal(err)
	}
	if response.Order.ID != "order-7" {
		t.Fatalf("response order ID = %q, want order-7", response.Order.ID)
	}

	request.ReplyTo = ""
	if err := handler(context.Background(), request); !errors.Is(err, weave.ErrNoReplyTo) {
		t.Fatalf("missing reply error = %v, want ErrNoReplyTo", err)
	}
}
