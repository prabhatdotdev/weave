package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/prabhatdotdev/weave/codec"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
	"github.com/prabhatdotdev/weave/transport/kafka"
)

func main() {
	backend := flag.String("backend", "amqp", "amqp or kafka")
	flag.Parse()

	broker, err := newBroker(*backend)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	received := make(chan string, 1)
	if err := broker.Subscribe(ctx, "weave.example", func(_ context.Context, msg *core.Message) error {
		received <- msg.BodyString()
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	msg, err := codec.MarshalMessage(codec.JSON, map[string]string{"status": "ok"})
	if err != nil {
		log.Fatal(err)
	}
	if err := broker.Publish(ctx, "weave.example", msg); err != nil {
		log.Fatal(err)
	}

	select {
	case body := <-received:
		fmt.Println(body)
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}

func newBroker(backend string) (core.MessageBroker, error) {
	config := core.DefaultConfig()
	switch backend {
	case "amqp":
		return amqp.NewBroker(config)
	case "kafka":
		config.Kafka = core.DefaultKafkaConfig()
		config.Kafka.ConsumerGroup = "weave-example"
		return kafka.NewBroker(config)
	default:
		return nil, fmt.Errorf("backend must be amqp or kafka")
	}
}
