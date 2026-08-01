package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
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

	destinations := []string{"weave.example.users", "weave.example.orders"}
	received := make(map[string]chan string, len(destinations))
	var panicUsers atomic.Bool
	var retryOrders atomic.Bool
	for _, destination := range destinations {
		received[destination] = make(chan string, 1)
		handler := func(handlerCtx context.Context, msg *core.Message) error {
			if *backend == "kafka" && destination == "weave.example.users" && !panicUsers.Swap(true) {
				panic("temporary user handler panic")
			}
			if destination == "weave.example.orders" && !retryOrders.Swap(true) {
				return errors.New("temporary order handler failure")
			}
			select {
			case received[destination] <- msg.BodyString():
				return nil
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		}
		if err := broker.Subscribe(ctx, destination, handler, core.WithHandlerErrorRetry()); err != nil {
			log.Fatal(err)
		}
	}

	for _, destination := range destinations {
		msg, err := codec.MarshalMessage(codec.JSON, map[string]string{"destination": destination, "status": "ok"})
		if err != nil {
			log.Fatal(err)
		}
		if err := broker.Publish(ctx, destination, msg); err != nil {
			log.Fatal(err)
		}
	}

	for _, destination := range destinations {
		select {
		case body := <-received[destination]:
			fmt.Printf("%s: %s\n", destination, body)
		case <-ctx.Done():
			log.Fatal(ctx.Err())
		}
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
		config.Kafka.AutoOffsetReset = "earliest"
		return kafka.NewBroker(config)
	default:
		return nil, fmt.Errorf("backend must be amqp or kafka")
	}
}
