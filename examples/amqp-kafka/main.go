package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/prabhatdotdev/weave/codec"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
	"github.com/prabhatdotdev/weave/transport/kafka"
)

func main() {
	backend := flag.String("backend", "amqp", "amqp or kafka")
	replyTopic := flag.String("reply-topic", "weave.example.client.replies", "provisioned Kafka reply topic")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := run(ctx, *backend, *replyTopic, os.Stdout)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, backend, replyTopic string, output io.Writer) error {
	runID := rand.Text()
	broker, err := newBroker(backend, "client", runID, replyTopic)
	if err != nil {
		return err
	}
	defer broker.Close()
	if err := broker.Connect(ctx); err != nil {
		return err
	}

	destinations := []string{"weave.example.users", "weave.example.orders"}
	received := make(map[string]chan string, len(destinations))
	for _, destination := range destinations {
		received[destination] = make(chan string, 1)
		handler := eventHandler(runID, destination, backend, received[destination])
		if err := broker.Subscribe(ctx, destination, handler, core.WithHandlerErrorRetry(), core.WithWorkerCount(4)); err != nil {
			return err
		}
	}
	if err := broker.(core.ConsumerReadiness).WaitReady(ctx); err != nil {
		return err
	}

	for _, destination := range destinations {
		msg, err := codec.MarshalMessage(codec.JSON, map[string]string{"destination": destination, "status": "ok"})
		if err != nil {
			return err
		}
		msg.MessageID = runID + "/" + destination
		msg.WithHeader("example-run", runID)
		publishOptions := []core.PublishOption{core.WithKey(destination), core.WithPersistent(), core.WithMandatory(), core.WithTimeout(5 * time.Second)}
		if backend == "kafka" && destination == "weave.example.orders" {
			publishOptions = append(publishOptions, core.WithPartition(1))
		}
		if err := broker.Publish(ctx, destination, msg, publishOptions...); err != nil {
			return err
		}
	}
	for _, destination := range destinations {
		select {
		case body := <-received[destination]:
			if _, err := fmt.Fprintf(output, "%s: %s\n", destination, body); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	responder, err := newBroker(backend, "responder", runID, "")
	if err != nil {
		return err
	}
	defer responder.Close()
	if err := responder.Connect(ctx); err != nil {
		return err
	}
	if err := responder.Subscribe(ctx, "weave.example.rpc", rpcHandler(runID, responder), core.WithHandlerErrorRetry()); err != nil {
		return err
	}
	if err := responder.(core.ConsumerReadiness).WaitReady(ctx); err != nil {
		return err
	}
	request := core.NewTextMessage("request").WithHeader("example-run", runID)
	request.MessageID = runID + "/rpc"
	for range 2 {
		response, err := broker.Call(ctx, "weave.example.rpc", request, core.WithTimeout(10*time.Second), core.WithMandatory())
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(output, "weave.example.rpc: %s\n", response.BodyString()); err != nil {
			return err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := responder.(core.Shutdowner).Shutdown(shutdownCtx); err != nil {
		return err
	}
	return broker.(core.Shutdowner).Shutdown(shutdownCtx)
}

func eventHandler(runID, destination, backend string, received chan<- string) core.Handler {
	var failed atomic.Bool
	policy := core.DefaultRetryPolicy()
	policy.Backoff = core.JitteredBackoff(core.ExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 2))
	return core.RetryHandler(func(ctx context.Context, msg *core.Message) error {
		// Demo topics retain records; earlier runs must not fill this run's result channel.
		if msg.GetHeader("example-run") != runID {
			return nil
		}
		if destination == "weave.example.orders" && !failed.Swap(true) {
			return errors.New("temporary order handler failure")
		}
		body := msg.BodyString()
		if backend == "kafka" {
			body = fmt.Sprintf("partition=%d %s", msg.Partition, body)
		}
		select {
		case received <- body:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, policy)
}

func rpcHandler(runID string, publisher core.Publisher) core.Handler {
	return func(ctx context.Context, request *core.Message) error {
		if request.GetHeader("example-run") != runID {
			return nil
		}
		if request.ReplyTo == "" {
			return core.ErrNoReplyTo
		}
		response := core.NewTextMessage("pong: " + request.BodyString()).WithCorrelationID(request.CorrelationID)
		return publisher.Publish(ctx, request.ReplyTo, response, core.WithTimeout(5*time.Second), core.WithMandatory())
	}
}

func newBroker(backend, instance, runID, replyTopic string) (core.MessageBroker, error) {
	config := core.DefaultConfig()
	config.MaxPendingCalls = 16
	switch backend {
	case "amqp":
		config.AMQP = core.DefaultAMQPConfig()
		config.AMQP.QueueDurable = true
		return amqp.NewBroker(config)
	case "kafka":
		config.Kafka = core.DefaultKafkaConfig()
		config.Kafka.ClientID = "weave-example-" + instance + "-" + runID
		config.Kafka.ConsumerGroup = config.Kafka.ClientID
		config.Kafka.ReplyTopic = replyTopic
		config.Kafka.RequiredAcks = -1
		config.Kafka.AutoOffsetReset = "earliest"
		return kafka.NewBroker(config)
	default:
		return nil, fmt.Errorf("backend must be amqp or kafka")
	}
}
