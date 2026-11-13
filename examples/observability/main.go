package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
	"github.com/prabhatdotdev/weave/transport/kafka"
)

func loggingHook(logger *slog.Logger) core.EventHook {
	return func(ctx context.Context, event core.Event) {
		level := slog.LevelInfo
		if event.Err != nil {
			level = slog.LevelError
		}
		logger.Log(ctx, level, "weave event",
			"event", event.Name, "backend", event.Backend,
			"operation", event.Operation, "destination", event.Destination,
			"duration_ms", float64(event.Duration)/float64(time.Millisecond),
			"active_handlers", event.ActiveHandlers, "pending_rpcs", event.PendingRPCs,
			"correlation_id", event.CorrelationID, "error", event.Err,
		)
	}
}

func newBroker(backend, instance string, logger *slog.Logger) (core.MessageBroker, error) {
	config := core.DefaultConfig()
	config.EventHook = loggingHook(logger.With("broker", instance))
	switch backend {
	case "amqp":
		config.AMQP = core.DefaultAMQPConfig()
		config.AMQP.QueueDurable = true
		return amqp.NewBroker(config)
	case "kafka":
		config.Kafka = core.DefaultKafkaConfig()
		config.Kafka.ClientID = "weave-observability-" + instance + "-" + rand.Text()
		config.Kafka.ConsumerGroup = config.Kafka.ClientID
		if instance == "client" {
			config.Kafka.ReplyTopic = "weave.example.observability.replies"
		}
		config.Kafka.AutoOffsetReset = "earliest"
		return kafka.NewBroker(config)
	default:
		return nil, fmt.Errorf("backend must be amqp or kafka")
	}
}

func run(ctx context.Context, backend string, logger *slog.Logger) error {
	client, err := newBroker(backend, "client", logger)
	if err != nil {
		return err
	}
	defer client.Close()
	responder, err := newBroker(backend, "responder", logger)
	if err != nil {
		return err
	}
	defer responder.Close()
	if err := client.Connect(ctx); err != nil {
		return err
	}
	if err := responder.Connect(ctx); err != nil {
		return err
	}
	if err := responder.Subscribe(ctx, "weave.example.observability", func(ctx context.Context, request *core.Message) error {
		response := core.NewTextMessage("observed").WithCorrelationID(request.CorrelationID)
		return responder.Publish(ctx, request.ReplyTo, response, core.WithMandatory())
	}, core.WithHandlerErrorRetry()); err != nil {
		return err
	}
	if err := responder.(core.ConsumerReadiness).WaitReady(ctx); err != nil {
		return err
	}
	response, err := client.Call(ctx, "weave.example.observability", core.NewTextMessage("observe me"), core.WithTimeout(10*time.Second), core.WithMandatory())
	if err != nil {
		return err
	}
	if response.BodyString() != "observed" {
		return fmt.Errorf("unexpected RPC response: %q", response.BodyString())
	}
	logger.InfoContext(ctx, "RPC completed", "body", response.BodyString())
	if err := responder.(core.Shutdowner).Shutdown(ctx); err != nil {
		return err
	}
	return client.(core.Shutdowner).Shutdown(ctx)
}

func main() {
	backend := flag.String("backend", "amqp", "amqp or kafka")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx, *backend, logger); err != nil {
		logger.Error("example failed", "error", err)
		os.Exit(1)
	}
}
