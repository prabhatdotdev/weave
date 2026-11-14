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
		switch event.Level {
		case core.EventLevelDebug:
			level = slog.LevelDebug
		case core.EventLevelWarn:
			level = slog.LevelWarn
		case core.EventLevelError:
			level = slog.LevelError
		}
		if event.Err != nil {
			level = slog.LevelError
		}
		logger.Log(ctx, level, "weave event",
			"event", event.Name, "backend", event.Backend,
			"operation", event.Operation, "destination", event.Destination,
			"duration_ms", float64(event.Duration)/float64(time.Millisecond),
			"active_handlers", event.ActiveHandlers, "pending_rpcs", event.PendingRPCs,
			"correlation_id", event.CorrelationID, "error", event.Err,
			"fields", event.Fields,
		)
	}
}

func newBroker(backend, instance, replyTopic string, logger *slog.Logger) (core.MessageBroker, error) {
	config := core.DefaultConfig()
	config.MaxPendingCalls = 16
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
			config.Kafka.ReplyTopic = replyTopic
		}
		config.Kafka.RequiredAcks = -1
		config.Kafka.AutoOffsetReset = "earliest"
		return kafka.NewBroker(config)
	default:
		return nil, fmt.Errorf("backend must be amqp or kafka")
	}
}

func run(ctx context.Context, backend, replyTopic string, logger *slog.Logger) error {
	runID := rand.Text()
	client, err := newBroker(backend, "client", replyTopic, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	responder, err := newBroker(backend, "responder", "", logger)
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
	if err := responder.Subscribe(ctx, "weave.example.observability", observedHandler(runID, responder), core.WithHandlerErrorRetry()); err != nil {
		return err
	}
	if err := responder.(core.ConsumerReadiness).WaitReady(ctx); err != nil {
		return err
	}
	request := core.NewTextMessage("observe me").WithHeader("example-run", runID)
	request.MessageID = runID + "/observe"
	response, err := client.Call(ctx, "weave.example.observability", request, core.WithTimeout(10*time.Second), core.WithMandatory())
	if err != nil {
		return err
	}
	if response.BodyString() != "observed" {
		return fmt.Errorf("unexpected RPC response: %q", response.BodyString())
	}
	logger.InfoContext(ctx, "RPC completed", "body", response.BodyString())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := responder.(core.Shutdowner).Shutdown(shutdownCtx); err != nil {
		return err
	}
	return client.(core.Shutdowner).Shutdown(shutdownCtx)
}

func observedHandler(runID string, publisher core.Publisher) core.Handler {
	return func(ctx context.Context, request *core.Message) error {
		if request.GetHeader("example-run") != runID {
			return nil
		}
		if request.ReplyTo == "" {
			return core.ErrNoReplyTo
		}
		response := core.NewTextMessage("observed").WithCorrelationID(request.CorrelationID)
		return publisher.Publish(ctx, request.ReplyTo, response, core.WithMandatory(), core.WithTimeout(5*time.Second))
	}
}

func main() {
	backend := flag.String("backend", "amqp", "amqp or kafka")
	replyTopic := flag.String("reply-topic", "weave.example.observability.replies", "provisioned Kafka reply topic")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := run(ctx, *backend, *replyTopic, logger)
	cancel()
	if err != nil {
		logger.Error("example failed", "error", err)
		os.Exit(1)
	}
}
