package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prabhatdotdev/weave"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
	_ "github.com/prabhatdotdev/weave/transport/kafka"
)

const clientTimeout = 30 * time.Second

var errTryAgain = errors.New("temporary example failure")

type order struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type getOrderRequest struct {
	ID string `json:"id"`
}

type getOrderResponse struct {
	Order order `json:"order"`
}

type receivedEvent struct {
	Order   order
	Message *weave.Message
}

type destinations struct {
	orders  string
	native  string
	missing string
}

func main() {
	backend := flag.String("backend", "amqp", "message broker: amqp or kafka")
	role := flag.String("role", "", "process role: server or client")
	namespace := flag.String("namespace", "weave.example", "shared destination prefix")
	flag.Parse()

	selectedBackend := strings.ToLower(*backend)
	selectedRole := strings.ToLower(*role)

	var err error
	switch selectedRole {
	case "server":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = runServer(ctx, selectedBackend, *namespace)
	case "client":
		ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
		defer cancel()
		err = runClient(ctx, selectedBackend, *namespace)
	default:
		err = fmt.Errorf("role must be server or client, got %q", selectedRole)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] failed: %v\n", strings.ToUpper(selectedRole), err)
		os.Exit(1)
	}
}

func runServer(ctx context.Context, backend, namespace string) error {
	destinations, err := newDestinations(namespace)
	if err != nil {
		return err
	}

	serverConfig, err := newConfig(backend, "server", namespace)
	if err != nil {
		return err
	}
	server, err := weave.NewServer(serverConfig)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}
	defer server.Stop()

	events := make(chan receivedEvent, 1)
	var attempts atomic.Int32
	server.Handle(destinations.orders, newOrderHandler(server, &attempts, events))

	if err := server.Start(ctx); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	health := server.HealthReport()
	if health.Status != weave.HealthStatusHealthy {
		return fmt.Errorf("server health is %s", health.Status)
	}

	nativeBroker, err := startNativeSubscriber(ctx, backend, namespace, destinations.native)
	if err != nil {
		return err
	}
	defer nativeBroker.Close()

	fmt.Printf("[SERVER] ready backend=%s health=%s\n", backend, health.Status)
	fmt.Printf("[SERVER] orders destination: %s\n", destinations.orders)
	fmt.Printf("[SERVER] native destination: %s\n", destinations.native)
	fmt.Println("[SERVER] waiting for client; press Ctrl+C to stop")

	for {
		select {
		case event := <-events:
			fmt.Printf(
				"[SERVER] processed event id=%s attempts=%d content-type=%s correlation=%s\n",
				event.Order.ID,
				attempts.Load(),
				event.Message.ContentType,
				event.Message.CorrelationID,
			)
		case <-ctx.Done():
			fmt.Println("[SERVER] stopped")
			return nil
		}
	}
}

func runClient(ctx context.Context, backend, namespace string) error {
	destinations, err := newDestinations(namespace)
	if err != nil {
		return err
	}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	clientConfig, err := newConfig(backend, "client", runID)
	if err != nil {
		return err
	}
	client, err := weave.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect client: %w", err)
	}

	health := client.HealthReport()
	if health.Status != weave.HealthStatusHealthy {
		return fmt.Errorf("client health is %s", health.Status)
	}
	fmt.Printf("[CLIENT] connected backend=%s health=%s\n", backend, health.Status)

	if err := publishEvent(ctx, client, destinations.orders, runID); err != nil {
		return err
	}
	if err := callOrder(ctx, client, destinations.orders, runID); err != nil {
		return err
	}

	_, err = client.Call(ctx, destinations.missing, weave.NewTextMessage("timeout"), weave.WithTimeout(250*time.Millisecond))
	if !weave.IsTimeout(err) {
		return fmt.Errorf("timeout check returned %v", err)
	}
	fmt.Println("[CLIENT] timeout classified as weave timeout")

	if err := publishBackendSpecific(ctx, backend, namespace, destinations.native, client); err != nil {
		return err
	}

	fmt.Printf("[CLIENT] done backend=%s; check the server terminal for consumed messages\n", backend)
	return nil
}

func publishEvent(ctx context.Context, publisher weave.Publisher, destination, runID string) error {
	message, err := weave.MarshalMessage(weave.JSON, order{ID: "order-1", Status: "created"})
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	message.
		WithHeader("example-kind", "event").
		WithHeader("source", "amqp-kafka-example").
		WithCorrelationID("event-" + runID)

	if err := publisher.Publish(ctx, destination, message); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	fmt.Printf("[CLIENT] published event id=order-1 content-type=%s correlation=%s\n", message.ContentType, message.CorrelationID)
	return nil
}

func callOrder(ctx context.Context, caller weave.Caller, destination, runID string) error {
	request, err := weave.MarshalMessage(weave.JSON, getOrderRequest{ID: "order-1"})
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	request.WithHeader("example-kind", "rpc").WithCorrelationID("rpc-" + runID)

	reply, err := caller.Call(ctx, destination, request, weave.WithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("call order service: %w", err)
	}
	var response getOrderResponse
	if err := weave.UnmarshalMessage(weave.JSON, reply, &response); err != nil {
		return fmt.Errorf("decode reply: %w", err)
	}
	if response.Order.ID != "order-1" || reply.CorrelationID != "rpc-"+runID {
		return errors.New("RPC response or correlation ID did not match")
	}
	fmt.Printf("[CLIENT] fetched result from server: %s\n", string(reply.Body))
	fmt.Printf("[CLIENT] RPC correlation=%s\n", reply.CorrelationID)
	return nil
}

func newOrderHandler(publisher weave.Publisher, attempts *atomic.Int32, events chan<- receivedEvent) weave.Handler {
	handler := func(ctx context.Context, msg *weave.Message) error {
		switch msg.GetHeader("example-kind") {
		case "event":
			var event order
			if err := weave.UnmarshalMessage(weave.JSON, msg, &event); err != nil {
				return err
			}
			if msg.ContentType != weave.JSON.ContentType() {
				return fmt.Errorf("content type = %q", msg.ContentType)
			}
			if msg.GetHeader("source") != "amqp-kafka-example" {
				return errors.New("source header was not preserved")
			}
			attempt := attempts.Add(1)
			fmt.Printf(
				"[SERVER] request kind=event body=%s correlation=%s attempt=%d\n",
				string(msg.Body),
				msg.CorrelationID,
				attempt,
			)
			if attempt == 1 {
				return errTryAgain
			}
			events <- receivedEvent{Order: event, Message: msg.Clone()}
			return nil
		case "rpc":
			var request getOrderRequest
			if err := weave.UnmarshalMessage(weave.JSON, msg, &request); err != nil {
				return err
			}
			if msg.ReplyTo == "" {
				return weave.ErrNoReplyTo
			}
			fmt.Printf(
				"[SERVER] request kind=rpc body=%s correlation=%s reply-to=%s\n",
				string(msg.Body),
				msg.CorrelationID,
				msg.ReplyTo,
			)
			reply, err := weave.MarshalMessage(weave.JSON, getOrderResponse{
				Order: order{ID: request.ID, Status: "created"},
			})
			if err != nil {
				return err
			}
			reply.CorrelationID = msg.CorrelationID
			return publisher.Publish(ctx, msg.ReplyTo, reply)
		default:
			return errors.New("unknown example message kind")
		}
	}

	return weave.RetryHandler(handler, weave.RetryPolicy{
		MaxAttempts: 2,
		Backoff:     weave.FixedBackoff(10 * time.Millisecond),
	})
}

func startNativeSubscriber(ctx context.Context, backend, namespace, destination string) (weave.MessageBroker, error) {
	config, err := newConfig(backend, "native", namespace)
	if err != nil {
		return nil, err
	}
	broker, err := weave.New(config)
	if err != nil {
		return nil, fmt.Errorf("create native broker: %w", err)
	}
	if err := broker.Connect(ctx); err != nil {
		broker.Close()
		return nil, fmt.Errorf("connect native broker: %w", err)
	}

	handler := func(_ context.Context, msg *weave.Message) error {
		fmt.Printf("[SERVER] request kind=native body=%s\n", string(msg.Body))
		switch backend {
		case "amqp":
			fmt.Println("[SERVER] AMQP metadata received with prefetch=1")
			return nil
		case "kafka":
			if msg.Subject != namespace || msg.Partition != 0 {
				return fmt.Errorf("Kafka metadata key=%q partition=%d", msg.Subject, msg.Partition)
			}
			fmt.Printf("[SERVER] Kafka metadata key=%s partition=%d offset=%d\n", msg.Subject, msg.Partition, msg.Offset)
			return nil
		default:
			return fmt.Errorf("unsupported backend %q", backend)
		}
	}

	var subscribeOptions []weave.SubscribeOption
	switch backend {
	case "amqp":
		subscribeOptions = []weave.SubscribeOption{weave.WithPrefetchCount(1)}
	case "kafka":
	default:
		broker.Close()
		return nil, fmt.Errorf("unsupported backend %q", backend)
	}

	if err := broker.Subscribe(ctx, destination, handler, subscribeOptions...); err != nil {
		broker.Close()
		return nil, fmt.Errorf("subscribe native example: %w", err)
	}
	return broker, nil
}

func publishBackendSpecific(ctx context.Context, backend, namespace, destination string, publisher weave.Publisher) error {
	message := weave.NewTextMessage(`{"native":true}`).WithContentType(weave.JSON.ContentType())

	switch backend {
	case "amqp":
		if err := publisher.Publish(
			ctx,
			destination,
			message,
			weave.WithPersistent(),
			weave.WithPriority(5),
			weave.WithExpiration("30000"),
		); err != nil {
			return fmt.Errorf("publish AMQP example: %w", err)
		}
		fmt.Println("[CLIENT] published AMQP metadata persistent=true priority=5 ttl=30s")
		return nil
	case "kafka":
		message.WithSubject(namespace)
		if err := publisher.Publish(ctx, destination, message, weave.WithKey(namespace), weave.WithPartition(0)); err != nil {
			return fmt.Errorf("publish Kafka example: %w", err)
		}
		fmt.Printf("[CLIENT] published Kafka metadata key=%s partition=0\n", namespace)
		return nil
	default:
		return fmt.Errorf("unsupported backend %q", backend)
	}
}

func newDestinations(namespace string) (destinations, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return destinations{}, errors.New("namespace must not be empty")
	}
	for _, char := range namespace {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '.' || char == '_' || char == '-' {
			continue
		}
		return destinations{}, fmt.Errorf("namespace %q contains unsupported character %q", namespace, char)
	}
	return destinations{
		orders:  namespace + ".orders",
		native:  namespace + ".native",
		missing: namespace + ".missing",
	}, nil
}

func newConfig(backend, role, runID string) (*weave.Config, error) {
	switch backend {
	case "amqp":
		port, err := envInt("AMQP_PORT", 5672)
		if err != nil {
			return nil, err
		}
		config := weave.DefaultConfig()
		config.ConnectionName = "weave-example-" + role + "-" + runID
		config.ConnectionRetry = 1
		config.RetryDelay = 200 * time.Millisecond
		config.AMQP.Host = env("AMQP_HOST", "localhost")
		config.AMQP.Port = port
		config.AMQP.Username = env("AMQP_USERNAME", "guest")
		config.AMQP.Password = env("AMQP_PASSWORD", "guest")
		config.AMQP.VHost = env("AMQP_VHOST", "/")
		config.AMQP.QueueAutoDelete = true
		config.AMQP.QueueExclusive = true
		return config, nil
	case "kafka":
		brokers := splitNonEmpty(env("KAFKA_BROKERS", "localhost:9092"))
		if len(brokers) == 0 {
			return nil, errors.New("KAFKA_BROKERS must contain at least one address")
		}
		kafka := weave.DefaultKafkaConfig()
		kafka.Brokers = brokers
		kafka.ClientID = "weave-example-" + role + "-" + runID
		kafka.ConsumerGroup = kafka.ClientID
		kafka.AutoOffsetReset = "earliest"
		return &weave.Config{
			Backend:         "kafka",
			ConnectionName:  kafka.ClientID,
			ConnectionRetry: 1,
			RetryDelay:      200 * time.Millisecond,
			Kafka:           kafka,
		}, nil
	default:
		return nil, fmt.Errorf("backend must be amqp or kafka, got %q", backend)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	value := env(key, strconv.Itoa(fallback))
	number, err := strconv.Atoi(value)
	if err != nil || number < 1 || number > 65535 {
		return 0, fmt.Errorf("%s must be a port number, got %q", key, value)
	}
	return number, nil
}

func splitNonEmpty(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}
