package validation_test

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/testkit"
	"github.com/prabhatdotdev/weave/transport/amqp"
	"github.com/prabhatdotdev/weave/transport/kafka"
)

func TestPublicOperationsRejectInvalidInputs(t *testing.T) {
	for _, constructor := range []struct {
		name string
		new  func() (core.MessageBroker, error)
	}{
		{"amqp", func() (core.MessageBroker, error) { return amqp.NewBroker(nil) }},
		{"kafka", func() (core.MessageBroker, error) { return kafka.NewBroker(nil) }},
		{"mock", func() (core.MessageBroker, error) { return testkit.NewMockBroker(), nil }},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			broker, err := constructor.new()
			if err != nil {
				t.Fatal(err)
			}
			defer broker.Close()
			if err := broker.Connect(nil); !errors.Is(err, core.ErrInvalidConfig) {
				t.Fatalf("Connect(nil) = %v", err)
			}
			for _, input := range []struct {
				name        string
				ctx         context.Context
				destination string
				message     *core.Message
				opts        []core.PublishOption
			}{
				{"nil context", nil, "orders", core.NewMessage(nil), nil},
				{"empty destination", context.Background(), "", core.NewMessage(nil), nil},
				{"blank destination", context.Background(), " \t", core.NewMessage(nil), nil},
				{"nil message", context.Background(), "orders", nil, nil},
				{"nil option", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{nil}},
				{"negative timeout", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithTimeout(-time.Second)}},
				{"negative partition", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithPartition(-2)}},
				{"explicit negative partition", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithPartition(-1)}},
				{"overflow partition", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithPartition(math.MaxInt32 + 1)}},
				{"invalid priority", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithPriority(10)}},
				{"invalid delivery mode", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{func(o *core.PublishOptions) { o.DeliveryMode = 3 }}},
				{"invalid expiration", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithExpiration("forever")}},
				{"negative expiration", context.Background(), "orders", core.NewMessage(nil), []core.PublishOption{core.WithExpiration("-1")}},
			} {
				t.Run(input.name, func(t *testing.T) {
					if err := broker.Publish(input.ctx, input.destination, input.message, input.opts...); !errors.Is(err, core.ErrInvalidConfig) {
						t.Errorf("Publish() = %v, want ErrInvalidConfig", err)
					}
					if _, err := broker.Call(input.ctx, input.destination, input.message, input.opts...); !errors.Is(err, core.ErrInvalidConfig) {
						t.Errorf("Call() = %v, want ErrInvalidConfig", err)
					}
				})
			}
			handler := func(context.Context, *core.Message) error { return nil }
			for _, input := range []struct {
				name        string
				ctx         context.Context
				destination string
				handler     core.Handler
				opts        []core.SubscribeOption
			}{
				{"nil context", nil, "orders", handler, nil},
				{"nil handler", context.Background(), "orders", nil, nil},
				{"empty destination", context.Background(), "", handler, nil},
				{"nil option", context.Background(), "orders", handler, []core.SubscribeOption{nil}},
				{"negative workers", context.Background(), "orders", handler, []core.SubscribeOption{core.WithWorkerCount(-1)}},
				{"negative prefetch", context.Background(), "orders", handler, []core.SubscribeOption{core.WithPrefetchCount(-1)}},
				{"overflow prefetch", context.Background(), "orders", handler, []core.SubscribeOption{core.WithPrefetchCount(65536)}},
				{"empty binding", context.Background(), "orders", handler, []core.SubscribeOption{core.WithQueueBind("", "key")}},
				{"blank consumer group", context.Background(), "orders", handler, []core.SubscribeOption{core.WithConsumerGroup(" ")}},
				{"unknown retry policy", context.Background(), "orders", handler, []core.SubscribeOption{func(o *core.SubscribeOptions) { o.HandlerErrorPolicy = "unknown" }}},
			} {
				t.Run("subscribe "+input.name, func(t *testing.T) {
					if err := broker.Subscribe(input.ctx, input.destination, input.handler, input.opts...); !errors.Is(err, core.ErrInvalidConfig) {
						t.Errorf("Subscribe() = %v, want ErrInvalidConfig", err)
					}
				})
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, err := range []error{
				broker.Connect(ctx),
				broker.Publish(ctx, "orders", core.NewMessage(nil)),
				broker.Subscribe(ctx, "orders", handler),
			} {
				if !errors.Is(err, context.Canceled) || core.IsTimeout(err) {
					t.Errorf("canceled operation returned %v", err)
				}
			}
			if _, err := broker.Call(ctx, "orders", core.NewMessage(nil)); !errors.Is(err, context.Canceled) || core.IsTimeout(err) {
				t.Errorf("canceled Call() returned %v", err)
			}
			var invalidDestinations []string
			if constructor.name == "kafka" {
				invalidDestinations = []string{".", "..", "bad/topic", "with spaces", strings.Repeat("a", 250)}
			} else if constructor.name == "amqp" {
				invalidDestinations = []string{strings.Repeat("a", 256), string([]byte{0xff})}
			}
			for _, destination := range invalidDestinations {
				if err := broker.Publish(context.Background(), destination, core.NewMessage(nil)); !errors.Is(err, core.ErrInvalidConfig) {
					t.Errorf("invalid destination Publish() returned %v", err)
				}
				if err := broker.Subscribe(context.Background(), destination, handler); !errors.Is(err, core.ErrInvalidConfig) {
					t.Errorf("invalid destination Subscribe() returned %v", err)
				}
			}
		})
	}
}

func TestConstructorsRejectInvalidConfiguration(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*core.Config)
	}{
		{"negative retries", func(c *core.Config) { c.ConnectionRetry = -1 }},
		{"negative backoff", func(c *core.Config) { c.RetryDelay = -time.Second }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			config := core.DefaultConfig()
			scenario.change(config)
			if _, err := amqp.NewBroker(config); !errors.Is(err, core.ErrInvalidConfig) {
				t.Errorf("AMQP constructor returned %v", err)
			}
			if _, err := kafka.NewBroker(config); !errors.Is(err, core.ErrInvalidConfig) {
				t.Errorf("Kafka constructor returned %v", err)
			}
		})
	}
	for _, scenario := range []struct {
		name   string
		change func(*core.AMQPConfig)
	}{
		{"empty host", func(c *core.AMQPConfig) { c.Host = "" }},
		{"host with scheme", func(c *core.AMQPConfig) { c.Host = "amqp://localhost" }},
		{"zero port", func(c *core.AMQPConfig) { c.Port = 0 }},
		{"overflow port", func(c *core.AMQPConfig) { c.Port = 65536 }},
		{"negative heartbeat", func(c *core.AMQPConfig) { c.Heartbeat = -time.Second }},
		{"long exchange", func(c *core.AMQPConfig) { c.Exchange = strings.Repeat("x", 256) }},
		{"invalid TLS range", func(c *core.AMQPConfig) {
			c.TLS = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS12}
		}},
	} {
		t.Run("AMQP "+scenario.name, func(t *testing.T) {
			config := core.DefaultConfig()
			config.AMQP = core.DefaultAMQPConfig()
			scenario.change(config.AMQP)
			if _, err := amqp.NewBroker(config); !errors.Is(err, core.ErrInvalidConfig) {
				t.Fatalf("constructor returned %v", err)
			}
		})
	}
	for _, scenario := range []struct {
		name   string
		change func(*core.KafkaConfig)
	}{
		{"no brokers", func(c *core.KafkaConfig) { c.Brokers = nil }},
		{"missing port", func(c *core.KafkaConfig) { c.Brokers = []string{"localhost"} }},
		{"invalid port", func(c *core.KafkaConfig) { c.Brokers = []string{"localhost:65536"} }},
		{"empty host", func(c *core.KafkaConfig) { c.Brokers = []string{":9092"} }},
		{"invalid offset", func(c *core.KafkaConfig) { c.AutoOffsetReset = "newest" }},
		{"zero acknowledgments", func(c *core.KafkaConfig) { c.RequiredAcks = 0 }},
		{"invalid acknowledgments", func(c *core.KafkaConfig) { c.RequiredAcks = 2 }},
		{"negative retries", func(c *core.KafkaConfig) { c.MaxRetries = -1 }},
		{"negative backoff", func(c *core.KafkaConfig) { c.RetryBackoff = -time.Second }},
		{"invalid reply topic", func(c *core.KafkaConfig) { c.ReplyTopic = "bad/topic" }},
		{"invalid heartbeat", func(c *core.KafkaConfig) { c.HeartbeatInterval = c.SessionTimeout }},
		{"empty SASL username", func(c *core.KafkaConfig) { c.SASL = &core.SASLConfig{Enable: true, Password: "password"} }},
		{"invalid TLS range", func(c *core.KafkaConfig) {
			c.TLS = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS12}
		}},
	} {
		t.Run("Kafka "+scenario.name, func(t *testing.T) {
			config := core.DefaultConfig()
			config.Kafka = core.DefaultKafkaConfig()
			scenario.change(config.Kafka)
			if _, err := kafka.NewBroker(config); !errors.Is(err, core.ErrInvalidConfig) {
				t.Fatalf("constructor returned %v", err)
			}
		})
	}
}
