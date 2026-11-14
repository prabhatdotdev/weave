// Package validation checks broker inputs before they reach a transport.
package validation

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/prabhatdotdev/weave/core"
)

func invalid(message string) error { return fmt.Errorf("%w: %s", core.ErrInvalidConfig, message) }

func Context(ctx context.Context) error {
	if ctx == nil {
		return invalid("context must not be nil")
	}
	return ctx.Err()
}

// Config snapshots caller-owned configuration before applying transport defaults.
func Config(config *core.Config) (*core.Config, error) {
	if config == nil {
		config = core.DefaultConfig()
	}
	if config.ConnectionRetry < 0 || config.RetryDelay < 0 {
		return nil, invalid("connection retries and retry delay must not be negative")
	}
	if config.MaxPendingCalls < 0 {
		return nil, invalid("pending RPC limit must not be negative")
	}
	copy := *config
	if config.AMQP != nil {
		amqp := *config.AMQP
		amqp.Endpoints = append([]string(nil), amqp.Endpoints...)
		amqp.TLS = cloneTLS(amqp.TLS)
		copy.AMQP = &amqp
	}
	if config.Kafka != nil {
		kafka := *config.Kafka
		kafka.Brokers = append([]string(nil), kafka.Brokers...)
		kafka.TLS = cloneTLS(kafka.TLS)
		if kafka.SASL != nil {
			sasl := *kafka.SASL
			kafka.SASL = &sasl
		}
		copy.Kafka = &kafka
	}
	return &copy, nil
}

func cloneTLS(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	copy := config.Clone()
	if copy.RootCAs != nil {
		copy.RootCAs = copy.RootCAs.Clone()
	}
	if copy.ClientCAs != nil {
		copy.ClientCAs = copy.ClientCAs.Clone()
	}
	if copy.MinVersion == 0 {
		copy.MinVersion = tls.VersionTLS12
	}
	return copy
}

func validateTLS(config *tls.Config) error {
	if config != nil && (config.MinVersion < tls.VersionTLS10 || config.MinVersion > tls.VersionTLS13 || config.MaxVersion != 0 && (config.MaxVersion < tls.VersionTLS10 || config.MaxVersion > tls.VersionTLS13)) {
		return invalid("unsupported TLS protocol version")
	}
	if config != nil && config.MaxVersion != 0 && config.MinVersion > config.MaxVersion {
		return invalid("TLS minimum version exceeds its maximum version")
	}
	return nil
}

func AMQP(config *core.AMQPConfig) error {
	if len(config.Endpoints) == 0 {
		if err := amqpHost(config.Host); err != nil {
			return err
		}
		if config.Port < 1 || config.Port > 65535 {
			return invalid("AMQP port must be between 1 and 65535")
		}
	}
	for _, endpoint := range config.Endpoints {
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil || amqpHost(host) != nil {
			return invalid("AMQP endpoints must use host:port addresses")
		}
		if port == "" || strings.IndexFunc(port, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return invalid("AMQP endpoint ports must be numeric")
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return invalid("AMQP endpoint ports must be between 1 and 65535")
		}
	}
	if config.Heartbeat < 0 {
		return invalid("AMQP heartbeat must not be negative")
	}
	if err := ShortStrings(config.Exchange, config.VHost); err != nil {
		return err
	}
	return validateTLS(config.TLS)
}

func amqpHost(host string) error {
	if !utf8.ValidString(host) || host == "" || strings.ContainsAny(host, "/@\\?#") || strings.IndexFunc(host, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return invalid("AMQP host must be a hostname or IP address")
	}
	if strings.Contains(host, ":") {
		if _, err := netip.ParseAddr(host); err != nil {
			return invalid("AMQP host must not include a port or invalid IPv6 address")
		}
	} else if strings.Contains(host, "%") {
		return invalid("AMQP hostname must not contain percent escapes")
	}
	return nil
}

func Kafka(config *core.KafkaConfig) error {
	if len(config.Brokers) == 0 {
		return invalid("at least one Kafka broker is required")
	}
	for _, address := range config.Brokers {
		host, port, err := net.SplitHostPort(address)
		if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " /@\t\r\n") {
			return invalid("Kafka brokers must use host:port addresses")
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return invalid("Kafka broker ports must be between 1 and 65535")
		}
	}
	if config.RequiredAcks != 1 && config.RequiredAcks != -1 {
		return invalid("Kafka RequiredAcks must be 1 (leader) or -1 (all in-sync replicas)")
	}
	if config.MaxRetries < 0 || config.RetryBackoff < 0 {
		return invalid("Kafka retries and retry backoff must not be negative")
	}
	if config.AutoOffsetReset != "latest" && config.AutoOffsetReset != "earliest" {
		return invalid("Kafka AutoOffsetReset must be latest or earliest")
	}
	if config.ConsumerGroup != "" && strings.TrimSpace(config.ConsumerGroup) == "" {
		return invalid("Kafka consumer group must not be blank")
	}
	if config.ReplyTopic != "" {
		if err := KafkaTopic(config.ReplyTopic); err != nil {
			return err
		}
	}
	return validateTLS(config.TLS)
}

var kafkaTopic = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func KafkaTopic(topic string) error {
	if len(topic) == 0 || len(topic) > 249 || topic == "." || topic == ".." || !kafkaTopic.MatchString(topic) {
		return invalid("Kafka topic must contain 1–249 ASCII letters, digits, dots, underscores, or hyphens, and cannot be . or ..")
	}
	return nil
}

func ShortStrings(values ...string) error {
	for _, value := range values {
		if len(value) > 255 || !utf8.ValidString(value) {
			return invalid("AMQP short strings must be valid UTF-8 and at most 255 bytes")
		}
	}
	return nil
}

func Publish(ctx context.Context, destination string, message *core.Message, opts ...core.PublishOption) (*core.PublishOptions, error) {
	if err := Context(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(destination) == "" || message == nil {
		return nil, invalid("destination and message are required")
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, invalid("publish option must not be nil")
		}
	}
	options := core.ApplyPublishOptions(opts...)
	if options.Timeout < 0 || options.Partition < -1 || options.Partition > math.MaxInt32 || options.PartitionSet && options.Partition < 0 {
		return nil, invalid("timeout must not be negative and partition must fit a non-negative int32")
	}
	if options.Priority > 9 || options.DeliveryMode != 1 && options.DeliveryMode != 2 {
		return nil, invalid("priority must be between 0 and 9 and delivery mode must be 1 or 2")
	}
	if options.Expiration != "" {
		if _, err := strconv.ParseUint(options.Expiration, 10, 64); err != nil {
			return nil, invalid("expiration must be a non-negative number of milliseconds")
		}
	}
	return options, nil
}

func Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) (*core.SubscribeOptions, error) {
	if err := Context(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(destination) == "" || handler == nil {
		return nil, invalid("destination and handler are required")
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, invalid("subscribe option must not be nil")
		}
	}
	options := core.ApplySubscribeOptions(opts...)
	if options.WorkerCount < 0 || options.PrefetchCount < 0 || options.PrefetchCount > 65535 {
		return nil, invalid("worker count must not be negative and prefetch must be between 0 and 65535")
	}
	if options.HandlerErrorPolicy != core.HandlerErrorNoRetry && options.HandlerErrorPolicy != core.HandlerErrorRetry {
		return nil, invalid("unknown handler error policy")
	}
	if options.QueueBind && strings.TrimSpace(options.Exchange) == "" {
		return nil, invalid("queue binding requires an exchange")
	}
	if options.ConsumerGroup != "" && strings.TrimSpace(options.ConsumerGroup) == "" {
		return nil, invalid("consumer group must not be blank")
	}
	return options, nil
}

// ContextError keeps cancellation distinct from deadline expiry.
func ContextError(err error, operation, duration string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		if duration == "0s" || duration == "" {
			duration = "context deadline"
		}
		return &core.ErrTimeout{Operation: operation, Duration: duration}
	}
	return err
}
