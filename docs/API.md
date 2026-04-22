# API Guide

This document is a high-level guide to the currently implemented Weave surface.

For canonical package signatures and symbol-level docs, use pkg.go.dev:

- https://pkg.go.dev/github.com/prabhatdotdev/weave
- https://pkg.go.dev/github.com/prabhatdotdev/weave/core
- https://pkg.go.dev/github.com/prabhatdotdev/weave/runtime

## Implemented Backends

The transport implementations currently present in this repository are:

- AMQP / RabbitMQ
- Apache Kafka

## Core Types

### MessageBroker

```go
type MessageBroker interface {
    Connect(ctx context.Context) error
    Close() error
    IsConnected() bool
    Backend() string

    Publish(ctx context.Context, destination string, message *Message, opts ...PublishOption) error
    Call(ctx context.Context, destination string, message *Message, opts ...PublishOption) (*Message, error)
    Subscribe(ctx context.Context, destination string, handler Handler, opts ...SubscribeOption) error
}
```

### Message

```go
type Message struct {
    Body          []byte
    CorrelationID string
    ReplyTo       string
    Headers       map[string]string
    ContentType   string
    MessageID     string
    Timestamp     time.Time
    Subject       string
    Partition     int32
    Offset        int64
}
```

Common constructors and helpers:

```go
func NewMessage(body []byte) *Message
func NewTextMessage(body string) *Message

func (m *Message) WithCorrelationID(id string) *Message
func (m *Message) WithReplyTo(replyTo string) *Message
func (m *Message) WithHeader(key, value string) *Message
func (m *Message) WithContentType(contentType string) *Message
func (m *Message) WithSubject(subject string) *Message
func (m *Message) Clone() *Message
func (m *Message) BodyString() string
func (m *Message) GetHeader(key string) string
```

### Handler

```go
type Handler func(ctx context.Context, msg *Message) error
```

Handlers receive the full `Message`, including metadata such as `ReplyTo`, `CorrelationID`, headers, and content type.

## Factory Functions

### New

```go
func New(config *Config) (MessageBroker, error)
```

Creates a broker using `config.Backend`.

### NewWithBackend

```go
func NewWithBackend(backend string, config *Config) (MessageBroker, error)
```

Creates a broker using an explicit backend name.

### Register

```go
func Register(name string, factory BrokerFactory)
```

Registers a backend implementation.

### AvailableBackends

```go
func AvailableBackends() []string
func IsBackendAvailable(name string) bool
```

Returns registered backends at runtime.

## Configuration

### Config

```go
type Config struct {
    Backend         string
    ConnectionName  string
    ConnectionRetry int
    RetryDelay      time.Duration

    Logger         EventLogger
    EventHook      EventHook
    Metrics        MetricsHook
    Tracing        TracingHook
    HealthReporter HealthReporter
    HealthHook     HealthHook

    AMQP  *AMQPConfig
    Kafka *KafkaConfig

    Host     string
    Port     int
    Username string
    Password string
    VHost    string
}
```

Notes:

- `AMQP` and `Kafka` are backed by implemented transports.
- `Logger`, `EventHook`, `Metrics`, `Tracing`, `HealthReporter`, and `HealthHook` are optional observability integrations.

### Defaults

```go
func DefaultConfig() *Config
func DefaultAMQPConfig() *AMQPConfig
func DefaultKafkaConfig() *KafkaConfig
```

The default broker config targets AMQP.

## Runtime APIs

### Client

```go
func NewClient(config *Config) (*Client, error)
func NewClientWithBroker(broker MessageBroker, config *Config) *Client
```

Implemented methods:

```go
func (c *Client) Connect(ctx context.Context) error
func (c *Client) Close() error
func (c *Client) IsConnected() bool
func (c *Client) Backend() string
func (c *Client) Publish(ctx context.Context, destination string, msg *Message, opts ...PublishOption) error
func (c *Client) Call(ctx context.Context, destination string, msg *Message, opts ...PublishOption) (*Message, error)
func (c *Client) Broker() MessageBroker
func (c *Client) Config() *Config
```

### Server

```go
func NewServer(config *Config) (*Server, error)
func NewServerWithBroker(broker MessageBroker, config *Config) *Server
```

Implemented methods:

```go
func (s *Server) Handle(destination string, handler Handler) *Server
func (s *Server) Start(ctx context.Context) error
func (s *Server) Stop() error
func (s *Server) Publish(ctx context.Context, destination string, msg *Message, opts ...PublishOption) error
func (s *Server) Call(ctx context.Context, destination string, msg *Message, opts ...PublishOption) (*Message, error)
func (s *Server) Broker() MessageBroker
func (s *Server) Config() *Config
func (s *Server) IsStarted() bool
```

`Service` and the `NewService` helpers remain as deprecated aliases for backward compatibility.

## Options

Publish and subscribe operations support functional options defined in `core/options.go`, including timeouts and transport-specific behavior such as AMQP persistence or Kafka keys.

See [TRANSPORTS.md](TRANSPORTS.md) and the examples in [README.md](README.md) for concrete usage.

## Production Helpers

Weave now includes transport-agnostic helpers for common production patterns:

### Handler retry

```go
type RetryPolicy struct {
    MaxAttempts int
    Backoff     BackoffStrategy
    Retryable   RetryPredicate
    OnRetry     RetryHook
}

func DefaultRetryPolicy() RetryPolicy
func FixedBackoff(delay time.Duration) BackoffStrategy
func ExponentialBackoff(initialDelay, maxDelay time.Duration, multiplier float64) BackoffStrategy
func RetryAttempt(ctx context.Context) int
func RetryHandler(handler Handler, policy RetryPolicy) Handler
```

Use `RetryHandler(...)` for bounded in-process retries, then combine it with transport-level subscribe options such as `WithHandlerErrorNoRetry()` or `WithHandlerErrorRetry()` depending on how you want the broker to behave after the wrapper gives up.

### Structured error payloads and dead-letter envelopes

```go
const (
    ErrorContentType      = "application/vnd.weave.error+json"
    DeadLetterContentType = "application/vnd.weave.dead-letter+json"
)

type ErrorPayload struct { ... }
type DeadLetterOptions struct { ... }
type DeadLetterEnvelope struct { ... }

func NewErrorPayload(code string, err error) ErrorPayload
func EncodeErrorPayload(payload ErrorPayload) ([]byte, error)
func DecodeErrorPayload(data []byte) (ErrorPayload, error)
func NewErrorMessage(payload ErrorPayload) (*Message, error)
func DecodeErrorMessage(msg *Message) (ErrorPayload, error)
func NewDeadLetterEnvelope(msg *Message, err error, opts DeadLetterOptions) DeadLetterEnvelope
func DecodeDeadLetterMessage(msg *Message) (DeadLetterEnvelope, error)
```

These helpers provide a stable JSON envelope for RPC errors and dead-letter messages across AMQP and Kafka.

### RPC retry and circuit breaking

`runtime.Client` and `runtime.Server` now expose:

```go
func (c *Client) CallWithPolicy(ctx context.Context, destination string, msg *Message, policy CallPolicy, opts ...PublishOption) (*Message, error)
func (s *Server) CallWithPolicy(ctx context.Context, destination string, msg *Message, policy CallPolicy, opts ...PublishOption) (*Message, error)
```

with:

```go
type CallPolicy struct {
    MaxAttempts    int
    Backoff        BackoffStrategy
    Retryable      RetryPredicate
    OnRetry        CallRetryHook
    CircuitBreaker *CircuitBreaker
}

func DefaultCallPolicy() CallPolicy
func NewCircuitBreaker(options CircuitBreakerOptions) *CircuitBreaker
```

### Health snapshots

`runtime.Client` and `runtime.Server` now expose `HealthReport()` helpers, and `Config` can forward health snapshots through `HealthReporter` and `HealthHook`.

## Protocol Buffers

There is no dedicated `ProtobufService`, typed protobuf runtime, or worker-pool API implemented in this repository.

Current protobuf support is provided through the generic codec layer:

1. Use `codec.Protobuf` (or root re-exports like `weave.Protobuf`) to encode and decode protobuf payloads.
2. Use `codec.MarshalMessage(...)` / `codec.UnmarshalMessage(...)` when working directly with `Message`.
3. Use `Client.PublishWithCodec(...)`, `Client.CallWithCodec(...)`, `Server.PublishWithCodec(...)`, or `Server.CallWithCodec(...)` to keep protobuf encoding/decoding on the runtime path.
4. Fall back to manual `proto.Marshal` / `proto.Unmarshal` when you need total control over wire bytes or mixed request/response handling.

See [PROTOBUF.md](PROTOBUF.md) and [examples/protobuf](../examples/protobuf) for the supported pattern.

## Error Handling

Weave exposes typed errors in `core/errors.go`, including connection, publish, subscribe, timeout, and backend selection errors.

At the root package, helper predicates are re-exported for common checks such as timeout and not-connected handling. See the source in `core/errors.go` and examples in [README.md](README.md).

## See Also

- [Architecture Overview](ARCHITECTURE.md)
- [Transport Configuration](TRANSPORTS.md)
- [Protocol Buffers Guide](PROTOBUF.md)
- [Quick Start](QUICKSTART.md)
