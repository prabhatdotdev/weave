# Weave

[![Go Reference](https://pkg.go.dev/badge/github.com/prabhatdotdev/weave.svg)](https://pkg.go.dev/github.com/prabhatdotdev/weave)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/prabhatdotdev/weave)](https://goreportcard.com/report/github.com/prabhatdotdev/weave)

A unified abstraction layer for building message-driven microservices across message queue backends. Today, the implemented transports are RabbitMQ/AMQP and Apache Kafka.

## Feature Status

| Feature | Status | Details |
|---------|--------|---------|
| Client & Server abstractions | ✅ Stable | Documented API with automated unit and integration-style coverage |
| AMQP (RabbitMQ) transport | ✅ Stable | Covered by automated transport, reconnect, and race-detector tests |
| Kafka transport | ✅ Stable | Covered by automated transport, reconnect, and race-detector tests |
| Connection retry (basic) | ✅ Stable | Implemented for both backends |
| Observability hooks | ✅ Stable | Logging, metrics, and tracing extension points |
| Request-reply RPC | ✅ Stable | Core feature |
| Timeout support | ✅ Stable | Context-based |
| Testing utilities | ✅ Stable | Mock broker available |
| Connection recovery & reconnect | ✅ Stable | Recovery paths are covered by automated reconnect tests; semantics differ by backend |
| Protobuf integration | ✅ Stable | Built-in protobuf codec and codec-aware helpers; no typed APIs |
| Dead-letter handling | ✅ Stable | Standard dead-letter envelope helpers are implemented; broker-native routing remains backend-specific |
| Worker pool abstractions | 📋 Planned | Future enhancement |
| Schema validation helpers | 📋 Planned | Future enhancement |
| Tracing integration | ✅ Stable | Hook-based span integration with adapter examples |
| Health check endpoints | 📋 Planned | Future enhancement |
| Additional backends (NATS, Redis, etc.) | 📋 Reserved | Design-phase only, not under development |

**Status Legend:**
- ✅ Stable: Implemented, documented, and covered by automated validation for the currently supported surface
- 🔶 In Progress: Partially implemented, testing in progress
- 📋 Planned: Designed but not yet implemented
- 📦 Reserved: Considered for future work but not yet designed

## Features (Stable)

- 🔌 **Multi-Backend** - Single API for AMQP (RabbitMQ) and Apache Kafka
- 🖥️ **Client & Server** - Dedicated abstractions for both client and server roles
- 🚀 **Simple API** - REST-like request-response pattern over any message queue
- ⏱️ **Timeout Support** - Context-based timeout handling for all requests
- 📊 **Connection Management** - Automatic connection retry (basic) and monitoring
- 🎯 **Flexible Payloads** - Works with JSON, Protocol Buffers, and other payload formats via `Message.Body`
- 🧪 **Testable** - Built-in mock broker for unit testing
- 📊 **Concurrent** - Handles multiple concurrent requests efficiently
- 🧰 **Extensible** - Registry-based transport design for adding future backends
- 🔍 **Observability** - Structured logging, metrics, and tracing hooks

## Installation

```bash
go get github.com/prabhatdotdev/weave
```

Import the transport(s) you need:

```go
import (
    "github.com/prabhatdotdev/weave"
    
    // Import one or more implemented transports
    _ "github.com/prabhatdotdev/weave/transport/amqp"   // RabbitMQ
    _ "github.com/prabhatdotdev/weave/transport/kafka"  // Apache Kafka
)
```

## Quick Start

Weave provides two high-level abstractions:

- **Client** - For sending messages and making RPC calls (no subscriptions)
- **Server** - For subscribing to queues/topics and handling incoming messages

Payload format is intentionally transport-agnostic. Weave now ships built-in JSON and Protocol Buffers codecs, plus helpers like `weave.MarshalMessage(...)`, `weave.UnmarshalMessage(...)`, `Client.PublishWithCodec(...)`, and `Client.CallWithCodec(...)` so you can keep serialization concerns out of most call sites.

### Client Example (Sending Messages / RPC)

Use `Client` when your application only needs to send messages:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
    // Create client
    client, err := weave.NewClient(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }

    // Fire-and-forget publish
    msg := weave.NewMessage([]byte(`{"order_id": 123}`))
    if err := client.Publish(ctx, "orders", msg); err != nil {
        log.Fatal(err)
    }
    fmt.Println("Order sent!")

    // Request-reply RPC call
    request := weave.NewMessage([]byte(`{"user_id": 456}`))
    response, err := client.Call(ctx, "users.get", request, 
        weave.WithTimeout(5*time.Second))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("User data: %s\n", string(response.Body))
}
```

### Server Example (Handling Messages)

Use `Server` when building message-driven services:

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
    // Create server
    server, err := weave.NewServer(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    
    // Register handlers
    server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
        fmt.Printf("Processing order: %s\n", string(msg.Body))
        return nil
    })
    
    server.Handle("users.get", func(ctx context.Context, msg *weave.Message) error {
        fmt.Printf("User request: %s\n", string(msg.Body))
        // Reply is handled automatically via ReplyTo field
        return nil
    })

    // Start server (connects and subscribes to all handlers)
    if err := server.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()
    
    fmt.Println("Server running...")
    select {} // Keep running
}
```

## Architecture

### Client vs Server

| Feature | Client | Server |
|---------|--------|--------|
| `Connect()` | ✅ | ✅ |
| `Close()` | ✅ | ✅ (via `Stop()`) |
| `Publish()` | ✅ | ✅ |
| `Call()` (RPC) | ✅ | ✅ |
| `Subscribe()` | ❌ | ✅ (via `Handle()`) |
| Use case | Send messages, make RPC calls | Handle incoming messages |

### When to Use What

- **Use `Client`** when your application:
  - Only sends messages (e.g., a web API publishing events)
  - Makes RPC calls to other services
  - Doesn't need to handle incoming messages

- **Use `Server`** when your application:
  - Processes incoming messages from queues/topics
  - Implements a microservice that handles requests
  - Needs to subscribe to multiple destinations

- **Use raw `MessageBroker`** when you need:
  - Full control over both client and server operations
  - Direct access to all broker methods
  - Custom connection management

## Package Structure

```
weave/
├── weave.go           # Root package - re-exports core types
├── core/              # Core interfaces and types
│   ├── broker.go      # MessageBroker interface (Connector, Publisher, Caller, Subscriber)
│   ├── message.go     # Message type
│   ├── config.go      # Configuration types
│   ├── options.go     # Functional options
│   ├── errors.go      # Error types
│   └── registry.go    # Backend registry
├── transport/         # Transport implementations
│   ├── amqp/          # RabbitMQ/AMQP transport
│   └── kafka/         # Apache Kafka transport
├── runtime/           # High-level abstractions
│   ├── client.go      # Client (publish, call only)
│   └── service.go     # Server (subscribe, handle)
├── codec/             # Message encoding/decoding
│   └── codec.go       # JSON + protobuf codecs
└── testkit/           # Testing utilities
    └── mock.go        # Mock broker for testing
```

## Interface Composition

Weave uses composable interfaces for flexibility:

```go
// Connector - connection lifecycle
type Connector interface {
    Connect(ctx context.Context) error
    Close() error
    IsConnected() bool
    Backend() string
}

// Publisher - fire-and-forget messaging
type Publisher interface {
    Publish(ctx context.Context, destination string, message *Message, opts ...PublishOption) error
}

// Caller - request-reply RPC
type Caller interface {
    Call(ctx context.Context, destination string, message *Message, opts ...PublishOption) (*Message, error)
}

// Subscriber - message consumption
type Subscriber interface {
    Subscribe(ctx context.Context, destination string, handler Handler, opts ...SubscribeOption) error
}

// MessageBroker combines all interfaces
type MessageBroker interface {
    Connector
    Publisher
    Caller
    Subscriber
}

// Client interface (no Subscribe)
type Client interface {
    Connector
    Publisher
    Caller
}

// Server interface (no Publish/Call)
type Server interface {
    Connector
    Subscriber
}
```

This allows type-safe function signatures:

```go
// Function that only needs to publish
func SendNotification(pub weave.Publisher, msg *weave.Message) error {
    return pub.Publish(ctx, "notifications", msg)
}

// Function that only needs to make RPC calls
func GetUser(caller weave.Caller, userID string) (*User, error) {
    resp, err := caller.Call(ctx, "users.get", weave.NewTextMessage(userID))
    // ...
}
```

## Configuration

### Observability Hooks

Weave emits structured runtime and transport events through optional hooks on `Config`.

```go
type Config struct {
    Logger    weave.EventLogger
    EventHook weave.EventHook
    Metrics   weave.MetricsHook
    Tracing   weave.TracingHook
}
```

Standard event names include:

- `weave.EventConnect`
- `weave.EventDisconnect`
- `weave.EventPublishFailed`
- `weave.EventSubscribeFailed`
- `weave.EventTimeout`

Use `Logger` when you want a consistent structured logging sink, `EventHook` for lightweight callbacks, and `Metrics` to bridge counters/durations into your monitoring system.
Use `Tracing` to start spans for high-level `Client` and `Server` operations and bridge them into OpenTelemetry or another tracing backend.

For production wiring examples with `slog`, Prometheus-style metrics, and OpenTelemetry adapters, see [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).

### Transport Capabilities

Each backend provides different guarantees. See [Transport Capability Matrix](docs/TRANSPORT_MATRIX.md) for detailed comparison of:

- Message ordering and delivery guarantees
- Dead-letter and error handling
- Consumer group and partition behavior
- Connection recovery semantics

Choose your backend based on whether you need AMQP's simplicity and priority support or Kafka's horizontal scaling and topic retention.

### AMQP (RabbitMQ)

```go
config := &weave.Config{
    Backend: "amqp",
    AMQP: &weave.AMQPConfig{
        Host:         "localhost",
        Port:         5672,
        Username:     "guest",
        Password:     "guest",
        VHost:        "/",
        Heartbeat:    10 * time.Second,
        Exchange:     "",           // Default exchange
        QueueDurable: true,
    },
    ConnectionRetry: 3,
    RetryDelay:      2 * time.Second,
}
```

### Kafka

```go
config := &weave.Config{
    Backend: "kafka",
    Kafka: &weave.KafkaConfig{
        Brokers:       []string{"localhost:9092"},
        ConsumerGroup: "my-service",
        ClientID:      "my-client",
        RequiredAcks:  1,
        AutoOffsetReset: "latest",
    },
}
```

## Publish Options

```go
// Set timeout
client.Publish(ctx, "queue", msg, weave.WithTimeout(5*time.Second))

// Make message persistent (AMQP)
client.Publish(ctx, "queue", msg, weave.WithPersistent())

// Set partition key (Kafka)
client.Publish(ctx, "topic", msg, weave.WithKey("user-123"))

// Set priority (AMQP)
client.Publish(ctx, "queue", msg, weave.WithPriority(5))
```

## Subscribe Options (Server)

```go
// Auto-acknowledge messages
server.Handle("queue", handler) // Then configure via broker

// Via raw broker:
broker.Subscribe(ctx, "queue", handler, weave.WithAutoAck())
broker.Subscribe(ctx, "queue", handler, weave.WithPrefetchCount(10))
broker.Subscribe(ctx, "topic", handler, weave.WithConsumerGroup("my-group"))
broker.Subscribe(ctx, "topic", handler, weave.WithStartFromBeginning())
```

## Testing

### Testing Client Code

```go
import (
    "testing"
    "context"
    
    "github.com/prabhatdotdev/weave/testkit"
    "github.com/prabhatdotdev/weave/runtime"
)

func TestOrderService(t *testing.T) {
    // Create mock broker
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Create client with mock
    client := runtime.NewClientWithBroker(broker, nil)
    
    // Test your code
    orderService := NewOrderService(client)
    err := orderService.PlaceOrder(ctx, order)
    
    // Verify publish was called
    broker.AssertPublished(t, "orders")
    broker.AssertPublishCount(t, "orders", 1)
}
```

### Testing RPC Calls

```go
func TestUserService(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Set up mock response
    broker.SetCallResponse("users.get", &core.Message{
        Body: []byte(`{"id": 1, "name": "John"}`),
    })
    
    client := runtime.NewClientWithBroker(broker, nil)
    userService := NewUserService(client)
    
    user, err := userService.GetUser(ctx, "1")
    if err != nil {
        t.Fatal(err)
    }
    if user.Name != "John" {
        t.Errorf("expected John, got %s", user.Name)
    }
}
```

### Testing Server Handlers

```go
func TestOrderHandler(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    server := runtime.NewServerWithBroker(broker, nil)
    server.Handle("orders", orderHandler)
    server.Start(context.Background())
    
    // Simulate incoming message
    err := broker.SimulateMessage(ctx, "orders", &core.Message{
        Body: []byte(`{"order_id": 123}`),
    })
    if err != nil {
        t.Fatal(err)
    }
}
```

## Error Handling

```go
import "github.com/prabhatdotdev/weave"

err := client.Publish(ctx, "queue", msg)
if err != nil {
    if weave.IsNotConnected(err) {
        // Handle disconnection - reconnect or retry
    }
    if weave.IsTimeout(err) {
        // Handle timeout - retry with backoff
    }
}
```

## Migration from Service to Server

If you were using `runtime.Service`, rename to `runtime.Server`:

```go
// Before (still works, but deprecated)
service, _ := weave.NewService(config)
service.Handle("queue", handler)
service.Start(ctx)

// After (preferred)
server, _ := weave.NewServer(config)
server.Handle("queue", handler)
server.Start(ctx)
```

## Docker Compose

Start local message brokers for development:

```bash
# Start RabbitMQ
make rabbitmq-start

# Start Kafka
make kafka-start
```

## Runnable Examples

- [Complete examples guide](examples/README.md) — prerequisites, selection guide,
  exact run commands, configuration, validation, cleanup, and troubleshooting
- [AMQP + Kafka](examples/amqp-kafka/README.md) — separate server and client
  processes for JSON publish, RPC, retry, timeout, and transport-specific options
- [JSON microservices](examples/json/README.md)
- [Protocol Buffers microservices](examples/protobuf/README.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
