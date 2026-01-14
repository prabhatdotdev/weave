# API Reference

Complete API documentation for Weave.

## Core Types

### MessageBroker Interface

The primary interface implemented by all backends:

```go
type MessageBroker interface {
    // Connect establishes a connection to the message broker
    Connect(ctx context.Context) error
    
    // Close gracefully shuts down the connection
    Close() error
    
    // Publish sends a message to the specified destination
    Publish(ctx context.Context, destination string, msg *Message) error
    
    // Subscribe registers a handler for messages from destination
    Subscribe(ctx context.Context, destination string, handler Handler) error
    
    // Call implements request-response pattern
    Call(ctx context.Context, destination string, msg *Message, timeout time.Duration) (*Message, error)
    
    // IsConnected returns connection status
    IsConnected() bool
    
    // Backend returns the backend name
    Backend() string
}
```

### Message

Represents a message to be sent or received:

```go
type Message struct {
    Body          []byte            // Message payload
    CorrelationID string            // For request-response correlation
    ReplyTo       string            // Reply destination for RPC
    Headers       map[string]string // Custom message headers
    ContentType   string            // MIME type (e.g., "application/json")
    Subject       string            // Message subject/routing key
    Partition     int32             // Partition number (Kafka)
    Offset        int64             // Message offset (Kafka, Kinesis)
}
```

#### Constructor

```go
func NewMessage(body []byte) *Message
```

Creates a new message with the given body.

**Example:**
```go
msg := mqservice.NewMessage([]byte("hello"))
msg.Headers = map[string]string{"user-id": "123"}
```

#### Methods

```go
func (m *Message) WithCorrelationID(id string) *Message
```

Sets correlation ID and returns message (fluent interface).

```go
func (m *Message) Clone() *Message
```

Creates a deep copy of the message.

### Handler

Function type for processing messages:

```go
type Handler func(ctx context.Context, body []byte) ([]byte, error)
```

The handler receives message body and returns response body (or error).

**Example:**
```go
handler := func(ctx context.Context, body []byte) ([]byte, error) {
    // Process message
    result := process(body)
    return result, nil
}
```

---

## Factory Functions

### New

```go
func New(backend string, config *Config) (MessageBroker, error)
```

Creates a new MessageBroker instance for the specified backend.

**Parameters:**
- `backend`: Backend name ("amqp", "kafka", "kinesis", etc.)
- `config`: Configuration struct

**Returns:**
- `MessageBroker`: The broker instance
- `error`: Error if backend not found or invalid config

**Example:**
```go
broker, err := mqservice.New("amqp", config)
if err != nil {
    log.Fatal(err)
}
defer broker.Close()
```

### Register

```go
func Register(name string, factory BrokerFactory)
```

Registers a backend implementation. Called by backend packages in `init()`.

**Parameters:**
- `name`: Backend identifier
- `factory`: Function that creates broker instances

**Example (backend implementation):**
```go
func init() {
    mqservice.Register("mybackend", NewMyBroker)
}
```

---

## Configuration

### Config

Main configuration struct:

```go
type Config struct {
    Backend         string        // Backend to use
    ConnectionName  string        // Connection identifier
    ConnectionRetry int           // Retry attempts
    RetryDelay      time.Duration // Delay between retries
    
    // Backend-specific configs
    AMQP     *AMQPConfig
    Kafka    *KafkaConfig
    Kinesis  *KinesisConfig
    ActiveMQ *ActiveMQConfig
    NATS     *NATSConfig
    Redis    *RedisConfig
}
```

### DefaultConfig

```go
func DefaultConfig() *Config
```

Returns a Config with sensible defaults (AMQP backend).

**Example:**
```go
config := mqservice.DefaultConfig()
config.AMQP.Host = "rabbitmq.local"
```

### Backend-Specific Configs

#### AMQPConfig

```go
type AMQPConfig struct {
    Host            string
    Port            int
    Username        string
    Password        string
    VHost           string
    Heartbeat       time.Duration
    TLS             *TLSConfig
    Exchange        string
    ExchangeType    string
    QueueDurable    bool
    QueueAutoDelete bool
    QueueExclusive  bool
}
```

```go
func DefaultAMQPConfig() *AMQPConfig
```

#### KafkaConfig

```go
type KafkaConfig struct {
    Brokers           []string
    ClientID          string
    ConsumerGroup     string
    TLS               *TLSConfig
    RequiredAcks      int
    MaxRetries        int
    RetryBackoff      time.Duration
    CompressionType   string
    AutoOffsetReset   string
    SessionTimeout    time.Duration
    HeartbeatInterval time.Duration
    SASL              *SASLConfig
}
```

```go
func DefaultKafkaConfig() *KafkaConfig
```

See [TRANSPORTS.md](TRANSPORTS.md) for full transport configuration details.

---

## Protocol Buffers Support

### ProtobufService

Wrapper that provides type-safe Protocol Buffer messaging:

```go
type ProtobufService struct {
    // Private fields
}
```

#### NewProtobufService

```go
func NewProtobufService(broker MessageBroker, defaultWorkers int) *ProtobufService
```

Creates a ProtobufService with the given broker and default worker count.

**Parameters:**
- `broker`: Underlying MessageBroker
- `defaultWorkers`: Default number of worker threads per handler

**Example:**
```go
service := mqservice.NewProtobufService(broker, 10)
defer service.Close()
```

#### NewProtobufServiceWithConfig

```go
func NewProtobufServiceWithConfig(backend string, config *Config, defaultWorkers int) (*ProtobufService, error)
```

Creates broker and ProtobufService in one call.

**Example:**
```go
service, err := mqservice.NewProtobufServiceWithConfig("amqp", config, 10)
if err != nil {
    log.Fatal(err)
}
defer service.Close()
```

### RegisterHandler

```go
func RegisterHandler[Req proto.Message, Resp proto.Message](
    ps *ProtobufService,
    method string,
    handler TypedHandler[Req, Resp],
    newReq func() Req,
    newResp func() Resp,
    workers int,
) error
```

Registers a typed handler for a specific method.

**Type Parameters:**
- `Req`: Request message type
- `Resp`: Response message type

**Parameters:**
- `ps`: ProtobufService instance
- `method`: Method name (routing key)
- `handler`: Handler function
- `newReq`: Factory function for request messages
- `newResp`: Factory function for response messages
- `workers`: Number of concurrent workers for this handler

**Example:**
```go
handler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
    return &pb.UserResponse{
        UserId: req.UserId,
        Name:   "John Doe",
    }, nil
}

err := mqservice.RegisterHandler(
    service,
    "user.get",
    handler,
    func() *pb.UserRequest { return &pb.UserRequest{} },
    func() *pb.UserResponse { return &pb.UserResponse{} },
    5, // 5 workers
)
```

### ListenAndServeProtobuf

```go
func (ps *ProtobufService) ListenAndServeProtobuf(queueName string) error
```

Starts the service listening on the specified queue.

**Example:**
```go
if err := service.ListenAndServeProtobuf("user-service"); err != nil {
    log.Fatal(err)
}
```

### CallProtobuf

```go
func CallProtobuf[Req proto.Message, Resp proto.Message](
    ps *ProtobufService,
    ctx context.Context,
    queueName string,
    method string,
    reqMsg Req,
    newResp func() Resp,
    timeout time.Duration,
) (Resp, error)
```

Makes a typed RPC call.

**Example:**
```go
ctx := context.Background()
req := &pb.UserRequest{UserId: "123"}

resp, err := mqservice.CallProtobuf(
    service,
    ctx,
    "user-service",
    "user.get",
    req,
    func() *pb.UserResponse { return &pb.UserResponse{} },
    5*time.Second,
)
if err != nil {
    log.Fatal(err)
}
fmt.Println(resp.Name)
```

### TypedHandler

```go
type TypedHandler[Req proto.Message, Resp proto.Message] func(ctx context.Context, req Req) (Resp, error)
```

Generic handler function for typed protobuf messages.

---

## Error Types

### ErrNotConnected

```go
type ErrNotConnected struct {
    Backend string
}
```

Returned when operation is attempted before connection.

### ErrConnectionLost

```go
type ErrConnectionLost struct {
    Backend string
    Cause   error
}
```

Returned when connection is lost during operation.

### ErrTimeout

```go
type ErrTimeout struct {
    Operation string
    Duration  string
}
```

Returned when operation times out.

### ErrPublishFailed

```go
type ErrPublishFailed struct {
    Backend     string
    Destination string
    Cause       error
}
```

Returned when message publish fails.

### ErrSubscribeFailed

```go
type ErrSubscribeFailed struct {
    Backend     string
    Destination string
    Cause       error
}
```

Returned when subscription fails.

### ErrClosed

```go
var ErrClosed = errors.New("broker already closed")
```

Returned when operation is attempted on closed broker.

### ErrUnknownBackend

```go
type ErrUnknownBackend struct {
    Backend string
}
```

Returned when backend is not registered.

---

## Worker Pools

### WorkerPool

```go
type WorkerPool struct {
    // Private fields
}
```

Manages concurrent message processing.

#### NewWorkerPool

```go
func NewWorkerPool(workers int) *WorkerPool
```

Creates a worker pool with the specified number of workers.

#### Start

```go
func (p *WorkerPool) Start()
```

Starts the worker pool.

#### Submit

```go
func (p *WorkerPool) Submit(task func()) error
```

Submits a task to the worker pool.

#### Stop

```go
func (p *WorkerPool) Stop()
```

Stops the worker pool and waits for tasks to complete.

---

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    mqservice "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
    // Create config
    config := mqservice.DefaultConfig()
    config.AMQP.Host = "localhost"
    
    // Create broker
    broker, err := mqservice.New("amqp", config)
    if err != nil {
        log.Fatal(err)
    }
    defer broker.Close()
    
    // Connect
    ctx := context.Background()
    if err := broker.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    
    // Subscribe
    handler := func(ctx context.Context, body []byte) ([]byte, error) {
        fmt.Printf("Received: %s\n", body)
        return []byte("pong"), nil
    }
    
    go broker.Subscribe(ctx, "my-queue", handler)
    
    // Publish
    msg := mqservice.NewMessage([]byte("ping"))
    if err := broker.Publish(ctx, "my-queue", msg); err != nil {
        log.Fatal(err)
    }
    
    // Call (RPC)
    response, err := broker.Call(ctx, "my-queue", msg, 5*time.Second)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Response: %s\n", response.Body)
}
```

---

## See Also

- [Architecture Overview](ARCHITECTURE.md)
- [Transport Configuration](TRANSPORTS.md)
- [Protocol Buffers Guide](PROTOBUF.md)
- [Quick Start](QUICKSTART.md)
