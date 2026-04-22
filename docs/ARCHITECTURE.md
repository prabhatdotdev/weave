# Architecture Overview

## Design Philosophy

Weave follows a **layered architecture** with three key principles:

1. **Driver-based backends** - Similar to Go's `database/sql`, write once and switch backends via configuration
2. **Client/Server separation** - Clear distinction between message producers (clients) and consumers (servers)
3. **Interface composition** - Small, focused interfaces that compose into larger capabilities

This design allows applications to:

- Write code once against a unified interface
- Switch backends without code changes (only config)
- Use type-safe abstractions (Client for sending, Server for handling)
- Test with mock implementations
- Add new backends without breaking existing code

## Architecture Layers

```
┌─────────────────────────────────────────────────────┐
│  Application Layer                                   │
│  (Your Business Logic)                              │
└─────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  Runtime Layer (runtime/)                            │
│  • Client  - Publish & RPC calls                    │
│  • Server  - Subscribe & handle messages            │
└─────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  Core Layer (core/)                                  │
│  • MessageBroker interface                          │
│  • Message, Config, Options                         │
│  • Backend registry                                 │
└─────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  Transport Layer (transport/)                        │
│  • AMQP implementation                              │
│  • Kafka implementation                             │
└─────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  Message Queue (External)                            │
│  RabbitMQ / Kafka                                   │
└─────────────────────────────────────────────────────┘
```

## Core Components

### 1. Interface Composition

Weave uses **composable interfaces** for maximum flexibility:

```go
// Base connection interface
type Connector interface {
    Connect(ctx context.Context) error
    Close() error
    IsConnected() bool
    Backend() string
}

// Publishing interface (fire-and-forget)
type Publisher interface {
    Publish(ctx context.Context, destination string, message *Message, opts ...PublishOption) error
}

// RPC interface (request-reply)
type Caller interface {
    Call(ctx context.Context, destination string, message *Message, opts ...PublishOption) (*Message, error)
}

// Subscription interface (server-side)
type Subscriber interface {
    Subscribe(ctx context.Context, destination string, handler Handler, opts ...SubscribeOption) error
}

// Full broker = all interfaces
type MessageBroker interface {
    Connector
    Publisher
    Caller
    Subscriber
}

// Client = no subscription
type Client interface {
    Connector
    Publisher
    Caller
}

// Server = no publishing (only via broker)
type Server interface {
    Connector
    Subscriber
}
```

This composition allows:
- Type-safe function signatures (accept only `Publisher` if that's all you need)
- Clear separation of concerns
- Easy testing with focused mocks
- Future extension without breaking changes

### 2. Runtime Abstractions

#### Client (`runtime/client.go`)

High-level abstraction for **sending messages**:

```go
client, _ := weave.NewClient(config)
client.Connect(ctx)

// Fire-and-forget
client.Publish(ctx, "orders", msg)

// Request-reply RPC
response, _ := client.Call(ctx, "users.get", request)
```

**Use Client when:**
- Web API publishing events
- Making RPC calls to services
- Only sending, never receiving

#### Server (`runtime/service.go`)

High-level abstraction for **handling messages**:

```go
server, _ := weave.NewServer(config)

server.Handle("orders", orderHandler)
server.Handle("payments", paymentHandler)

server.Start(ctx) // Connects and subscribes all handlers
```

**Use Server when:**
- Building microservices
- Processing queue messages
- Handling RPC requests

### 3. Backend Registration

Backends register themselves using the `init()` pattern:

```go
// In transport/amqp/amqp.go
func init() {
    core.Register("amqp", NewBroker)
}
```

This enables automatic discovery when importing:

```go
import _ "github.com/prabhatdotdev/weave/transport/amqp"
```

### 4. Factory Pattern

Applications create brokers using the factory:

```go
// Automatic backend selection from config
broker, err := weave.New(config)

// Explicit backend selection
broker, err := weave.NewWithBackend("kafka", config)
```

The factory looks up the registered backend and creates an instance.


## Message Flow

### Publish Flow (Client → Queue)

```
Application
    ↓
Client.Publish(ctx, "orders", msg)
    ↓
MessageBroker.Publish()
    ↓
Transport (AMQP/Kafka) - serialization
    ↓
Message Queue
```

### Subscribe Flow (Queue → Server)

```
Message Queue
    ↓
Transport (AMQP/Kafka) - deserialization
    ↓
MessageBroker.Subscribe() - dispatch to handler
    ↓
Handler Function
    ↓
Application Logic
```

### Request-Response Flow (RPC)

**Client Side:**
```
1. Client.Call(ctx, "users.get", request)
2. Generate unique CorrelationID
3. Create exclusive reply queue (if not exists)
4. Publish with ReplyTo = reply queue
5. Wait for response with matching CorrelationID
6. Return response to caller
```

**Server Side:**
```
1. Server receives message via Subscribe
2. Handler processes request
3. If ReplyTo is set, publish response to ReplyTo
4. Include original CorrelationID in response
```

**Flow Diagram:**
```
Client                          Queue                          Server
  │                              │                               │
  ├─ Call("users.get", req) ────→│                               │
  │  (CorrelationID: abc123)     │                               │
  │  (ReplyTo: reply-queue)      │                               │
  │                              ├──→ Deliver message ──────────→│
  │                              │                               │
  │                              │                    Handler processes
  │                              │                               │
  │                              │←── Publish to reply-queue ────┤
  │                              │    (CorrelationID: abc123)    │
  │←── Receive response ─────────┤                               │
  │                              │                               │
  └─ Return response             │                               │
```

## Package Structure

```
weave/
├── weave.go                    # Root package - re-exports
│
├── core/                       # Core abstractions
│   ├── broker.go              # Interfaces (MessageBroker, Client, Server, etc.)
│   ├── message.go             # Message struct and builders
│   ├── config.go              # Configuration types
│   ├── options.go             # Functional options
│   ├── errors.go              # Error types
│   └── registry.go            # Backend factory registry
│
├── runtime/                    # High-level abstractions
│   ├── client.go              # Client (publish/call only)
│   └── service.go             # Server (subscribe/handle)
│
├── transport/                  # Backend implementations
│   ├── amqp/
│   │   └── amqp.go           # RabbitMQ implementation
│   └── kafka/
│       └── kafka.go          # Kafka implementation
│
├── codec/                      # Message encoding/decoding
│   └── codec.go              # JSON + protobuf codecs
│
└── testkit/                    # Testing utilities
    └── mock.go               # Mock broker for tests
```

## Transport Implementations

Each transport lives in its own package under `transport/`:

```
transport/
├── amqp/
│   └── amqp.go      # RabbitMQ implementation
├── kafka/
│   └── kafka.go     # Apache Kafka implementation
└── nats/            # Future
    └── nats.go
```

### Transport Requirements

A transport implementation must:

1. **Implement `core.MessageBroker` interface**
   ```go
   type Broker struct {
       config *core.Config
       // ... internal state
   }
   
   func (b *Broker) Connect(ctx context.Context) error { ... }
   func (b *Broker) Publish(...) error { ... }
   // ... other methods
   ```

2. **Register itself in `init()`**
   ```go
   func init() {
       core.Register("amqp", func(cfg *core.Config) (core.MessageBroker, error) {
           return NewBroker(cfg), nil
       })
   }
   ```

3. **Handle connection lifecycle**
   - Connect/disconnect properly
   - Implement IsConnected()
   - Support reconnection

4. **Translate between `Message` and native format**
   ```go
   // AMQP example
   func toAMQPMessage(msg *core.Message) amqp.Publishing {
       return amqp.Publishing{
           Body:          msg.Body,
           CorrelationId: msg.CorrelationID,
           ReplyTo:       msg.ReplyTo,
           // ...
       }
   }
   ```

5. **Implement request-response pattern**
   - Create reply queues/topics
   - Track correlation IDs
   - Support timeouts

### Backend-Specific Features

Transports can expose backend-specific features through options:

```go
// AMQP-specific
client.Publish(ctx, dest, msg, 
    weave.WithPersistent(),    // AMQP delivery mode
    weave.WithPriority(5),      // AMQP priority
)

// Kafka-specific
client.Publish(ctx, dest, msg,
    weave.WithKey("user-123"),     // Partition key
    weave.WithPartition(3),        // Specific partition
)
```


## Message Structure

The `Message` struct is designed to work across all backends while preserving backend-specific features:

```go
type Message struct {
    // Universal fields (all backends)
    Body          []byte            // Message payload
    CorrelationID string            // For request-response pattern
    ReplyTo       string            // Reply destination
    Headers       map[string]string // Custom headers
    ContentType   string            // MIME type (e.g., "application/json")
    MessageID     string            // Unique identifier
    Timestamp     time.Time         // Message creation time
    
    // Backend-specific fields
    Subject       string            // AMQP: routing key, Kafka: message key
    Partition     int32             // Kafka/Kinesis partition
    Offset        int64             // Kafka/Kinesis offset (read-only)
}
```

### Message Builders

```go
// Basic message
msg := weave.NewMessage([]byte("data"))

// Text message
msg := weave.NewTextMessage("hello")

// With metadata
msg := weave.NewMessage(data)
msg.ContentType = "application/json"
msg.Headers = map[string]string{
    "X-User-ID": "123",
    "X-Source":  "web-api",
}

// Builder pattern
msg := weave.NewMessage(data).
    WithHeader("X-Trace-ID", traceID).
    WithContentType("application/json")
```

### Message Cloning

Messages can be cloned for modification without affecting the original:

```go
original := weave.NewMessage(data)
modified := original.Clone()
modified.Headers["X-Modified"] = "true"
```

## Configuration Architecture

Configuration uses a hierarchical structure with backend-specific sections:

```go
type Config struct {
    // Universal settings
    Backend         string        // "amqp", "kafka", etc.
    ConnectionName  string        // For monitoring/logging
    ConnectionRetry int           // Number of retry attempts
    RetryDelay      time.Duration // Delay between retries
    
    // Backend-specific configs (only relevant one is used)
    AMQP  *AMQPConfig
    Kafka *KafkaConfig
}
```

### Backend-Specific Configuration

#### AMQP Configuration
```go
type AMQPConfig struct {
    Host         string        // RabbitMQ host
    Port         int           // RabbitMQ port
    Username     string        // Authentication
    Password     string        // Authentication
    VHost        string        // Virtual host
    Exchange     string        // Default exchange
    QueueDurable bool          // Durable queues
    Heartbeat    time.Duration // Connection heartbeat
    TLS          *TLSConfig    // TLS settings
}
```

#### Kafka Configuration
```go
type KafkaConfig struct {
    Brokers         []string      // Kafka broker addresses
    ConsumerGroup   string        // Consumer group ID
    ClientID        string        // Client identifier
    RequiredAcks    int           // Ack level (0, 1, -1)
    AutoOffsetReset string        // "earliest" or "latest"
    SASL            *SASLConfig   // SASL authentication
    TLS             *TLSConfig    // TLS settings
}
```

### Default Configurations

```go
// Get defaults
config := weave.DefaultConfig()

// Backend-specific defaults
amqpConfig := weave.DefaultAMQPConfig()
kafkaConfig := weave.DefaultKafkaConfig()
```

## Client/Server Architecture

### Client Design

The `Client` abstraction is designed for **producers** (applications that send messages):

```go
type Client struct {
    broker core.MessageBroker  // Underlying broker
    config *core.Config        // Configuration
    
    // Internal state
    mu        sync.RWMutex
    connected bool
    closeOnce sync.Once
}
```

**Key characteristics:**
- Thread-safe operations
- Connection state tracking
- Graceful shutdown via `Close()`
- No subscription capabilities

**Typical usage patterns:**
- Web APIs publishing events
- CLI tools sending commands
- API gateways making RPC calls

### Server Design

The `Server` abstraction is designed for **consumers** (applications that handle messages):

```go
type Server struct {
    broker   core.MessageBroker         // Underlying broker
    config   *core.Config               // Configuration
    handlers map[string]core.Handler    // Destination → handler mapping
    
    // Lifecycle management
    mu        sync.RWMutex
    started   bool
    ctx       context.Context
    cancel    context.CancelFunc
    closeOnce sync.Once
}
```

**Key characteristics:**
- Handler registration before start
- Automatic subscription on `Start()`
- Context-based lifecycle
- Graceful shutdown via `Stop()`

**Typical usage patterns:**
- Microservices processing queues
- Event handlers
- RPC servers

### When to Use Raw MessageBroker

Use the raw `MessageBroker` interface when you need:
- Both client and server operations in one component
- Fine-grained control over subscriptions
- Custom connection management
- Direct access to backend-specific features

```go
broker, _ := weave.New(config)
broker.Connect(ctx)

// Act as both client and server
broker.Publish(ctx, "orders", msg)           // Client operation
broker.Subscribe(ctx, "payments", handler)    // Server operation
```


## Error Handling

Weave defines typed errors for consistent error handling across backends:

```go
// Sentinel errors
var (
    ErrClosed            = errors.New("broker is closed")
    ErrNoReplyTo         = errors.New("no reply-to destination")
    ErrAlreadyConnected  = errors.New("already connected")
    ErrInvalidConfig     = errors.New("invalid configuration")
)

// Structured errors
type ErrNotConnected struct {
    Backend string
}

type ErrConnectionLost struct {
    Backend string
    Reason  string
}

type ErrTimeout struct {
    Operation string
    Duration  string
}

type ErrPublishFailed struct {
    Backend     string
    Destination string
    Reason      error
}

type ErrSubscribeFailed struct {
    Backend     string
    Destination string
    Reason      error
}
```

### Error Checking Functions

```go
// Check error types
if weave.IsNotConnected(err) {
    // Handle disconnection
}
if weave.IsTimeout(err) {
    // Handle timeout
}
if weave.IsConnectionLost(err) {
    // Handle connection loss
}
```

### Error Propagation

Backends should wrap their native errors:

```go
// In transport implementation
func (b *Broker) Publish(ctx context.Context, dest string, msg *Message) error {
    if !b.connected {
        return &core.ErrNotConnected{Backend: "amqp"}
    }
    
    if err := b.channel.Publish(...); err != nil {
        return &core.ErrPublishFailed{
            Backend:     "amqp",
            Destination: dest,
            Reason:      err,
        }
    }
    return nil
}
```

## Concurrency Model

### Connection Management

**Single connection per broker instance:**
```go
client, _ := weave.NewClient(config)
client.Connect(ctx)  // Opens one connection

// All operations use this connection
client.Publish(ctx, "queue1", msg1)
client.Publish(ctx, "queue2", msg2)
```

**Thread-safe operations:**
- `Publish()` can be called concurrently from multiple goroutines
- `Call()` tracks correlation IDs safely
- Connection state uses mutexes

**Connection monitoring:**
```go
// AMQP: Heartbeat mechanism
config.AMQP.Heartbeat = 10 * time.Second

// Kafka: Session timeout
config.Kafka.SessionTimeout = 10 * time.Second
```

### Message Handling (Server)

**Concurrent message consumption:**
```go
server.Handle("orders", orderHandler)
server.Handle("payments", paymentHandler)
server.Start(ctx)

// Each handler runs in its own goroutine(s)
// Multiple messages can be processed simultaneously
```

**Handler concurrency depends on backend:**
- **AMQP**: Controlled by prefetch count
  ```go
  broker.Subscribe(ctx, "queue", handler, 
      weave.WithPrefetchCount(10))  // 10 concurrent messages
  ```
- **Kafka**: One goroutine per partition in consumer group

### Request-Response Concurrency

**Multiple concurrent RPC calls:**
```go
// These can run concurrently
var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        response, _ := client.Call(ctx, "service", request)
    }(i)
}
wg.Wait()
```

**Correlation ID tracking:**
- Each `Call()` gets a unique correlation ID
- Reply queue consumer matches responses by correlation ID
- Thread-safe internal maps track pending requests

### Context Propagation

```go
// Context flows through the system
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Timeout applies to entire operation
response, err := client.Call(ctx, "service", request)

// In handler
server.Handle("service", func(ctx context.Context, msg *Message) error {
    // Context includes deadline, cancellation, values
    select {
    case <-ctx.Done():
        return ctx.Err()  // Timeout or cancellation
    case result := <-process(msg):
        return nil
    }
})
```

## Testing Strategy

### 1. Mock Broker (Unit Tests)

Use `testkit.MockBroker` for fast unit tests:

```go
func TestOrderService(t *testing.T) {
    // Create mock
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Create client/server with mock
    client := runtime.NewClientWithBroker(broker, nil)
    
    // Test your code
    service := NewOrderService(client)
    service.PlaceOrder(ctx, order)
    
    // Verify interactions
    broker.AssertPublished(t, "orders")
    broker.AssertPublishCount(t, "orders", 1)
}
```

**MockBroker features:**
- Records all published messages
- Simulates subscription handlers
- Configurable RPC responses
- No external dependencies

### 2. Interface Mocks (Focused Tests)

Mock specific interfaces:

```go
type MockPublisher struct {
    published []*Message
}

func (m *MockPublisher) Publish(ctx context.Context, dest string, msg *Message, opts ...PublishOption) error {
    m.published = append(m.published, msg)
    return nil
}

// Test code that only needs Publisher
func ProcessData(pub weave.Publisher, data []byte) error {
    return pub.Publish(ctx, "processed", weave.NewMessage(data))
}
```

### 3. Integration Tests (Docker)

Test with real message brokers:

```go
func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    // Use docker-compose to start RabbitMQ/Kafka
    config := &weave.Config{
        Backend: "amqp",
        AMQP: &weave.AMQPConfig{
            Host: "localhost",
            Port: 5672,
        },
    }
    
    broker, _ := weave.New(config)
    broker.Connect(context.Background())
    defer broker.Close()
    
    // Test with real broker...
}
```

### 4. Contract Tests

Verify backends implement the interface correctly:

```go
// Test suite that all backends must pass
func TestBrokerContract(t *testing.T, createBroker func() MessageBroker) {
    broker := createBroker()
    
    // Test Connect/Close
    t.Run("Connect", func(t *testing.T) {
        err := broker.Connect(context.Background())
        assert.NoError(t, err)
        assert.True(t, broker.IsConnected())
    })
    
    // Test Publish
    t.Run("Publish", func(t *testing.T) {
        err := broker.Publish(ctx, "test", msg)
        assert.NoError(t, err)
    })
    
    // ... more contract tests
}
```


## Performance Considerations

### Connection Pooling

**Reuse broker instances across requests:**

```go
// ❌ Bad - creates new connection per request
func HandleRequest(w http.ResponseWriter, r *http.Request) {
    client, _ := weave.NewClient(config)  // New connection!
    client.Connect(r.Context())
    defer client.Close()
    client.Publish(r.Context(), "events", msg)
}

// ✅ Good - shared connection
type App struct {
    client *weave.Client
}

func NewApp() *App {
    client, _ := weave.NewClient(config)
    client.Connect(context.Background())
    return &App{client: client}
}

func (a *App) HandleRequest(w http.ResponseWriter, r *http.Request) {
    a.client.Publish(r.Context(), "events", msg)
}
```

### Batch Publishing

For high throughput, publish in batches when possible:

```go
// Serial publishing
for _, msg := range messages {
    client.Publish(ctx, "queue", msg)  // Round-trip per message
}

// Concurrent publishing (better)
sem := make(chan struct{}, 10)  // Limit concurrency
var wg sync.WaitGroup
for _, msg := range messages {
    wg.Add(1)
    sem <- struct{}{}
    go func(m *Message) {
        defer wg.Done()
        defer func() { <-sem }()
        client.Publish(ctx, "queue", m)
    }(msg)
}
wg.Wait()
```

**Backend-specific batching:**
- **Kafka**: Producer automatically batches messages
  ```go
  config.Kafka.BatchSize = 16384        // Batch size in bytes
  config.Kafka.LingerMs = 10            // Wait up to 10ms to batch
  ```
- **AMQP**: Consider using transactions or confirms for batches

### Prefetch/Buffer Tuning

Control how many messages are buffered for processing:

**AMQP:**
```go
broker.Subscribe(ctx, "queue", handler,
    weave.WithPrefetchCount(20))  // Buffer 20 messages
```

**Guidelines:**
- CPU-bound handlers: prefetch = 1-2x CPU cores
- I/O-bound handlers: prefetch = 10-50
- Memory-limited: Lower prefetch to reduce memory usage

**Kafka:**
```go
config.Kafka.FetchMinBytes = 1024        // Min bytes per fetch
config.Kafka.FetchMaxWaitMs = 500        // Max wait time
config.Kafka.MaxPartitionFetchBytes = 1048576  // Max per partition
```

### Message Size Optimization

**Keep messages small:**
```go
// ❌ Bad - large message
msg := weave.NewMessage(largeBlob)  // 10 MB

// ✅ Good - reference pattern
msg := weave.NewMessage([]byte(`{"s3_key": "large-blob.bin"}`))
```

**Use compression for large payloads:**
```go
func publishCompressed(client *weave.Client, dest string, data []byte) error {
    var buf bytes.Buffer
    gz := gzip.NewWriter(&buf)
    gz.Write(data)
    gz.Close()
    
    msg := weave.NewMessage(buf.Bytes())
    msg.ContentType = "application/gzip"
    return client.Publish(ctx, dest, msg)
}
```

### Memory Management

**Reuse message objects:**
```go
// Message pool
var msgPool = sync.Pool{
    New: func() interface{} {
        return &weave.Message{
            Headers: make(map[string]string),
        }
    },
}

func publishWithPool(data []byte) {
    msg := msgPool.Get().(*weave.Message)
    defer msgPool.Put(msg)
    
    msg.Body = data
    client.Publish(ctx, "queue", msg)
    
    // Clear for reuse
    msg.Body = nil
    for k := range msg.Headers {
        delete(msg.Headers, k)
    }
}
```

### Monitoring & Metrics

**Track key metrics:**

```go
type Metrics struct {
    publishCount    int64
    publishErrors   int64
    publishDuration time.Duration
}

func (m *Metrics) Publish(client *weave.Client, dest string, msg *weave.Message) error {
    start := time.Now()
    err := client.Publish(context.Background(), dest, msg)
    duration := time.Since(start)
    
    atomic.AddInt64(&m.publishCount, 1)
    atomic.AddInt64(&m.publishDuration, int64(duration))
    
    if err != nil {
        atomic.AddInt64(&m.publishErrors, 1)
    }
    
    return err
}
```

**Important metrics:**
- Publish rate (messages/second)
- Publish latency (p50, p95, p99)
- Error rate
- Connection status
- Queue depth (backend-specific)

## Extension Points

### Adding a New Backend

1. **Create transport package:**
   ```
   transport/nats/
   └── nats.go
   ```

2. **Implement MessageBroker interface:**
   ```go
   package nats
   
   import "github.com/prabhatdotdev/weave/core"
   
   type Broker struct {
       config *core.Config
       conn   *nats.Conn
       // ...
   }
   
   func NewBroker(config *core.Config) (core.MessageBroker, error) {
       return &Broker{config: config}, nil
   }
   
   func (b *Broker) Connect(ctx context.Context) error { ... }
   func (b *Broker) Publish(...) error { ... }
   // ... implement all methods
   ```

3. **Register backend:**
   ```go
   func init() {
       core.Register("nats", NewBroker)
   }
   ```

4. **Add configuration:**
   ```go
   // In core/config.go
   type NATSConfig struct {
       URL           string
       ClusterID     string
       ClientID      string
       // ...
   }
   
   type Config struct {
       // ...
       NATS *NATSConfig
   }
   ```

5. **Write tests:**
   ```go
   func TestNATSBroker(t *testing.T) {
       broker := NewBroker(testConfig)
       // Test all operations...
   }
   ```

6. **Update documentation:**
    - Add to `TRANSPORTS.md`
   - Update `README.md`
   - Add usage examples

### Custom Codecs

Weave now ships built-in `codec.JSON` and `codec.Protobuf` implementations. Extend `codec/` for additional serialization formats:

```go
package codec

type MessagePackCodec struct{}

func (c *MessagePackCodec) Encode(v interface{}) ([]byte, error) {
    return msgpack.Marshal(v)
}

func (c *MessagePackCodec) Decode(data []byte, v interface{}) error {
    return msgpack.Unmarshal(data, v)
}

func (c *MessagePackCodec) ContentType() string {
    return "application/msgpack"
}
```

### Middleware Pattern

Wrap handlers for cross-cutting concerns:

```go
// Logging middleware
func LoggingMiddleware(next weave.Handler) weave.Handler {
    return func(ctx context.Context, msg *weave.Message) error {
        log.Printf("Received message: %s", msg.MessageID)
        err := next(ctx, msg)
        if err != nil {
            log.Printf("Handler error: %v", err)
        }
        return err
    }
}

// Metrics middleware
func MetricsMiddleware(next weave.Handler) weave.Handler {
    return func(ctx context.Context, msg *weave.Message) error {
        start := time.Now()
        err := next(ctx, msg)
        duration := time.Since(start)
        
        metrics.RecordHandlerDuration(duration)
        if err != nil {
            metrics.RecordHandlerError()
        }
        return err
    }
}

// Usage
server.Handle("orders", 
    LoggingMiddleware(
        MetricsMiddleware(
            orderHandler)))
```

### Custom Options

Add custom functional options:

```go
// In your package
func WithCustomHeader(key, value string) weave.PublishOption {
    return func(msg *weave.Message) {
        if msg.Headers == nil {
            msg.Headers = make(map[string]string)
        }
        msg.Headers[key] = value
    }
}

// Usage
client.Publish(ctx, "queue", msg, 
    WithCustomHeader("X-Source", "api"),
    WithCustomHeader("X-Version", "2.0"))
```

## Design Patterns

### Circuit Breaker

Protect services from cascading failures:

```go
type CircuitBreakerClient struct {
    client  *weave.Client
    breaker *gobreaker.CircuitBreaker
}

func (c *CircuitBreakerClient) Call(ctx context.Context, dest string, msg *weave.Message) (*weave.Message, error) {
    result, err := c.breaker.Execute(func() (interface{}, error) {
        return c.client.Call(ctx, dest, msg)
    })
    if err != nil {
        return nil, err
    }
    return result.(*weave.Message), nil
}
```

### Retry with Backoff

Handle transient failures:

```go
func PublishWithRetry(client *weave.Client, dest string, msg *weave.Message, maxRetries int) error {
    backoff := time.Second
    for i := 0; i < maxRetries; i++ {
        err := client.Publish(context.Background(), dest, msg)
        if err == nil {
            return nil
        }
        
        if !weave.IsConnectionLost(err) && !weave.IsTimeout(err) {
            return err  // Don't retry on other errors
        }
        
        time.Sleep(backoff)
        backoff *= 2  // Exponential backoff
    }
    return fmt.Errorf("failed after %d retries", maxRetries)
}
```

### Request Deduplication

Prevent duplicate processing:

```go
var processed sync.Map  // message ID → bool

server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    // Check if already processed
    if _, exists := processed.LoadOrStore(msg.MessageID, true); exists {
        log.Printf("Skipping duplicate message: %s", msg.MessageID)
        return nil  // Acknowledge but don't process
    }
    
    // Process message...
    return processOrder(ctx, msg)
})
```

## Security Considerations

### Authentication

**AMQP:**
```go
config.AMQP.Username = os.Getenv("RABBITMQ_USER")
config.AMQP.Password = os.Getenv("RABBITMQ_PASS")
```

**Kafka SASL:**
```go
config.Kafka.SASL = &weave.SASLConfig{
    Enabled:   true,
    Mechanism: "PLAIN",
    Username:  os.Getenv("KAFKA_USER"),
    Password:  os.Getenv("KAFKA_PASS"),
}
```

### TLS/SSL

```go
config.AMQP.TLS = &weave.TLSConfig{
    Enabled:            true,
    InsecureSkipVerify: false,
    CertFile:           "/path/to/cert.pem",
    KeyFile:            "/path/to/key.pem",
    CAFile:             "/path/to/ca.pem",
}
```

### Message Encryption

Encrypt sensitive data at the application level:

```go
func publishEncrypted(client *weave.Client, dest string, data []byte, key []byte) error {
    encrypted, _ := encrypt(data, key)
    msg := weave.NewMessage(encrypted)
    msg.Headers["X-Encrypted"] = "aes-256-gcm"
    return client.Publish(ctx, dest, msg)
}
```

## Future Enhancements

- **Circuit breaker integration** - Built-in circuit breaker support
- **Observability helpers** - Additional OpenTelemetry helper package
- **Dead letter queue support** - Automatic DLQ handling
- **Message schema validation** - JSON Schema / Protobuf validation
- **Streaming support** - Large message streaming
- **Saga pattern support** - Distributed transactions
- **Priority queues** - Priority-based message routing
- **Message scheduling** - Delayed message delivery
- **Batch operations** - Native batch publish/subscribe

## References

- [Server Tutorial](SERVER.md) - Build message handlers
- [Client Tutorial](CLIENT.md) - Send messages and RPC
- [Transport Configuration](TRANSPORTS.md) - Configure transports
- [API Guide](API.md) - Public API overview and pkg.go.dev links
