# Adding New Transport Backends

This guide defines the requirements and process for adding new message broker backends to Weave. Follow this guide to ensure new backends are tested, documented, and compatible with the existing abstraction.

## Before You Start

**Prerequisites:**
- New backends should only be added **after** the existing backends (AMQP and Kafka) are stable.
- New backends add maintenance cost and should be added one at a time.
- You should have experience with both Weave and the target message broker.

**Expected effort:**
- Implementation: 2-4 weeks
- Tests and documentation: 1-2 weeks
- Total: 3-6 weeks per backend

## Step 1: Define Backend Capabilities

Before implementing, document what your backend can and cannot do.

### Create a Capability Matrix

Create `docs/TRANSPORTS.md` section (or `docs/CAPABILITY_MATRIX.md`) listing which operations each backend supports:

```
| Feature                      | AMQP | Kafka | Your Backend |
|------------------------------|------|-------|--------------|
| Publish                      | ✅   | ✅    | ✅           |
| Subscribe                    | ✅   | ✅    | ✅           |
| Request-Reply (Call)         | ✅   | ✅    | ✅ or ❌     |
| Routing Key / Topics         | ✅   | ✅    | ?            |
| Durable Queues               | ✅   | ✅    | ?            |
| Message Persistence          | ✅   | ✅    | ?            |
| Explicit Acknowledgment      | ✅   | ✅    | ?            |
| Partition / Sharding         | ❌   | ✅    | ?            |
| Consumer Groups              | ❌   | ✅    | ?            |
| TLS/SSL Support              | ✅   | ✅    | ?            |
| SASL Authentication          | ❌   | ✅    | ?            |
| Message Expiration           | ✅   | ❌    | ?            |
```

### Document Limitations

If your backend doesn't support certain features, document the limitations clearly:

```markdown
## MyBackend Limitations

- **No Built-in Request-Reply:** MyBackend does not natively support correlation IDs. 
  Weave will simulate request-reply using a naming convention for reply queues.
  
- **No Consumer Groups:** MyBackend uses individual subscriptions. Multiple instances 
  will all receive all messages. Use application-level filtering for load balancing.
  
- **No Durable Queues:** All queues are ephemeral. Messages not consumed before 
  disconnection are lost.
```

### List Required Config Fields

Define what configuration your backend needs:

```go
// MyBackendConfig holds MyBackend-specific configuration.
type MyBackendConfig struct {
    // Required
    Servers         []string
    
    // Optional
    Username        string
    Password        string
    TLS             *TLSConfig
    ConnectionPool  int           // Number of connections
    RequestTimeout  time.Duration // Timeout for operations
    
    // Backend-specific
    Namespace       string        // MyBackend namespace/account
    Cluster         string        // Cluster name
}
```

## Step 2: Implement the Broker

### Create Package Structure

```
transport/mybackend/
├── mybackend.go          # Main implementation
├── mybackend_test.go     # Unit tests
├── connection.go         # Connection management (optional)
└── errors.go             # Backend-specific error handling (optional)
```

### Implement core.MessageBroker Interface

Your broker must implement all methods:

```go
package mybackend

import (
    "context"
    "github.com/prabhatdotdev/weave/core"
)

const backendName = "mybackend"

type Broker struct {
    config     *core.Config
    mbConfig   *core.MyBackendConfig
    
    // Backend connection
    conn       *MyBackendConnection
    connected  bool
    mu         sync.RWMutex
    
    // Request-reply tracking
    pending    map[string]chan *core.Message
    pendingMu  sync.RWMutex
}

// NewBroker creates a new MyBackend broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
    if config == nil {
        config = core.DefaultConfig()
    }
    
    // Validate backend-specific config
    mbConfig := config.MyBackend
    if mbConfig == nil {
        mbConfig = DefaultMyBackendConfig()
    }
    
    broker := &Broker{
        config:   config,
        mbConfig: mbConfig,
        pending:  make(map[string]chan *core.Message),
    }
    
    return broker, nil
}

func (b *Broker) Connect(ctx context.Context) error {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    if b.connected {
        return core.ErrAlreadyConnected
    }
    
    // Establish connection to backend
    conn, err := dialMyBackend(b.mbConfig)
    if err != nil {
        return &core.ErrConnectionFailed{
            Backend: backendName,
            Reason:  err,
        }
    }
    
    b.conn = conn
    b.connected = true
    return nil
}

func (b *Broker) Close() error {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    if !b.connected {
        return nil
    }
    
    err := b.conn.Close()
    b.connected = false
    b.cancelAllPending()
    return err
}

func (b *Broker) IsConnected() bool {
    b.mu.RLock()
    defer b.mu.RUnlock()
    return b.connected
}

func (b *Broker) Backend() string {
    return backendName
}

func (b *Broker) Publish(
    ctx context.Context, 
    destination string, 
    message *core.Message, 
    opts ...core.PublishOption,
) error {
    // ... implementation
}

func (b *Broker) Subscribe(
    ctx context.Context, 
    destination string, 
    handler core.Handler, 
    opts ...core.SubscribeOption,
) error {
    // ... implementation
}

func (b *Broker) Call(
    ctx context.Context, 
    destination string, 
    message *core.Message, 
    opts ...core.PublishOption,
) (*core.Message, error) {
    // ... implementation
}
```

### Register in init()

```go
func init() {
    core.Register(backendName, NewBroker)
}
```

This allows automatic discovery:

```go
import _ "github.com/prabhatdotdev/weave/transport/mybackend"
```

### Handle Request-Reply Pattern

Request-reply requires correlation ID tracking:

```go
// For backends WITH native request-reply support:
// Use backend-specific correlation features directly

// For backends WITHOUT native request-reply support:
// Simulate using temporary reply destinations

func (b *Broker) Call(ctx context.Context, destination string, message *core.Message, opts ...core.PublishOption) (*core.Message, error) {
    // 1. Generate correlation ID
    corrID := uuid.New().String()
    message.CorrelationID = corrID
    
    // 2. Create reply destination (queue/topic)
    replyDest := b.generateReplyDestination(corrID)
    message.ReplyTo = replyDest
    
    // 3. Register channel for response
    replyChan := make(chan *core.Message, 1)
    b.pendingMu.Lock()
    b.pending[corrID] = replyChan
    b.pendingMu.Unlock()
    defer func() {
        b.pendingMu.Lock()
        delete(b.pending, corrID)
        b.pendingMu.Unlock()
    }()
    
    // 4. Subscribe to reply destination
    replyReceived := make(chan error, 1)
    go func() {
        replyReceived <- b.Subscribe(ctx, replyDest, func(ctx context.Context, resp *core.Message) error {
            if resp.CorrelationID == corrID {
                replyChan <- resp
            }
            return nil
        })
    }()
    
    // 5. Publish request
    if err := b.Publish(ctx, destination, message, opts...); err != nil {
        return nil, err
    }
    
    // 6. Wait for reply with timeout
    select {
    case <-ctx.Done():
        return nil, &core.ErrTimeout{
            Operation: "Call",
            Duration:  ctx.Err().Error(),
        }
    case resp := <-replyChan:
        return resp, nil
    }
}
```

### Handle Message Conversion

Convert between Weave's `Message` and your backend's native format:

```go
// Convert Weave message to backend format
func toMyBackendMessage(msg *core.Message) MyBackendMessage {
    return MyBackendMessage{
        Data:          msg.Body,
        CorrelationID: msg.CorrelationID,
        ReplyTo:       msg.ReplyTo,
        Headers:       msg.Headers,
        ContentType:   msg.ContentType,
        MessageID:     msg.MessageID,
        Timestamp:     msg.Timestamp,
    }
}

// Convert backend message to Weave format
func fromMyBackendMessage(nbMsg MyBackendMessage) *core.Message {
    return &core.Message{
        Body:          nbMsg.Data,
        CorrelationID: nbMsg.CorrelationID,
        ReplyTo:       nbMsg.ReplyTo,
        Headers:       nbMsg.Headers,
        ContentType:   nbMsg.ContentType,
        MessageID:     nbMsg.MessageID,
        Timestamp:     nbMsg.Timestamp,
    }
}
```

### Error Handling

Map backend-specific errors to Weave error types:

```go
func (b *Broker) Publish(...) error {
    if !b.IsConnected() {
        return &core.ErrNotConnected{Backend: backendName}
    }
    
    if err := b.conn.Send(...); err != nil {
        if isConnectionError(err) {
            return &core.ErrConnectionLost{
                Backend: backendName,
                Cause:   err,
            }
        }
        return &core.ErrPublishFailed{
            Backend:     backendName,
            Destination: destination,
            Cause:       err,
        }
    }
    
    return nil
}
```

## Step 3: Add Comprehensive Tests

### Unit Tests (Required)

Create `transport/mybackend/mybackend_test.go`:

```go
func TestBrokerConnectAndClose(t *testing.T) {
    broker := NewBroker(&core.Config{MyBackend: DefaultMyBackendConfig()})
    
    // Should not be connected initially
    if broker.IsConnected() {
        t.Fatal("broker should not be connected initially")
    }
    
    // Connect
    if err := broker.Connect(context.Background()); err != nil {
        t.Fatalf("Connect() error = %v", err)
    }
    
    if !broker.IsConnected() {
        t.Fatal("broker should be connected")
    }
    
    // Close
    if err := broker.Close(); err != nil {
        t.Fatalf("Close() error = %v", err)
    }
    
    if broker.IsConnected() {
        t.Fatal("broker should not be connected after close")
    }
}

func TestBrokerPublish(t *testing.T) {
    broker := NewBroker(&core.Config{MyBackend: DefaultMyBackendConfig()})
    broker.Connect(context.Background())
    defer broker.Close()
    
    msg := core.NewMessage([]byte("test"))
    if err := broker.Publish(context.Background(), "test-queue", msg); err != nil {
        t.Fatalf("Publish() error = %v", err)
    }
}

func TestBrokerSubscribe(t *testing.T) {
    broker := NewBroker(&core.Config{MyBackend: DefaultMyBackendConfig()})
    broker.Connect(context.Background())
    defer broker.Close()
    
    received := make(chan *core.Message)
    handler := func(ctx context.Context, msg *core.Message) error {
        received <- msg
        return nil
    }
    
    // Subscribe
    err := broker.Subscribe(context.Background(), "test-queue", handler)
    if err != nil {
        t.Fatalf("Subscribe() error = %v", err)
    }
    
    // Publish
    sent := core.NewMessage([]byte("hello"))
    broker.Publish(context.Background(), "test-queue", sent)
    
    // Verify received
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    select {
    case <-ctx.Done():
        t.Fatal("timeout waiting for message")
    case msg := <-received:
        if string(msg.Body) != "hello" {
            t.Fatalf("received body = %q, want %q", string(msg.Body), "hello")
        }
    }
}

func TestBrokerCall(t *testing.T) {
    broker := NewBroker(&core.Config{MyBackend: DefaultMyBackendConfig()})
    broker.Connect(context.Background())
    defer broker.Close()
    
    // Register handler
    broker.Subscribe(context.Background(), "users.get", func(ctx context.Context, msg *core.Message) error {
        // Echo response
        response := core.NewMessage([]byte("user: john"))
        response.CorrelationID = msg.CorrelationID
        broker.Publish(ctx, msg.ReplyTo, response)
        return nil
    })
    
    // Make RPC call
    request := core.NewMessage([]byte("get user 1"))
    response, err := broker.Call(
        context.Background(), 
        "users.get", 
        request,
        core.WithTimeout(5*time.Second),
    )
    if err != nil {
        t.Fatalf("Call() error = %v", err)
    }
    
    if string(response.Body) != "user: john" {
        t.Fatalf("response = %q, want %q", string(response.Body), "user: john")
    }
}
```

### Contract Tests (Required)

Test that your backend matches the MessageBroker interface contract:

```go
// In a separate test file to be shared across backends
func TestMessageBrokerContract(t *testing.T) {
    tests := []struct {
        name   string
        testFn func(*testing.T, core.MessageBroker)
    }{
        {"Lifecycle", testLifecycle},
        {"Publish", testPublish},
        {"Subscribe", testSubscribe},
        {"RequestReply", testRequestReply},
        {"Timeout", testTimeout},
        {"ConcurrentPublish", testConcurrentPublish},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            broker := NewBroker(&core.Config{MyBackend: DefaultMyBackendConfig()})
            tt.testFn(t, broker)
        })
    }
}
```

### Integration Tests (Optional but Recommended)

Test against a real backend instance (using Docker):

```go
// In mybackend_integration_test.go with +build integration
func TestIntegrationPublishSubscribe(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    // Assumes MyBackend is running on localhost:5000
    config := &core.Config{
        Backend: "mybackend",
        MyBackend: &core.MyBackendConfig{
            Servers: []string{"localhost:5000"},
        },
    }
    
    broker, _ := NewBroker(config)
    broker.Connect(context.Background())
    defer broker.Close()
    
    // ... test logic
}
```

**Run integration tests:**
```bash
go test -tags=integration ./transport/mybackend/...
```

### Test Coverage Requirements

Minimum coverage requirements:

- **Connection lifecycle:** 100%
  - Connect, IsConnected, Close, errors

- **Publishing:** 100%
  - Basic publish
  - Publish with options
  - Publish to non-existent queue
  - Publish when disconnected
  - Concurrent publishes

- **Subscription:** 100%
  - Basic subscribe
  - Multiple handlers
  - Unsubscribe / cleanup
  - Handler errors
  - Concurrent message handling

- **Request-Reply:** 100%
  - Successful RPC call
  - Timeout handling
  - Multiple concurrent calls
  - Call with no ReplyTo
  - Connection loss during Call

- **Error paths:** 95%+
  - All documented error types
  - Connection failures
  - Invalid configuration
  - Backend-specific errors

## Step 4: Document the Backend

### Create Transport Documentation

Add a section to `docs/TRANSPORTS.md`:

```markdown
## MyBackend

### Basic Configuration

\`\`\`go
config := &weave.Config{
    Backend: "mybackend",
    MyBackend: &weave.MyBackendConfig{
        Servers: []string{"mybackend.example.com:5000"},
        Username: "myapp",
        Password: "secret",
    },
}

broker, err := weave.New(config)
\`\`\`

### Features and Limitations

- ✅ Publish and Subscribe
- ✅ Request-Reply (RPC calls)
- ✅ TLS/SSL
- ❌ Consumer Groups (use application-level filtering)
- ❌ Durable Queues (ephemeral only)

### Backend-Specific Behavior

**Partition Key Handling:**
MyBackend uses the Message.Subject field for partitioning.

\`\`\`go
client.Publish(ctx, "events", msg, 
    weave.WithKey("user-123"))  // Routes to partition
\`\`\`

**Timeout Configuration:**

\`\`\`go
config.MyBackend.RequestTimeout = 5 * time.Second
\`\`\`
```

### Add README Example

Add a complete example in `examples/mybackend/`:

```
examples/mybackend/
├── README.md
├── client/
│   └── main.go
└── server/
    └── main.go
```

Document:
- How to set up MyBackend locally
- How to run the example
- What the example demonstrates
- Expected output

### Add API Reference

Update `docs/API.md` with the new config type:

```markdown
### MyBackendConfig

\`\`\`go
type MyBackendConfig struct {
    Servers       []string
    Username      string
    Password      string
    TLS           *TLSConfig
    RequestTimeout time.Duration
}
\`\`\`
```

## Step 5: Add to Build and CI

### Update Import Path

Add to `weave.go` main package (optional, for convenience):

```go
import (
    // ... existing imports
    _ "github.com/prabhatdotdev/weave/transport/mybackend"
)
```

Or document that users must explicitly import:

```go
import _ "github.com/prabhatdotdev/weave/transport/mybackend"
```

### Add CI Tests

Update `.github/workflows/test.yml` to test against MyBackend:

```yaml
- name: Start MyBackend
  run: docker-compose up -d mybackend

- name: Test MyBackend
  run: go test -tags=integration ./transport/mybackend/...

- name: Stop MyBackend
  run: docker-compose down
```

### Update go.mod

Add MyBackend client library dependency if needed:

```bash
go get github.com/mybackend/client-go
go mod tidy
```

## Step 6: Create Capability Matrix

Document exactly what your backend supports:

### Publish

- ✅ Async fire-and-forget
- ✅ Optional persistence
- Topic/Queue routing
- Message ordering

### Subscribe

- ✅ Multiple subscribers per destination
- ✅ Concurrent message handling
- Prefetch/buffer control
- Explicit/implicit ack

### Request-Reply

- ✅ Correlation ID tracking
- ✅ Reply-to addressing
- Timeout enforcement
- Message filtering

### Configuration

- Required connection parameters
- Optional security/TLS
- Timeouts and retry behavior
- Backend-specific tuning

## Verification Checklist

Before proposing a new backend for inclusion, verify:

### Code Quality
- [ ] All tests pass: `go test ./transport/mybackend/...`
- [ ] Code coverage >= 90%: `go test -cover ./transport/mybackend/...`
- [ ] No linting issues: `golangci-lint run ./transport/mybackend/...`
- [ ] Follows Weave code style and patterns

### Documentation
- [ ] `docs/TRANSPORTS.md` section added
- [ ] Capability matrix documented
- [ ] Example implementation in `examples/mybackend/`
- [ ] API docs updated with config types
- [ ] Limitations clearly documented

### Testing
- [ ] Unit tests cover all code paths
- [ ] Contract tests pass
- [ ] Integration tests provided
- [ ] Tests run in CI
- [ ] Docker Compose setup included (if needed)

### Compatibility
- [ ] Implements full `core.MessageBroker` interface
- [ ] Registers backend in `init()`
- [ ] Handles all error types properly
- [ ] Works with existing Client/Server APIs
- [ ] Compatible with existing examples

### Maintenance
- [ ] Maintainer contact information documented
- [ ] Issue/bug report process documented
- [ ] Regular testing scheduled (in CI)
- [ ] Dependency version constraints specified

## Example: Adding Redis Streams Backend

Here's how the Redis Streams backend would be added:

**File structure:**
```
transport/redis/
├── redis.go           # Main implementation
├── redis_test.go      # Unit tests
└── connection.go      # Connection pooling
```

**Key implementation details:**
1. Use XADD for publishing (FIFO stream)
2. Use XREAD for subscribing (consumer groups)
3. Use generated reply queue names for request-reply
4. Implement connection pooling for efficiency
5. Use Lua scripts for atomic operations

**Capability limitations:**
- ✅ Publish and Subscribe
- ✅ Request-Reply (simulated)
- ✅ Consumer Groups
- ❌ Routing keys (use topic per message type)
- ❌ Message TTL (application-level)

**Config type:**
```go
type RedisConfig struct {
    Addr          string
    Password      string
    DB            int
    TLS           *TLSConfig
    ConsumerGroup string
    ConsumerName  string
    MaxLen        int64
    PoolSize      int
}
```

## FAQ

**Q: How long does it take to add a backend?**
A: 3-6 weeks including implementation, testing, and documentation.

**Q: Can I use Weave with unsupported backends?**
A: Yes, fork the repo and add your backend. Then contribute it back!

**Q: What if my backend doesn't support request-reply?**
A: Weave can simulate it using temporary reply queues and correlation IDs.

**Q: Do I need 100% test coverage?**
A: We require >= 90%, with 100% for critical paths (connect, publish, subscribe, call).

**Q: What about backward compatibility?**
A: Backends must remain compatible with the `core.MessageBroker` interface. Breaking changes require Weave version bumps.

## See Also

- [Transport Architecture](ARCHITECTURE.md#transport-implementations)
- [Testing Guide](../docs/testing.md) (if exists)
- [AMQP Implementation](../transport/amqp/amqp.go) - Reference implementation
- [Kafka Implementation](../transport/kafka/kafka.go) - Reference implementation
