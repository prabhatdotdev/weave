# Client Tutorial

Send messages and make RPC calls to message-driven services.

## Table of Contents

1. [Overview](#overview)
2. [Getting Started](#getting-started)
3. [Creating a Client](#creating-a-client)
4. [Publishing Messages](#publishing-messages)
5. [Making RPC Calls](#making-rpc-calls)
6. [Publish Options](#publish-options)
7. [Error Handling](#error-handling)
8. [Connection Management](#connection-management)
9. [Configuration](#configuration)
10. [Testing Clients](#testing-clients)
11. [Best Practices](#best-practices)
12. [Complete Examples](#complete-examples)

---

## Overview

The `Client` abstraction in Weave is designed for **sending messages** to message-driven services. A client can:

- **Publish** messages (fire-and-forget)
- **Call** services with request-reply (synchronous RPC)
- Manage connection lifecycle

### When to Use Client

| Use Case | Client? |
|----------|---------|
| Web API publishing events | ✅ Yes |
| Making RPC calls to microservices | ✅ Yes |
| Sending notifications | ✅ Yes |
| Processing incoming messages | ❌ Use [Server](SERVER.md) |
| Building message handlers | ❌ Use [Server](SERVER.md) |

### Client vs Server

| Feature | Client | Server |
|---------|--------|--------|
| `Publish()` | ✅ | ✅ |
| `Call()` (RPC) | ✅ | ✅ |
| `Handle()` / `Subscribe()` | ❌ | ✅ |
| Use case | Send messages | Receive messages |

---

## Getting Started

### Installation

```bash
go get github.com/prabhatdotdev/weave
```

### Import

```go
import (
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"  // or kafka
)
```

### Minimal Example

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
    client, err := weave.NewClient(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }

    // Send a message
    msg := weave.NewMessage([]byte(`{"event": "user.created"}`))
    if err := client.Publish(ctx, "events", msg); err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Message sent!")
}
```

---

## Creating a Client

### Using Default Configuration

```go
client, err := weave.NewClient(weave.DefaultConfig())
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Using Custom Configuration

```go
config := &weave.Config{
    Backend: "amqp",
    AMQP: &weave.AMQPConfig{
        Host:     "rabbitmq.example.com",
        Port:     5672,
        Username: "myuser",
        Password: "mypassword",
        VHost:    "/production",
    },
    ConnectionRetry: 5,
    RetryDelay:      3 * time.Second,
}

client, err := weave.NewClient(config)
```

### Using Kafka

```go
config := &weave.Config{
    Backend: "kafka",
    Kafka: &weave.KafkaConfig{
        Brokers:  []string{"kafka1:9092", "kafka2:9092"},
        ClientID: "my-web-api",
    },
}

client, err := weave.NewClient(config)
```

### With Existing Broker

For advanced use cases or testing:

```go
broker, _ := weave.New(config)
client := weave.NewClientWithBroker(broker, config)
```

---

## Publishing Messages

Publishing sends a message **without expecting a response** (fire-and-forget).

### Basic Publish

```go
msg := weave.NewMessage([]byte(`{"order_id": 123}`))
err := client.Publish(ctx, "orders", msg)
if err != nil {
    log.Printf("Failed to publish: %v", err)
}
```

### Publishing JSON

```go
type Order struct {
    ID       string  `json:"id"`
    Customer string  `json:"customer"`
    Amount   float64 `json:"amount"`
}

order := Order{ID: "123", Customer: "Alice", Amount: 99.99}
data, _ := json.Marshal(order)

msg := weave.NewMessage(data)
msg.ContentType = "application/json"

err := client.Publish(ctx, "orders.new", msg)
```

### Publishing Text

```go
msg := weave.NewTextMessage("Hello, World!")
err := client.Publish(ctx, "notifications", msg)
```

### Publishing with Headers

```go
msg := weave.NewMessage(data)
msg.Headers = map[string]string{
    "X-Source":     "web-api",
    "X-Request-ID": requestID,
    "X-User-ID":    userID,
}
err := client.Publish(ctx, "events", msg)
```

### Publishing with Message Key (Kafka)

```go
msg := weave.NewMessage(data)
msg.Subject = "user-123"  // Used as partition key

// Or use the option
err := client.Publish(ctx, "events", msg, weave.WithKey("user-123"))
```

---

## Making RPC Calls

The `Call` method sends a message and **waits for a response** (synchronous RPC).

### Basic Call

```go
request := weave.NewTextMessage("user-123")
response, err := client.Call(ctx, "users.get", request)
if err != nil {
    log.Printf("RPC failed: %v", err)
    return
}

fmt.Printf("Response: %s\n", string(response.Body))
```

### Call with Timeout

```go
request := weave.NewMessage([]byte(`{"id": "123"}`))

response, err := client.Call(ctx, "users.get", request, 
    weave.WithTimeout(5*time.Second))

if err != nil {
    if weave.IsTimeout(err) {
        log.Println("Request timed out")
    } else {
        log.Printf("RPC failed: %v", err)
    }
    return
}
```

### Call with JSON Request/Response

```go
type GetUserRequest struct {
    ID string `json:"id"`
}

type GetUserResponse struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

func getUser(ctx context.Context, client *weave.Client, userID string) (*GetUserResponse, error) {
    // Build request
    req := GetUserRequest{ID: userID}
    reqData, _ := json.Marshal(req)
    
    // Make call
    response, err := client.Call(ctx, "users.get", weave.NewMessage(reqData),
        weave.WithTimeout(10*time.Second))
    if err != nil {
        return nil, err
    }
    
    // Parse response
    var user GetUserResponse
    if err := json.Unmarshal(response.Body, &user); err != nil {
        return nil, err
    }
    
    return &user, nil
}

// Usage
user, err := getUser(ctx, client, "123")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("User: %s (%s)\n", user.Name, user.Email)
```

### Handling RPC Errors

```go
response, err := client.Call(ctx, "users.get", request)
if err != nil {
    log.Printf("RPC failed: %v", err)
    return nil, err
}

// Check for application-level errors in response
if response.Headers["X-Error"] == "true" {
    return nil, fmt.Errorf("service error: %s", string(response.Body))
}
```

---

## Publish Options

Customize publish and call behavior with functional options.

### Timeout

Set operation timeout:

```go
client.Publish(ctx, "queue", msg, weave.WithTimeout(5*time.Second))
client.Call(ctx, "service", msg, weave.WithTimeout(10*time.Second))
```

### Persistent Messages (AMQP)

Make messages survive broker restart:

```go
client.Publish(ctx, "orders", msg, weave.WithPersistent())
```

### Message Priority (AMQP)

Set message priority (0-9):

```go
client.Publish(ctx, "orders", msg, weave.WithPriority(5))
```

### Message Expiration

Set message TTL:

```go
client.Publish(ctx, "events", msg, weave.WithExpiration(60*time.Second))
```

### Partition Key (Kafka)

Route messages to specific partition:

```go
// Using option
client.Publish(ctx, "orders", msg, weave.WithKey("customer-123"))

// Or via message field
msg.Subject = "customer-123"
client.Publish(ctx, "orders", msg)
```

### Specific Partition (Kafka)

```go
client.Publish(ctx, "orders", msg, weave.WithPartition(3))
```

### Exchange (AMQP)

Publish to specific exchange:

```go
client.Publish(ctx, "routing.key", msg, weave.WithExchange("my-exchange"))
```

### Mandatory (AMQP)

Require message to be routed:

```go
client.Publish(ctx, "orders", msg, weave.WithMandatory())
```

### Combined Options

```go
err := client.Publish(ctx, "orders", msg,
    weave.WithTimeout(5*time.Second),
    weave.WithPersistent(),
    weave.WithPriority(5),
    weave.WithKey("customer-123"),
)
```

---

## Error Handling

### Checking Error Types

```go
err := client.Publish(ctx, "orders", msg)
if err != nil {
    switch {
    case weave.IsNotConnected(err):
        // Not connected to broker
        log.Println("Not connected, attempting reconnect...")
        client.Connect(ctx)
        
    case weave.IsTimeout(err):
        // Operation timed out
        log.Println("Operation timed out")
        
    case weave.IsConnectionLost(err):
        // Connection dropped
        log.Println("Connection lost")
        
    default:
        log.Printf("Publish failed: %v", err)
    }
}
```

### Retry Pattern

```go
func publishWithRetry(ctx context.Context, client *weave.Client, 
    dest string, msg *weave.Message, maxRetries int) error {
    
    var lastErr error
    for i := 0; i < maxRetries; i++ {
        err := client.Publish(ctx, dest, msg)
        if err == nil {
            return nil
        }
        
        lastErr = err
        
        if weave.IsNotConnected(err) {
            // Try to reconnect
            if err := client.Connect(ctx); err != nil {
                continue
            }
        }
        
        // Exponential backoff
        time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
    }
    
    return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}
```

### Call Error Handling

```go
response, err := client.Call(ctx, "users.get", request,
    weave.WithTimeout(5*time.Second))

if err != nil {
    if weave.IsTimeout(err) {
        // Consider retry or fallback
        return nil, fmt.Errorf("service unavailable: timeout")
    }
    return nil, err
}

// Check for error response from service
if errMsg := response.Headers["X-Error"]; errMsg != "" {
    return nil, fmt.Errorf("service error: %s", errMsg)
}
```

---

## Connection Management

### Manual Connection

```go
client, _ := weave.NewClient(config)

// Must call Connect before Publish/Call
if err := client.Connect(ctx); err != nil {
    log.Fatal("Failed to connect:", err)
}
```

### Connection Status

```go
if client.IsConnected() {
    // Safe to publish
    client.Publish(ctx, "queue", msg)
} else {
    // Need to reconnect
    client.Connect(ctx)
}
```

### Reconnection Pattern

```go
func ensureConnected(ctx context.Context, client *weave.Client) error {
    if client.IsConnected() {
        return nil
    }
    return client.Connect(ctx)
}

// Usage
if err := ensureConnected(ctx, client); err != nil {
    return err
}
client.Publish(ctx, "queue", msg)
```

### Proper Cleanup

```go
client, err := weave.NewClient(config)
if err != nil {
    log.Fatal(err)
}
defer client.Close() // Always close when done

client.Connect(ctx)
// ... use client ...
```

---

## Configuration

### Client Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Backend` | string | `"amqp"` | Backend type: `amqp`, `kafka` |
| `ConnectionRetry` | int | `3` | Number of connection retries |
| `RetryDelay` | Duration | `2s` | Delay between retries |

### AMQP-Specific Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Host` | string | `localhost` | RabbitMQ host |
| `Port` | int | `5672` | RabbitMQ port |
| `Username` | string | `guest` | Authentication username |
| `Password` | string | `guest` | Authentication password |
| `VHost` | string | `/` | Virtual host |
| `Heartbeat` | Duration | `10s` | Connection heartbeat |

### Kafka-Specific Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Brokers` | []string | `["localhost:9092"]` | Kafka broker addresses |
| `ClientID` | string | `""` | Client identifier |
| `RequiredAcks` | int | `1` | Ack level: 0, 1, or -1 (all) |

### Environment-Based Configuration

```go
func configFromEnv() *weave.Config {
    config := weave.DefaultConfig()
    
    if host := os.Getenv("RABBITMQ_HOST"); host != "" {
        config.AMQP.Host = host
    }
    if port := os.Getenv("RABBITMQ_PORT"); port != "" {
        p, _ := strconv.Atoi(port)
        config.AMQP.Port = p
    }
    if user := os.Getenv("RABBITMQ_USER"); user != "" {
        config.AMQP.Username = user
    }
    if pass := os.Getenv("RABBITMQ_PASS"); pass != "" {
        config.AMQP.Password = pass
    }
    
    return config
}
```

---

## Testing Clients

### Using MockBroker

```go
import (
    "testing"
    "context"
    
    "github.com/prabhatdotdev/weave/testkit"
    "github.com/prabhatdotdev/weave/runtime"
    "github.com/prabhatdotdev/weave/core"
)

func TestOrderService_PlaceOrder(t *testing.T) {
    // Create mock broker
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Create client with mock
    client := runtime.NewClientWithBroker(broker, nil)
    
    // Test your service
    orderService := NewOrderService(client)
    err := orderService.PlaceOrder(context.Background(), Order{
        ID: "123",
        Customer: "Alice",
    })
    
    if err != nil {
        t.Fatalf("PlaceOrder failed: %v", err)
    }
    
    // Verify publish was called
    broker.AssertPublished(t, "orders.new")
    broker.AssertPublishCount(t, "orders.new", 1)
    
    // Verify message content
    messages := broker.PublishedTo("orders.new")
    if len(messages) != 1 {
        t.Fatalf("expected 1 message, got %d", len(messages))
    }
    
    var order Order
    json.Unmarshal(messages[0].Message.Body, &order)
    if order.ID != "123" {
        t.Errorf("expected order ID 123, got %s", order.ID)
    }
}
```

### Testing RPC Calls

```go
func TestUserService_GetUser(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Set up mock response
    responseData, _ := json.Marshal(User{
        ID:    "123",
        Name:  "Alice",
        Email: "alice@example.com",
    })
    broker.SetCallResponse("users.get", &core.Message{
        Body: responseData,
    })
    
    client := runtime.NewClientWithBroker(broker, nil)
    userService := NewUserService(client)
    
    user, err := userService.GetUser(context.Background(), "123")
    if err != nil {
        t.Fatalf("GetUser failed: %v", err)
    }
    
    if user.Name != "Alice" {
        t.Errorf("expected Alice, got %s", user.Name)
    }
}
```

### Testing Call Errors

```go
func TestUserService_GetUser_NotFound(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Set up error response
    broker.SetCallError("users.get", &core.ErrTimeout{
        Operation: "Call",
        Duration:  "5s",
    })
    
    client := runtime.NewClientWithBroker(broker, nil)
    userService := NewUserService(client)
    
    _, err := userService.GetUser(context.Background(), "999")
    if err == nil {
        t.Error("expected error for non-existent user")
    }
}
```

### Verifying No Publish

```go
func TestOrderService_ValidationFails(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    client := runtime.NewClientWithBroker(broker, nil)
    orderService := NewOrderService(client)
    
    // Should fail validation - no publish
    err := orderService.PlaceOrder(ctx, Order{}) // Invalid order
    
    if err == nil {
        t.Error("expected validation error")
    }
    
    // Verify nothing was published
    broker.AssertNotPublished(t, "orders.new")
}
```

---

## Best Practices

### 1. Always Close Clients

```go
client, err := weave.NewClient(config)
if err != nil {
    return err
}
defer client.Close() // Always cleanup
```

### 2. Use Connection Pooling in HTTP Handlers

```go
type App struct {
    client *weave.Client
}

func NewApp(config *weave.Config) (*App, error) {
    client, err := weave.NewClient(config)
    if err != nil {
        return nil, err
    }
    
    if err := client.Connect(context.Background()); err != nil {
        return nil, err
    }
    
    return &App{client: client}, nil
}

func (a *App) HandleOrder(w http.ResponseWriter, r *http.Request) {
    // Reuse client connection
    msg := weave.NewMessage(body)
    if err := a.client.Publish(r.Context(), "orders", msg); err != nil {
        http.Error(w, "Failed to publish", 500)
        return
    }
    w.WriteHeader(http.StatusAccepted)
}

func (a *App) Close() error {
    return a.client.Close()
}
```

### 3. Set Appropriate Timeouts

```go
// Short timeout for non-critical events
client.Publish(ctx, "analytics", msg, weave.WithTimeout(1*time.Second))

// Longer timeout for critical operations
response, err := client.Call(ctx, "payments.process", msg,
    weave.WithTimeout(30*time.Second))
```

### 4. Include Correlation IDs

```go
func publishWithCorrelation(client *weave.Client, dest string, body []byte, correlationID string) error {
    msg := weave.NewMessage(body)
    msg.CorrelationID = correlationID
    msg.Headers["X-Request-ID"] = correlationID
    return client.Publish(context.Background(), dest, msg)
}
```

### 5. Log Publish Operations

```go
func publishWithLogging(client *weave.Client, dest string, msg *weave.Message) error {
    start := time.Now()
    err := client.Publish(context.Background(), dest, msg)
    duration := time.Since(start)
    
    if err != nil {
        log.Printf("PUBLISH FAILED dest=%s duration=%v error=%v", dest, duration, err)
    } else {
        log.Printf("PUBLISH OK dest=%s duration=%v msg_id=%s", dest, duration, msg.MessageID)
    }
    
    return err
}
```

### 6. Handle Backpressure

```go
func publishBatch(client *weave.Client, dest string, messages []*weave.Message) error {
    sem := make(chan struct{}, 10) // Limit concurrent publishes
    var wg sync.WaitGroup
    var firstErr error
    var errOnce sync.Once
    
    for _, msg := range messages {
        wg.Add(1)
        sem <- struct{}{} // Acquire
        
        go func(m *weave.Message) {
            defer wg.Done()
            defer func() { <-sem }() // Release
            
            if err := client.Publish(context.Background(), dest, m); err != nil {
                errOnce.Do(func() { firstErr = err })
            }
        }(msg)
    }
    
    wg.Wait()
    return firstErr
}
```

---

## Complete Examples

### Web API with Event Publishing

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

type Order struct {
    ID       string  `json:"id"`
    Customer string  `json:"customer"`
    Amount   float64 `json:"amount"`
}

type App struct {
    client *weave.Client
}

func main() {
    // Create client
    client, err := weave.NewClient(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    if err := client.Connect(context.Background()); err != nil {
        log.Fatal(err)
    }

    app := &App{client: client}

    // Setup routes
    http.HandleFunc("POST /orders", app.CreateOrder)
    http.HandleFunc("GET /health", app.Health)

    // Start server
    server := &http.Server{Addr: ":8080"}
    
    go func() {
        log.Println("Starting server on :8080")
        if err := server.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatal(err)
        }
    }()

    // Graceful shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    server.Shutdown(ctx)
}

func (a *App) CreateOrder(w http.ResponseWriter, r *http.Request) {
    var order Order
    if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }

    // Publish order event
    data, _ := json.Marshal(order)
    msg := weave.NewMessage(data)
    msg.ContentType = "application/json"
    
    if err := a.client.Publish(r.Context(), "orders.new", msg,
        weave.WithTimeout(5*time.Second)); err != nil {
        log.Printf("Failed to publish order: %v", err)
        http.Error(w, "Failed to process order", http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusAccepted)
    json.NewEncoder(w).Encode(map[string]string{
        "status": "accepted",
        "order_id": order.ID,
    })
}

func (a *App) Health(w http.ResponseWriter, r *http.Request) {
    status := "healthy"
    if !a.client.IsConnected() {
        status = "degraded"
    }
    json.NewEncoder(w).Encode(map[string]string{"status": status})
}
```

### API Gateway with RPC

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "time"

    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

type User struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

type Gateway struct {
    client *weave.Client
}

func main() {
    client, _ := weave.NewClient(weave.DefaultConfig())
    client.Connect(context.Background())
    defer client.Close()

    gw := &Gateway{client: client}

    http.HandleFunc("GET /users/{id}", gw.GetUser)
    http.HandleFunc("GET /users", gw.ListUsers)
    
    log.Println("Gateway starting on :8080")
    http.ListenAndServe(":8080", nil)
}

func (g *Gateway) GetUser(w http.ResponseWriter, r *http.Request) {
    userID := r.PathValue("id")
    
    // Make RPC call to user service
    response, err := g.client.Call(r.Context(), "users.get", 
        weave.NewTextMessage(userID),
        weave.WithTimeout(5*time.Second))
    
    if err != nil {
        if weave.IsTimeout(err) {
            http.Error(w, "Service timeout", http.StatusGatewayTimeout)
        } else {
            http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
        }
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.Write(response.Body)
}

func (g *Gateway) ListUsers(w http.ResponseWriter, r *http.Request) {
    response, err := g.client.Call(r.Context(), "users.list",
        weave.NewMessage(nil),
        weave.WithTimeout(10*time.Second))
    
    if err != nil {
        http.Error(w, "Failed to fetch users", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.Write(response.Body)
}
```

### Background Job Publisher

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/kafka"
)

type Job struct {
    ID        string    `json:"id"`
    Type      string    `json:"type"`
    Payload   string    `json:"payload"`
    CreatedAt time.Time `json:"created_at"`
}

func main() {
    config := &weave.Config{
        Backend: "kafka",
        Kafka: &weave.KafkaConfig{
            Brokers:  []string{"localhost:9092"},
            ClientID: "job-scheduler",
        },
    }

    client, err := weave.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    if err := client.Connect(context.Background()); err != nil {
        log.Fatal(err)
    }

    // Schedule jobs every minute
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for range ticker.C {
        job := Job{
            ID:        generateID(),
            Type:      "cleanup",
            Payload:   `{"action": "delete_old_records"}`,
            CreatedAt: time.Now(),
        }

        data, _ := json.Marshal(job)
        msg := weave.NewMessage(data)
        msg.Subject = job.Type // Partition key
        
        if err := client.Publish(context.Background(), "jobs", msg); err != nil {
            log.Printf("Failed to schedule job: %v", err)
        } else {
            log.Printf("Scheduled job: %s", job.ID)
        }
    }
}
```

---

## Next Steps

- [Server Tutorial](SERVER.md) - Build message handlers
- [Transport Configuration](TRANSPORTS.md) - Configure AMQP, Kafka
- [Testing Guide](TESTING.md) - Best practices for testing
- [API Reference](API.md) - Complete API documentation
