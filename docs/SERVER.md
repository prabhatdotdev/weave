# Server Tutorial

Build message-driven services that handle incoming messages from queues and topics.

## Table of Contents

1. [Overview](#overview)
2. [Getting Started](#getting-started)
3. [Creating a Server](#creating-a-server)
4. [Registering Handlers](#registering-handlers)
5. [Starting the Server](#starting-the-server)
6. [Request-Reply Pattern](#request-reply-pattern)
7. [Error Handling](#error-handling)
8. [Graceful Shutdown](#graceful-shutdown)
9. [Configuration](#configuration)
10. [Testing Servers](#testing-servers)
11. [Best Practices](#best-practices)
12. [Complete Examples](#complete-examples)

---

## Overview

The `Server` abstraction in Weave is designed for building **message-driven microservices**. A server:

- Subscribes to one or more queues/topics
- Routes incoming messages to registered handlers
- Manages connection lifecycle automatically
- Supports multiple concurrent handlers

### When to Use Server

| Use Case | Server? |
|----------|---------|
| Process orders from a queue | ✅ Yes |
| Handle RPC requests from clients | ✅ Yes |
| React to events from other services | ✅ Yes |
| Only send messages (no receiving) | ❌ Use [Client](CLIENT.md) |
| Web API that publishes events | ❌ Use [Client](CLIENT.md) |

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
    server, err := weave.NewServer(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }

    server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
        fmt.Printf("Order received: %s\n", string(msg.Body))
        return nil
    })

    if err := server.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()

    select {} // Keep running
}
```

---

## Creating a Server

### Using Default Configuration

```go
server, err := weave.NewServer(weave.DefaultConfig())
if err != nil {
    log.Fatal(err)
}
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

server, err := weave.NewServer(config)
```

### Using Kafka

```go
config := &weave.Config{
    Backend: "kafka",
    Kafka: &weave.KafkaConfig{
        Brokers:       []string{"kafka1:9092", "kafka2:9092"},
        ConsumerGroup: "order-service",
        ClientID:      "order-service-1",
    },
}

server, err := weave.NewServer(config)
```

### With Existing Broker

For advanced use cases or testing:

```go
broker, _ := weave.New(config)
server := weave.NewServerWithBroker(broker, config)
```

---

## Registering Handlers

Handlers are functions that process incoming messages. Register them before calling `Start()`.

### Handler Signature

```go
type Handler func(ctx context.Context, msg *weave.Message) error
```

### Basic Handler

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    fmt.Printf("Received: %s\n", string(msg.Body))
    return nil
})
```

### Multiple Handlers

Register handlers for different destinations:

```go
server.Handle("orders", orderHandler)
server.Handle("payments", paymentHandler)
server.Handle("notifications", notificationHandler)
```

### Method Chaining

Handlers can be chained:

```go
server.
    Handle("orders", orderHandler).
    Handle("payments", paymentHandler).
    Handle("notifications", notificationHandler)
```

### Handler with Struct Methods

```go
type OrderService struct {
    db *sql.DB
}

func (s *OrderService) HandleOrder(ctx context.Context, msg *weave.Message) error {
    var order Order
    if err := json.Unmarshal(msg.Body, &order); err != nil {
        return err
    }
    return s.db.SaveOrder(ctx, order)
}

// Registration
orderService := &OrderService{db: db}
server.Handle("orders", orderService.HandleOrder)
```

### Accessing Message Metadata

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    // Message metadata
    fmt.Printf("Message ID: %s\n", msg.MessageID)
    fmt.Printf("Correlation ID: %s\n", msg.CorrelationID)
    fmt.Printf("Content-Type: %s\n", msg.ContentType)
    fmt.Printf("Timestamp: %s\n", msg.Timestamp)
    
    // Headers
    for key, value := range msg.Headers {
        fmt.Printf("Header %s: %s\n", key, value)
    }
    
    // Kafka-specific
    fmt.Printf("Partition: %d, Offset: %d\n", msg.Partition, msg.Offset)
    
    // Body
    fmt.Printf("Body: %s\n", string(msg.Body))
    
    return nil
})
```

---

## Starting the Server

### Basic Start

```go
ctx := context.Background()
if err := server.Start(ctx); err != nil {
    log.Fatal(err)
}
```

### What `Start()` Does

1. Connects to the message broker (if not already connected)
2. Creates queues/topics as needed
3. Subscribes to all registered destinations
4. Begins consuming messages in background goroutines

### Start with Context

Use context for startup timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := server.Start(ctx); err != nil {
    log.Fatal("Failed to start server:", err)
}
```

### Checking Server Status

```go
if server.IsStarted() {
    fmt.Println("Server is running")
}
```

---

## Request-Reply Pattern

Servers can respond to RPC calls from clients. The response is automatically routed back via the `ReplyTo` field.

### Server-Side (Responder)

```go
server.Handle("users.get", func(ctx context.Context, msg *weave.Message) error {
    // Parse request
    userID := string(msg.Body)
    
    // Fetch user from database
    user, err := db.GetUser(ctx, userID)
    if err != nil {
        return err
    }
    
    // The response is sent automatically if handler returns nil
    // For explicit response, use the broker directly:
    response, _ := json.Marshal(user)
    return server.Publish(ctx, msg.ReplyTo, weave.NewMessage(response))
})
```

### Client-Side (Caller)

```go
// See CLIENT.md for full client documentation
response, err := client.Call(ctx, "users.get", weave.NewTextMessage("user-123"))
```

### Handling RPC Errors

```go
server.Handle("users.get", func(ctx context.Context, msg *weave.Message) error {
    user, err := db.GetUser(ctx, string(msg.Body))
    if err != nil {
        // Return error response
        errorMsg := weave.NewMessage([]byte(`{"error": "user not found"}`))
        errorMsg.Headers["X-Error"] = "true"
        return server.Publish(ctx, msg.ReplyTo, errorMsg)
    }
    
    response, _ := json.Marshal(user)
    return server.Publish(ctx, msg.ReplyTo, weave.NewMessage(response))
})
```

---

## Error Handling

### Handler Errors

If a handler returns an error, the message is typically **not acknowledged** (depending on backend configuration), allowing for retry.

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    if err := processOrder(msg); err != nil {
        // Returning error = message not acked = will be redelivered
        return fmt.Errorf("failed to process order: %w", err)
    }
    // Returning nil = message acknowledged = processed successfully
    return nil
})
```

### Handling Panics

Wrap handlers to recover from panics:

```go
func recoverHandler(h weave.Handler) weave.Handler {
    return func(ctx context.Context, msg *weave.Message) (err error) {
        defer func() {
            if r := recover(); r != nil {
                err = fmt.Errorf("handler panic: %v", r)
            }
        }()
        return h(ctx, msg)
    }
}

server.Handle("orders", recoverHandler(orderHandler))
```

### Dead Letter Handling

For messages that fail repeatedly, configure dead letter queues at the broker level:

```go
// AMQP example - configure in RabbitMQ
config := &weave.AMQPConfig{
    // ... other config
    QueueArgs: map[string]interface{}{
        "x-dead-letter-exchange":    "dlx",
        "x-dead-letter-routing-key": "failed.orders",
    },
}
```

---

## Graceful Shutdown

### Basic Shutdown

```go
func main() {
    server, _ := weave.NewServer(config)
    server.Handle("orders", orderHandler)
    server.Start(context.Background())

    // Handle shutdown signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    fmt.Println("Shutting down...")
    if err := server.Stop(); err != nil {
        log.Printf("Error during shutdown: %v", err)
    }
}
```

### Shutdown with Timeout

```go
func shutdown(server *weave.Server, timeout time.Duration) error {
    done := make(chan error, 1)
    go func() {
        done <- server.Stop()
    }()

    select {
    case err := <-done:
        return err
    case <-time.After(timeout):
        return fmt.Errorf("shutdown timed out after %v", timeout)
    }
}

// Usage
if err := shutdown(server, 30*time.Second); err != nil {
    log.Fatal(err)
}
```

### Complete Signal Handling

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    server, err := weave.NewServer(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }

    server.Handle("orders", orderHandler)

    // Start server
    if err := server.Start(ctx); err != nil {
        log.Fatal(err)
    }

    // Wait for interrupt
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    
    sig := <-quit
    log.Printf("Received signal: %v", sig)
    
    // Graceful shutdown
    cancel() // Cancel context
    
    shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer shutdownCancel()
    
    done := make(chan struct{})
    go func() {
        server.Stop()
        close(done)
    }()
    
    select {
    case <-done:
        log.Println("Server stopped gracefully")
    case <-shutdownCtx.Done():
        log.Println("Shutdown timed out")
    }
}
```

---

## Configuration

### Server Configuration Options

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
| `QueueDurable` | bool | `true` | Durable queues survive restart |
| `Heartbeat` | Duration | `10s` | Connection heartbeat interval |

### Kafka-Specific Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Brokers` | []string | `["localhost:9092"]` | Kafka broker addresses |
| `ConsumerGroup` | string | `""` | Consumer group ID |
| `ClientID` | string | `""` | Client identifier |
| `AutoOffsetReset` | string | `"latest"` | `"earliest"` or `"latest"` |

---

## Testing Servers

### Using MockBroker

```go
import (
    "testing"
    "context"
    
    "github.com/prabhatdotdev/weave/testkit"
    "github.com/prabhatdotdev/weave/runtime"
    "github.com/prabhatdotdev/weave/core"
)

func TestOrderHandler(t *testing.T) {
    // Create mock broker
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    // Create server with mock
    server := runtime.NewServerWithBroker(broker, nil)
    
    // Track processed orders
    var processedOrder string
    server.Handle("orders", func(ctx context.Context, msg *core.Message) error {
        processedOrder = string(msg.Body)
        return nil
    })
    
    server.Start(context.Background())
    defer server.Stop()
    
    // Simulate incoming message
    err := broker.SimulateMessage(context.Background(), "orders", &core.Message{
        Body: []byte(`{"id": 123}`),
    })
    
    if err != nil {
        t.Fatalf("handler error: %v", err)
    }
    
    if processedOrder != `{"id": 123}` {
        t.Errorf("expected order to be processed, got: %s", processedOrder)
    }
}
```

### Testing Handler Errors

```go
func TestOrderHandler_Error(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    server := runtime.NewServerWithBroker(broker, nil)
    server.Handle("orders", func(ctx context.Context, msg *core.Message) error {
        return fmt.Errorf("processing failed")
    })
    server.Start(context.Background())
    
    err := broker.SimulateMessage(context.Background(), "orders", &core.Message{
        Body: []byte("invalid"),
    })
    
    if err == nil {
        t.Error("expected error from handler")
    }
}
```

### Verifying Subscriptions

```go
func TestServerSubscriptions(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    server := runtime.NewServerWithBroker(broker, nil)
    server.Handle("orders", orderHandler)
    server.Handle("payments", paymentHandler)
    server.Start(context.Background())
    
    // Verify subscriptions
    if !broker.HasSubscription("orders") {
        t.Error("expected subscription to 'orders'")
    }
    if !broker.HasSubscription("payments") {
        t.Error("expected subscription to 'payments'")
    }
    
    subs := broker.Subscriptions()
    if len(subs) != 2 {
        t.Errorf("expected 2 subscriptions, got %d", len(subs))
    }
}
```

---

## Best Practices

### 1. Keep Handlers Focused

```go
// ✅ Good - single responsibility
server.Handle("orders.created", handleOrderCreated)
server.Handle("orders.shipped", handleOrderShipped)
server.Handle("orders.cancelled", handleOrderCancelled)

// ❌ Bad - handler does too much
server.Handle("orders", handleAllOrderEvents)
```

### 2. Use Structured Logging

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    log.Printf("[%s] Processing order, correlation_id=%s", 
        msg.MessageID, msg.CorrelationID)
    
    if err := processOrder(msg); err != nil {
        log.Printf("[%s] Failed to process order: %v", msg.MessageID, err)
        return err
    }
    
    log.Printf("[%s] Order processed successfully", msg.MessageID)
    return nil
})
```

### 3. Validate Messages Early

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    var order Order
    if err := json.Unmarshal(msg.Body, &order); err != nil {
        // Log and return nil to acknowledge (don't retry invalid messages)
        log.Printf("Invalid message format: %v", err)
        return nil
    }
    
    if order.ID == "" {
        log.Printf("Missing order ID")
        return nil
    }
    
    return processOrder(ctx, order)
})
```

### 4. Use Context for Timeouts

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    // Add processing timeout
    ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()
    
    return processOrder(ctx, msg)
})
```

### 5. Idempotent Handlers

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    var order Order
    json.Unmarshal(msg.Body, &order)
    
    // Check if already processed (idempotency)
    if processed, _ := db.IsOrderProcessed(ctx, order.ID); processed {
        log.Printf("Order %s already processed, skipping", order.ID)
        return nil
    }
    
    return db.ProcessOrder(ctx, order)
})
```

---

## Complete Examples

### Order Processing Service

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

type Order struct {
    ID       string  `json:"id"`
    Customer string  `json:"customer"`
    Amount   float64 `json:"amount"`
}

func main() {
    config := weave.DefaultConfig()
    config.AMQP.Host = os.Getenv("RABBITMQ_HOST")
    
    server, err := weave.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    server.Handle("orders.new", func(ctx context.Context, msg *weave.Message) error {
        var order Order
        if err := json.Unmarshal(msg.Body, &order); err != nil {
            log.Printf("Invalid order format: %v", err)
            return nil // Acknowledge bad messages
        }
        
        log.Printf("Processing order %s for %s: $%.2f", 
            order.ID, order.Customer, order.Amount)
        
        // Process order...
        
        // Publish event
        event, _ := json.Marshal(map[string]string{
            "order_id": order.ID,
            "status":   "processed",
        })
        return server.Publish(ctx, "orders.processed", weave.NewMessage(event))
    })

    if err := server.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    log.Println("Order service started")

    // Graceful shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    log.Println("Shutting down...")
    server.Stop()
}
```

### User Service with RPC

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

type User struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

var users = map[string]User{
    "1": {ID: "1", Name: "Alice", Email: "alice@example.com"},
    "2": {ID: "2", Name: "Bob", Email: "bob@example.com"},
}

func main() {
    server, _ := weave.NewServer(weave.DefaultConfig())

    // RPC handler - responds to client.Call()
    server.Handle("users.get", func(ctx context.Context, msg *weave.Message) error {
        userID := string(msg.Body)
        
        user, exists := users[userID]
        if !exists {
            errorResp := []byte(`{"error": "user not found"}`)
            return server.Publish(ctx, msg.ReplyTo, weave.NewMessage(errorResp))
        }
        
        response, _ := json.Marshal(user)
        return server.Publish(ctx, msg.ReplyTo, weave.NewMessage(response))
    })

    server.Handle("users.list", func(ctx context.Context, msg *weave.Message) error {
        var userList []User
        for _, u := range users {
            userList = append(userList, u)
        }
        
        response, _ := json.Marshal(userList)
        return server.Publish(ctx, msg.ReplyTo, weave.NewMessage(response))
    })

    server.Start(context.Background())
    defer server.Stop()

    log.Println("User service started")
    select {}
}
```

---

## Next Steps

- [Client Tutorial](CLIENT.md) - Learn how to send messages and make RPC calls
- [Transport Configuration](TRANSPORTS.md) - Configure AMQP, Kafka, and more
- [Testing Servers](SERVER.md#testing-servers) - Best practices for testing server code
- [API Guide](API.md) - Public API overview and pkg.go.dev links
