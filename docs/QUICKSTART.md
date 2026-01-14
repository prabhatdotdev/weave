# Quick Start Guide

Get up and running with Weave in 5 minutes!

## Prerequisites

- Go 1.21 or higher
- Docker (for running RabbitMQ or Kafka)

## Installation

```bash
go get github.com/prabhatdotdev/weave
```

## Start RabbitMQ

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management
```

Or using Docker Compose (from repository):

```bash
docker-compose up -d
```

## Your First Service

### 1. Create a Server

Create `server.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "strings"
    
    mqservice "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
    // Create configuration
    config := mqservice.DefaultConfig()
    
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

    // Define handler
    handler := func(ctx context.Context, body []byte) ([]byte, error) {
        fmt.Printf("Received: %s\n", string(body))
        return []byte(strings.ToUpper(string(body))), nil
    }

    // Start listening
    fmt.Println("Server listening on 'demo-queue'...")
    log.Fatal(broker.Subscribe(ctx, "demo-queue", handler))
}
```

Run the server:

```bash
go run server.go
```

### 2. Create a Client

In a new terminal, create `client.go`:

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
    // Create client broker
    config := mqservice.DefaultConfig()
    client, err := mqservice.New("amqp", config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // Connect
    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }

    // Make request
    msg := mqservice.NewMessage([]byte("hello world"))
    response, err := client.Call(ctx, "demo-queue", msg, 5*time.Second)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Response: %s\n", string(response.Body))
}
```

Run the client:

```bash
go run client.go
```

You should see:
- Server: `Received: hello world`
- Client: `Response: HELLO WORLD`

## Switching to Kafka

Want to use Kafka instead? Just change the config:

```go
import (
    mqservice "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/kafka"
)

config := &mqservice.Config{
    Backend: "kafka",
    Kafka: &mqservice.KafkaConfig{
        Brokers:       []string{"localhost:9092"},
        ConsumerGroup: "my-service",
    },
}

broker, err := mqservice.New("kafka", config)
// ... rest of the code stays the same!
```

Your application code doesn't change - that's the power of unified abstraction!

## Next Steps

### Working with JSON

```go
import "encoding/json"

type Request struct {
    Name string `json:"name"`
}

type Response struct {
    Greeting string `json:"greeting"`
}

handler := func(ctx context.Context, body []byte) ([]byte, error) {
    var req Request
    json.Unmarshal(body, &req)
    
    resp := Response{
        Greeting: fmt.Sprintf("Hello, %s!", req.Name),
    }
    
    return json.Marshal(resp)
}
```

### Adding Timeout

```go
// 1 second timeout
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
defer cancel()

response, err := client.Call(ctx, "queue", request)
if errors.Is(err, mqservice.ErrTimeout) {
    fmt.Println("Request timed out!")
}
```

### Error Handling

```go
handler := func(ctx context.Context, body []byte) ([]byte, error) {
    if len(body) == 0 {
        return nil, fmt.Errorf("empty request")
    }
    
    // Process...
    return response, nil
}

// Client side
response, err := client.Call(ctx, "queue", request)
if err != nil {
    switch {
    case errors.Is(err, mqservice.ErrTimeout):
        log.Println("Timeout")
    case errors.Is(err, mqservice.ErrConnectionLost):
        log.Println("Connection lost")
    default:
        log.Printf("Error: %v", err)
    }
}
```

## Examples

Check out the examples directory for more:

- **Simple** - Basic request-response
- **Microservices** - Multiple services with JSON
- **Middleware** - Logging, metrics, retry logic
- **Circuit Breaker** - Resilient service calls

```bash
# Run simple example
make run-simple

# Run microservices example
make run-microservices
```

## Common Patterns

### Multiple Services

```go
// Start multiple services
go service1.ListenAndServe("users", userHandler)
go service2.ListenAndServe("orders", orderHandler)
go service3.ListenAndServe("payments", paymentHandler)
```

### Concurrent Requests

```go
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        ctx := context.WithTimeout(context.Background(), 5*time.Second)
        client.Call(ctx, "queue", request)
    }()
}
wg.Wait()
```

### Production Configuration

```go
config := &mqservice.Config{
    Host:            "rabbitmq.prod.example.com",
    Port:            5672,
    Username:        os.Getenv("RABBITMQ_USER"),
    Password:        os.Getenv("RABBITMQ_PASS"),
    VHost:           "/prod",
    ConnectionName:  "my-service-v1",
    Heartbeat:       10 * time.Second,
    ConnectionRetry: 5,
    RetryDelay:      3 * time.Second,
}
```

## Troubleshooting

### RabbitMQ Connection Failed

```bash
# Check if RabbitMQ is running
docker ps | grep rabbitmq

# Check logs
docker logs rabbitmq

# Restart RabbitMQ
docker restart rabbitmq
```

### Timeout Issues

- Increase timeout: `context.WithTimeout(ctx, 30*time.Second)`
- Check server processing time
- Verify network connectivity

### Connection Lost

- Check RabbitMQ health: `http://localhost:15672` (guest/guest)
- Verify heartbeat settings
- Check network stability

## Help & Resources

- Full documentation: [README.md](README.md)
- Examples: [examples/](examples/)
- Issues: [GitHub Issues](https://github.com/prabhatdotdev/weave/issues)
- RabbitMQ Management UI: http://localhost:15672 (guest/guest)

## Testing

```bash
# Run tests
make test

# With coverage
make test-coverage

# Benchmarks
make bench
```

That's it! You're ready to build message-driven microservices with Go! 🚀
