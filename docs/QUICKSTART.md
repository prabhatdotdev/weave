# Quick Start Guide

Get up and running with Weave using the current public API.

## Prerequisites

- Go 1.21 or higher
- Docker or Docker Compose to run RabbitMQ locally

## Installation

```bash
go get github.com/prabhatdotdev/weave
```

Import Weave and at least one transport package:

```go
import (
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)
```

## Start RabbitMQ

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management
```

Or from the repository root:

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

    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
    server, err := weave.NewServer(weave.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }

    server.Handle("demo-queue", func(ctx context.Context, msg *weave.Message) error {
        fmt.Printf("Received: %s\n", string(msg.Body))
        msg.Body = []byte(strings.ToUpper(string(msg.Body)))
        return nil
    })

    if err := server.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()

    fmt.Println("Server listening on demo-queue...")
    select {}
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

    msg := weave.NewMessage([]byte("hello world"))
    response, err := client.Call(ctx, "demo-queue", msg, weave.WithTimeout(5*time.Second))
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

To switch to Kafka, change the config and import the Kafka transport:

```go
import (
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/kafka"
)

config := weave.DefaultConfig().WithKafka(&weave.KafkaConfig{
    Brokers:       []string{"localhost:9092"},
    ConsumerGroup: "my-service",
})

client, err := weave.NewClient(config)
```

The application-facing `Client` and `Server` APIs stay the same.

## Next Steps

### Working with JSON

```go
type Request struct {
    Name string `json:"name"`
}

type Response struct {
    Greeting string `json:"greeting"`
}

server.Handle("greeter", func(ctx context.Context, msg *weave.Message) error {
    var req Request
    if err := json.Unmarshal(msg.Body, &req); err != nil {
        return err
    }

    body, err := json.Marshal(Response{Greeting: fmt.Sprintf("Hello, %s!", req.Name)})
    if err != nil {
        return err
    }

    msg.Body = body
    return nil
})
```

### Adding Timeout

```go
response, err := client.Call(ctx, "greeter", request, weave.WithTimeout(1*time.Second))
if err != nil && weave.IsTimeout(err) {
    fmt.Println("Request timed out")
}
```

### Error Handling

```go
response, err := client.Call(ctx, "greeter", request, weave.WithTimeout(5*time.Second))
if err != nil {
    switch {
    case weave.IsTimeout(err):
        log.Println("Timeout")
    case weave.IsConnectionLost(err):
        log.Println("Connection lost")
    case weave.IsNotConnected(err):
        log.Println("Broker is not connected")
    default:
        log.Printf("Call failed: %v", err)
    }
    return
}

fmt.Printf("Response: %s\n", string(response.Body))
```

## Examples

Runnable examples live in the repository:

- `examples/json/`
- `examples/protobuf/`

## Production Notes

- Set `ConnectionRetry` and `RetryDelay` in `Config` to control reconnect behavior.
- Use transport-specific config for broker details such as AMQP credentials or Kafka consumer groups.
- Keep request timeouts explicit for RPC-style calls.
- Treat `ErrConnectionLost` and `ErrTimeout` as explicit application decisions; Weave does not automatically replay in-flight RPCs after reconnect.
- Design handlers and RPC endpoints to be idempotent so reconnect-triggered redelivery is safe.

## Troubleshooting

### RabbitMQ Connection Failed

```bash
docker ps | grep rabbitmq
docker logs rabbitmq
docker restart rabbitmq
```

### Timeout Issues

- Increase the per-call timeout with `weave.WithTimeout`.
- Check handler processing time and broker health.

### Connection Lost

- Check RabbitMQ health at `http://localhost:15672`.
- Verify broker credentials and network reachability.

## Help And Resources

- Full documentation index: [README.md](README.md)
- API docs: https://pkg.go.dev/github.com/prabhatdotdev/weave
- Issues: https://github.com/prabhatdotdev/weave/issues

You're ready to build message-driven services with Weave.
