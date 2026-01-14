# Documentation

Welcome to the Weave documentation!

## Getting Started

- **[Quick Start Guide](QUICKSTART.md)** - Get running in 5 minutes
- **[Installation & Setup](QUICKSTART.md#installation)** - Installation instructions

## Tutorials

- **[Server Tutorial](SERVER.md)** - Build message-driven services that handle incoming messages
- **[Client Tutorial](CLIENT.md)** - Send messages and make RPC calls

## Core Documentation

- **[Architecture Overview](ARCHITECTURE.md)** - System design and architecture
- **[API Reference](API.md)** - Complete API documentation
- **[Transport Configuration](TRANSPORTS.md)** - Configure AMQP, Kafka, etc.

## Advanced Topics

- **[Protocol Buffers Support](PROTOBUF.md)** - Type-safe messaging with protobuf
- **[Multiple Handlers](MULTIPLE_HANDLERS.md)** - Register multiple handlers per service

## Contributing

- **[Contributing Guide](../CONTRIBUTING.md)** - Contribution guidelines

## Quick Links

### Server vs Client

| Use Case | What to Use |
|----------|------------|
| Process messages from queues | [Server](SERVER.md) |
| Handle RPC requests | [Server](SERVER.md) |
| Send messages (fire-and-forget) | [Client](CLIENT.md) |
| Make RPC calls | [Client](CLIENT.md) |
| Both send and receive | Use both or raw `MessageBroker` |

### Supported Transports

| Transport | Status | Documentation |
|-----------|--------|---------------|
| AMQP (RabbitMQ) | ✅ Ready | [TRANSPORTS.md](TRANSPORTS.md#amqp-rabbitmq) |
| Apache Kafka | ✅ Ready | [TRANSPORTS.md](TRANSPORTS.md#apache-kafka) |

### Common Tasks

| Task | Documentation |
|------|---------------|
| Create a server | [Server Tutorial](SERVER.md#creating-a-server) |
| Register message handlers | [Server Tutorial](SERVER.md#registering-handlers) |
| Create a client | [Client Tutorial](CLIENT.md#creating-a-client) |
| Publish messages | [Client Tutorial](CLIENT.md#publishing-messages) |
| Make RPC calls | [Client Tutorial](CLIENT.md#making-rpc-calls) |
| Handle errors | [Client Tutorial](CLIENT.md#error-handling) |
| Test your code | [Server Testing](SERVER.md#testing-servers) / [Client Testing](CLIENT.md#testing-clients) |
| Switch transports | [TRANSPORTS.md](TRANSPORTS.md#switching-transports) |
| Use Protocol Buffers | [PROTOBUF.md](PROTOBUF.md) |

### Quick Examples

**Server (Handle Messages)**
```go
import (
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

server, _ := weave.NewServer(weave.DefaultConfig())

server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    fmt.Printf("Order received: %s\n", string(msg.Body))
    return nil
})

server.Start(context.Background())
defer server.Stop()
```

**Client (Send Messages)**
```go
import (
    "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
)

client, _ := weave.NewClient(weave.DefaultConfig())
client.Connect(context.Background())
defer client.Close()

// Fire-and-forget
client.Publish(ctx, "orders", weave.NewMessage(data))

// Request-reply RPC
response, _ := client.Call(ctx, "users.get", weave.NewTextMessage("123"))
```

## Examples

| Example | Description |
|---------|-------------|
| [JSON Example](../examples/json) | Basic microservices using JSON serialization |
| [Protobuf Example](../examples/protobuf) | Type-safe messaging with Protocol Buffers |

## Documentation Structure

```
docs/
├── README.md              # This file - documentation index
├── QUICKSTART.md          # Quick start guide
├── SERVER.md              # Server tutorial (handling messages)
├── CLIENT.md              # Client tutorial (sending messages)
├── ARCHITECTURE.md        # Architecture & design
├── API.md                 # API reference
├── TRANSPORTS.md          # Transport configuration
├── PROTOBUF.md            # Protocol Buffers guide
└── MULTIPLE_HANDLERS.md   # Multiple handlers guide
```

## Need Help?

- **Issues**: [GitHub Issues](https://github.com/prabhatdotdev/weave/issues)
- **Discussions**: [GitHub Discussions](https://github.com/prabhatdotdev/weave/discussions)

## Project Status

Weave is actively developed and ready for production use.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
