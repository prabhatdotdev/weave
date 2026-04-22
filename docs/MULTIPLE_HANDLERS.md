# Multiple Handlers Guide

## Overview

The current `Server` API supports registering multiple handlers for multiple destinations:

```go
server.Handle("users.get", handleGetUser)
server.Handle("users.list", handleListUsers)
server.Handle("orders.create", handleCreateOrder)
```

Each `Handle()` call maps one destination to one handler.

## What Is Implemented Today

Supported pattern:

- multiple handlers on different destinations
- a single `Server` instance subscribing to all registered destinations when `Start()` is called

Not implemented as a built-in feature:

- typed handler registry APIs such as `RegisterHandler`
- one-queue method dispatch runtime such as `ProtobufService`
- per-handler worker-pool configuration in the library API

## Recommended Pattern

Use one destination per operation or message type.

Example:

```go
server, _ := weave.NewServer(config)

server.Handle("users.get", handleGetUser)
server.Handle("users.list", handleListUsers)
server.Handle("users.created", handleUserCreated)

if err := server.Start(context.Background()); err != nil {
    log.Fatal(err)
}
```

This is the pattern used by the current examples and runtime implementation.

## If You Need One Destination With Manual Dispatch

Weave does not currently provide a built-in method router for a single queue or topic. If you need that behavior, implement it inside a normal handler.

Example using a header-based dispatch:

```go
server.Handle("users", func(ctx context.Context, msg *weave.Message) error {
    switch msg.GetHeader("method") {
    case "get":
        return handleGetUser(ctx, msg)
    case "create":
        return handleCreateUser(ctx, msg)
    default:
        return fmt.Errorf("unknown method: %s", msg.GetHeader("method"))
    }
})
```

You can also dispatch based on:

- `msg.Subject`
- protobuf or JSON fields inside `msg.Body`
- transport-specific routing conventions already encoded in the destination name

## Tradeoffs

### Multiple Destinations

Pros:

- simpler handlers
- clearer routing
- matches the built-in API directly
- easier to document and test

### Single Destination With Manual Dispatch

Pros:

- useful when an upstream system requires one queue or topic

Cons:

- routing is your responsibility
- validation and error handling are application-level concerns
- the library does not manage per-method concurrency or worker pools for you

## Protobuf Note

The protobuf examples in this repository use the same destination-per-handler model. Protobuf is only the payload format; it does not change how handlers are registered.

See [PROTOBUF.md](PROTOBUF.md) and [examples/protobuf](../examples/protobuf).
