# Protocol Buffers

## Official Strategy

Weave treats **Protocol Buffers as a payload format**, not as a runtime abstraction.

This repository **intentionally does not** implement:
- Dedicated `ProtobufService` or typed protobuf runtime
- Typed handler registry APIs (`RegisterHandler`, etc.)
- Typed RPC helper (`CallProtobuf`, etc.)
- Code generation or schema-aware message dispatch

This repository **does** provide:
- A built-in `codec.Protobuf` implementation
- `codec.MarshalMessage(...)` and `codec.UnmarshalMessage(...)`
- Generic runtime helpers such as `Client.PublishWithCodec(...)` and `Client.CallWithCodec(...)`

This design keeps Weave lightweight, transport-agnostic, and easy to extend. If you need a more specialized protobuf framework, consider combining Weave with a dedicated protobuf service library.

## Supported Pattern

- Encode protobuf messages with `codec.Protobuf` or `proto.Marshal`.
- Send the bytes in `Message.Body`, or let `codec.MarshalMessage(...)` build the message for you.
- Use `Client.PublishWithCodec(...)` / `CallWithCodec(...)` and the matching `Server` helpers when you want runtime-level convenience.
- Unmarshal `msg.Body` or `response.Body` with `codec.UnmarshalMessage(...)` or `proto.Unmarshal`.

The protobuf examples in [examples/protobuf](../examples/protobuf) follow this pattern.

## Why This Design?

**Keeping Weave simple and transport-agnostic:**

- Weave is meant to be a thin abstraction over message brokers, not a code-generation framework.
- Not every team uses Protocol Buffers; some prefer JSON, MessagePack, or other formats.
- Typed runtime APIs (like `ProtobufService`) require code generation and lock users into a specific schema approach.
- Manual serialization is explicit and doesn't hide implicit conversions or validation.

**Benefits of this approach:**

- You control serialization and know exactly what's on the wire.
- Weave remains compatible with any Go struct-like type, not just protobuf.
- You can use different message formats (protobuf, JSON, custom) in the same application.
- Less maintenance burden on the Weave project and fewer breaking changes to adopt.

## Quick Start

### 1. Define Your Protocol Buffers

```protobuf
syntax = "proto3";

package myservice;

message UserRequest {
  string user_id = 1;
  string action = 2;
}

message UserResponse {
  string user_id = 1;
  string name = 2;
  string email = 3;
  string status = 4;
}
```

### 2. Generate Go Code

Generate Go code from your `.proto` files with `protoc` or your preferred protobuf toolchain.

Example:

```bash
protoc --go_out=. --go_opt=paths=source_relative proto/service.proto
```

See [examples/protobuf/proto](../examples/protobuf/proto) for working protobuf definitions used by the examples.

### 3. Create a Server with Protobuf Handlers

```go
package main

import (
    "context"
    "log"
    
    mqservice "github.com/prabhatdotdev/weave"
    pb "github.com/prabhatdotdev/weave/examples/protobuf/proto"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
    "google.golang.org/protobuf/proto"
)

func main() {
    config := mqservice.DefaultConfig()
    
    server, err := mqservice.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }
    defer server.Stop()
    
    server.Handle("users.get", func(ctx context.Context, msg *mqservice.Message) error {
        var req pb.UserRequest
        if err := proto.Unmarshal(msg.Body, &req); err != nil {
            return err
        }

        resp := &pb.UserResponse{
            UserId: req.UserId,
            Name:   "John Doe",
            Email:  "john@example.com",
            Status: "active",
        }

        if msg.ReplyTo == "" {
            return nil
        }

        body, err := proto.Marshal(resp)
        if err != nil {
            return err
        }

        reply := mqservice.NewMessage(body)
        reply.ContentType = "application/x-protobuf"
        reply.CorrelationID = msg.CorrelationID

        return server.Publish(ctx, msg.ReplyTo, reply)
    })
    
    log.Fatal(server.Start(context.Background()))
}
```

### 4. Make Requests from Client

```go
package main

import (
    "context"
    "log"
    "time"
    
    mqservice "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
    pb "github.com/prabhatdotdev/weave/examples/protobuf/proto"
    "google.golang.org/protobuf/proto"
)

func main() {
    config := mqservice.DefaultConfig()
    client, err := mqservice.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    if err := client.Connect(context.Background()); err != nil {
        log.Fatal(err)
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    req := &pb.UserRequest{
        UserId: "user-123",
        Action: "get",
    }
    
    body, err := proto.Marshal(req)
    if err != nil {
        log.Fatal(err)
    }

    response, err := client.Call(
        ctx,
        "users.get",
        mqservice.NewMessage(body).WithContentType("application/x-protobuf"),
        mqservice.WithTimeout(5*time.Second),
    )
    
    if err != nil {
        log.Fatal(err)
    }
    
    var resp pb.UserResponse
    if err := proto.Unmarshal(response.Body, &resp); err != nil {
        log.Fatal(err)
    }

    log.Printf("User: %s (%s)", resp.Name, resp.Email)
}
```

## Reply Pattern

Request-reply remains the same as JSON usage:

- client uses `Call()`
- server handler reads protobuf from `msg.Body`
- server publishes response bytes to `msg.ReplyTo`
- response keeps the original `CorrelationID`

This is the pattern used in [examples/protobuf/client](../examples/protobuf/client) and [examples/protobuf/user-service](../examples/protobuf/user-service).

## Multiple Handlers

With the current implementation, protobuf handlers are registered the same way as any other handler: one destination per `server.Handle(...)` call.

If you need method-based routing on a single destination, implement that dispatch inside your handler code. See [MULTIPLE_HANDLERS.md](MULTIPLE_HANDLERS.md).

## Troubleshooting

### Protoc Not Found

```bash
# macOS
brew install protobuf

# Ubuntu/Debian
apt-get install protobuf-compiler

# Or use Go install
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### Handler Does Not Understand Payload

Make sure:

1. Client and server use the same protobuf schema.
2. The handler unmarshals into the correct message type.
3. The correct destination is used in `Call()` or `Publish()`.

## Best Practices

1. **One Payload Contract Per Destination**: Keep payload format and schema consistent per destination.
2. **Version Your Proto**: Use package versioning for compatibility
3. **Use Context**: Always pass context for timeout and cancellation.
4. **Handle Errors**: Return handler errors and check client-side `Call()` failures.
5. **Set ContentType**: Use `application/x-protobuf` when it helps downstream consumers and debugging.

## Additional Resources

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [Example Code](../examples/protobuf/)
- [Main README](README.md)
