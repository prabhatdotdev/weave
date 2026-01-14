# Protocol Buffers Support

AMQP Service Starter now supports Protocol Buffers for type-safe, efficient message serialization with multithreaded, non-blocking handlers.

## Features

- ✅ **Type-Safe Handlers** - Strongly typed request/response handlers using Go generics
- ✅ **Worker Pools** - Configurable worker threads for concurrent request processing
- ✅ **Non-Blocking** - Handlers execute in separate goroutines via worker pools
- ✅ **Protocol Buffers** - Efficient binary serialization with protobuf
- ✅ **Method Routing** - Register multiple handlers on a single queue
- ✅ **Error Handling** - Structured error responses with success/failure status

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

**Using buf (no protoc installation needed - recommended):**
```bash
make proto-gen-buf
```

**Or using Docker (zero local installation):**
```bash
make proto-gen-docker
```

**Or using go generate (auto-installs plugins):**
```bash
make proto-gen-go
```

**Or manually with protoc (requires protoc installed):**
```bash
protoc --go_out=. --go_opt=paths=source_relative proto/service.proto
```

See the proto files in the examples directory for method definitions.

### 3. Create a Service with Handlers

```go
package main

import (
    "context"
    "log"
    
    mqservice "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
    pb "github.com/prabhatdotdev/weave/proto"
)

func main() {
    config := mqservice.DefaultConfig()
    
    // Create service with 10 default worker threads
    service, err := mqservice.NewProtobufService(config, 10)
    if err != nil {
        log.Fatal(err)
    }
    defer service.Close()
    
    // Define typed handler
    handler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
        log.Printf("Processing user: %s", req.UserId)
        
        return &pb.UserResponse{
            UserId: req.UserId,
            Name:   "John Doe",
            Email:  "john@example.com",
            Status: "active",
        }, nil
    }
    
    // Register handler with 5 worker threads
    err = mqservice.RegisterHandler(
        service,
        "user.get",                                    // Method name
        handler,                                       // Handler function
        func() *pb.UserRequest { return &pb.UserRequest{} },   // Request factory
        func() *pb.UserResponse { return &pb.UserResponse{} }, // Response factory
        5,                                             // Worker threads
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // Start listening
    log.Fatal(service.ListenAndServeProtobuf("user-service"))
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
    pb "github.com/prabhatdotdev/weave/proto"
)

func main() {
    config := mqservice.DefaultConfig()
    client, err := mqservice.NewProtobufService(config, 10)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    req := &pb.UserRequest{
        UserId: "user-123",
        Action: "get",
    }
    
    resp, err := mqservice.CallProtobuf(
        client,
        ctx,
        "user-service",                              // Queue name
        "user.get",                                  // Method name
        req,                                         // Request
        func() *pb.UserResponse { return &pb.UserResponse{} }, // Response factory
    )
    
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("User: %s (%s)", resp.Name, resp.Email)
}
```

## Architecture

### Request/Response Flow

```
Client                  AMQP Queue              Service
  |                         |                      |
  |-- 1. Proto Request ---->|                      |
  |    (with method)        |                      |
  |                         |-- 2. Dispatch ------>|
  |                         |                      |- 3. Worker Pool
  |                         |                      |   (5 threads)
  |                         |                      |   - Thread 1: Processing
  |                         |                      |   - Thread 2: Available
  |                         |                      |   - Thread 3: Processing
  |                         |                      |   - Thread 4: Available
  |                         |                      |   - Thread 5: Processing
  |                         |                      |
  |                         |<-- 4. Proto Response-|
  |<-- 5. Return Response --|                      |
```

### Worker Pool Model

Each registered handler has its own worker pool:

- **Non-blocking**: Requests are submitted to a worker pool queue
- **Concurrent**: Multiple workers process requests in parallel
- **Configurable**: Set worker count per handler
- **Isolated**: Each handler has independent worker pool

### Message Structure

All messages use a generic wrapper:

```protobuf
message Request {
  string request_id = 1;     // Correlation ID
  string method = 2;         // Handler method name
  bytes payload = 3;         // Serialized request message
  map<string, string> metadata = 4;
  int64 timestamp = 5;
}

message Response {
  string request_id = 1;     // Same as request
  bool success = 2;          // Success/failure flag
  bytes payload = 3;         // Serialized response or error
  string error_message = 4;  // Error details if failed
  map<string, string> metadata = 5;
  int64 timestamp = 6;
}
```

## Advanced Usage

### Multiple Handlers on Same Queue

```go
// Register multiple handlers
mqservice.RegisterHandler(service, "user.get", getUserHandler, ...)
mqservice.RegisterHandler(service, "user.create", createUserHandler, ...)
mqservice.RegisterHandler(service, "user.update", updateUserHandler, ...)

// All handled by the same queue
service.ListenAndServeProtobuf("user-service")
```

### Concurrent Request Processing

```go
// Make 100 concurrent requests
for i := 0; i < 100; i++ {
    go func(id int) {
        req := &pb.UserRequest{UserId: fmt.Sprintf("user-%d", id)}
        resp, err := mqservice.CallProtobuf(client, ctx, "user-service", "user.get", req, ...)
        // Process response...
    }(i)
}
```

### Custom Worker Pool Sizes

```go
// Different worker counts for different handlers
mqservice.RegisterHandler(service, "user.get", handler, ..., 10)    // 10 workers
mqservice.RegisterHandler(service, "user.create", handler, ..., 5)  // 5 workers
mqservice.RegisterHandler(service, "order.process", handler, ..., 20) // 20 workers
```

### Error Handling

```go
handler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
    if req.UserId == "" {
        return nil, fmt.Errorf("user_id is required")
    }
    
    // Errors are automatically wrapped in Response.error_message
    return response, nil
}

// Client side
resp, err := mqservice.CallProtobuf(...)
if err != nil {
    // Network errors, timeouts, or handler errors
    log.Printf("Error: %v", err)
}
```

### Workflow Orchestration

```go
// Chain multiple service calls
userResp, _ := mqservice.CallProtobuf(client, ctx, "user-service", "user.get", userReq, ...)

orderReq := &pb.OrderRequest{
    UserId: userResp.UserId,
    Items:  []string{"item1", "item2"},
}
orderResp, _ := mqservice.CallProtobuf(client, ctx, "order-service", "order.create", orderReq, ...)

paymentReq := &pb.PaymentRequest{
    OrderId: orderResp.OrderId,
    Amount:  orderResp.TotalAmount,
}
paymentResp, _ := mqservice.CallProtobuf(client, ctx, "payment-service", "payment.process", paymentReq, ...)
```

## Performance Considerations

### Worker Pool Sizing

- **CPU-bound**: Workers = CPU cores
- **I/O-bound**: Workers = 2-3x CPU cores
- **Mixed workload**: Start with 10, tune based on metrics

### Benchmarking

```bash
# Run protobuf example with load testing
make run-protobuf

# The example tests:
# - 10 concurrent user requests
# - Full workflow (user -> order -> payment)
# - 20 concurrent mixed requests
```

### Monitoring

Track these metrics per handler:
- Active workers
- Queue depth
- Processing time
- Success/failure rate
- Worker utilization

## Complete Example

See [examples/protobuf/main.go](examples/protobuf/main.go) for a complete example with:
- 3 services (User, Order, Payment)
- Multithreaded handlers (5-8 workers each)
- Concurrent request testing
- Workflow orchestration
- Performance measurement

Run it:

```bash
make run-protobuf
```

## Comparison: JSON vs Protobuf

| Feature | JSON | Protobuf |
|---------|------|----------|
| Type Safety | ❌ Runtime | ✅ Compile-time |
| Size | Larger | Smaller (binary) |
| Speed | Slower | Faster |
| Schema | Implicit | Explicit (.proto) |
| Versioning | Manual | Built-in |
| Worker Pools | ❌ | ✅ |
| Method Routing | Manual | Built-in |

## Migration from JSON

1. Define .proto files for your messages
2. Generate Go code: `make proto-gen`
3. Replace `NewService` with `NewProtobufService`
4. Replace handlers with typed handlers
5. Use `RegisterHandler` instead of `ListenAndServe`
6. Use `CallProtobuf` instead of `Call`

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

### Handler Not Found

```text
Error: no handler registered for method: user.get
```

Make sure:
1. Handler is registered before `ListenAndServeProtobuf`
2. Method name matches in registration and client call
3. Handler registration succeeded (check error)

### Worker Pool Exhaustion

If requests are timing out under load:
1. Increase worker count in `RegisterHandler`
2. Increase default workers in `NewProtobufService`
3. Monitor queue depth and processing time

## Best Practices

1. **One Service Per Queue**: Don't mix JSON and Protobuf on same queue
2. **Version Your Proto**: Use package versioning for compatibility
3. **Tune Workers**: Start conservative, increase based on metrics
4. **Use Context**: Always pass context for timeout/cancellation
5. **Handle Errors**: Return errors from handlers, check on client
6. **Monitor Performance**: Track latency, throughput, worker utilization

## Additional Resources

- [Protocol Buffers Documentation](https://protobuf.dev/)
- [Example Code](examples/protobuf/)
- [Main README](README.md)
