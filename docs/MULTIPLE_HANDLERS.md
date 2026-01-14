# Multiple Handlers Guide

## Overview

The library supports **multiple handlers** in two patterns:
1. **Multiple handlers on ONE queue** (recommended) - Method-based routing
2. **Different handlers on different queues** - Service-based routing

## Pattern 1: Multiple Handlers on Single Queue ⭐ Recommended

### Architecture

```
                    ┌─────────────────────────────────────┐
                    │      user-service (Queue)           │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────┴──────────────────────┐
                    │      Handler Registry                │
                    └──────────────┬──────────────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
    ┌────▼─────┐            ┌─────▼──────┐          ┌──────▼─────┐
    │user.get  │            │user.create │          │user.update │
    │5 workers │            │3 workers   │          │4 workers   │
    └──────────┘            └────────────┘          └────────────┘
```

### Implementation

```go
service, _ := mqservice.NewProtobufService(config, 10)

// Register multiple handlers
mqservice.RegisterHandler(service, "user.get", getUserHandler, ..., 5)
mqservice.RegisterHandler(service, "user.create", createUserHandler, ..., 3)
mqservice.RegisterHandler(service, "user.update", updateUserHandler, ..., 4)
mqservice.RegisterHandler(service, "user.delete", deleteUserHandler, ..., 2)
mqservice.RegisterHandler(service, "user.list", listUsersHandler, ..., 5)

// ONE queue handles ALL methods
service.ListenAndServeProtobuf("user-service")
```

### How It Works

1. **Client sends request** with method name:
   ```go
   CallProtobuf(client, ctx, "user-service", "user.create", req, ...)
   ```

2. **Library extracts method** from request wrapper:
   ```protobuf
   message Request {
     string method = 2;  // "user.create"
     bytes payload = 3;  // Your protobuf message
   }
   ```

3. **Registry routes to handler**:
   - Looks up "user.create" in registry
   - Submits to that handler's worker pool
   - Returns response

### Benefits

✅ **Single connection** - One AMQP connection per service  
✅ **Easy deployment** - One process, one queue  
✅ **Clear organization** - All user operations in one place  
✅ **Independent scaling** - Different worker counts per operation  
✅ **Simpler monitoring** - One queue to watch  

### Complete Example

```go
package main

import (
    "context"
    "fmt"
    mqservice "github.com/prabhatdotdev/weave"
    _ "github.com/prabhatdotdev/weave/transport/amqp"
    pb "github.com/prabhatdotdev/weave/proto"
)

func main() {
    config := mqservice.DefaultConfig()
    service, _ := mqservice.NewProtobufService(config, 10)
    
    // Handler 1: CRUD - Create
    createHandler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
        // Your create logic
        return &pb.UserResponse{UserId: "new-id", Name: "Created"}, nil
    }
    
    // Handler 2: CRUD - Read
    getHandler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
        // Your get logic
        return &pb.UserResponse{UserId: req.UserId, Name: "Found"}, nil
    }
    
    // Handler 3: CRUD - Update
    updateHandler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
        // Your update logic
        return &pb.UserResponse{UserId: req.UserId, Name: "Updated"}, nil
    }
    
    // Handler 4: CRUD - Delete
    deleteHandler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
        // Your delete logic
        return &pb.UserResponse{UserId: req.UserId, Status: "deleted"}, nil
    }
    
    // Register all handlers
    mqservice.RegisterHandler(service, "user.create", createHandler, ..., 5)
    mqservice.RegisterHandler(service, "user.get", getHandler, ..., 10)
    mqservice.RegisterHandler(service, "user.update", updateHandler, ..., 5)
    mqservice.RegisterHandler(service, "user.delete", deleteHandler, ..., 3)
    
    // Start service - handles all 4 operations
    service.ListenAndServeProtobuf("user-service")
}
```

### Client Usage

```go
client, _ := mqservice.NewProtobufService(config, 10)

// Call different handlers on same queue
createResp, _ := mqservice.CallProtobuf(client, ctx, "user-service", "user.create", createReq, ...)
getResp, _ := mqservice.CallProtobuf(client, ctx, "user-service", "user.get", getReq, ...)
updateResp, _ := mqservice.CallProtobuf(client, ctx, "user-service", "user.update", updateReq, ...)
deleteResp, _ := mqservice.CallProtobuf(client, ctx, "user-service", "user.delete", deleteReq, ...)
```

## Pattern 2: Different Handlers on Different Queues

### When to Use

- **Separate microservices** with distinct responsibilities
- **Different teams** owning different services
- **Independent deployment** requirements
- **Different scaling needs** at service level

### Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│user-service  │    │order-service │    │payment-svc   │
│(Queue)       │    │(Queue)       │    │(Queue)       │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                    │
   ┌───┴────┐         ┌────┴────┐         ┌────┴────┐
   │user.get│         │order    │         │payment  │
   │5 workers         │.create  │         │.process │
   └────────┘         │8 workers│         │6 workers│
                      └─────────┘         └─────────┘
```

### Implementation

```go
// User Service
userService, _ := mqservice.NewProtobufService(config, 10)
mqservice.RegisterHandler(userService, "user.get", getUserHandler, ..., 5)
mqservice.RegisterHandler(userService, "user.create", createUserHandler, ..., 3)
go userService.ListenAndServeProtobuf("user-service")

// Order Service (separate process/microservice)
orderService, _ := mqservice.NewProtobufService(config, 10)
mqservice.RegisterHandler(orderService, "order.create", createOrderHandler, ..., 8)
mqservice.RegisterHandler(orderService, "order.cancel", cancelOrderHandler, ..., 4)
go orderService.ListenAndServeProtobuf("order-service")

// Payment Service (separate process/microservice)
paymentService, _ := mqservice.NewProtobufService(config, 10)
mqservice.RegisterHandler(paymentService, "payment.process", processPaymentHandler, ..., 6)
go paymentService.ListenAndServeProtobuf("payment-service")

// Keep main thread alive
select {}
```

## Scaling Worker Pools

### Per-Handler Worker Counts

You can configure different worker counts based on expected load:

```go
// High-read operation - 10 workers
mqservice.RegisterHandler(service, "user.get", getHandler, ..., 10)

// Medium-write operation - 5 workers
mqservice.RegisterHandler(service, "user.create", createHandler, ..., 5)

// Low-frequency operation - 2 workers
mqservice.RegisterHandler(service, "user.delete", deleteHandler, ..., 2)

// Expensive operation - 3 workers (CPU-bound)
mqservice.RegisterHandler(service, "report.generate", reportHandler, ..., 3)
```

### Guidelines

| Operation Type | Workers | Example |
|----------------|---------|---------|
| High-frequency reads | 8-15 | user.get, product.search |
| Medium writes | 5-8 | user.create, order.update |
| Low-frequency | 2-4 | user.delete, admin.action |
| CPU-intensive | 2-4 | report.generate, image.process |
| I/O-intensive | 10-20 | api.call, file.upload |

## Real-World Example: E-Commerce Service

```go
// Product Service - Single queue, multiple handlers
productService, _ := mqservice.NewProtobufService(config, 20)

// Public operations - high traffic
mqservice.RegisterHandler(productService, "product.search", searchHandler, ..., 15)
mqservice.RegisterHandler(productService, "product.get", getHandler, ..., 10)

// Admin operations - medium traffic
mqservice.RegisterHandler(productService, "product.create", createHandler, ..., 5)
mqservice.RegisterHandler(productService, "product.update", updateHandler, ..., 5)

// Rare operations - low traffic
mqservice.RegisterHandler(productService, "product.delete", deleteHandler, ..., 2)
mqservice.RegisterHandler(productService, "product.bulk_import", bulkHandler, ..., 3)

productService.ListenAndServeProtobuf("product-service")
```

## Concurrent Request Handling

Multiple handlers process requests **concurrently**:

```go
// 20 concurrent requests to different handlers
for i := 0; i < 20; i++ {
    go func(id int) {
        method := []string{"user.get", "user.create", "user.update"}[id%3]
        
        resp, _ := mqservice.CallProtobuf(
            client, ctx, "user-service", method, req, ...
        )
        // All processed in parallel across worker pools
    }(i)
}
```

**Result:**
- `user.get` handler: Processing with 5 workers
- `user.create` handler: Processing with 3 workers
- `user.update` handler: Processing with 4 workers
- **Total: 12 workers** processing concurrently

## Error Handling

Each handler can return errors independently:

```go
createHandler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
    if req.UserId == "" {
        return nil, fmt.Errorf("user_id is required")
    }
    
    if existingUser, _ := db.Get(req.UserId); existingUser != nil {
        return nil, fmt.Errorf("user already exists")
    }
    
    // Create user...
    return response, nil
}

// Client receives error
resp, err := mqservice.CallProtobuf(...)
if err != nil {
    // Error from handler: "user already exists"
    log.Printf("Error: %v", err)
}
```

## Monitoring Multiple Handlers

Track metrics per handler:

```go
type HandlerMetrics struct {
    HandlerName   string
    RequestCount  int64
    SuccessCount  int64
    ErrorCount    int64
    AvgDuration   time.Duration
    ActiveWorkers int
}

// In your handler
handler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
    start := time.Now()
    metrics.RequestCount++
    
    resp, err := processRequest(req)
    
    duration := time.Since(start)
    metrics.AvgDuration = updateAverage(metrics.AvgDuration, duration)
    
    if err != nil {
        metrics.ErrorCount++
    } else {
        metrics.SuccessCount++
    }
    
    return resp, err
}
```

## Best Practices

### 1. Naming Convention

Use consistent method naming:

```go
// Good - Clear hierarchy
"user.get"
"user.create"
"user.update"
"user.delete"
"user.list"

// Bad - Inconsistent
"getUser"
"CreateUser"
"user_update"
"delUser"
```

### 2. Handler Organization

Keep handlers focused:

```go
// Good - Single responsibility
getUserHandler := func(ctx, req) { /* just get */ }
createUserHandler := func(ctx, req) { /* just create */ }

// Bad - Mixed responsibilities
userHandler := func(ctx, req) {
    switch req.Action {
    case "get": // ...
    case "create": // ...
    case "update": // ...
    }
}
```

### 3. Worker Sizing

Start conservative, measure, adjust:

```go
// Initial deployment
RegisterHandler(service, "user.get", handler, ..., 5)

// Monitor metrics for 1 week
// If seeing queue buildup → Increase to 10
// If workers idle 80% of time → Decrease to 3
```

### 4. Error Handling

Return structured errors:

```go
handler := func(ctx context.Context, req *pb.UserRequest) (*pb.UserResponse, error) {
    if err := validateRequest(req); err != nil {
        return nil, fmt.Errorf("validation failed: %w", err)
    }
    
    if err := checkPermissions(ctx, req); err != nil {
        return nil, fmt.Errorf("permission denied: %w", err)
    }
    
    // Process...
    return response, nil
}
```

## Run the Example

```bash
# See multiple handlers in action
make run-multiple-handlers

# The example demonstrates:
# - 5 handlers on 1 queue
# - Different worker counts (2-5 workers)
# - CRUD operations
# - Concurrent requests to different handlers
# - Performance measurement
```

## Summary

✅ **Multiple handlers** fully supported  
✅ **Single queue** for related operations (recommended)  
✅ **Independent worker pools** per handler  
✅ **Method-based routing** automatic  
✅ **Concurrent processing** across all handlers  
✅ **Easy to scale** individual operations  

Choose Pattern 1 (single queue) for most use cases. Use Pattern 2 (separate queues) when you need true service isolation.
