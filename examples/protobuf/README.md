# Protocol Buffers Microservices Example

This example demonstrates a complete microservices architecture using Weave with Protocol Buffers serialization.

For broker setup, repository-wide example selection, validation, cleanup, and
troubleshooting, see the [complete examples guide](../README.md).

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   API Client    │────▶│  User Service   │────▶│ Profile Service │
│  (client-only)  │     │ (server+client) │     │  (server-only)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        │                       │                       │
        ▼                       ▼                       ▼
    users.get              users.get              profiles.get
    users.list             users.list             profiles.list
    events.user            profiles.get
```

## Services

### Profile Service (Leaf Service)
- **Type:** Server-only
- **Routes:** `profiles.get`, `profiles.list`
- **Description:** Manages user profiles using protobuf. This is a "leaf" service with no external dependencies.

### User Service (Intermediate Service)
- **Type:** Server + Client
- **Routes:** `users.get`, `users.list`
- **Description:** Manages users and enriches responses with profile data by calling the Profile Service using protobuf.

### API Client (External Consumer)
- **Type:** Client-only
- **Description:** Simulates an external application making requests to the User Service using protobuf.

## Running the Example

### Prerequisites
- RabbitMQ running locally (or via Docker)
- Go 1.24+

Generated Go code is committed. The Protocol Buffers compiler (`protoc`) and Go
plugin are needed only when changing and regenerating `proto/services.proto`.

### Optional: Install Protocol Buffers Tools

```bash
# Install protoc (macOS)
brew install protobuf

# Install Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### Generate Protobuf Code

```bash
# From the examples/protobuf directory
cd examples/protobuf
protoc --go_out=. --go_opt=paths=source_relative proto/services.proto
```

### Start RabbitMQ

From the repository root:

```bash
docker compose up -d --wait rabbitmq
```

### Run the Services

**Terminal 1 - Start Profile Service (start this first):**
```bash
go run ./examples/protobuf/profile-service
```

**Terminal 2 - Start User Service:**
```bash
go run ./examples/protobuf/user-service
```

**Terminal 3 - Run API Client:**
```bash
go run ./examples/protobuf/client
```

Run all three commands from the repository root. Keep the first two terminals
running until the client finishes, then stop both services with `Ctrl+C`.

Stop RabbitMQ:

```bash
docker compose stop rabbitmq
```

## Message Flow

### Get User by ID
1. Client calls `users.get` with `GetUserRequest{Id: "1"}`
2. User Service receives request, looks up user
3. User Service calls `profiles.get` with `GetProfileRequest{UserId: "1"}`
4. Profile Service returns `GetProfileResponse` with profile data
5. User Service combines user + profile into `EnrichedUser`
6. Client receives `GetUserResponse` with enriched user

### List All Users
1. Client calls `users.list` with `ListUsersRequest{}`
2. User Service retrieves all users
3. User Service calls `profiles.get` for each user (concurrent)
4. Profile Service returns each profile
5. User Service combines all data
6. Client receives `ListUsersResponse` with list of enriched users

## Key Concepts Demonstrated

### Protobuf Message Definition
```protobuf
message GetUserRequest {
  string id = 1;
}

message GetUserResponse {
  EnrichedUser user = 1;
  string error = 2;
}
```

### Server Pattern with Protobuf
```go
// Create server with handlers
server, _ := weave.NewServer(config)
server.Handle("profiles.get", handleGetProfile)
server.Handle("profiles.list", handleListProfiles)
server.Start(ctx)

// Handler using protobuf
func handleGetProfile(ctx context.Context, msg *weave.Message) error {
    var req pb.GetProfileRequest
    if err := weave.UnmarshalAndValidate(weave.Protobuf, msg, &req, func(req *pb.GetProfileRequest) error {
        if req.UserId == "" {
            return errors.New("user_id is required")
        }
        return nil
    }); err != nil {
        return sendResponse(ctx, msg, &pb.GetProfileResponse{Error: "invalid request"})
    }
    // Process and respond...
}
```

### Client Pattern with Protobuf
```go
// Create client for RPC calls
client, _ := weave.NewClient(config)
client.Connect(ctx)

// Make typed request
reqData, _ := proto.Marshal(&pb.GetUserRequest{Id: "1"})
response, _ := client.Call(ctx, "users.get", 
    weave.NewMessage(reqData), 
    weave.WithTimeout(5*time.Second))

var resp pb.GetUserResponse
proto.Unmarshal(response.Body, &resp)
```

### Combined Server + Client
```go
// User Service acts as both
server, _ := weave.NewServer(config)    // Receive requests
client, _ := weave.NewClient(config)    // Call Profile Service
```

### Fire-and-Forget Publishing
```go
// Publish events without waiting for response
eventData, _ := proto.Marshal(&pb.UserEvent{
    EventType: "user.viewed",
    UserId:    "1",
    Timestamp: time.Now().Unix(),
})
client.Publish(ctx, "events.user", weave.NewMessage(eventData))
```

## Data Models (Protobuf)

### User
```protobuf
message User {
  string id = 1;
  string name = 2;
  string email = 3;
}
```

### Profile
```protobuf
message Profile {
  string user_id = 1;
  string bio = 2;
  string avatar = 3;
  string location = 4;
  string website = 5;
  bool verified = 6;
}
```

### EnrichedUser (Combined)
```protobuf
message EnrichedUser {
  string id = 1;
  string name = 2;
  string email = 3;
  Profile profile = 4;
}
```

## Comparison: JSON vs Protobuf

| Aspect | JSON Example | Protobuf Example |
|--------|--------------|------------------|
| Serialization | `encoding/json` | `google.golang.org/protobuf` |
| Schema | Implicit (struct tags) | Explicit (`.proto` files) |
| Type Safety | Runtime | Compile-time |
| Message Size | Larger (text) | Smaller (binary) |
| Human Readable | Yes | No (binary) |
| Code Generation | No | Yes (`protoc`) |
| Performance | Good | Better |

## Error Handling

- **User not found:** Returns `GetUserResponse{Error: "user not found"}`
- **Profile not found:** User returned without profile data (graceful degradation)
- **Timeout:** Demonstrates handling with short timeout example
- **Invalid protobuf:** Returns error response

## Extending This Example

1. **Add gRPC support** - Use protobuf definitions with gRPC
2. **Add authentication** - Validate tokens in metadata
3. **Add circuit breaker** - Protect against cascading failures
4. **Add caching** - Cache profile data to reduce calls
5. **Add metrics** - Track request latency and error rates
6. **Add tracing** - Distributed tracing across services
7. **Schema evolution** - Demonstrate backward-compatible changes
