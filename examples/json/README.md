# JSON Microservices Example

This example demonstrates a complete microservices architecture using Weave with JSON serialization.

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
- **Description:** Manages user profiles. This is a "leaf" service with no external dependencies.

### User Service (Intermediate Service)
- **Type:** Server + Client
- **Routes:** `users.get`, `users.list`
- **Description:** Manages users and enriches responses with profile data by calling the Profile Service.

### API Client (External Consumer)
- **Type:** Client-only
- **Description:** Simulates an external application making requests to the User Service.

## Running the Example

### Prerequisites
- RabbitMQ running locally (or via Docker)
- Go 1.21+

### Start RabbitMQ
```bash
# Using Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### Run the Services

**Terminal 1 - Start Profile Service (start this first):**
```bash
cd examples/json/profile-service
go run main.go
```

**Terminal 2 - Start User Service:**
```bash
cd examples/json/user-service
go run main.go
```

**Terminal 3 - Run API Client:**
```bash
cd examples/json/client
go run main.go
```

## Message Flow

### Get User by ID
1. Client calls `users.get` with `{"id": "1"}`
2. User Service receives request, looks up user
3. User Service calls `profiles.get` with `{"user_id": "1"}`
4. Profile Service returns profile data
5. User Service combines user + profile
6. Client receives enriched user response

### List All Users
1. Client calls `users.list` with `{}`
2. User Service retrieves all users
3. User Service calls `profiles.get` for each user (concurrent)
4. Profile Service returns each profile
5. User Service combines all data
6. Client receives list of enriched users

## Key Concepts Demonstrated

### Server Pattern
```go
// Create server with handlers
server, _ := weave.NewServer(config)
server.Handle("profiles.get", handleGetProfile)
server.Handle("profiles.list", handleListProfiles)
server.Start(ctx)
```

### Client Pattern
```go
// Create client for RPC calls
client, _ := weave.NewClient(config)
client.Connect(ctx)
response, _ := client.Call(ctx, "users.get", message, weave.WithTimeout(5*time.Second))
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
client.Publish(ctx, "events.user", eventMessage)
```

## Data Models

### User
```json
{
  "id": "1",
  "name": "Alice Smith",
  "email": "alice@example.com"
}
```

### Profile
```json
{
  "user_id": "1",
  "bio": "Software engineer and open source enthusiast",
  "avatar": "https://example.com/avatars/alice.jpg",
  "location": "San Francisco, CA",
  "website": "https://alice.dev",
  "verified": true
}
```

### Enriched User (Combined)
```json
{
  "id": "1",
  "name": "Alice Smith",
  "email": "alice@example.com",
  "profile": {
    "user_id": "1",
    "bio": "Software engineer and open source enthusiast",
    "location": "San Francisco, CA",
    "verified": true
  }
}
```

## Error Handling

- **User not found:** Returns `{"error": "user not found"}`
- **Profile not found:** User returned without profile data
- **Timeout:** Demonstrates handling with short timeout example
- **Service unavailable:** Graceful degradation

## Extending This Example

1. **Add authentication middleware** - Validate tokens before processing
2. **Add circuit breaker** - Protect against cascading failures
3. **Add caching** - Cache profile data to reduce calls
4. **Add metrics** - Track request latency and error rates
5. **Add tracing** - Distributed tracing across services
