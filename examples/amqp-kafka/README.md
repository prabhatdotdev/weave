# AMQP + Kafka Server/Client Example

This independent Go module runs the same JSON workflow through RabbitMQ/AMQP or
Apache Kafka.

The server and client are separate operating-system processes. Run them in two
terminals with matching `-backend` and `-namespace` values:

```text
terminal 1: server -> subscribes, handles events, sends RPC replies
terminal 2: client -> publishes an event, calls RPC, checks timeout, exits
```

Transport selection is limited to startup configuration and the small
backend-specific demonstration. The order handler uses the same portable Weave
API for both brokers.

## What It Demonstrates

| Behavior | AMQP | Kafka | Process |
|---|---:|---:|---|
| Connect, close, and health | ✓ | ✓ | Both |
| JSON publish/subscribe | ✓ | ✓ | Client → server |
| Headers and content type | ✓ | ✓ | Client → server |
| Correlation-preserving RPC | ✓ | ✓ | Client ↔ server |
| Bounded handler retry | ✓ | ✓ | Server |
| Timeout classification | ✓ | ✓ | Client |
| Persistence, priority, TTL, prefetch | ✓ | — | Both |
| Key, partition, offset, consumer group | — | ✓ | Both |

The server intentionally fails the first event-handler attempt. `RetryHandler`
retries once in-process and the server reports `attempts=2`.

## Architecture

```text
                         RabbitMQ or Kafka
┌──────────────┐      ┌────────────────────┐      ┌──────────────┐
│ Client role  │─────▶│ namespace.orders   │─────▶│ Server role  │
│              │◀─────│ dynamic RPC reply  │◀─────│              │
│              │─────▶│ namespace.native   │─────▶│ native check │
└──────────────┘      └────────────────────┘      └──────────────┘
```

The order destination carries both event and RPC messages. The
`example-kind` header selects the handler behavior:

- `event`: decode JSON, verify metadata, fail once, then process;
- `rpc`: decode the request and publish a JSON reply to `ReplyTo` using the
  original correlation ID.

## Prerequisites

- Go 1.24 or newer
- Docker
- Docker Compose plugin

From this directory, confirm:

```bash
go version
docker version
docker compose version
```

## Start the Brokers

From `examples/amqp-kafka`:

```bash
docker compose up -d --wait
```

The Compose project starts:

| Service | Image | Port |
|---|---|---:|
| RabbitMQ | `rabbitmq:4.3.4-management` | `5672` |
| RabbitMQ management UI | same container | `15672` |
| Kafka | `apache/kafka:4.3.1` | `9092` |

Check readiness:

```bash
docker compose ps
```

RabbitMQ management is available at <http://localhost:15672> using
`guest` / `guest`.

## Run with AMQP

Use two terminals. Keep the server running while the client runs.

### Terminal 1: AMQP server

```bash
cd examples/amqp-kafka
go run . -role server -backend amqp
```

Wait for:

```text
[SERVER] ready backend=amqp health=healthy
[SERVER] orders destination: weave.example.orders
[SERVER] native destination: weave.example.native
[SERVER] waiting for client; press Ctrl+C to stop
```

### Terminal 2: AMQP client

```bash
cd examples/amqp-kafka
go run . -role client -backend amqp
```

Expected client output:

```text
[CLIENT] connected backend=amqp health=healthy
[CLIENT] published event id=order-1 content-type=application/json correlation=event-...
[CLIENT] fetched result from server: {"order":{"id":"order-1","status":"created"}}
[CLIENT] RPC correlation=rpc-...
[CLIENT] timeout classified as weave timeout
[CLIENT] published AMQP metadata persistent=true priority=5 ttl=30s
[CLIENT] done backend=amqp; check the server terminal for consumed messages
```

The server then reports:

```text
[SERVER] request kind=event body={"id":"order-1","status":"created"} correlation=event-... attempt=1
[SERVER] request kind=event body={"id":"order-1","status":"created"} correlation=event-... attempt=2
[SERVER] processed event id=order-1 attempts=2 content-type=application/json correlation=event-...
[SERVER] request kind=rpc body={"id":"order-1"} correlation=rpc-... reply-to=...
[SERVER] request kind=native body={"native":true}
[SERVER] AMQP metadata received with prefetch=1
```

AMQP priority is attached to the message. Observable priority ordering requires
a queue declared with a maximum priority, which this minimal environment does
not configure.

Stop the server with `Ctrl+C`.

## Run with Kafka

Use two terminals with the same backend and namespace.

### Terminal 1: Kafka server

```bash
cd examples/amqp-kafka
go run . -role server -backend kafka
```

Wait for:

```text
[SERVER] ready backend=kafka health=healthy
[SERVER] orders destination: weave.example.orders
[SERVER] native destination: weave.example.native
[SERVER] waiting for client; press Ctrl+C to stop
```

### Terminal 2: Kafka client

```bash
cd examples/amqp-kafka
go run . -role client -backend kafka
```

Expected client output:

```text
[CLIENT] connected backend=kafka health=healthy
[CLIENT] published event id=order-1 content-type=application/json correlation=event-...
[CLIENT] fetched result from server: {"order":{"id":"order-1","status":"created"}}
[CLIENT] RPC correlation=rpc-...
[CLIENT] timeout classified as weave timeout
[CLIENT] published Kafka metadata key=weave.example partition=0
[CLIENT] done backend=kafka; check the server terminal for consumed messages
```

The server then reports:

```text
[SERVER] request kind=event body={"id":"order-1","status":"created"} correlation=event-... attempt=1
[SERVER] request kind=event body={"id":"order-1","status":"created"} correlation=event-... attempt=2
[SERVER] processed event id=order-1 attempts=2 content-type=application/json correlation=event-...
[SERVER] request kind=rpc body={"id":"order-1"} correlation=rpc-... reply-to=...
[SERVER] request kind=native body={"native":true}
[SERVER] Kafka metadata key=weave.example partition=0 offset=...
```

Every application line begins with `[SERVER]` or `[CLIENT]`, so interleaved
terminal or container logs remain unambiguous.

Stop the server with `Ctrl+C`.

## CLI Options

```text
-role server|client
    Required. Starts the long-running consumer or the bounded producer/caller.

-backend amqp|kafka
    Optional. Defaults to amqp.

-namespace <prefix>
    Optional. Defaults to weave.example.
```

Examples:

```bash
go run . -role server -backend kafka -namespace team1.demo
go run . -role client -backend kafka -namespace team1.demo
```

The server and client must use exactly the same namespace. The namespace forms
these destinations:

```text
<namespace>.orders
<namespace>.native
<namespace>.missing
```

Only ASCII letters, digits, `.`, `_`, and `-` are accepted so the same names
are valid for Kafka topics and RabbitMQ queues.

Use a different namespace when multiple copies of the example share a broker:

```bash
# Pair 1
go run . -role server -backend amqp -namespace developer1.demo
go run . -role client -backend amqp -namespace developer1.demo

# Pair 2
go run . -role server -backend amqp -namespace developer2.demo
go run . -role client -backend amqp -namespace developer2.demo
```

## Environment Configuration

### AMQP

| Variable | Default | Meaning |
|---|---|---|
| `AMQP_HOST` | `localhost` | RabbitMQ host |
| `AMQP_PORT` | `5672` | RabbitMQ AMQP port |
| `AMQP_USERNAME` | `guest` | Login username |
| `AMQP_PASSWORD` | `guest` | Login password |
| `AMQP_VHOST` | `/` | RabbitMQ virtual host |

Apply the same broker settings to both terminals:

```bash
AMQP_HOST=rabbitmq.internal \
AMQP_USERNAME=app \
AMQP_PASSWORD=secret \
go run . -role server -backend amqp
```

```bash
AMQP_HOST=rabbitmq.internal \
AMQP_USERNAME=app \
AMQP_PASSWORD=secret \
go run . -role client -backend amqp
```

The example declares exclusive, auto-delete queues. RabbitMQ removes them when
the owning server connection closes.

### Kafka

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap brokers |

Example:

```bash
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092 \
go run . -role server -backend kafka -namespace team1.demo
```

```bash
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092 \
go run . -role client -backend kafka -namespace team1.demo
```

Server consumer-group names are stable for a role and namespace. Client
identities are unique per run so request-reply topics do not collide.

## Process Lifecycle

The server:

1. validates backend and namespace;
2. connects a portable order server;
3. connects a second broker instance for the backend-specific subscription;
4. prints `ready`;
5. waits for messages until `Ctrl+C` or `SIGTERM`;
6. closes both broker connections.

The client:

1. validates backend and namespace;
2. connects;
3. checks health;
4. publishes an event;
5. performs request-reply;
6. performs an expected 250-millisecond timeout;
7. publishes the backend-specific message;
8. closes and exits.

The client has a 30-second overall deadline. The server intentionally has no
automatic deadline because it is a long-running service process.

## Stop and Clean Up

After stopping the server with `Ctrl+C`, remove the local brokers:

```bash
docker compose down -v
```

This removes only the containers, network, and volumes in the
`weave-amqp-kafka-example` Compose project.

## Validate Without Brokers

Unit tests use Weave's mock broker:

```bash
go test ./...
go vet ./...
go build ./...
```

These checks validate configuration, namespace construction, JSON event retry,
and correlation-preserving replies. They do not replace the live two-terminal
broker runs.

## Troubleshooting

### `role must be server or client`

`-role` is required:

```bash
go run . -role server -backend amqp
```

### Client RPC times out

Confirm:

1. the server terminal is still running;
2. the server printed `ready`;
3. both commands use the same `-backend`;
4. both commands use the same `-namespace`;
5. both commands point to the same broker environment.

### Connection refused

Check the broker containers:

```bash
docker compose ps
docker compose logs rabbitmq
docker compose logs kafka
```

### Port already allocated

Another local service is using `5672`, `15672`, or `9092`. Stop that service or
change the Compose host-port mapping before starting this environment.

### Kafka client succeeds but server prints no native line

Kafka consumption is asynchronous. Keep the server running briefly after the
client exits. If it still does not appear, confirm the namespace and inspect:

```bash
docker compose logs kafka
```

### Invalid namespace

Use only letters, numbers, `.`, `_`, and `-`:

```bash
go run . -role server -backend kafka -namespace valid.team-1
```

### Interrupted environment

Reset only this module's containers:

```bash
docker compose down -v
docker compose up -d --wait
```

## Scope

This is a teaching example, not a production deployment template. TLS, SASL,
reconnect fault injection, custom exchanges, dead-letter topology, and
production observability adapters remain in the main documentation.
