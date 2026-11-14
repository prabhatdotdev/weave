[← All examples](../README.md) · [Library guide](../../README.md)

# One interface, two transports

Run the same two-destination pub/sub and request/reply flow against RabbitMQ or
Kafka. The example also demonstrates bounded handler retries, message identity,
Kafka keys and explicit partitions, consumer readiness, and graceful shutdown.

## Setup

Use Go 1.25+ and Docker Compose. This directory is a separate Go module whose
`replace` directive points to the library in this checkout. From the repository
root, enter it and start the brokers:

```sh
cd examples/amqp-kafka
docker compose up -d --wait
```

RabbitMQ listens on `localhost:5672` with `guest` / `guest`; Kafka listens on
`localhost:9092`. The fixture ports bind to loopback.

Before using Kafka, provision the topics:

```sh
docker compose exec kafka sh -c '
  for topic in weave.example.users weave.example.orders weave.example.rpc weave.example.client.replies; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
  done'
```

Weave does not auto-create topics. `weave.example.orders` needs at least two
partitions because the example publishes explicitly to partition 1.

## Run

Run these commands from this directory, one at a time:

```sh
go run . -backend amqp
go run . -backend kafka
```

AMQP output:

```text
weave.example.users: {"destination":"weave.example.users","status":"ok"}
weave.example.orders: {"destination":"weave.example.orders","status":"ok"}
weave.example.rpc: pong: request
weave.example.rpc: pong: request
```

Kafka output includes `partition=N` before each JSON body. The users key
selects its partition; orders always prints `partition=1`.

The program checks setup, waits for actual deliveries and RPC responses, then
drains both broker instances with a fresh deadline. Any failure returns a
nonzero exit status. `log.Fatal` runs only after `run` has returned and its
cleanup has executed.

## Patterns to reuse

| Pattern | Where to look |
| --- | --- |
| Shared interface | `newBroker` selects the transport; `run` uses `core.MessageBroker` |
| Readiness | `WaitReady` follows subscription registration for the event consumer and RPC responder |
| Bounded retry | `eventHandler` wraps processing with three total attempts and jittered backoff |
| Identity | Each event has a `MessageID`; the application reuses it for the same logical delivery |
| Kafka routing | `WithKey` selects a stable key; `WithPartition(1)` selects the orders partition |
| RPC | `rpcHandler` publishes to `ReplyTo` and preserves `CorrelationID`; failed response publishing returns an error |
| Backpressure | `MaxPendingCalls=16` bounds registered calls; workers and operation deadlines are explicit |
| Shutdown | A fresh context drains the responder before closing the caller |

The orders handler fails once to demonstrate an in-process retry on either
transport. Transport-level retry remains enabled if the wrapper exhausts its
budget; each redelivery gets a new budget. Permanent errors need a terminal
policy such as [retry → dead-letter handling](../../docs/API.md#core).

AMQP uses durable queues, persistent messages, mandatory routing, and publisher
confirmations. Kafka uses all-in-sync-replica acknowledgments. The development
fixture has one replica, so this is not a replicated durability demonstration.
Neither path provides exactly-once effects.

## Reruns and reply destinations

Kafka uses a fresh consumer group per broker instance and starts from the
earliest available offset. Handlers check the per-run `example-run` header so
retained records from an earlier demonstration do not fill the result channels
or trigger obsolete RPC responses. This filter is specific to this demo;
production consumers should process their backlog normally.

Run one copy at a time with the default shared destinations. For a different
preprovisioned Kafka reply topic:

```sh
go run . -backend kafka -reply-topic my.app.client-1.replies
```

The flag does not create that topic or isolate the other demo destinations.
Real concurrently running callers need unique reply topics and consumer groups.
The application owns reply-topic provisioning and cleanup. A publish/call
timeout after submission can leave delivery uncertain, so retry only with an
appropriate idempotency policy.

The two calls reuse one request without modifying caller-owned metadata.
Correlation IDs are generated internally when absent; caller-supplied IDs must
be unique among pending calls on the broker, or `ErrDuplicateCorrelationID` is
returned before publishing.

## Check and clean up

```sh
go test -race ./...
go vet ./...
WEAVE_EXAMPLES_E2E=1 go test -race ./... -count=1
docker compose down --volumes
```

The first two commands need no broker. After provisioning both brokers/topics,
the opt-in live regression runs each transport twice and verifies deliveries,
partition selection, both RPC replies, and successful draining.
