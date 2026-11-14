[← All examples](../README.md) · [Event reference](../../docs/API.md#events)

# Operational observability

Handle one real RPC and export `EventHook` signals as JSON through the standard
library's `log/slog`. The caller and responder have separate broker instances
and independent work counts. No metrics SDK or logging dependency is required.

## Setup and run

Use Go 1.25+ and Docker Compose. From the repository root:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait
go run ./examples/observability -backend amqp
```

RabbitMQ declares the durable request queue when subscribing. For Kafka,
provision the request and reply topics first:

```sh
docker compose -f examples/amqp-kafka/compose.yaml exec kafka sh -c '
  for topic in weave.example.observability weave.example.observability.replies; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
  done'
go run ./examples/observability -backend kafka
```

Run one instance at a time with these shared demo destinations. To select a
different preprovisioned reply topic, pass `-reply-topic my.app.client-1.replies`.
This does not create the topic or isolate the shared request destination.
Real concurrent callers need unique reply topics and consumer groups.

Kafka uses fresh consumer groups and starts at the earliest offset. A per-run
header filters out retained requests from earlier demonstrations before sending
responses. Older records can still produce handler lifecycle events as they
are examined and acknowledged. This filter is demonstration plumbing; a
production consumer should handle its backlog according to its own policy.

## What to look for

Each JSON event includes these fields:

| Field | Meaning |
| --- | --- |
| `broker` | `client` or `responder`, identifying the broker instance |
| `event`, `backend`, `operation`, `destination` | Transport signal and operation context |
| `duration_ms` | Completed-operation duration, in milliseconds |
| `active_handlers` | Currently executing callbacks on that broker |
| `pending_rpcs` | Registered pending calls, excluding callers waiting for capacity |
| `correlation_id` | Request/reply identity for logs or traces |
| `fields` | Transport details, including consumer groups, assignments, and AMQP endpoint addresses when present |
| `error` | Operation failure, or `null` on success |

Event severity is retained, and an error is logged at `ERROR`. The default
logger filters out debug events; set `slog.HandlerOptions.Level` to include them.

A successful run logs an `RPC completed` message with `body="observed"` and a
`call_finished` event with no error and `pending_rpcs=0`. Handler completion can
follow the call's finish event because the reply may arrive before its handler
returns. The program waits for responder readiness and drains both brokers
with a fresh shutdown deadline before exiting. `MaxPendingCalls=16` is set on
each broker to demonstrate bounded admission.

## Export metrics and traces

Observe durations on completion events. Successful publishes include broker
acknowledgment time; handler durations include the full callback. Start events
have no completed-operation duration.

Treat count fields as broker-local snapshots, not queue depth or consumer lag.
Concurrent callbacks can arrive out of order; do not treat their arrival order
as a strict sequence of gauge updates. `IsReady()` is the current readiness
snapshot, while assignment/revocation and Kafka rebalance fields explain
lifecycle changes.

Use bounded labels such as backend and operation. Put correlation IDs, consumer
generations, and endpoint addresses in logs or traces rather than unbounded
metric labels. Keep the hook fast, concurrency-safe, and free of panics;
exporters that may block should buffer their work. Hooks run synchronously
outside broker locks.

## Validate and clean up

```sh
go test -race ./examples/observability
WEAVE_EXAMPLES_E2E=1 go test -race ./examples/observability -count=1
docker compose -f examples/amqp-kafka/compose.yaml down --volumes
```

The first command needs no broker. The opt-in regression needs both brokers
and the Kafka topics; it checks that the RPC reaches the observed handler and
both instances finish with zero active handlers and pending calls.
`WEAVE_OBSERVABILITY_E2E=1` remains accepted for this example's live test.
