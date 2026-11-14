[← Library guide](../README.md)

# Examples

Small, runnable programs that demonstrate one part of Weave at a time. Start
with a codec example, compare the transports, then add operational behavior.
All commands below run from the **repository root**, unless a step explicitly
changes directories. Use **Go 1.25 or newer**.

## Choose an example

| Example | Broker | What you learn |
| --- | --- | --- |
| [Protocol Buffers](protobuf) | None | Encode and decode a typed payload without installing `protoc` |
| [Duplicate handling](deduplication) | None | Preserve message identity, skip completed duplicates, and retry failures |
| [JSON](json) | RabbitMQ | Publish a typed order, validate it, wait for delivery, and drain |
| [AMQP and Kafka](amqp-kafka) | Either | Use the same interface for pub/sub, retries, keys, partitions, and RPC |
| [Observability](observability) | Either | Export durations, work counts, and consumer lifecycle fields as JSON logs |
| [RPC load test](load-test) | RabbitMQ | Run a separate responder and measure successful RPC throughput and latency |

## Run without a broker

```sh
go run ./examples/protobuf
go run ./examples/deduplication
```

Expected output:

```text
hello
processed order-created-42: order created
```

## Start the local brokers

Use the shared fixture for all broker-backed examples. Start only the services
you need, or start both:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait
```

| Service | Address | Local credentials |
| --- | --- | --- |
| RabbitMQ AMQP | `localhost:5672` | `guest` / `guest` |
| RabbitMQ management UI | <http://localhost:15672> | `guest` / `guest` |
| Kafka | `localhost:9092` | No authentication in this fixture |

Ports bind to loopback. These are single-node development brokers, not a
production cluster. Avoid running another broker on the same ports.

### Provision Kafka topics

Weave does not auto-create topics. This command provisions every topic used by
the AMQP/Kafka and observability examples; it is safe to repeat:

```sh
docker compose -f examples/amqp-kafka/compose.yaml exec kafka sh -c '
  for topic in weave.example.users weave.example.orders weave.example.rpc \
    weave.example.client.replies weave.example.observability weave.example.observability.replies; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
  done'
```

The partition example needs at least two partitions for `weave.example.orders`.
The fixture's replication factor is one. `RequiredAcks=-1` in the examples
confirms all in-sync replicas; production durability also needs replicated
topics and an appropriate `min.insync.replicas` setting.

## Run against a broker

```sh
go run ./examples/json
go run ./examples/observability -backend amqp
go run ./examples/observability -backend kafka
```

The transport comparison is a **separate Go module** with a local `replace`
directive so it always uses this checkout. Run it in its own directory:

```sh
cd examples/amqp-kafka
go run . -backend amqp
go run . -backend kafka
```

Return to the repository root with `cd ../..`. See each example's README for
expected output and the [load-test guide](load-test) for its two-terminal setup.

Run one copy of each broker-backed demo at a time: request queues/topics and
default reply topics are shared demo destinations. Kafka examples use fresh
consumer groups and an `example-run` header to ignore retained records from
previous runs. This filtering belongs to the demo, not a general consumption
strategy. Real applications should process their backlog according to their
own consumer-group and idempotency requirements.

Both RPC demos accept `-reply-topic` for a preprovisioned Kafka reply topic.
Each concurrently running caller in a real deployment needs its own reply topic
and consumer group. Changing the flag does not create a topic or isolate the
demos' other shared destinations.

## Validate the examples

Unit tests need no broker. Validate both module roots:

```sh
go test -race ./examples/...
go vet ./examples/...
go build ./examples/...
```

```sh
cd examples/amqp-kafka
go test -race ./...
go vet ./...
go build ./...
```

After starting both brokers and provisioning the topics, run the opt-in live
regressions from the repository root:

```sh
WEAVE_EXAMPLES_E2E=1 go test -race ./examples/json ./examples/observability -count=1
```

Then, from `examples/amqp-kafka`:

```sh
WEAVE_EXAMPLES_E2E=1 go test -race ./... -count=1
```

The JSON and transport-comparison regressions run twice to check repeatability.
The observability regression checks that the RPC reaches its handler and both
brokers drain to zero active work. Broker-backed commands return an error when
setup, publishing, handling, or shutdown fails; a successful publish alone does
not prove handler completion.

CI provisions these topics and runs the broker-backed regressions with the race
detector, in addition to the regular unit tests for both module roots.

## Cleanup

From the repository root:

```sh
docker compose -f examples/amqp-kafka/compose.yaml down --volumes
```

This removes the example brokers and their fixture data. The JSON example owns
an exclusive temporary queue; the load-test server owns an exclusive queue that
is removed when its connection closes. Other demo queues and Kafka topics stay
available for reruns until you remove the fixture.

For retry/dead-letter composition, TLS, endpoint failover, and delivery-policy
choices, see the [library guide](../README.md), [API](../docs/API.md), and
[transport documentation](../docs/TRANSPORTS.md).
