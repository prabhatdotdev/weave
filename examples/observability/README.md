# Operational observability

This example connects to AMQP or Kafka, handles an RPC request, and logs
`EventHook` signals as JSON using the standard library's `log/slog` package.
The caller and responder use separate broker instances, labeled `client` and
`responder` in the logs, with independent work counts.
No metrics SDK or logging dependency is required.

From the repository root, start the local brokers:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait
```

AMQP declares its durable request queue; run:

```sh
go run ./examples/observability -backend amqp
```

For Kafka, provision the request and reply topics first:

```sh
docker compose -f examples/amqp-kafka/compose.yaml exec kafka sh -c 'for topic in weave.example.observability weave.example.observability.replies; do /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --partitions 1 --topic "$topic"; done'
go run ./examples/observability -backend kafka
```

Run one instance at a time with these fixed destination names. Give concurrent
applications their own reply topics. Kafka uses a new consumer group per run.
It reads retained requests from the beginning so startup cannot skip the new
request; rerunning it can also log processing of earlier example requests.

Each transport event logs `broker`, `duration_ms`, `active_handlers`, and `pending_rpcs`.
Successful publishes include broker acknowledgment time. `handler_started` and
`call_started` increase the corresponding count; their finish events release it
and include a duration and any error. A successful RPC ends with
`call_finished`, no error, and `pending_rpcs=0`. Handler completion can race the
reply, so the final `handler_finished` event may follow the call's finish event.

Adapt the hook to your metrics or tracing library: observe durations for
successful operations (`event.Err == nil`) on completion events, and sample
the count fields as gauges. Start events have no duration. Counts are snapshots
of one broker instance, and concurrent callbacks may arrive out of order;
do not treat arrival order as a strict sequence of gauge updates. Handlers,
publishes, and RPCs can overlap. Counts exclude messages waiting at the broker
and buffered deliveries that have not started a handler.

Use bounded labels such as backend and operation for metric series. The
correlation ID belongs in logs or traces, not in a metric label. Keep the hook
fast and safe for concurrent calls; exporters that may block should buffer
their work. The library calls hooks synchronously outside broker locks.

After provisioning both brokers and the Kafka topics above, run the live
regression to check that requests reach the observed handler and both broker
instances finish with zero work counts:

```sh
WEAVE_OBSERVABILITY_E2E=1 go test -race ./examples/observability -run TestRunWithLiveBrokers
```
