[← All examples](../README.md) · [Performance guide](../../docs/PERFORMANCE.md)

# AMQP RPC load test

Measure completed echo RPCs through a real RabbitMQ broker. The responder and
caller run in separate processes, and every successful response must match the
request bytes.

## Start a responder

Use Go 1.25+ and Docker Compose. From the repository root:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait rabbitmq
go run ./examples/load-test -mode server -workers 32
```

Wait for:

```text
ready queue=weave.load-test.rpc workers=32
```

The server waits for consumer readiness before printing that line. It owns an
exclusive, auto-delete request queue, so run only one server for that queue.

## Run a caller

In a second terminal, from the repository root:

```sh
go run ./examples/load-test -mode client -concurrency 8 -warmup 1s -duration 10s
```

The client first verifies one echo, runs a warm-up stage, and then reports the
measured stage. Output has this shape; values depend on your machine and broker:

```text
concurrency=8 attempts=... successes=... failures=0 rps=... p95=... p99=... elapsed=...
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-host`, `-port` | `localhost`, `5672` | RabbitMQ address, using the fixture's `guest` credentials |
| `-queue` | `weave.load-test.rpc` | Same queue on caller and responder |
| `-workers` | `256` | Maximum active responder handlers |
| `-concurrency` | `1` | Caller goroutines, with one RPC in flight per goroutine |
| `-payload-bytes` | `1024` | Echo payload size |
| `-warmup` | `5s` | Unmeasured stage; zero disables it |
| `-duration` | `30s` | Time window for starting measured calls |
| `-timeout` | `5s` | Connection/readiness, individual RPC, and server shutdown bound |

## Interpret the results

- `rps` counts successful RPCs over the measured elapsed time, including the
  final in-flight calls finishing. It does not count accepted publishes alone.
- `p95` and `p99` use nearest-rank percentiles of successful RPC latencies.
  Failed calls are counted separately and excluded from these percentiles.
- A warm-up failure stops the run. Measured failures are printed and produce a
  nonzero exit status; canceled runs also return an error.
- The single client connection uses confirmed AMQP publishing, so concurrency
  does not remove that connection's publish serialization.
- Latency samples are retained in memory for exact percentiles. Keep runs
  bounded; use a streaming histogram in a long-lived load generator.

This is an RPC exercise, not the library's publish-throughput benchmark. For
confirmed publishing benchmarks, sustained traffic, repeated failures, and
replicated-cluster tests, use the [performance guide](../../docs/PERFORMANCE.md).

## Stop and validate

Press Ctrl+C in the server terminal. It stops accepting work and drains admitted
handlers with a fresh deadline before closing. Its subscription context stays
alive during the drain. Ctrl+C in the caller cancels active requests and stops
new attempts, including when the broker returns cancellation immediately.

```sh
go test -race ./examples/load-test
docker compose -f examples/amqp-kafka/compose.yaml down --volumes
```

The unit tests need no broker and cover cancellation, response integrity, and
percentile calculation.
