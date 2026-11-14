[← All examples](../README.md) · [Library guide](../../README.md)

# JSON orders

A complete RabbitMQ publish/subscribe round trip using `codec.JSON`. The
program waits for consumer readiness, validates an incoming order, and waits
for handling before draining the broker.

## Run

Use Go 1.25+ and Docker Compose. From the repository root:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait rabbitmq
go run ./examples/json
```

Expected output:

```text
order-1
```

The example uses `localhost:5672` with `guest` / `guest`. Each run declares its
own exclusive, auto-delete queue, so it can run again without consuming an old
order or conflicting with an application's `orders` queue.

## Adapt it

- Replace the `order` struct with your payload. `codec.MarshalMessage` sets the
  JSON content type; `orderHandler` decodes and checks the required ID.
- The receive channel uses a context-aware send, so cancellation does not leave
  a handler blocked while the application exits.
- `MessageID` identifies the logical event. Keep it stable when retrying that
  event; this example does not deduplicate automatically.
- `WithMandatory` detects a missing AMQP route. Broker confirmation and handler
  completion are separate waits in `run`.
- `Shutdown` has a fresh deadline; `Close` remains deferred as immediate cleanup
  if a preceding operation fails. Errors return to `main` before it exits.

The queue is intentionally temporary. For durable work, provision a durable
queue, publish persistently, and select a retry/dead-letter policy. Invalid
payloads in this small demo return an ordinary handler error under the default
no-retry policy; they are not saved to a dead-letter destination.

## Check and clean up

```sh
go test -race ./examples/json
WEAVE_EXAMPLES_E2E=1 go test -race ./examples/json -count=1
docker compose -f examples/amqp-kafka/compose.yaml down --volumes
```

The first command runs without RabbitMQ. The opt-in live regression runs the
whole program twice and checks its output.
