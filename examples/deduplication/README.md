[← All examples](../README.md) · [Library guide](../../README.md)

# Duplicate handling

Skip completed message IDs while keeping failed work eligible for retry. This
is a broker-free demonstration of a handler wrapper, not a persistent inbox.

## Run

With Go 1.25+, from the repository root:

```sh
go run ./examples/deduplication
```

Expected output, despite delivering the same message twice:

```text
processed order-created-42: order created
```

The example delivers two copies of the same logical message to a `core.Handler`
and prints one processing result.

## Adapt it

Copy the `deduplicate` wrapper into your application and pass the resulting
handler to either transport:

```go
handler := deduplicate(processOrder)
err := broker.Subscribe(ctx, "orders", handler, core.WithHandlerErrorRetry())
```

Assign `msg.MessageID` before publishing and reuse that ID for retries of the
same logical event. Assign a different ID to each distinct event, even when its
body is identical. Both AMQP and Kafka preserve the ID. `CorrelationID` belongs
to request/reply matching and is separate from message identity.

The wrapper records an ID only after processing succeeds. An error leaves it
eligible for another attempt, while a completed duplicate returns nil so the
transport can acknowledge it. Messages without an ID are processed every time.
A mutex serializes processing so concurrent copies cannot both pass the check.
Cancellation prevents new processing, including for messages without an ID.
Use a separate wrapper for each logical consumer. This simple lock also
serializes distinct message IDs; choose a durable inbox with appropriate
concurrency when moving beyond a small demonstration.

## Production boundary

The map is an illustration: it grows without eviction, disappears on restart,
and is not shared across service instances. For durable duplicate handling,
store a unique `(consumer, message_id)` inbox record and the business change in
the same database transaction. External side effects need their own stable
idempotency key. Message identity alone does not provide exactly-once effects.

## Validate

```sh
go test -race ./examples/deduplication
```

The tests cover duplicate deliveries, concurrent copies, failed attempts,
identity mutation, and cancellation with and without a message ID.
