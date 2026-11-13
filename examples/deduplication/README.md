# Duplicate handling

Run without a broker:

```sh
go run ./examples/deduplication
```

The example delivers two copies of the same logical message to a `core.Handler`
and prints one processing result. The same handler can be passed to either
transport:

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
Use a separate wrapper for each logical consumer.

The map is an illustration: it grows without eviction, disappears on restart,
and is not shared across service instances. For durable duplicate handling,
store a unique `(consumer, message_id)` inbox record and the business change in
the same database transaction. External side effects need their own stable
idempotency key. Message identity alone does not provide exactly-once effects.
