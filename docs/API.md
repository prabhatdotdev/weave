# API

Canonical package documentation:

- <https://pkg.go.dev/github.com/prabhatdotdev/weave/core>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/codec>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/transport/amqp>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/transport/kafka>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/testkit>

## Core

`core.MessageBroker` composes connection, publish, call, and subscribe
operations. `core.Message` carries the body plus broker-neutral metadata.
AMQP, Kafka, and `testkit.MockBroker` also implement the optional
`core.Shutdowner` interface, whose `Shutdown(ctx)` drains admitted work before
closing. Existing `MessageBroker` implementations do not need a new method.

```go
msg := core.NewMessage(body).
	WithCorrelationID("request-1").
	WithHeader("source", "api")
```

Publish options:

- `WithTimeout`
- `WithMandatory`
- `WithExchange`
- `WithPartition`
- `WithKey`
- `WithPersistent`
- `WithPriority`
- `WithExpiration`

Subscribe options:

- `WithAutoAck`
- `WithExclusive`
- `WithConsumerTag`
- `WithPrefetchCount`
- `WithWorkerCount`
- `WithQueueBind`
- `WithConsumerGroup`
- `WithStartFromBeginning`
- `WithHandlerErrorRetry`
- `WithHandlerErrorNoRetry`

`core.RetryHandler` adds bounded, context-aware retries around a handler.
Cancellation and expired deadlines prevent new attempts, including when backoff
is omitted or zero. A context already done before the first attempt returns its
context error without calling the handler. Interruption after a failed attempt
preserves the last handler error.

`core.DeadLetterHandler` composes retries with a confirmed dead-letter publish:

```go
handler, err := core.DeadLetterHandler(
	processOrder,
	core.RetryPolicy{MaxAttempts: 3, Backoff: core.FixedBackoff(100 * time.Millisecond)},
	broker,
	"orders.dead",
	core.DeadLetterOptions{Backend: "amqp", Destination: "orders", Handler: "processOrder"},
	core.WithTimeout(5 * time.Second),
)
if err != nil {
	return err
}
return broker.Subscribe(ctx, "orders", handler) // manual acknowledgments are the default
```

Successful processing skips dead-letter publishing. Exhausted retries or an
error rejected by `RetryPolicy.Retryable` publish a `NewDeadLetterEnvelope` with
the last error, actual attempt count, and a snapshot of the original delivery.
`DeadLetterOptions.Destination` identifies the source; the separate destination
argument identifies the dead-letter target. `Details` must be JSON encodable and
are snapshotted when the helper is constructed.

The source is acknowledged or marked only after a successful handoff. Publish
or encoding failures return `ErrDeadLetterFailed`; AMQP requeues the delivery
and Kafka leaves it and later offsets unmarked, even under
`WithHandlerErrorNoRetry()`. Cancellation preserves unfinished work without
dead-lettering it. A panic also preserves the source. Each broker redelivery
starts a new retry budget.

Use manual acknowledgments; `WithAutoAck()` acknowledges too early for this
guarantee. The helper always enables mandatory AMQP routing so a missing queue
fails the handoff, and defaults to persistent publishes. Provision durable
queues or Kafka topics and choose the appropriate broker durability settings.
Publish options can select an AMQP exchange or Kafka key/partition.

Dead-letter publishing and source acknowledgment are separate operations. A
crash or uncertain publish result can produce duplicate envelopes; consumers
should deduplicate using the original message ID or source topic, partition,
and offset. This helper does not provide an atomic or exactly-once handoff.

## Codecs

`codec.JSON` and `codec.Protobuf` implement the same `Codec` interface.

```go
msg, err := codec.MarshalMessage(codec.JSON, value)
err = codec.UnmarshalMessage(codec.JSON, msg, &decoded)
```

`codec.UnmarshalAndValidate` performs typed decoding followed by an
application-owned validator.

## Events

Set `Config.EventHook` to receive structured connection, publish, subscribe,
timeout, and recovery events. Metrics and traces belong in the callback or in
the application's existing observability stack.

## Errors

Transport errors use concrete types such as `ErrNotConnected`, `ErrTimeout`,
and `ErrConnectionLost`. The `IsNotConnected`, `IsTimeout`, and
`IsConnectionLost` helpers use `errors.As`. `ErrDuplicateCorrelationID` is a
sentinel matched with `errors.Is` when another RPC call already has the same
caller-supplied correlation ID in flight.

`ErrInvalidConfig` identifies invalid public inputs. Publish failures wrap their
cause: AMQP mandatory returns match `ErrUnroutable`, and negative confirmations
match `ErrPublishRejected`. Deadline errors match both `IsTimeout` and
`context.DeadlineExceeded`; explicit cancellation matches `context.Canceled`.
See [transport guarantees and deadlines](TRANSPORTS.md#deadlines-and-delivery-outcomes).

`ErrShuttingDown` rejects new work during graceful shutdown. `ErrClosed`
identifies operations on a closed broker.

`IsDeadLetterFailed` matches wrapped `ErrDeadLetterFailed` errors. The error
unwraps both the processing failure and handoff failure for `errors.Is` and
`errors.As`.
