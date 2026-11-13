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
msg.MessageID = "order-created-42"
```

`MessageID` is an optional, application-assigned identity preserved through
publish/subscribe and request/reply by both transports. Reuse it when retrying
the same logical event. No ID is generated automatically, and it is separate
from the RPC `CorrelationID`. See the runnable
[duplicate-handling example](../examples/deduplication).

`Publish` and `Call` leave caller-owned messages unchanged. Publish retains its
own body and header copies, so callers may reuse or change their input after
the operation returns. Handler deliveries and RPC responses belong to the
receiver. Portable metadata includes correlation ID, reply destination, string
headers, content type, message ID, and timestamp. Timestamp precision follows
the protocol: AMQP uses whole seconds and Kafka uses milliseconds. `Subject`,
partition, and offset have transport-specific routing or delivery meanings.

`testkit.MockBroker` follows the same ownership and subscription-context
contract. It snapshots published messages and configured responses, returns
independent copies from `PublishedMessages` and `PublishedTo`, and passes an
owned copy to simulated handlers. Handler values come from the subscription
context; cancellation of that context, the simulation context, or the broker
interrupts the handler. Mock RPC responses remain explicitly configured,
immediate results rather than a simulated network wait.

Set `Config.MaxPendingCalls` to cap concurrent RPCs on either transport:

```go
config := core.DefaultConfig()
config.MaxPendingCalls = 128
// Pass config to amqp.NewBroker or kafka.NewBroker.
```

Zero (the default) is unlimited; negative limits return `ErrInvalidConfig`.
The limit is per broker across destinations and covers reply setup, publishing,
and awaiting responses. Excess `Call` invocations wait before registering a
correlation ID or publishing. Their context and `WithTimeout` cover that wait
and all subsequent work; cancellation matches `context.Canceled`, and deadline
expiry matches both `IsTimeout` and `context.DeadlineExceeded`.

Slots are released on every return path, including setup/publish failure and
connection loss. Duplicate in-flight correlation IDs are rejected immediately
even when capacity is full. Shutdown rejects queued calls with
`ErrShuttingDown`; immediate closure releases them with `ErrClosed`. Calls
already holding capacity can drain. Admitted handlers may take immediately
available capacity while draining, but cannot wait for more capacity then.

Queued callers are excluded from `Event.PendingRPCs` and emit no RPC start or
finish event unless admitted. A completed call's duration includes its capacity
wait. The limit applies to `Call`, not `Publish` or handler concurrency. Bound
caller goroutines and use deadlines as well: backpressure does not limit how
many goroutines an application starts to invoke `Call`.
It does not bound broker queues or cancel remote processing after a timeout.

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

`core.ConsumerReadiness` is an optional capability implemented by both transports
and `MockBroker`:

```go
type ConsumerReadiness interface {
    IsReady() bool
    WaitReady(context.Context) error
}
```

Readiness covers all current business subscriptions and resets during recovery
and Kafka session changes. See [consumer readiness](TRANSPORTS.md#common-lifecycle)
for startup, cancellation, and zero-partition member semantics.

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

`Event.Duration` is a `time.Duration`. Both transports report it on completed
connections, successful publishes and subscription registrations, and handler
and RPC completions. A publish duration ends at broker acknowledgment; a call
duration includes reply setup, publishing, and waiting for the response.
Handler durations include the full callback, including in-process retries and
dead-letter publishing if those wrappers are used.

| Event | Duration | Meaning |
| --- | --- | --- |
| `EventConnect` with operation `connect` | connection attempt | Inspect `Err` for the outcome; an already-connected no-op emits no event. |
| `EventPublishSucceeded` | confirmed publish | Successful broker acknowledgment, not consumer processing. |
| `EventSubscribeSucceeded` | registration | Successful setup, not a guarantee that a Kafka partition is already assigned. |
| `EventConsumerAssigned` | none | An AMQP consumer was accepted, a Kafka subscription joined its group, or a mock subscription was registered. |
| `EventConsumerRevoked` | none | That consumer assignment ended, was replaced, or was invalidated by loss or shutdown. |
| `EventRebalanceStarted` / `EventRebalanceCompleted` | none | Kafka only: initial join/session teardown and completion of session setup. |
| `EventHandlerStarted` / `EventHandlerFinished` | finish only | One callback invocation; a finish with `Err == nil` records successful handling. |
| `EventCallStarted` / `EventCallFinished` | finish only | One registered RPC; a finish with `Err == nil` records successful request/reply. |

Transport events also carry `ActiveHandlers` and `PendingRPCs` snapshots for
that broker instance. Handler counts cover executing user callbacks, excluding
reply routing and queued messages. RPC counts start at correlation registration
and end after cleanup on every return path, including publish failure, timeout,
cancellation, and connection loss. Calls rejected before registration do not
emit start/finish pairs. Forced closure interrupts work; a handler that ignores
cancellation remains active until it actually returns.

Hooks run synchronously outside broker locks and may execute concurrently.
Return promptly, do not panic, and make collectors safe for concurrency. Events
from different goroutines may arrive out of order, so their count fields are
snapshots rather than an ordered stream of gauge updates. Use bounded metric
labels; keep correlation IDs in logs or traces. The
[observability example](../examples/observability) logs the metrics as JSON.

Consumer assignment/revocation events identify their destination and carry a
`consumer_generation` (`uint64`) scoped to the subscription registration. AMQP
also supplies `consumer_tag`. Kafka supplies `consumer_group`, `member_id`,
`generation_id` (`int32`), and `partitions` (`[]int32`), which can be empty.
Kafka rebalance events target `"*"` and include the group; completed events and
teardown-start events also include member/generation IDs and an `assignments`
snapshot (`map[string][]int32`). Initial-start events have no prior assignment.
These slice/map snapshots are owned by the callback. Event delivery can overlap
and arrive out of order; use `IsReady()` for a current readiness snapshot.
`MockBroker.SetEventHook` installs a collector for mock assignment/revocation;
the mock does not simulate a Kafka coordinator or emit rebalance events.

## Errors

Transport errors use concrete types such as `ErrNotConnected`, `ErrTimeout`,
and `ErrConnectionLost`. The `IsNotConnected`, `IsTimeout`, and
`IsConnectionLost` helpers use `errors.As`. `ErrDuplicateCorrelationID` is a
sentinel matched with `errors.Is` when another RPC call already has the same
caller-supplied correlation ID in flight.

`ErrInvalidConfig` identifies invalid public inputs. Publish failures wrap their
cause: AMQP mandatory returns match `ErrUnroutable`, and negative confirmations
match `ErrPublishRejected`. Publish and RPC deadline errors match both `IsTimeout` and
`context.DeadlineExceeded`; explicit cancellation matches `context.Canceled`.
See [transport guarantees and deadlines](TRANSPORTS.md#deadlines-and-delivery-outcomes).

`ErrShuttingDown` rejects new work during graceful shutdown. `ErrClosed`
identifies operations on a closed broker.

`IsDeadLetterFailed` matches wrapped `ErrDeadLetterFailed` errors. The error
unwraps both the processing failure and handoff failure for `errors.Is` and
`errors.As`.
