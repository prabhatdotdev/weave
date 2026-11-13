# Transports

Applications select a transport by calling its constructor directly. There is
no global registry or string-based factory.

## AMQP

```go
config := core.DefaultConfig()
config.AMQP = core.DefaultAMQPConfig()
config.AMQP.Host = "rabbitmq"
config.AMQP.QueueDurable = true

broker, err := amqp.NewBroker(config)
```

AMQP supports exchange routing, mandatory delivery, persistence, priority,
expiration, queue binding, prefetch, exclusive consumers, request/reply, and
connection recovery.

Publish waits for a RabbitMQ publisher confirmation. With `WithMandatory()`,
an unroutable message returns `ErrPublishFailed` wrapping `ErrUnroutable`;
a negative confirmation wraps `ErrPublishRejected`. Without the mandatory
option, RabbitMQ can acknowledge a message that has no matching queue.
For durable delivery, provision durable queues and use `WithPersistent()`.
Publisher confirmation means broker acceptance, not consumer processing.

With manual acknowledgments, handler cancellation and deadline errors requeue
unfinished work, even under `HandlerErrorNoRetry`. An error returned after the
subscription context is canceled also requeues the message. A nil handler result
acknowledges completed work. `WithAutoAck()` acknowledges delivery before handler
completion, so interrupted work cannot be requeued by the handler.

`core.DeadLetterHandler` also requeues a failed dead-letter handoff regardless of
the ordinary handler error policy. It acknowledges the source only after the
dead-letter publish is confirmed. See the [helper and its delivery limits](API.md#core).

AMQP confirmations are currently serialized per broker connection. A timed-out
publish retains its slot until the late receipt arrives or the channel closes,
so another message cannot accidentally receive its acknowledgment.

## TLS

Set a non-nil `TLS` configuration on the selected transport:

```go
config.AMQP = core.DefaultAMQPConfig()
config.AMQP.TLS = &tls.Config{} // system trust roots and hostname verification
config.AMQP.Port = 5671

config.Kafka = core.DefaultKafkaConfig()
config.Kafka.TLS = &tls.Config{}
config.Kafka.Brokers = []string{"kafka.example:9093"}
```

Use `RootCAs` for a private CA and `Certificates` for client certificates.
TLS defaults to a minimum of TLS 1.2 and verifies certificates and hostnames;
failed verification never falls back to plaintext. Kafka TLS composes with
SASL. Constructors copy configuration, including broker addresses, SASL,
and TLS settings, before applying defaults.

The verified TLS fixtures cover both transports, untrusted certificates,
hostname mismatches, and Kafka SCRAM over TLS. See
[`testdata/tls/README.md`](../testdata/tls/README.md) for the commands.

## Kafka

```go
config := core.DefaultConfig()
config.Kafka = core.DefaultKafkaConfig()
config.Kafka.Brokers = []string{"kafka:9092"}
config.Kafka.ClientID = "orders"
config.Kafka.ConsumerGroup = "orders"
config.Kafka.ReplyTopic = "orders.replies"

broker, err := kafka.NewBroker(config)
```

Kafka supports keys, explicit partitions, consumer groups, offset selection,
request/reply, SASL configuration, and connection recovery.

`Message.MessageID` is preserved as the reserved `message-id` record header on
publishes, subscriptions, and RPC requests/replies, matching AMQP's native
message ID property. Set `MessageID` rather than an application header of that
name; the field takes precedence if both are supplied. External Kafka producers
can set the same header for interoperability. Missing IDs remain empty.
Identity does not suppress duplicates automatically; see the
[duplicate-handling example](../examples/deduplication).

Kafka Publish waits for the configured broker acknowledgment. `RequiredAcks=1`
(the default) waits for the leader; `RequiredAcks=-1` waits for all in-sync
replicas and should be combined with an appropriate broker
`min.insync.replicas` setting for replicated durability. `RequiredAcks=0` is
now rejected with `ErrInvalidConfig`, because it cannot confirm acceptance.

`WithConsumerGroup(group)` overrides the broker's default group for a
subscription and also works when no default group is configured.
`WithStartFromBeginning()` selects the earliest offset for partitions without
a committed group offset; it does not rewind existing group offsets.
Subscriptions with the same group and starting-offset policy share a consumer
loop. Different groups can independently consume the same topic, and their
settings are preserved across reconnects.

Asynchronous Kafka consumer failures reach `EventHook` as `subscribe_failed`
events with operation `consumer_errors`. The broker attempts to reconnect and
restore active subscriptions after these failures.

If a handler returns an error after its subscription or consumer session is
canceled, Kafka leaves that record and later offsets uncommitted, regardless
of the handler error policy. Completed handlers and ordinary errors still
follow the configured policy.

`core.DeadLetterHandler` leaves failed handoffs and later offsets unmarked,
regardless of the ordinary handler error policy. Successful dead-letter
publishing allows the source offset to advance.

SASL supports `PLAIN` (also the default for an empty mechanism),
`SCRAM-SHA-256`, and `SCRAM-SHA-512`. Unknown enabled mechanisms return
`ErrInvalidConfig` from the constructor.

The authenticated Kafka regression tests use an isolated broker with both
SCRAM mechanisms enabled:

```sh
docker compose -f transport/kafka/testdata/scram/compose.yaml up -d --wait
WEAVE_KAFKA_SCRAM_E2E=1 go test -race ./transport/kafka -run SCRAMAuthenticationWithLiveKafka
docker compose -f transport/kafka/testdata/scram/compose.yaml down
```

`WithPartition(n)` selects the exact zero-based partition. An out-of-range
partition returns `ErrPublishFailed`; without an explicit partition, Kafka
retains its normal key-based partitioning and random fallback for keyless
messages.

Kafka does not auto-create topics. Provision publish, subscription, and reply
topics before use. `ReplyTopic` is required only for `Call`; the application
owns it and must delete it when the caller deployment is retired. Use a unique
reply topic and consumer group per concurrently running caller so another
caller cannot consume its responses.

## Common lifecycle

```go
if err := broker.Connect(ctx); err != nil {
	return err
}
defer broker.Close()
```

`IsConnected` reports transport connectivity. `IsRecovering` distinguishes an active
reconnect from a closed or never-connected broker.

Both transports and `MockBroker` implement the optional `core.ConsumerReadiness`
interface. Register business subscriptions, then wait for consumer setup:

```go
if err := broker.Subscribe(ctx, "orders", handleOrder); err != nil {
    return err
}
startupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
defer cancel()
if ready, ok := broker.(core.ConsumerReadiness); ok {
    if err := ready.WaitReady(startupCtx); err != nil {
        return err
    }
}
```

`IsReady()` requires connectivity and setup of all active business subscriptions.
AMQP becomes ready after the broker accepts each consumer. Kafka becomes ready
after Sarama session setup for every current subscription group. A joined Kafka
member with no assigned partitions is ready; readiness describes group setup,
not record processing or ownership of a partition. Canceled subscriptions are
excluded, and a connected publisher with no business subscriptions is ready.
Internal RPC reply consumers are not independently included in readiness.

Readiness changes during connection loss, consumer teardown, and rebalances.
`WaitReady` observes the existing setup/recovery lifecycle and respects its own
context without canceling subscriptions or holding up shutdown. Call `Connect`
first; a wait before the first connection returns `ErrNotConnected`. Shutdown
and closure interrupt waits with `ErrShuttingDown` and `ErrClosed`. A successful
wait is a snapshot, so health checks should continue using `IsReady()`.

Kafka emits assignment and rebalance events through `EventHook`; AMQP and the
mock emit consumer assignment/revocation events. See [event fields](API.md#events).

`Shutdown(ctx)` is available through the optional `core.Shutdowner` interface:

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if drainer, ok := broker.(core.Shutdowner); ok {
    return drainer.Shutdown(shutdownCtx)
}
return broker.Close()
```

Shutdown makes `IsConnected()` false and rejects new operations with
`ErrShuttingDown`. Admitted handlers, publishes, and RPC calls drain before
clients close. Handlers can finish publishing replies using the context passed
to them; a saved context cannot admit more work after its handler returns.
RabbitMQ business consumers are canceled at the broker, and buffered manually
acknowledged deliveries without an admitted handler are requeued. Reply consumers
remain available for admitted RPC calls. Kafka marks completed records before
closing its consumers; records without an admitted handler remain uncommitted.

If the shutdown context expires or is canceled, Shutdown returns its context
error promptly, cancels active handler contexts, and starts closing remaining
clients. Client cleanup can continue in the background; Go cannot stop a handler
that ignores its context. Expiry of any concurrent shutdown call forces closure
for the broker. Repeated shutdown calls share one drain and close operation.
After closure, new operations return `ErrClosed`. `Shutdown(nil)` returns
`ErrInvalidConfig` without starting shutdown.

Call Shutdown from the application's owner, outside handlers and event hooks.
Keep subscription contexts alive during draining and use a fresh shutdown
context; canceling a subscription context first interrupts its handlers.
`Close()` remains the immediate-close path. Automatic acknowledgments cannot
restore deliveries that were already acknowledged before handling.

`Connect(ctx)` observes cancellation during connection setup, concurrent
connection waits, and retry backoff. Closing the broker interrupts setup and
reconnect waits. Once connected, cancelling the setup context does not close
the established connection; use `Close` to end its lifetime.

Set `Config.ConnectionBackoff` to an optional `core.BackoffStrategy` for
connection retries and recovery. Wrap `core.ExponentialBackoff` with
`core.JitteredBackoff` to spread retries across instances after an outage.
Backoff waits honor cancellation, closure, and graceful shutdown, even when
the chosen jitter delay is zero. See [backoff configuration](API.md#core).

## Deadlines and delivery outcomes

`WithTimeout(d)` bounds Publish and Call, including connection/reply setup,
waiting for RPC capacity or a publish slot, and broker acknowledgment. Call uses
the same deadline while waiting for its reply. An earlier context deadline takes
precedence; zero uses only that context. Negative timeouts are invalid.

`Config.MaxPendingCalls` optionally limits active RPCs across destinations on
one broker. Zero is unlimited. Queued calls wait with their existing deadline
and publish only after acquiring capacity; shutdown or closure interrupts
that wait. See the [configuration and accounting contract](API.md#core).

Deadline expiry matches both `core.IsTimeout(err)` and
`errors.Is(err, context.DeadlineExceeded)`. Explicit cancellation matches
`context.Canceled` and is not reported as a timeout.

After a publish has been submitted, timeout or connection loss leaves its
delivery uncertain: the broker may already have accepted it. A retry can
produce duplicates. Neither transport promises exactly-once delivery or
consumer processing. Use application-level idempotency for retries. Queued
messages own copies of the caller's body and headers, so returning on timeout
does not retain mutable caller data.

## Invalid inputs

Constructors reject invalid connection settings, broker addresses, offset
policies, TLS version ranges, and unsupported SASL or acknowledgment modes.
Public Publish, Call, and Subscribe reject nil contexts/messages/handlers or
options, blank destinations, negative counts and timeouts, overflowed partitions
or prefetch, invalid priorities, delivery modes and expiration strings, and
unknown handler policies before touching a broker. AMQP short strings and Kafka
topic names are also checked. Invalid inputs match `ErrInvalidConfig`;
unsupported AMQP immediate publishing returns `ErrUnsupportedOperation`.

## Events

Both transports emit structured events through `Config.EventHook`:

```go
config.EventHook = func(ctx context.Context, event core.Event) {
	log.Printf("backend=%s operation=%s duration=%s active=%d pending=%d error=%v",
		event.Backend, event.Operation, event.Duration,
		event.ActiveHandlers, event.PendingRPCs, event.Err)
}
```

Completed operations report durations, and handler/RPC start and finish events
report broker-local work counts. See the [event contract](API.md#events) and
[observability example](../examples/observability).

Connection, recovery, and shutdown hooks run outside lifecycle locks, so they
can inspect broker state. Connection and shutdown hooks may also call `Close`.
