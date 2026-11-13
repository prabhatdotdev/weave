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

`IsConnected` reports readiness. `IsRecovering` distinguishes an active
reconnect from a closed or never-connected broker.

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

## Deadlines and delivery outcomes

`WithTimeout(d)` bounds Publish and Call, including connection/reply setup,
waiting for a publish slot, and broker acknowledgment. Call uses the same
deadline while waiting for its reply. An earlier context deadline takes
precedence; zero uses only that context. Negative timeouts are invalid.

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
	log.Printf("backend=%s operation=%s error=%v",
		event.Backend, event.Operation, event.Err)
}
```

Derive metrics or traces in that callback if needed.

Connection, recovery, and shutdown hooks run outside lifecycle locks, so they
can inspect broker state. Connection and shutdown hooks may also call `Close`.
