<a id="readme-top"></a>

<div align="center">

<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/weave-logo-white.png">
    <img src="docs/assets/weave-logo-transparent.png" alt="Weave logo: two interwoven bundles of strands" width="180" height="180">
  </picture>
</p>

<h1>Weave</h1>

<p><strong>One Go interface. RabbitMQ and Kafka.</strong></p>

<p>Publish, subscribe, and request/reply with explicit delivery guarantees,<br>
context-aware operations, and control over the full consumer lifecycle.</p>

<p>
  <a href="https://github.com/prabhatdotdev/weave/actions/workflows/ci.yml?query=branch%3Amaster">
    <img src="https://github.com/prabhatdotdev/weave/actions/workflows/ci.yml/badge.svg?branch=master" alt="CI status on master">
  </a>
  <a href="go.mod">
    <img src="https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&amp;logoColor=white" alt="Go 1.25 or newer">
  </a>
  <a href="https://pkg.go.dev/github.com/prabhatdotdev/weave/core">
    <img src="https://pkg.go.dev/badge/github.com/prabhatdotdev/weave/core.svg" alt="Go package reference">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-Apache%202.0-4A5568" alt="Apache License 2.0">
  </a>
</p>

<p>
  <a href="#quick-start-with-rabbitmq"><strong>Quick start</strong></a> &nbsp;·&nbsp;
  <a href="#using-kafka">Kafka</a> &nbsp;·&nbsp;
  <a href="examples/README.md">Examples</a> &nbsp;·&nbsp;
  <a href="docs/API.md">API reference</a>
</p>

</div>

> [!NOTE]
> **Development guide · upcoming v0.2.0.** This README describes `master`.
> The published v0.1.0 API differs; read the [migration guide](#migrating-from-v010)
> before upgrading.

## At a glance

Weave connects both transports through `core.MessageBroker`, with explicit
configuration for broker-specific behavior.

<table>
  <tr>
    <td width="50%" valign="top">
      <strong><a href="#delivery-guarantees">Confirmed publishing</a></strong><br>
      Broker acknowledgments, operation deadlines, and clear delivery outcomes.
    </td>
    <td width="50%" valign="top">
      <strong><a href="#requestreply">Bounded request/reply</a></strong><br>
      Correlated responses and context-aware waiting for pending-call capacity.
    </td>
  </tr>
  <tr>
    <td valign="top">
      <strong><a href="#readiness-and-graceful-shutdown">Consumer lifecycle</a></strong><br>
      Readiness checks, assignment events, and graceful draining with a deadline.
    </td>
    <td valign="top">
      <strong><a href="#retries-and-dead-letter-handling">Failure handling</a></strong><br>
      Bounded retries, optional jitter, and confirmed dead-letter handoffs.
    </td>
  </tr>
  <tr>
    <td valign="top">
      <strong><a href="#recovery-and-endpoint-failover">Connection recovery</a></strong><br>
      Restored subscriptions, reconnect backoff, and AMQP endpoint failover.
    </td>
    <td valign="top">
      <strong><a href="#testing-and-development">Test and observe</a></strong><br>
      A mock broker for application tests and structured operational events.
    </td>
  </tr>
</table>

<details>
<summary><strong>Explore the full guide</strong></summary>

- [Requirements and installation](#requirements-and-installation)
- [Quick start with RabbitMQ](#quick-start-with-rabbitmq)
- [Using Kafka](#using-kafka)
- [Request/reply](#requestreply)
- [Delivery guarantees](#delivery-guarantees)
- [Retries and dead-letter handling](#retries-and-dead-letter-handling)
- [Configuration and transport options](#configuration-and-transport-options)
- [TLS and authentication](#tls-and-authentication)
- [Recovery and endpoint failover](#recovery-and-endpoint-failover)
- [Readiness and graceful shutdown](#readiness-and-graceful-shutdown)
- [Messages and encoding](#messages-and-encoding)
- [Observability](#observability)
- [Testing and development](#testing-and-development)
- [Migrating from v0.1.0](#migrating-from-v010)
- [Documentation and examples](#documentation-and-examples)

</details>

## Requirements and installation

- **Go 1.25 or newer.**
- RabbitMQ or Kafka for transport operations. Docker with Compose is convenient
  for the examples and live-broker tests.
- Provision Kafka topics and custom RabbitMQ exchanges before using them.

Until v0.2.0 is tagged, install the development API documented here:

```sh
mkdir weave-demo
cd weave-demo
go mod init example.com/weave-demo
go get github.com/prabhatdotdev/weave@master
```

`go get` records the resolved version in `go.mod`. Pin `@v0.2.0` once that
release is published. An unversioned `go get` currently selects v0.1.0 and
does not provide all APIs in this guide.

Packages live under `github.com/prabhatdotdev/weave`:

| Package | Purpose |
| --- | --- |
| [core](core) | Interfaces, messages, configuration, options, retries, and errors |
| [transport/amqp](transport/amqp) | RabbitMQ constructor |
| [transport/kafka](transport/kafka) | Kafka constructor |
| [codec](codec) | JSON and Protocol Buffers payloads |
| [testkit](testkit) | In-memory application-test broker |

Both transport constructors return `(core.MessageBroker, error)` and accept
`nil` for defaults. Import the transport you use; there is no root-package
facade or transport registry.

## Quick start with RabbitMQ

From a clone of this repository, start the example RabbitMQ fixture:

```sh
git clone https://github.com/prabhatdotdev/weave.git
cd weave
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait rabbitmq
```

It listens on `localhost:5672` with `guest` / `guest` credentials for local
development. Save the following as `main.go` in the `weave-demo` module created
above, then run `go run .`. It declares a durable queue, waits for its consumer,
publishes a persistent message, waits for handling, and drains before closing.
The expected output is `order-created`.

<details open>
<summary><strong>Complete example</strong> · <code>main.go</code></summary>

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Keep the subscription alive until Shutdown has drained its handlers.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.QueueDurable = true
	broker, err := amqp.NewBroker(config)
	if err != nil {
		return err
	}
	defer broker.Close()

	startupCtx, stopStartup := context.WithTimeout(ctx, 15*time.Second)
	defer stopStartup()
	if err := broker.Connect(startupCtx); err != nil {
		return err
	}

	received := make(chan string, 1)
	if err := broker.Subscribe(ctx, "weave.readme.orders", func(handlerCtx context.Context, msg *core.Message) error {
		select {
		case received <- msg.BodyString():
			return nil
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		}
	}); err != nil {
		return err
	}
	if err := broker.(core.ConsumerReadiness).WaitReady(startupCtx); err != nil {
		return err
	}

	msg := core.NewTextMessage("order-created")
	msg.MessageID = "order-created-42"
	if err := broker.Publish(ctx, "weave.readme.orders", msg,
		core.WithPersistent(), core.WithMandatory(), core.WithTimeout(5*time.Second)); err != nil {
		return err
	}
	select {
	case body := <-received:
		fmt.Println(body)
	case <-startupCtx.Done():
		return startupCtx.Err()
	}

	shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopShutdown()
	return broker.(core.Shutdowner).Shutdown(shutdownCtx)
}
```

</details>

The default AMQP exchange routes by queue name. `Subscribe` declares the queue;
`Publish` alone does not. Existing queue properties must match the configuration.
For custom routing, provision the exchange and use `WithExchange` and
`WithQueueBind`; see [AMQP configuration](docs/TRANSPORTS.md#amqp).

To stop the example fixtures, run this from the repository root:

```sh
docker compose -f examples/amqp-kafka/compose.yaml down
```

## Using Kafka

Kafka uses the same `Publish`, `Subscribe`, and `Call` methods. Destinations are
topic names; the transport does not auto-create topics. From the repository
root, start Kafka and provision the topics for these examples:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait kafka
docker compose -f examples/amqp-kafka/compose.yaml exec kafka sh -c '
  for topic in weave.readme.orders weave.readme.rpc weave.readme.client-1.replies; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
  done'
```

This fixture has one broker. Configure replication and `min.insync.replicas`
on the Kafka cluster for production durability.

To adapt the quick start, import `transport/kafka` instead of `transport/amqp`
and replace its configuration and constructor with:

```go
config := core.DefaultConfig()
config.Kafka = core.DefaultKafkaConfig()
config.Kafka.Brokers = []string{"localhost:9092"}
config.Kafka.ClientID = "orders-client-1"
config.Kafka.ConsumerGroup = "orders-client-1"
config.Kafka.ReplyTopic = "weave.readme.client-1.replies"
config.Kafka.AutoOffsetReset = "earliest"
config.Kafka.RequiredAcks = -1

broker, err := kafka.NewBroker(config)
```

Keep the rest of the quick start unchanged. AMQP publish options such as
`WithPersistent` and `WithMandatory` have no effect on Kafka.

Members of the same consumer group share topic partitions; different groups
consume independently. `WithConsumerGroup("workers")` overrides the default
for one subscription. `WithStartFromBeginning()` or `AutoOffsetReset="earliest"`
selects the earliest offset only when the group has no committed offset; it
does not rewind a group.

> [!IMPORTANT]
> **Kafka RPC needs a dedicated reply destination.** `ReplyTopic` is required
> only for `Call`. Provision a unique reply topic and consumer group for each
> concurrently running caller so another caller cannot consume its responses.
> The application owns cleanup when a caller deployment is retired.

See the runnable [AMQP/Kafka example](examples/amqp-kafka) for both transports,
keys, partitions, handler failures, and RPC. Stop the fixtures with the Compose
`down` command shown above.

## Request/reply

`Call` publishes a request and waits for the matching correlation ID. The
responder must publish to the request's `ReplyTo` and preserve its
`CorrelationID`:

```go
func serveRPC(ctx context.Context, responder core.MessageBroker) error {
	return responder.Subscribe(ctx, "weave.readme.rpc", func(handlerCtx context.Context, request *core.Message) error {
		response := core.NewTextMessage("pong: " + request.BodyString()).
			WithCorrelationID(request.CorrelationID)
		return responder.Publish(handlerCtx, request.ReplyTo, response,
			core.WithTimeout(5*time.Second))
	}, core.WithHandlerErrorRetry())
}
```

Connect the responder, register its subscription, and wait for readiness before
sending requests. The caller uses:

```go
response, err := broker.Call(ctx, "weave.readme.rpc",
	core.NewTextMessage("hello"), core.WithTimeout(5*time.Second))
if err != nil {
	return err
}
fmt.Println(response.BodyString()) // pong: hello
```

AMQP creates an exclusive reply queue automatically. Kafka requires the
provisioned reply topic described above. Weave assigns a correlation ID when
one is absent; caller-supplied IDs must be unique among pending calls on that
broker. Duplicates return `core.ErrDuplicateCorrelationID` before publishing.

Set `config.MaxPendingCalls = 128` **before construction** to bound active RPCs
per broker across all destinations. The default `0` is unlimited. Excess calls
wait for capacity using their existing context and timeout, before publishing.
Bound application goroutines too: this does not limit callers waiting for a slot.

`WithTimeout` covers capacity waits, reply-consumer setup, publishing, and the
reply wait. An earlier context deadline wins. Timing out a call does not cancel
remote processing. Connection loss fails pending calls; recovery does not
replay requests automatically.

## Delivery guarantees

`Publish` waits for **broker acknowledgment**, not handler completion. Neither
transport provides exactly-once processing.

| Behavior | RabbitMQ / AMQP | Kafka |
| --- | --- | --- |
| Publish success | Publisher confirmation | Configured acknowledgment: leader (`1`, default) or all in-sync replicas (`-1`) |
| Durable acceptance | Durable queue, `WithPersistent()`, successful confirmation; replicate queues for node failure | Replication, `RequiredAcks=-1`, and appropriate `min.insync.replicas` |
| Routing | Default exchange routes by queue name; `WithMandatory()` detects no matching queue | Existing topic; optional key or explicit partition |
| Handler returns `nil` | Acknowledge delivery with manual acknowledgments | Mark the record for offset advancement |
| Ordinary handler error, default policy | Reject without requeue; discard unless a broker dead-letter route exists | Mark the failed record and continue |
| `WithHandlerErrorRetry()` | Requeue failed delivery | Leave failed record and later offsets unmarked for retry |
| Handler returns an error after its context is canceled | Requeue unfinished manually acknowledged work | Leave unfinished record and later offsets unmarked |

Manual acknowledgments are the default. AMQP also requeues returned
`context.Canceled` and `context.DeadlineExceeded` errors. A `nil` handler result
means completed work. AMQP `WithAutoAck()` acknowledges before handling and
cannot provide redelivery for interrupted work.

Choose `WithHandlerErrorRetry()` deliberately: permanent failures can redeliver
indefinitely. Use bounded retries and a dead-letter destination for terminal
failures, as described below.

> [!IMPORTANT]
> **A timeout after submission leaves delivery uncertain.** The same applies to
> connection loss: the broker may already have accepted the message. Retrying
> can create duplicates. Set a stable `MessageID` and make side effects idempotent.

<details>
<summary><strong>Error handling reference</strong></summary>

| Error check | Meaning |
| --- | --- |
| `errors.Is(err, core.ErrInvalidConfig)` | Invalid configuration or public input |
| `errors.Is(err, context.Canceled)` | Explicit cancellation |
| `core.IsTimeout(err)` or `errors.Is(err, context.DeadlineExceeded)` | Operation deadline expired |
| `core.IsConnectionLost(err)` | Connection lost; submitted work may have an uncertain outcome |
| `errors.Is(err, core.ErrUnroutable)` | Mandatory AMQP publish found no destination queue |
| `errors.Is(err, core.ErrShuttingDown)` | Broker is draining and rejects new work |
| `errors.Is(err, core.ErrClosed)` | Broker is closed |

</details>

Constructors and operations validate inputs before broker I/O, including nil
contexts, messages, handlers or options; blank destinations; invalid counts,
timeouts, topic names, and connection settings. See
[validation and outcome details](docs/TRANSPORTS.md#invalid-inputs).

## Retries and dead-letter handling

`core.RetryHandler` retries a handler in process using a `RetryPolicy`.
`DefaultRetryPolicy()` allows three total attempts, including the first, with
exponential backoff. Wrap it with `JitteredBackoff` to spread retry times across
instances. Cancellation prevents new attempts, including zero-delay retries.
`core.RetryAttempt(ctx)` reports the current attempt starting at one.

For a confirmed handoff after terminal failure, use `core.DeadLetterHandler`:

```go
func subscribeOrders(ctx context.Context, broker core.MessageBroker, processOrder core.Handler) error {
	policy := core.DefaultRetryPolicy()
	policy.Backoff = core.JitteredBackoff(policy.Backoff)
	handler, err := core.DeadLetterHandler(
		processOrder, policy, broker, "weave.readme.orders.dead",
		core.DeadLetterOptions{
			Backend: broker.Backend(), Destination: "weave.readme.orders",
			Handler: "processOrder",
		},
		core.WithTimeout(5*time.Second),
	)
	if err != nil {
		return err
	}
	return broker.Subscribe(ctx, "weave.readme.orders", handler)
}
```

Provision the dead-letter queue or topic first. The helper encodes the original
delivery and failure with `NewDeadLetterEnvelope`, publishes persistently by
default, and always enables mandatory AMQP routing. The source is acknowledged
or marked only after processing succeeds or dead-letter publishing succeeds.
Use manual acknowledgments.

A failed handoff returns `ErrDeadLetterFailed`; both transports preserve the
source for redelivery regardless of the ordinary handler error policy.
Cancellation preserves work without dead-lettering it. Each redelivery starts
a new retry budget. Source acknowledgment and dead-letter publishing are
separate operations, so a crash can still produce duplicate envelopes.

Set `RetryPolicy.Retryable` to select retryable errors. To retry without a
dead-letter destination, pass `core.RetryHandler(processOrder, policy)` directly
to `Subscribe` and choose the policy for errors after retries exhaust.

## Configuration and transport options

Start with `core.DefaultConfig()` and the selected transport's defaults.
Constructors snapshot configuration; set it before construction rather than
editing it to change a live broker.

| Common setting | Default | Purpose |
| --- | --- | --- |
| `ConnectionRetry` | `3` | Additional initial-connection retry rounds; zero also selects this default |
| `RetryDelay` | `2s` | Fixed connection retry delay |
| `ConnectionBackoff` | `nil` | Optional strategy overriding connection retry delays |
| `MaxPendingCalls` | `0` | Maximum active RPC calls; zero is unlimited |
| `EventHook` | `nil` | Synchronous structured-event callback |

Background recovery continues until success or the broker stops; the initial
`Connect` retry budget does not limit its lifetime.

<details>
<summary><strong>Transport settings and options</strong></summary>

| Transport | Settings and options |
| --- | --- |
| AMQP connection | `Host`, `Port`, optional `Endpoints`, `Username`, `Password`, `VHost`, `Heartbeat`, `TLS` |
| AMQP topology | `Exchange`, `QueueDurable`, `QueueAutoDelete`, `QueueExclusive`; `WithQueueBind(exchange, key)` |
| AMQP publish | `WithExchange`, `WithMandatory`, `WithPersistent`, `WithPriority`, `WithExpiration` |
| AMQP consumption | `WithPrefetchCount`, `WithWorkerCount`, `WithExclusive`, `WithConsumerTag`, `WithAutoAck` |
| Kafka connection | `Brokers`, `ClientID`, `TLS`, `SASL`, `SessionTimeout`, `HeartbeatInterval` |
| Kafka publish | `RequiredAcks`, `MaxRetries`, `RetryBackoff`; `WithKey` and zero-based `WithPartition` |
| Kafka consumption / RPC | `ConsumerGroup`, `AutoOffsetReset`, `ReplyTopic`; `WithConsumerGroup`, `WithStartFromBeginning`, `WithWorkerCount` |
| Shared operations | `WithTimeout` for publish/call; `WithHandlerErrorRetry` or `WithHandlerErrorNoRetry` for subscriptions |

Kafka keys select partitions unless `WithPartition(n)` is supplied; an
out-of-range partition fails publishing. Ordering is per partition, not across
a topic. `WithWorkerCount` bounds executing handlers without increasing Kafka
parallelism beyond assigned partition claims. Use transport-specific options
with the matching backend. See [API](docs/API.md) and [transport details](docs/TRANSPORTS.md).

</details>

## TLS and authentication

Import `crypto/tls`. A non-nil TLS configuration enables encryption with
certificate and hostname verification:

```go
config := core.DefaultConfig()
config.AMQP = core.DefaultAMQPConfig()
config.AMQP.Host = "rabbit.example.com"
config.AMQP.Port = 5671
config.AMQP.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
```

Kafka TLS can be combined with SASL (this fragment also imports `os`):

```go
config := core.DefaultConfig()
config.Kafka = core.DefaultKafkaConfig()
config.Kafka.Brokers = []string{"kafka.example.com:9093"}
config.Kafka.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
config.Kafka.SASL = &core.SASLConfig{
	Enable: true, Mechanism: "SCRAM-SHA-512",
	Username: os.Getenv("KAFKA_USERNAME"), Password: os.Getenv("KAFKA_PASSWORD"),
}
```

Supply `RootCAs` for a private CA and `Certificates` for mutual TLS. The default
minimum TLS version is 1.2; failed TLS connections never fall back to plaintext.
Kafka supports `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`; an empty enabled
mechanism selects `PLAIN`. Use TLS with password authentication and supply
secrets through the application. See [TLS details](docs/TRANSPORTS.md#tls) and
[verified TLS fixtures](testdata/tls/README.md).

## Recovery and endpoint failover

Both transports restore active subscriptions after connection loss. Recovery
does not replay publishes or pending RPC requests. `IsRecovering()` reports
active recovery; operations may fail while the connection is unavailable.

For RabbitMQ cluster endpoints, set:

```go
config.AMQP.Endpoints = []string{"rabbit-1:5672", "rabbit-2:5672", "rabbit-3:5672"}
config.ConnectionBackoff = core.JitteredBackoff(
	core.ExponentialBackoff(100*time.Millisecond, 30*time.Second, 2),
)
```

The ordered `host:port` list overrides `Host` and `Port`. Startup begins at the
first address; recovery after loss starts at the next and wraps around. Each
retry round tries the whole list before backoff. A healthy connection stays on
its current endpoint. Endpoints share credentials, vhost, and TLS settings;
use TLS ports in every entry when TLS is enabled.

Keep `TLS.ServerName` empty to verify each endpoint's hostname. An explicit
server name applies to all endpoints. An earlier context deadline can stop
setup before every endpoint has been tried.

Endpoint failover cannot make a single-node queue available after its owning
node fails. Provision replicated queues, such as quorum queues, through broker
administration. Weave does not configure queue replication. See
[AMQP failover details](docs/TRANSPORTS.md#amqp).

The same `ConnectionBackoff` works for Kafka reconnects. It is separate from
handler `RetryPolicy.Backoff` and Kafka producer `RetryBackoff`. Jitter is opt-in;
existing connection delays and deterministic handler backoff remain defaults.
Custom strategies must return promptly and be safe for concurrent use.

## Readiness and graceful shutdown

Both transports and `MockBroker` implement optional `core.ConsumerReadiness`
(`IsReady`, `WaitReady`) and `core.Shutdowner` (`Shutdown`) interfaces.

`IsConnected()` describes connectivity. For service readiness, call `Connect`,
register business subscriptions, then `WaitReady` with a startup deadline.
Readiness requires setup of every active business subscription, not completed
processing or absence of backlog. A Kafka member with no assigned partitions
can be ready. Canceled subscriptions are excluded; a connected publisher with
no subscriptions is ready. Internal RPC reply consumers are not independently
included.

Readiness is a snapshot that changes during reconnects and rebalances. Use
`IsReady()` in health checks rather than inferring state from lifecycle-event
ordering. Call `Connect` before `WaitReady`; a wait before first connection
returns `ErrNotConnected`.

On termination, call `Shutdown` from the application owner with a fresh bounded
context, as in the quick start. It rejects new work with `ErrShuttingDown`,
cancels RabbitMQ business consumers at the broker, and drains admitted
handlers, publishes, and RPCs. Handlers may finish publishing replies using
their handler context while draining.

Keep subscription contexts alive until draining finishes; canceling them first
interrupts active handlers. Call `Shutdown` outside handlers and event hooks.
If its deadline expires, it returns promptly, cancels active handler contexts,
and starts client cleanup, which may continue in the background. Handlers must
respect cancellation; Go cannot stop a callback that ignores it.

`Close()` is the immediate-close path. After closure, operations return
`ErrClosed`, and that broker instance cannot be reconnected. The quick start
asserts interfaces implemented by both built-in transports. For a custom broker,
check optional-interface support:

```go
if drainer, ok := broker.(core.Shutdowner); ok {
	return drainer.Shutdown(shutdownCtx)
}
return broker.Close()
```

## Messages and encoding

`core.Message` carries `Body`, string `Headers`, `ContentType`, `MessageID`,
`CorrelationID`, `ReplyTo`, and `Timestamp`. `Subject`, `Partition`, and `Offset`
have transport-specific meanings. `Publish` and `Call` leave their input
unchanged and retain copies of its body and headers. Reuse or modify inputs
after the operation returns; deliveries and responses belong to the receiver.
Timestamp precision follows the protocol: whole seconds for AMQP, milliseconds
for Kafka.

Set an application-owned `MessageID` for the same logical event across retries.
Both transports preserve it, using AMQP's native property and Kafka's reserved
`message-id` header. IDs are not generated automatically, and identity does not
deduplicate messages. Keep it distinct from an RPC correlation ID and use the
field rather than an application header of the same name.

The [duplicate-handling example](examples/deduplication) skips completed IDs
and retries failures. Its in-memory store is illustrative; production
idempotency needs persistence and coordination with side effects.

Use `codec` to encode typed payloads and set their content type:

```go
type Order struct {
	ID string `json:"id"`
}

msg, err := codec.MarshalMessage(codec.JSON, Order{ID: "42"})
if err != nil {
	return err
}
msg.MessageID = "order-created-42"

var decoded Order
if err := codec.UnmarshalMessage(codec.JSON, msg, &decoded); err != nil {
	return err
}
```

`codec.Protobuf` uses the same API with `proto.Message` values.
`codec.UnmarshalAndValidate` adds application-owned validation after decoding.
See the [JSON](examples/json) and [Protocol Buffers](examples/protobuf) examples.

## Observability

Set `EventHook` before constructing the broker:

```go
config.EventHook = func(ctx context.Context, event core.Event) {
	log.Printf("backend=%s event=%s destination=%s duration=%s active=%d pending=%d error=%v",
		event.Backend, event.Name, event.Destination, event.Duration,
		event.ActiveHandlers, event.PendingRPCs, event.Err)
}
```

Events cover connection/recovery, successful and failed operations, handler and
RPC starts/completions, and consumer assignment/revocation. Kafka also reports
rebalance lifecycle and asynchronous consumer errors. Successful AMQP connect
events include the endpoint in `Fields["address"]`.

`Duration` measures completed work: publishes end at acknowledgment, calls
include capacity and reply waits, and handlers include wrapped retries.
`ActiveHandlers` and `PendingRPCs` are broker-local snapshots, not queue depth or
consumer lag. Calls waiting for capacity are excluded from `PendingRPCs`.

Hooks run synchronously and may run concurrently or arrive out of order. Return
promptly, do not panic, and make collectors safe for concurrency. Use bounded
metric labels and keep correlation IDs in logs or traces. See the runnable
[observability example](examples/observability) and [event reference](docs/API.md#events).

## Testing and development

For application tests, inject `testkit.NewMockBroker()` wherever your code
accepts a `core.Publisher` or `core.MessageBroker`. Connect it before use.

<details>
<summary><strong>MockBroker test example</strong></summary>

```go
func TestPublish(t *testing.T) {
	broker := testkit.NewMockBroker()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	if err := broker.Publish(context.Background(), "orders", core.NewTextMessage("created")); err != nil {
		t.Fatal(err)
	}
	broker.AssertPublishCount(t, "orders", 1)
}
```

The test imports `context`, `testing`, `core`, and `testkit`.

</details>

Use `SetCallResponse` / `SetCallError` for RPC, `SimulateMessage` for subscribed
handlers, and `PublishedMessages` / `PublishedTo` to inspect owned snapshots.
Publishing records a message; use `SimulateMessage` explicitly to deliver one.
The mock does not simulate persistence, replication, or network timing.

Run normal validation from the repository root; no live broker is required:

```sh
go test -race ./...
go vet ./...
go build ./...
make test-coverage-check
```

Shared contract tests check ownership, metadata, cancellation, and closed-state
behavior across AMQP, Kafka, and the mock. The separate example module also
needs validation:

```sh
cd examples/amqp-kafka
go test ./...
go vet ./...
go build ./...
```

From the repository root, run isolated live-broker tests with Docker available:

```sh
./scripts/test_stress.sh          # sustained traffic and repeated crashes, both transports
./scripts/test_stress.sh bench    # confirmed AMQP publish throughput
./scripts/test_clusters.sh        # replicated failover and peer-network partitions, both transports
```

These scripts manage and remove their own fixtures. The cluster suite includes
killing the connected AMQP endpoint and recovering through the endpoint list.
It requires Docker Compose 2.33.1 or newer and covers one failed or isolated
member with a surviving majority, not majority loss or every partition shape.
See [performance and failure testing](docs/PERFORMANCE.md) for longer runs,
benchmark interpretation, resource requirements, and scope. TLS and Kafka SCRAM
fixtures are documented in [transports](docs/TRANSPORTS.md).

CI runs `govulncheck` for both modules using a separate current stable Go
toolchain. Follow [CONTRIBUTING.md](CONTRIBUTING.md) for scanner installation
and the full validation workflow; the library minimum remains Go 1.25.

## Migrating from v0.1.0

v0.2.0 is a breaking pre-1.0 upgrade. Update imports and construction before
adopting the new operational APIs:

| v0.1.0 usage | Current API / action |
| --- | --- |
| Go 1.24 | Upgrade to Go 1.25 or newer, including build and CI images |
| Root `weave` aliases and convenience constructors | Import `core` for shared types/options; call `amqp.NewBroker` or `kafka.NewBroker` directly |
| Registry / `NewWithBackend` / registered factories | Select the transport explicitly in application construction |
| `core.Client` / `core.Server` | Use capability interfaces (`Publisher`, `Caller`, `Subscriber`, `Connector`) or define a small application interface |
| Runtime and health wrappers | Own application lifecycle and health checks using the current interfaces, handler wrappers, and events |
| Assuming `Close` drains active work | Call optional `Shutdown(ctx)` with a deadline; use `Close` for immediate cleanup |
| Treating `Publish` as fire-and-forget | Budget for broker acknowledgment; handle uncertain timeout/disconnect outcomes and duplicates |
| Kafka RPC without a provisioned reply topic | Provision `ReplyTopic`; give each concurrent caller its own reply topic and consumer group |
| Kafka `RequiredAcks=0` | Select `1` or `-1`; unacknowledged publishing is rejected |
| Reusing an in-flight RPC correlation ID | Let Weave generate IDs or assign unique pending-call IDs |
| Depending on permissive input handling | Handle `ErrInvalidConfig`; invalid addresses, nil options, negative limits/timeouts, and unsupported settings are rejected |

`core.MessageBroker` keeps its core method set. Shutdown and readiness are
optional interfaces, so custom brokers can adopt them separately. Review the
[delivery policies](#delivery-guarantees): ordinary handler errors need an
explicit retry or dead-letter policy to preserve failed work. Jitter, endpoint
lists, and pending-RPC limits are opt-in.

## Documentation and examples

| Guide | What it covers |
| --- | --- |
| [API reference](docs/API.md) | Ownership, options, retries, dead-letter envelopes, RPC limits, and event fields |
| [Transport guide](docs/TRANSPORTS.md) | Broker-specific behavior, security, lifecycle, validation, and delivery outcomes |
| [Performance and resilience](docs/PERFORMANCE.md) | Sustained load, repeated crashes, AMQP benchmarks, clusters, and partitions |
| [Examples](examples/README.md) | JSON, Protobuf, AMQP/Kafka, duplicate handling, observability, and load generation |
| [Contributing](CONTRIBUTING.md) | Development checks, shared contracts, vulnerability scanning, and design conventions |
| [Release checklist](RELEASE_CHECKLIST.md) | Validation and release preparation |

---

<p align="center">
  <a href="#readme-top">Back to top ↑</a> &nbsp;·&nbsp;
  Licensed under <a href="LICENSE">Apache License 2.0</a>
</p>
