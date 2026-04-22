# Transport Capability Matrix

This document defines what behavior guarantees each transport backend provides.

## Quick Reference

| Capability | AMQP (RabbitMQ) | Apache Kafka | Notes |
|------------|-----------------|--------------|-------|
| **Publish/Fire-and-Forget** | ✅ | ✅ | Core feature |
| **Request-Reply RPC** | ✅ | ✅ | Core feature |
| **Subscriptions** | ✅ | ✅ | Core feature |
| **Message Ordering** | ⚠️ Limited | ✅ Per-partition | See notes |
| **At-Least-Once Delivery** | ✅ | ✅ | Default behavior |
| **Exactly-Once Delivery** | ⚠️ Limited | ⚠️ Config-dependent | Application responsibility |
| **Dead-Letter Routing** | ✅ Native | 🔲 Manual | See notes |
| **Message TTL/Expiration** | ✅ | 🔲 No native support | Manual application logic |
| **Priority Queues** | ✅ | 🔲 No support | Application responsibility |
| **Connection Recovery** | 🔶 Partial | 🔶 Partial | Recovery exists, but semantics differ by transport; see notes |
| **Bounded In-Process Handler Retry** | ✅ Via helper | ✅ Via helper | `RetryHandler(...)` |
| **Structured RPC Error Payloads** | ✅ Via helper | ✅ Via helper | `ErrorPayload` / `NewErrorMessage(...)` |
| **Standard Dead-Letter Envelope** | ✅ Via helper | ✅ Via helper | `NewDeadLetterEnvelope(...)` |
| **RPC Backoff / Circuit Breaking** | ✅ Via helper | ✅ Via helper | `CallWithPolicy(...)` |
| **Runtime Health Snapshots** | ✅ | ✅ | `HealthReport()` + health hooks |
| **Consumer Groups** | ⚠️ Queue-based | ✅ Full support | Different patterns |
| **Partition/Shard Awareness** | ❌ No | ✅ Via key | Kafka only |
| **Offset/Position Management** | ⚠️ Queue depth | ✅ Explicit offsets | Different models |
| **Timeout Handling** | ✅ | ✅ | Context-based |
| **Observability Hooks** | ✅ | ✅ | Logger, EventHook, Metrics, Tracing |

**Legend:**
- ✅ Fully supported
- ⚠️ Supported with limitations or differences
- 🔶 Partial support (under development)
- 🔲 No native support (workarounds possible)
- ❌ Not supported

---

## Detailed Capabilities

### AMQP (RabbitMQ)

**Version:** RabbitMQ 3.8+  
**Status:** Stable

#### Message Delivery

- **At-Least-Once:** ✅ Default with auto-ack disabled
- **Ordering:** ⚠️ Guaranteed only within a single queue; competing consumers may reorder
- **Persistence:** ✅ Via `WithPersistent()` option
- **TTL/Expiration:** ✅ Via queue or message TTL
- **Priority:** ✅ Via `WithPriority()` option

#### Subscriptions (Consuming)

- **Multiple handlers on same destination:** ⚠️ Load-balanced across handlers
- **Consumer groups:** ⚠️ Queue-based; all consumers read the same queue
- **Prefetch/QoS:** ✅ Via `WithPrefetchCount()` option
- **Auto-acknowledge:** ✅ Via `WithAutoAck()` option
- **Dead-letter:** ✅ Native DLX (Dead Letter Exchange)

#### Connection Behavior

- **Auto-reconnect:** ✅ Background reconnect loop with backoff
- **Re-subscribe after reconnect:** ✅ Existing subscriptions are re-declared automatically after reconnect
- **Pending RPCs on disconnect:** ⚠️ Canceled and returned as `ErrConnectionLost`
- **Operations during reconnect:** ⚠️ Fail fast with `ErrNotConnected` until the channel is restored
- **Heartbeat:** ✅ Configurable heartbeat
- **Connection pooling:** ✅ Single connection per broker instance

#### Observability

- **Events emitted:** Connect, Disconnect, Publish, Subscribe, Timeout, Error
- **Logger integration:** ✅ Via `Config.Logger`
- **Metrics hooks:** ✅ Via `Config.Metrics`
- **Tracing hooks:** ✅ Via `Config.Tracing` on high-level runtime operations

#### Limitations

- No native concept of partitions (single queue serves all consumers)
- Consumer groups are implicit (all consumers on a queue are a group)
- Exactly-once delivery requires application-level idempotency
- Dead-letter handling requires pre-configured DLX
- Broker-level dead-letter routing is still configured outside Weave; the helper only standardizes payload shape

---

### Apache Kafka

**Version:** 2.8+  
**Status:** Stable

#### Message Delivery

- **At-Least-Once:** ✅ Default behavior
- **Ordering:** ✅ Per-partition; cross-partition ordering not guaranteed
- **Persistence:** ✅ Always persisted to disk (configurable retention)
- **TTL/Expiration:** 🔲 No native support; use topic retention instead
- **Priority:** 🔲 No built-in support

#### Subscriptions (Consuming)

- **Multiple handlers on same destination:** ✅ Via consumer groups
- **Consumer groups:** ✅ Full support with group coordination
- **Prefetch/QoS:** ✅ Via `WithFetchMinBytes()`, `WithFetchMaxWaitMs()`
- **Auto-acknowledge:** ✅ Via `WithAutoOffsetReset()`
- **Dead-letter:** 🔲 No native DLT; implement manually or via external tools

#### Partition Awareness

- **Partition selection:** ✅ Via `WithKey()` option (deterministic partitioning)
- **Explicit partition assignment:** ❌ Not exposed in Weave API
- **Rebalancing:** ✅ Automatic rebalancing on consumer group changes

#### Connection Behavior

- **Auto-reconnect:** ✅ On-demand reconnect after a prior successful connection
- **Re-subscribe after reconnect:** ✅ Existing consume loops resume with the same destination and handler after reconnect
- **Pending RPCs on disconnect:** ⚠️ Canceled and returned as `ErrConnectionLost`
- **Operations during reconnect:** ⚠️ Trigger reconnect on demand after a prior successful `Connect()`
- **Heartbeat:** ✅ Automatic via session timeout
- **Connection pooling:** ✅ Automatic multi-broker connections

#### Offset Management

- **Auto-commit:** ✅ Configurable
- **Manual commit:** ✅ Via `WithAutoAck(false)` 
- **Offset reset policy:** ✅ Via `WithAutoOffsetReset()`
- **Offset tracking:** ✅ Explicit offsets per partition

#### Observability

- **Events emitted:** Connect, Disconnect, Publish, Subscribe, Timeout, Error
- **Logger integration:** ✅ Via `Config.Logger`
- **Metrics hooks:** ✅ Via `Config.Metrics`
- **Tracing hooks:** ✅ Via `Config.Tracing` on high-level runtime operations

#### Limitations

- No explicit message priority support
- No native TTL (use topic retention policies)
- Exactly-once delivery requires coordination with external systems
- Dead-letter pattern must be implemented manually
- Retry timing for uncommitted handler failures depends on consumer-group behavior rather than immediate redelivery

---

## Cross-Transport Patterns

### Message Ordering

**AMQP:** Messages from a single queue maintain order. Competing consumers may process out-of-order due to redelivery.

**Kafka:** Messages within a partition maintain order. Different partitions process independently. Use `WithKey()` to ensure related messages go to the same partition.

**Recommendation:** For order-critical messages, use a single handler per queue/topic or use keyed partitioning.

### Exactly-Once Delivery

Neither transport guarantees exactly-once delivery out of the box. Implement idempotent handlers:

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    orderId := extractOrderID(msg.Body)
    
    // Check if already processed (database, cache, etc.)
    if alreadyProcessed(orderId) {
        return nil // Idempotent: skip reprocessing
    }
    
    // Process
    err := processOrder(orderId, msg.Body)
    if err != nil {
        return err // Will trigger redelivery
    }
    
    // Mark processed
    markProcessed(orderId)
    return nil
})
```

### Dead-Letter Handling

**AMQP:** Configure Dead Letter Exchange (DLX) on queues. Kafka: Implement manually:

```go
server.Handle("orders", func(ctx context.Context, msg *weave.Message) error {
    err := processOrder(msg.Body)
    if err != nil {
        // Application retry logic or send to DLQ topic
        deadLetterClient.Publish(ctx, "orders.dead-letter", msg)
        return nil // Acknowledge to prevent broker redelivery
    }
    return nil
})
```

For both transports, `NewDeadLetterEnvelope(...)` provides a shared JSON payload shape so dead-letter consumers can parse failures consistently regardless of backend.

### RPC Retry And Circuit Breaking

Use `CallWithPolicy(...)` with `DefaultCallPolicy()` or a custom `CallPolicy` when you want bounded retries with backoff and an optional circuit breaker.

Compatibility guarantee:

- retries are transport-agnostic and happen at the runtime layer
- timeout and connection-style failures are retryable by default in `DefaultCallPolicy()`
- side-effect safety remains an application concern for non-idempotent calls

---

## Choosing a Transport

### Use AMQP (RabbitMQ) when:

- You need native dead-letter handling
- Message priority is important
- Message TTL/expiration is critical
- You want simpler deployment (single queue model)
- Maximum simplicity is preferred over massive scale

### Use Kafka when:

- You need horizontal scaling beyond a single broker
- You have high-throughput, high-volume requirements
- You need explicit consumer group coordination
- You want to retain message history for replay
- You need partition-based ordering guarantees

### Use Both when:

- Different services have different requirements
- You're migrating from one to another
- You need to decouple multiple microservices with different backends

---

## Future Planned Transports

The following transports are **reserved** for future consideration but are **not currently under development**:

- NATS (JetStream)
- Redis Streams
- AWS Kinesis
- Apache Pulsar
- ActiveMQ

These may be added based on community demand. See [Next_release.md](../Next_release.md) for roadmap details.

---

## Reporting Gaps or Inconsistencies

If you discover that actual behavior differs from this matrix:

1. Check the [error handling documentation](ERROR_POLICY.md) for expected failure modes
2. Open an issue with:
   - Your transport backend and version
   - The capability that differs
   - How it actually behaves vs. documented
   - Steps to reproduce (if applicable)

This matrix is versioned with each release and should be updated alongside release notes whenever transport guarantees change.
