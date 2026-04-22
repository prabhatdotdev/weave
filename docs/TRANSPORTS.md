# Transport Configuration Guide

This guide covers the transports currently implemented in this repository. The `Backend` field in `Config` selects which transport implementation to use.

## Implemented Transports

- `amqp` - RabbitMQ / AMQP 0.9.1
- `kafka` - Apache Kafka

## Future Transports

The following transports are planned but not currently implemented:

- `kinesis` - AWS Kinesis
- `nats` - NATS Streaming
- `activemq` - Apache ActiveMQ
- `redis` - Redis Streams

**Note:** If you would like to implement a new backend, see [ADDING_BACKENDS.md](ADDING_BACKENDS.md) for the implementation guide and requirements.

---

### Basic Configuration

```go
config := &mqservice.Config{
    Backend: "amqp",
    AMQP: &mqservice.AMQPConfig{
        Host:     "localhost",
        Port:     5672,
        Username: "guest",
        Password: "guest",
        VHost:    "/",
    },
}

broker, err := mqservice.New(config)
if err != nil {
    log.Fatal(err)
}
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend:         "amqp",
    ConnectionName:  "my-service",
    ConnectionRetry: 3,
    RetryDelay:      2 * time.Second,
    
    AMQP: &mqservice.AMQPConfig{
        Host:      "rabbitmq.example.com",
        Port:      5672,
        Username:  "myapp",
        Password:  "secret",
        VHost:     "/production",
        Heartbeat: 10 * time.Second,
        
        // Exchange settings
        Exchange:     "my-exchange",
        ExchangeType: "topic", // direct, fanout, topic, headers
        
        // Queue settings
        QueueDurable:    true,  // Survive broker restart
        QueueAutoDelete: false, // Don't delete when last consumer disconnects
        QueueExclusive:  false, // Allow multiple consumers
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:   true,
            CertFile: "/path/to/cert.pem",
            KeyFile:  "/path/to/key.pem",
            CAFile:   "/path/to/ca.pem",
        },
    },
}
```

### Connection URL

Alternatively, use a connection URL:

```go
// Set via environment or config
os.Setenv("AMQP_URL", "amqp://user:pass@rabbitmq.example.com:5672/vhost")
```

### Default Values

```go
config := mqservice.DefaultConfig() // Returns AMQP defaults
// Host: "localhost"
// Port: 5672
// Username: "guest"
// Password: "guest"
// VHost: "/"
// Heartbeat: 10s
```

---

## Apache Kafka

### Basic Configuration

```go
import _ "github.com/prabhatdotdev/weave/transport/kafka"

config := &mqservice.Config{
    Backend: "kafka",
    Kafka: &mqservice.KafkaConfig{
        Brokers:       []string{"localhost:9092"},
        ConsumerGroup: "my-service-group",
    },
}

broker, err := mqservice.New(config)
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend: "kafka",
    
    Kafka: &mqservice.KafkaConfig{
        Brokers:  []string{
            "kafka1.example.com:9092",
            "kafka2.example.com:9092",
            "kafka3.example.com:9092",
        },
        ClientID:      "my-service-client",
        ConsumerGroup: "my-service-group",
        
        // Producer settings
        RequiredAcks:    -1, // Wait for all replicas (0=none, 1=leader, -1=all)
        MaxRetries:      3,
        RetryBackoff:    100 * time.Millisecond,
        CompressionType: "snappy", // none, gzip, snappy, lz4, zstd
        
        // Consumer settings
        AutoOffsetReset:   "latest", // or "earliest"
        SessionTimeout:    10 * time.Second,
        HeartbeatInterval: 3 * time.Second,
        
        // SASL Authentication
        SASL: &mqservice.SASLConfig{
            Enable:    true,
            Mechanism: "SCRAM-SHA-512", // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
            Username:  "kafka-user",
            Password:  "kafka-pass",
        },
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:   true,
            CAFile:   "/path/to/ca.pem",
            CertFile: "/path/to/cert.pem",
            KeyFile:  "/path/to/key.pem",
        },
    },
}
```

### Kafka-Specific Features

#### Topics vs Partitions

```go
// Subscribe to topic
broker.Subscribe(ctx, "user-events", handler)

// Publish to specific partition
msg := mqservice.NewMessage(data)
msg.Subject = "user-123" // Used as partition key
broker.Publish(ctx, "user-events", msg)
```

#### Consumer Groups

Multiple instances with the same `ConsumerGroup` share message consumption:

```go
config.Kafka.ConsumerGroup = "my-service-group"
```

### Default Values

```go
config := mqservice.DefaultKafkaConfig()
// Brokers: ["localhost:9092"]
// RequiredAcks: 1
// MaxRetries: 3
// AutoOffsetReset: "latest"
```

---

## Planned Transports

Kinesis, NATS, ActiveMQ, and Redis are roadmap transports only. They are not implemented in this repository and are not part of the current `Config` backend surface.

Until those transports exist under `transport/`, do not import or configure them as active backends in application code.

---

## Environment Variables

This repository does not currently expose a `LoadConfigFromEnv()` helper. Environment-based configuration should be handled in application code.

Example:

```bash
# AMQP
export MQ_BACKEND=amqp
export AMQP_HOST=rabbitmq.example.com
export AMQP_PORT=5672
export AMQP_USERNAME=myapp
export AMQP_PASSWORD=secret

# Kafka
export MQ_BACKEND=kafka
export KAFKA_BROKERS=kafka1:9092,kafka2:9092
export KAFKA_CONSUMER_GROUP=my-service

```

---

## Connection Retry

Configure retry behavior:

```go
config := &mqservice.Config{
    Backend:         "amqp",
    ConnectionRetry: 5,                // Retry 5 times
    RetryDelay:      3 * time.Second, // Wait 3s between attempts
    AMQP:            mqservice.DefaultAMQPConfig(),
}
```

## Connection Loss and Recovery Behavior

Recovery is intentionally explicit rather than magical. The contract below describes what Weave guarantees today across both implemented transports.

### Shared Contract

- Before the first successful `Connect()`, `Publish()`, `Subscribe()`, and `Call()` return `ErrNotConnected`.
- After a transport has connected at least once, a detected connection loss marks the broker disconnected and cancels all pending RPC waits.
- In-flight `Call()` operations waiting for a reply fail with `ErrConnectionLost`; Weave does not automatically retry them.
- `CallWithPolicy(...)` can retry a failed RPC after `ErrConnectionLost`, but that retry is opt-in because the original call may already have reached the remote service.
- Weave does not snapshot or roll back handler-local state. If your handler performs side effects before a disconnect near the ack/commit boundary, the message may be redelivered after recovery. Handlers should therefore be idempotent.
- Recovery only restores Weave-managed transport wiring such as connections, reply consumers/queues, and registered subscriptions. It does not recreate application-owned state, transactions, or external dependency sessions.

### AMQP (RabbitMQ)

- After a successful `Connect`, unexpected connection loss is detected via AMQP close notifications.
- The broker marks itself disconnected, cancels all pending RPC calls, and retries reconnecting in the background using `ConnectionRetry`/`RetryDelay`.
- Once reconnected, all previously registered subscriptions are re-declared, bindings are re-applied, and handlers are reattached automatically.
- In-flight `Call()` operations waiting for a reply fail with `ErrConnectionLost` when the connection drops.
- Operations issued while background reconnect is still in progress fail fast with `ErrNotConnected`.
- The same handler function values are reused after reconnect; Weave does not rebuild captured state inside closures.
- Operations attempted before the first successful `Connect` still return `ErrNotConnected`.

### Kafka

- Before the first successful `Connect`, operations return `ErrNotConnected`.
- After at least one successful `Connect`, if producer/consumer connectivity is lost, the broker marks itself disconnected, closes stale clients, and cancels pending RPC calls.
- `Publish()`/`Subscribe()`/`Call()` trigger reconnect attempts on demand after a detected loss.
- Subscriber consume loops continue running and rejoin consumption after reconnect using the same destination and handler.
- In-flight `Call()` operations fail with `ErrConnectionLost` when a loss is detected before a response arrives.
- The same handler function values remain registered across reconnects; consumer-group recovery resumes the existing handler wiring rather than constructing new application state.

### Retry Guidance During Recovery

- Do not assume a reconnect makes an in-flight RPC safe to repeat automatically.
- Prefer `CallWithPolicy(...)` only for idempotent operations or when the remote side can deduplicate by correlation/request ID.
- Prefer `RetryHandler(...)` for bounded in-process retries of transient handler failures, and keep handler side effects idempotent so reconnect-triggered redelivery is safe.

For standardized handler failure and retry semantics (including dead-letter guidance), see [ERROR_POLICY.md](ERROR_POLICY.md).

### Observability During Recovery

Weave emits events and metrics during reconnect and subscription restoration to support production observability:

#### Events
- `reconnect_started` - Recovery process has begun (Level: Info)
- `reconnect_attempt` - A connection attempt was made with attempt counter (Level: Debug)
- `reconnect_attempt_failed` - A single connection attempt failed with error (Level: Warn)
- `reconnect_succeeded` - Connection and subscription restore completed successfully (Level: Info)
- `subscription_restore_started` - Subscription restoration process began with subscription count (Level: Info)
- `subscription_restore_failed` - A single subscription failed to restore with destination and error (Level: Error)
- `subscription_restore_completed` - All subscriptions restored successfully (Level: Info)

#### Metrics
- `weave.transport.reconnect.started` (counter) - Incremented once per reconnect cycle start
- `weave.transport.reconnect.succeeded` (counter) - Incremented on successful reconnect completion
- `weave.transport.reconnect.attempt_failures` (counter) - Incremented per failed connection attempt with backend label
- `weave.transport.subscription.restore.success` (counter) - Incremented per successfully restored subscription with backend and destination labels
- `weave.transport.subscription.restore.failures` (counter) - Incremented per failed subscription restore with backend and destination labels

#### Health Contract
- `HealthReport.Status` returns `HealthStatusDegraded` while recovery is in progress
- `HealthReport.Details["recovering"]` is set to `true` during reconnect windows
- `Broker.IsRecovering()` returns `true` only while reconnect is actively running; once `IsConnected()` returns true, `IsRecovering()` is false
- **Runtime Client/Server**: Health reports transition to `Degraded` during recovery and return to `Healthy` once `IsConnected()` succeeds

This observability enables infrastructure to:
1. Distinguish transient recovery (`Degraded`) from terminal disconnection (`Unhealthy`)
2. Monitor reconnect attempt frequency and success rates
3. Alert on subscription restore failures per destination
4. Track recovery latency from loss detection to operational readiness

---

## Best Practices

### 1. Use Default Configs

Start with defaults and override only what's needed:

```go
config := mqservice.DefaultConfig()
config.AMQP.Host = "production-rabbitmq"
```

### 2. Externalize Credentials

Never hardcode credentials:

```go
config.AMQP.Username = os.Getenv("RABBITMQ_USERNAME")
config.AMQP.Password = os.Getenv("RABBITMQ_PASSWORD")
```

### 3. Enable TLS in Production

Always use TLS for production:

```go
config.AMQP.TLS = &mqservice.TLSConfig{
    Enable: true,
    CAFile: "/etc/ssl/ca.pem",
}
```

### 4. Set Connection Names

For debugging and monitoring:

```go
config.ConnectionName = "user-service-prod-1"
```

## Switching Transports

To switch transports, only change the config:

```go
// Was using AMQP
config := &mqservice.Config{
    Backend: "amqp",
    AMQP: &mqservice.AMQPConfig{...},
}

// Now using Kafka - no code changes!
config := &mqservice.Config{
    Backend: "kafka",
    Kafka: &mqservice.KafkaConfig{...},
}
```

Your application code remains the same!
