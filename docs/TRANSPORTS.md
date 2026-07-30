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

## Kafka

```go
config := core.DefaultConfig()
config.Kafka = core.DefaultKafkaConfig()
config.Kafka.Brokers = []string{"kafka:9092"}
config.Kafka.ClientID = "orders"
config.Kafka.ConsumerGroup = "orders"

broker, err := kafka.NewBroker(config)
```

Kafka supports keys, explicit partitions, consumer groups, offset selection,
request/reply, SASL configuration, and connection recovery.

## Common lifecycle

```go
if err := broker.Connect(ctx); err != nil {
	return err
}
defer broker.Close()
```

`IsConnected` reports readiness. `IsRecovering` distinguishes an active
reconnect from a closed or never-connected broker.

## Events

Both transports emit structured events through `Config.EventHook`:

```go
config.EventHook = func(ctx context.Context, event core.Event) {
	log.Printf("backend=%s operation=%s error=%v",
		event.Backend, event.Operation, event.Err)
}
```

Derive metrics or traces in that callback if needed.
