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
config.Kafka.ReplyTopic = "orders.replies"

broker, err := kafka.NewBroker(config)
```

Kafka supports keys, explicit partitions, consumer groups, offset selection,
request/reply, SASL configuration, and connection recovery.

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

## Events

Both transports emit structured events through `Config.EventHook`:

```go
config.EventHook = func(ctx context.Context, event core.Event) {
	log.Printf("backend=%s operation=%s error=%v",
		event.Backend, event.Operation, event.Err)
}
```

Derive metrics or traces in that callback if needed.
