# Quick start

Start RabbitMQ:

```sh
docker compose up -d rabbitmq
```

Create a broker and connect:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
)

func main() {
	ctx := context.Background()
	broker, err := amqp.NewBroker(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	if err := broker.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if err := broker.Subscribe(ctx, "demo", func(_ context.Context, msg *core.Message) error {
		fmt.Println(msg.BodyString())
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := broker.Publish(ctx, "demo", core.NewTextMessage("hello")); err != nil {
		log.Fatal(err)
	}
}
```

For request/reply:

```go
reply, err := broker.Call(
	ctx,
	"users.get",
	core.NewTextMessage("42"),
	core.WithTimeout(5*time.Second),
)
```

For Kafka:

```go
config := core.DefaultConfig()
config.Kafka = core.DefaultKafkaConfig()
config.Kafka.ConsumerGroup = "demo"
config.Kafka.ReplyTopic = "demo.replies"
broker, err := kafka.NewBroker(config)
```

Provision `demo.replies` before using `Call`. Kafka topic auto-creation is
disabled by the transport, and the application owns cleanup of its reply topic.

Both constructors return `core.MessageBroker`, so the rest of the program is
transport-independent.
