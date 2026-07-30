# Weave

Weave provides one small message-broker interface with AMQP and Kafka
implementations.

## Install

```sh
go get github.com/prabhatdotdev/weave
```

Import only the transport you use:

```go
import (
	"context"
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
	if err := broker.Publish(ctx, "orders", core.NewTextMessage("created")); err != nil {
		log.Fatal(err)
	}
}
```

Kafka uses the same interface:

```go
config := core.DefaultConfig()
config.Kafka = core.DefaultKafkaConfig()
config.Kafka.ConsumerGroup = "orders"

broker, err := kafka.NewBroker(config)
```

## Interface

```go
type MessageBroker interface {
	Connect(context.Context) error
	Close() error
	IsConnected() bool
	IsRecovering() bool
	Backend() string
	Publish(context.Context, string, *Message, ...PublishOption) error
	Call(context.Context, string, *Message, ...PublishOption) (*Message, error)
	Subscribe(context.Context, string, Handler, ...SubscribeOption) error
}
```

Use `codec.JSON` or `codec.Protobuf` when payload encoding should be shared:

```go
msg, err := codec.MarshalMessage(codec.JSON, order)
err = codec.UnmarshalMessage(codec.JSON, msg, &decoded)
```

Transport events are available through one optional callback:

```go
config.EventHook = func(ctx context.Context, event core.Event) {
	log.Printf("%s: %v", event.Name, event.Err)
}
```

See [docs/QUICKSTART.md](docs/QUICKSTART.md), [docs/API.md](docs/API.md), and
[docs/TRANSPORTS.md](docs/TRANSPORTS.md).

## Development

```sh
go test ./...
go test -race ./...
```

AMQP and Kafka recovery paths have focused tests under their transport
packages. Runnable examples live in [`examples`](examples).

Apache-2.0 licensed.
