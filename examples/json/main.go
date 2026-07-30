package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prabhatdotdev/weave/codec"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
)

type order struct {
	ID string `json:"id"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := amqp.NewBroker(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()
	if err := broker.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	received := make(chan order, 1)
	if err := broker.Subscribe(ctx, "orders", func(_ context.Context, msg *core.Message) error {
		var decoded order
		if err := codec.UnmarshalMessage(codec.JSON, msg, &decoded); err != nil {
			return err
		}
		received <- decoded
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	msg, err := codec.MarshalMessage(codec.JSON, order{ID: "order-1"})
	if err != nil {
		log.Fatal(err)
	}
	if err := broker.Publish(ctx, "orders", msg); err != nil {
		log.Fatal(err)
	}
	select {
	case decoded := <-received:
		fmt.Println(decoded.ID)
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}
