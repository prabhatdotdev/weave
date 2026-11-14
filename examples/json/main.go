package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
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
	err := run(ctx, os.Stdout)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, output io.Writer) error {
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.QueueExclusive = true
	config.AMQP.QueueAutoDelete = true
	broker, err := amqp.NewBroker(config)
	if err != nil {
		return err
	}
	defer broker.Close()
	if err := broker.Connect(ctx); err != nil {
		return err
	}

	destination := "weave.example.json." + rand.Text()
	received := make(chan order, 1)
	if err := broker.Subscribe(ctx, destination, orderHandler(received)); err != nil {
		return err
	}
	if err := broker.(core.ConsumerReadiness).WaitReady(ctx); err != nil {
		return err
	}

	msg, err := codec.MarshalMessage(codec.JSON, order{ID: "order-1"})
	if err != nil {
		return err
	}
	msg.MessageID = "order-created-1"
	if err := broker.Publish(ctx, destination, msg, core.WithMandatory(), core.WithTimeout(5*time.Second)); err != nil {
		return err
	}
	select {
	case decoded := <-received:
		if _, err := fmt.Fprintln(output, decoded.ID); err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return broker.(core.Shutdowner).Shutdown(shutdownCtx)
}

func orderHandler(received chan<- order) core.Handler {
	return func(ctx context.Context, msg *core.Message) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var decoded order
		if err := codec.UnmarshalMessage(codec.JSON, msg, &decoded); err != nil {
			return err
		}
		if decoded.ID == "" {
			return fmt.Errorf("order ID is required")
		}
		select {
		case received <- decoded:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
