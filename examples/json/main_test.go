package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestOrderHandlerValidatesBeforeDelivery(t *testing.T) {
	for _, body := range []string{"{", `{}`, `{"id":""}`} {
		received := make(chan order, 1)
		if err := orderHandler(received)(context.Background(), core.NewTextMessage(body)); err == nil {
			t.Fatalf("invalid order %q was accepted", body)
		}
		if len(received) != 0 {
			t.Fatal("invalid order reached application code")
		}
	}
	received := make(chan order, 1)
	if err := orderHandler(received)(context.Background(), core.NewTextMessage(`{"id":"order-1"}`)); err != nil {
		t.Fatal(err)
	}
	if got := <-received; got.ID != "order-1" {
		t.Fatalf("decoded order=%+v", got)
	}
}

func TestOrderHandlerHonorsCancellation(t *testing.T) {
	for _, capacity := range []int{0, 1} {
		received := make(chan order, capacity)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := orderHandler(received)(ctx, core.NewTextMessage(`{"id":"order-1"}`)); !errors.Is(err, context.Canceled) {
			t.Fatalf("capacity=%d: error=%v", capacity, err)
		}
		if len(received) != 0 {
			t.Fatal("canceled delivery reached application code")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- orderHandler(make(chan order))(ctx, core.NewTextMessage(`{"id":"order-1"}`))
	}()
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked delivery error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler remained blocked after cancellation")
	}
}

func TestRunWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_EXAMPLES_E2E") != "1" {
		t.Skip("set WEAVE_EXAMPLES_E2E=1 and start the example RabbitMQ fixture")
	}
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var output bytes.Buffer
		err := run(ctx, &output)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if output.String() != "order-1\n" {
			t.Fatalf("output=%q", output.String())
		}
	}
}
