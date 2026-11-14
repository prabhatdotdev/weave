package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/testkit"
)

func TestEventHandlerIgnoresRetainedTrafficAndRetriesCurrentOrder(t *testing.T) {
	received := make(chan string, 1)
	handler := eventHandler("current", "weave.example.orders", "kafka", received)
	old := core.NewTextMessage("old order").WithHeader("example-run", "previous")
	if err := handler(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if len(received) != 0 {
		t.Fatal("retained traffic filled the current run's result channel")
	}
	current := core.NewTextMessage("current order").WithHeader("example-run", "current")
	current.Partition = 1
	if err := handler(context.Background(), current); err != nil {
		t.Fatalf("transient failure escaped in-process retries: %v", err)
	}
	if got := <-received; got != "partition=1 current order" {
		t.Fatalf("result=%q", got)
	}
}

func TestRPCHandlerPreservesReplyRoutingAndFailures(t *testing.T) {
	broker := testkit.NewMockBroker()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	handler := rpcHandler("current", broker)
	request := core.NewTextMessage("request").WithHeader("example-run", "previous")
	if err := handler(context.Background(), request); err != nil {
		t.Fatalf("retained request should be ignored: %v", err)
	}
	broker.AssertNotPublished(t, "replies")
	request.WithHeader("example-run", "current")
	if err := handler(context.Background(), request); !errors.Is(err, core.ErrNoReplyTo) {
		t.Fatalf("missing reply destination error=%v", err)
	}
	request.WithReplyTo("replies").WithCorrelationID("call-42")
	if err := handler(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	published := broker.PublishedTo("replies")
	if len(published) != 1 || published[0].Message.CorrelationID != "call-42" || published[0].Message.BodyString() != "pong: request" {
		t.Fatalf("reply=%+v", published)
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handler(context.Background(), request); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("failed reply publish was acknowledged: %v", err)
	}
}

func TestRunWithLiveBrokers(t *testing.T) {
	if os.Getenv("WEAVE_EXAMPLES_E2E") != "1" {
		t.Skip("set WEAVE_EXAMPLES_E2E=1 and provision the example brokers/topics")
	}
	for _, backend := range []string{"amqp", "kafka"} {
		t.Run(backend, func(t *testing.T) {
			for range 2 {
				ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
				var output bytes.Buffer
				err := run(ctx, backend, "weave.example.client.replies", &output)
				cancel()
				if err != nil {
					t.Fatalf("example failed: %v\n%s", err, output.String())
				}
				lines := strings.Split(strings.TrimSpace(output.String()), "\n")
				if len(lines) != 4 || !strings.HasPrefix(lines[0], "weave.example.users: ") || !strings.HasPrefix(lines[1], "weave.example.orders: ") || lines[2] != "weave.example.rpc: pong: request" || lines[3] != lines[2] {
					t.Fatalf("unexpected output: %s", output.String())
				}
				if backend == "kafka" && !strings.HasPrefix(lines[1], "weave.example.orders: partition=1 ") {
					t.Fatalf("explicit partition was lost: %s", lines[1])
				}
			}
		})
	}
}
