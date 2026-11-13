package amqp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestDeadLetterDestinationFailureWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_AMQP_E2E") != "1" {
		t.Skip("set WEAVE_AMQP_E2E=1 to run the RabbitMQ integration test")
	}
	conn := dialLiveRabbitMQ(t)
	t.Cleanup(func() { _ = conn.Close() })
	admin, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	source := fmt.Sprintf("weave-dead-letter-%d", time.Now().UnixNano())
	destination := source + ".dead"
	if _, err := admin.QueueDeclare(source, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{source, destination} {
		t.Cleanup(func() { _, _ = admin.QueueDelete(name, false, false, false) })
	}
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.QueueDurable = true
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	firstFailure, published := make(chan error, 1), make(chan error, 1)
	redelivered, release := make(chan struct{}), make(chan struct{})
	var publishes, attempts atomic.Int32
	publisher := deadLetterPublisherFunc(func(ctx context.Context, target string, msg *core.Message, opts ...core.PublishOption) error {
		if publishes.Add(1) == 1 {
			err := broker.Publish(ctx, target, msg, opts...)
			firstFailure <- err
			return err
		}
		close(redelivered)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		err := broker.Publish(ctx, target, msg, opts...)
		published <- err
		return err
	})
	handler, err := core.DeadLetterHandler(func(context.Context, *core.Message) error {
		attempts.Add(1)
		return errors.New("poison order")
	}, core.RetryPolicy{MaxAttempts: 3}, publisher, destination, core.DeadLetterOptions{Backend: "amqp", Destination: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(ctx, source, handler, core.WithWorkerCount(1), core.WithPrefetchCount(1)); err != nil {
		t.Fatal(err)
	}
	msg := core.NewTextMessage("original").WithHeader("trace", "trace-1")
	msg.MessageID = "original-id"
	if err := broker.Publish(ctx, source, msg, core.WithMandatory()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstFailure:
		if !errors.Is(err, core.ErrUnroutable) {
			t.Fatalf("missing target error=%v, want ErrUnroutable", err)
		}
	case <-ctx.Done():
		t.Fatal("missing destination was not reported")
	}
	select {
	case <-redelivered:
	case <-ctx.Done():
		t.Fatal("failed handoff discarded the original delivery")
	}
	if _, err := admin.QueueDeclare(destination, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-published:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("restored destination did not receive the handoff")
	}
	if err := broker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 6 || publishes.Load() != 2 {
		t.Fatalf("attempts=%d publishes=%d", attempts.Load(), publishes.Load())
	}
	dead, ok, err := admin.Get(destination, true)
	if err != nil || !ok {
		t.Fatalf("dead-letter missing: ok=%v err=%v", ok, err)
	}
	envelope, err := core.DecodeDeadLetterMessage(&core.Message{Body: dead.Body})
	if err != nil || envelope.Attempt != 3 || envelope.Original.MessageID != "original-id" || string(envelope.Original.Body) != "original" || envelope.Original.Headers["trace"] != "trace-1" || dead.DeliveryMode != 2 {
		t.Fatalf("unexpected dead-letter delivery: %+v mode=%d err=%v", envelope, dead.DeliveryMode, err)
	}
	if _, ok, err := admin.Get(source, true); err != nil || ok {
		t.Fatalf("confirmed handoff left the source unacknowledged: ok=%v err=%v", ok, err)
	}
}
