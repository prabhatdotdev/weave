package amqp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestConsumerReadinessWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_AMQP_E2E") != "1" {
		t.Skip("set WEAVE_AMQP_E2E=1")
	}
	conn := dialLiveRabbitMQ(t)
	t.Cleanup(func() { _ = conn.Close() })
	admin, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	queue := fmt.Sprintf("weave-readiness-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = admin.QueueDelete(queue, false, false, false) })
	events := make(chan core.Event, 16)
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.QueueDurable = true
	config.EventHook = func(_ context.Context, event core.Event) {
		if event.Name == core.EventConsumerAssigned || event.Name == core.EventConsumerRevoked {
			events <- event
		}
	}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	subCtx, unsubscribe := context.WithCancel(ctx)
	defer unsubscribe()
	if err := broker.Subscribe(subCtx, queue, func(context.Context, *core.Message) error { return nil }, core.WithConsumerTag("readiness-test")); err != nil {
		t.Fatal(err)
	}
	if err := broker.WaitReady(ctx); err != nil || !broker.IsReady() {
		t.Fatalf("accepted consumer not ready: %v", err)
	}
	awaitAMQPReadinessEvent(t, events, core.EventConsumerAssigned, queue)
	broker.closeMu.RLock()
	channel := broker.channel
	broker.closeMu.RUnlock()
	if err := channel.Cancel("readiness-test", false); err != nil {
		t.Fatal(err)
	}
	awaitAMQPReadinessEvent(t, events, core.EventConsumerRevoked, queue)
	if broker.IsReady() || !broker.IsConnected() {
		t.Fatal("consumer readiness was not distinguished from connectivity")
	}
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if err := broker.WaitReady(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	unsubscribe()
	if err := broker.Subscribe(ctx, queue, func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := broker.WaitReady(ctx); err != nil {
		t.Fatalf("replacement consumer not ready: %v", err)
	}
}
