package amqp

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestShutdownDrainsAndRequeuesWithLiveRabbitMQ(t *testing.T) {
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
	queue := fmt.Sprintf("weave-shutdown-%d", time.Now().UnixNano())
	replyQueue := queue + ".replies"
	for _, name := range []string{queue, replyQueue} {
		if _, err := admin.QueueDeclare(name, true, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
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
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	if err := broker.Subscribe(ctx, queue, func(handlerCtx context.Context, _ *core.Message) error {
		if calls.Add(1) == 1 {
			started <- handlerCtx
		}
		select {
		case <-release:
			return broker.Publish(handlerCtx, replyQueue, core.NewTextMessage("completed"), core.WithMandatory())
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		}
	}, core.WithWorkerCount(1), core.WithPrefetchCount(2)); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"inflight", "queued"} {
		if err := broker.Publish(ctx, queue, core.NewTextMessage(body), core.WithMandatory()); err != nil {
			t.Fatal(err)
		}
	}
	var handlerCtx context.Context
	select {
	case handlerCtx = <-started:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	waitQueue := func(match func(amqplib.Queue) bool) {
		t.Helper()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			state, err := admin.QueueInspect(queue)
			if err != nil {
				t.Fatal(err)
			}
			if match(state) {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatalf("queue did not reach expected state: %+v", state)
			}
		}
	}
	waitQueue(func(q amqplib.Queue) bool { return q.Messages == 0 && q.Consumers == 1 })
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	waitQueue(func(q amqplib.Queue) bool { return q.Consumers == 0 })
	broker.closeMu.RLock()
	connected := broker.conn != nil
	broker.closeMu.RUnlock()
	if !connected || handlerCtx.Err() != nil {
		t.Fatal("shutdown closed the connection or interrupted the active handler before draining")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want only the admitted message", calls.Load())
	}
	queued, ok, err := admin.Get(queue, false)
	if err != nil || !ok || string(queued.Body) != "queued" || !queued.Redelivered {
		t.Fatalf("unstarted delivery was not preserved: body=%q redelivered=%v ok=%v err=%v", queued.Body, queued.Redelivered, ok, err)
	}
	if err := queued.Ack(false); err != nil {
		t.Fatal(err)
	}
	if reply, ok, err := admin.Get(replyQueue, true); err != nil || !ok || string(reply.Body) != "completed" {
		t.Fatalf("handler reply was not confirmed: body=%q ok=%v err=%v", reply.Body, ok, err)
	}
	if _, ok, err := admin.Get(queue, true); err != nil || ok {
		t.Fatalf("completed delivery was not acknowledged: extra=%v err=%v", ok, err)
	}
}
