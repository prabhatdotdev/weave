package kafka

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

func TestShutdownDrainsAndCommitsWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "shutdown", 2)
	topic, replies := topics[0], topics[1]
	for _, body := range []string{"inflight", "queued"} {
		if err := broker.Publish(ctx, topic, core.NewTextMessage(body)); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan *core.Message, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	if err := broker.Subscribe(ctx, topic, func(handlerCtx context.Context, msg *core.Message) error {
		if calls.Add(1) == 1 {
			started <- msg
		}
		select {
		case <-release:
			return broker.Publish(handlerCtx, replies, core.NewTextMessage("completed"))
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	var first *core.Message
	select {
	case first = <-started:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("shutdown started an unadmitted handler: calls=%d", calls.Load())
	}
	admin, err := sarama.NewClusterAdmin(broker.kafkaConfig.Brokers, broker.buildSaramaConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	offsets, err := admin.ListConsumerGroupOffsets(broker.kafkaConfig.ConsumerGroup, map[string][]int32{topic: {first.Partition}})
	if err != nil {
		t.Fatal(err)
	}
	block := offsets.GetBlock(topic, first.Partition)
	if block == nil || block.Err != sarama.ErrNoError || block.Offset != first.Offset+1 {
		t.Fatalf("committed offset = %+v; want only completed offset %d", block, first.Offset+1)
	}
	restartedAny, err := NewBroker(broker.config)
	if err != nil {
		t.Fatal(err)
	}
	restarted := restartedAny.(*Broker)
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	received := make(chan *core.Message, 1)
	if err := restarted.Subscribe(ctx, topic, func(_ context.Context, msg *core.Message) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.BodyString() != "queued" || msg.Offset != first.Offset+1 || msg.Partition != first.Partition {
			t.Fatalf("replacement consumer received %+v, want next uncommitted record", msg)
		}
	case <-ctx.Done():
		t.Fatal("replacement consumer did not receive unstarted work")
	}
}
