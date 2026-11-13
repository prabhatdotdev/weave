package kafka

import (
	"context"
	"testing"

	"github.com/prabhatdotdev/weave/core"
)

func liveAssignment(t *testing.T, ctx context.Context, events <-chan core.Event, topic string, after int32) core.Event {
	t.Helper()
	for {
		select {
		case event := <-events:
			if event.Name == core.EventConsumerAssigned && event.Destination == topic && event.Fields["generation_id"].(int32) > after {
				return event
			}
		case <-ctx.Done():
			t.Fatal("missing consumer assignment after rebalance")
		}
	}
}

func TestConsumerReadinessAndRebalanceWithLiveKafka(t *testing.T) {
	firstEvents, secondEvents := make(chan core.Event, 64), make(chan core.Event, 64)
	hook := func(events chan<- core.Event) core.EventHook {
		return func(_ context.Context, event core.Event) {
			if event.Name == core.EventConsumerAssigned || event.Name == core.EventConsumerRevoked || event.Name == core.EventRebalanceStarted || event.Name == core.EventRebalanceCompleted {
				events <- event
			}
		}
	}
	first, topics, ctx := newLiveKafkaBrokerWithConfig(t, "readiness", 1, 1, func(config *core.Config) { config.EventHook = hook(firstEvents) })
	topic := topics[0]
	if err := first.Subscribe(ctx, topic, func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := first.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	initial := liveAssignment(t, ctx, firstEvents, topic, -1)
	config := *first.config
	kafkaConfig := *first.kafkaConfig
	config.Kafka, config.EventHook = &kafkaConfig, hook(secondEvents)
	config.Kafka.ClientID += "-second"
	secondAny, err := NewBroker(&config)
	if err != nil {
		t.Fatal(err)
	}
	second := secondAny.(*Broker)
	t.Cleanup(func() { _ = second.Close() })
	if err := second.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Subscribe(ctx, topic, func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := second.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	joined := liveAssignment(t, ctx, secondEvents, topic, initial.Fields["generation_id"].(int32))
	rebalanced := liveAssignment(t, ctx, firstEvents, topic, initial.Fields["generation_id"].(int32))
	if err := first.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	firstPartitions := rebalanced.Fields["partitions"].([]int32)
	secondPartitions := joined.Fields["partitions"].([]int32)
	if len(firstPartitions)+len(secondPartitions) != 1 || !first.IsReady() || !second.IsReady() {
		t.Fatalf("both members must be ready, including the idle member: first=%v second=%v", firstPartitions, secondPartitions)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := liveAssignment(t, ctx, firstEvents, topic, rebalanced.Fields["generation_id"].(int32))
	if len(recovered.Fields["partitions"].([]int32)) != 1 {
		t.Fatal("remaining member did not regain the partition")
	}
	if err := first.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}
