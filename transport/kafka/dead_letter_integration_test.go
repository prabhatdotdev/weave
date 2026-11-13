package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

func TestDeadLetterDestinationFailureWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "dead-letter", 1)
	source, destination := topics[0], topics[0]+".dead"
	admin, err := sarama.NewClusterAdmin(broker.kafkaConfig.Brokers, broker.buildSaramaConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(destination); _ = admin.Close() })
	type failedHandoff struct {
		err      error
		original core.DeadLetterMessage
	}
	firstFailure, published := make(chan failedHandoff, 1), make(chan struct{}, 1)
	redelivered, release := make(chan struct{}), make(chan struct{})
	var redeliveryOnce sync.Once
	var publishes atomic.Int32
	publisher := deadLetterPublisherFunc(func(ctx context.Context, target string, msg *core.Message, opts ...core.PublishOption) error {
		if publishes.Add(1) == 1 {
			options := append([]core.PublishOption(nil), opts...)
			err := broker.Publish(ctx, target, msg, append(options, core.WithTimeout(200*time.Millisecond))...)
			envelope, decodeErr := core.DecodeDeadLetterMessage(msg)
			if decodeErr != nil {
				return decodeErr
			}
			firstFailure <- failedHandoff{err: err, original: envelope.Original}
			return err
		}
		redeliveryOnce.Do(func() { close(redelivered) })
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		err := broker.Publish(ctx, target, msg, opts...)
		if err == nil {
			published <- struct{}{}
		}
		return err
	})
	handler, err := core.DeadLetterHandler(func(context.Context, *core.Message) error {
		return errors.New("poison order")
	}, core.RetryPolicy{MaxAttempts: 3}, publisher, destination, core.DeadLetterOptions{Backend: "kafka", Destination: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, source, core.NewTextMessage("original")); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(ctx, source, handler); err != nil {
		t.Fatal(err)
	}
	var original core.DeadLetterMessage
	select {
	case failed := <-firstFailure:
		if failed.err == nil {
			t.Fatal("missing topic unexpectedly accepted the dead-letter publish")
		}
		original = failed.original
	case <-ctx.Done():
		t.Fatal("missing destination was not reported")
	}
	select {
	case <-redelivered:
	case <-ctx.Done():
		t.Fatal("failed handoff advanced past the original offset")
	}
	offsets, err := admin.ListConsumerGroupOffsets(broker.kafkaConfig.ConsumerGroup, map[string][]int32{source: {original.Partition}})
	if err != nil {
		t.Fatal(err)
	}
	if block := offsets.GetBlock(source, original.Partition); block != nil && block.Offset > original.Offset {
		t.Fatalf("failed handoff committed original offset: %+v", block)
	}
	if err := admin.CreateTopic(destination, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-published:
	case <-ctx.Done():
		t.Fatal("restored destination did not receive the handoff")
	}
	if err := broker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if publishes.Load() < 2 {
		t.Fatalf("publishes=%d, want at least 2", publishes.Load())
	}
	consumer, err := sarama.NewConsumer(broker.kafkaConfig.Brokers, broker.buildSaramaConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partition, err := consumer.ConsumePartition(destination, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()
	select {
	case record := <-partition.Messages():
		envelope, err := core.DecodeDeadLetterMessage(&core.Message{Body: record.Value})
		if err != nil || envelope.Attempt != 3 || envelope.Original.Offset != original.Offset || envelope.Original.Partition != original.Partition || string(envelope.Original.Body) != "original" {
			t.Fatalf("unexpected dead letter: %+v err=%v", envelope, err)
		}
	case <-ctx.Done():
		t.Fatal("dead-letter record is missing")
	}
	offsets, err = admin.ListConsumerGroupOffsets(broker.kafkaConfig.ConsumerGroup, map[string][]int32{source: {original.Partition}})
	if err != nil {
		t.Fatal(err)
	}
	if block := offsets.GetBlock(source, original.Partition); block == nil || block.Err != sarama.ErrNoError || block.Offset != original.Offset+1 {
		t.Fatalf("confirmed handoff did not commit source: %+v", block)
	}
}
