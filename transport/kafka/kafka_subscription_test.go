package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

func TestSubscriptionOptionsSelectConsumerGroupsAndOffsets(t *testing.T) {
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.ConsumerGroup = "default"
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	broker.newProducer = func([]string, *sarama.Config) (messageProducer, error) {
		return &fakeSyncProducer{}, nil
	}
	var mu sync.Mutex
	created, active, maximum := map[string]int{}, map[string]int{}, map[string]int{}
	offsets := map[string]int64{}
	broker.newConsumerGroup = func(_ []string, group string, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
		mu.Lock()
		created[group]++
		offsets[group] = cfg.Consumer.Offsets.Initial
		mu.Unlock()
		return &fakeConsumerGroup{consumeFn: func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
			mu.Lock()
			active[group]++
			maximum[group] = max(maximum[group], active[group])
			mu.Unlock()
			defer func() { mu.Lock(); active[group]--; mu.Unlock() }()
			for _, topic := range topics {
				messages := make(chan *sarama.ConsumerMessage, 1)
				messages <- &sarama.ConsumerMessage{Topic: topic, Value: []byte(group)}
				close(messages)
				if err := handler.ConsumeClaim(&fakeSession{ctx: ctx}, &fakeClaim{messages: messages}); err != nil {
					return err
				}
			}
			<-ctx.Done()
			return nil
		}}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	deliveries := make(chan string, 32)
	for _, subscription := range []struct {
		group, topic string
		opts         []core.SubscribeOption
	}{
		{"default", "orders", nil},
		{"alpha", "orders", []core.SubscribeOption{core.WithConsumerGroup("alpha")}},
		{"beta", "orders", []core.SubscribeOption{core.WithConsumerGroup("beta"), core.WithStartFromBeginning()}},
		{"alpha", "users", []core.SubscribeOption{core.WithConsumerGroup("alpha")}},
	} {
		if err := broker.Subscribe(context.Background(), subscription.topic, func(ctx context.Context, msg *core.Message) error {
			if msg.BodyString() != subscription.group {
				return fmt.Errorf("message reached group %s through %s", subscription.group, msg.BodyString())
			}
			select {
			case deliveries <- subscription.group + "/" + subscription.topic:
			case <-ctx.Done():
			}
			return nil
		}, subscription.opts...); err != nil {
			t.Fatalf("Subscribe(%s, %s): %v", subscription.group, subscription.topic, err)
		}
	}
	seen := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(seen) < 4 {
		select {
		case delivery := <-deliveries:
			seen[delivery] = true
		case <-timeout:
			t.Fatalf("subscriptions did not receive independently: %v", seen)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, group := range []string{"default", "alpha", "beta"} {
		if created[group] != 1 || maximum[group] != 1 {
			t.Errorf("group %s: created=%d, concurrent Consume=%d; want 1 each", group, created[group], maximum[group])
		}
		wantOffset := int64(sarama.OffsetNewest)
		if group == "beta" {
			wantOffset = sarama.OffsetOldest
		}
		if offsets[group] != wantOffset {
			t.Errorf("group %s initial offset = %d, want %d", group, offsets[group], wantOffset)
		}
	}
}

func TestSubscriptionConsumerCreationFailureIsReturned(t *testing.T) {
	for _, opts := range [][]core.SubscribeOption{
		{core.WithConsumerGroup("custom")},
		{core.WithStartFromBeginning()},
	} {
		failure := errors.New("consumer creation failed")
		broker := &Broker{
			config: &core.Config{}, kafkaConfig: &core.KafkaConfig{ConsumerGroup: "default"},
			producer: &fakeSyncProducer{}, consumer: &fakeConsumerGroup{}, connected: true,
			closeChan: make(chan struct{}),
			newConsumerGroup: func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) {
				return nil, failure
			},
		}
		err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }, opts...)
		if !errors.Is(err, failure) {
			t.Fatalf("Subscribe() error = %v, want consumer creation failure", err)
		}
		if groups := broker.activeSubscriptionGroups(); len(groups) != 0 {
			t.Fatalf("failed Subscribe() retained registrations: %v", groups)
		}
		_ = broker.Close()
	}
}

func TestSubscriptionOptionsSurviveReconnect(t *testing.T) {
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.ConsumerGroup = "default"
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	broker.runner = true
	broker.newProducer = func([]string, *sarama.Config) (messageProducer, error) {
		return &fakeSyncProducer{}, nil
	}
	created := map[consumerKey]int{}
	broker.newConsumerGroup = func(_ []string, group string, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
		if !cfg.Consumer.Return.Errors {
			t.Errorf("consumer group %q received disabled error reporting", group)
		}
		created[consumerKey{group: group, earliest: cfg.Consumer.Offsets.Initial == sarama.OffsetOldest}]++
		return &fakeConsumerGroup{}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	opts := []core.SubscribeOption{core.WithConsumerGroup("custom"), core.WithStartFromBeginning()}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }, opts...); err != nil {
		t.Fatal(err)
	}
	key := broker.subscriptionConsumerKey(core.ApplySubscribeOptions(opts...))
	previous := broker.currentConsumerFor(key).(*fakeConsumerGroup)
	broker.handleConnectionLoss(errors.New("disconnected"))
	if !previous.closed {
		t.Fatal("disconnected custom consumer was not closed")
	}
	if err := broker.ensureConnected(context.Background()); err != nil {
		t.Fatal(err)
	}
	if current := broker.currentConsumerFor(key); current == nil || current == previous {
		t.Fatal("custom consumer was not replaced after reconnect")
	}
	if created[key] != 2 || created[consumerKey{group: "default"}] != 2 {
		t.Fatalf("consumer configurations after reconnect = %v", created)
	}
}
