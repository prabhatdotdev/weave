package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

type fakeSyncProducer struct {
	mu        sync.Mutex
	lastMsg   *sarama.ProducerMessage
	sendErr   error
	afterSend func(*sarama.ProducerMessage)
	closed    bool
}

func (p *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.mu.Lock()
	p.lastMsg = msg
	afterSend := p.afterSend
	err := p.sendErr
	p.mu.Unlock()
	if afterSend != nil {
		afterSend(msg)
	}
	return 0, 0, err
}

func (p *fakeSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	if len(msgs) > 0 {
		_, _, err := p.SendMessage(msgs[0])
		return err
	}
	return nil
}
func (p *fakeSyncProducer) Close() error                            { p.closed = true; return nil }
func (p *fakeSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (p *fakeSyncProducer) IsTransactional() bool                   { return false }
func (p *fakeSyncProducer) BeginTxn() error                         { return nil }
func (p *fakeSyncProducer) CommitTxn() error                        { return nil }
func (p *fakeSyncProducer) AbortTxn() error                         { return nil }
func (p *fakeSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (p *fakeSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

type fakeConsumerGroup struct {
	mu         sync.Mutex
	consumeErr error
	consumeFn  func(context.Context, []string, sarama.ConsumerGroupHandler) error
	errorsCh   chan error
	closed     bool
	consumed   []string
}

func (g *fakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	g.mu.Lock()
	g.consumed = append(g.consumed, topics...)
	consumeFn := g.consumeFn
	err := g.consumeErr
	g.mu.Unlock()

	if consumeFn != nil {
		return consumeFn(ctx, topics, handler)
	}

	if err == nil {
		select {
		case <-ctx.Done():
			return nil
		}
	}
	return err
}
func (g *fakeConsumerGroup) Errors() <-chan error      { return g.errorsCh }
func (g *fakeConsumerGroup) Close() error              { g.closed = true; return nil }
func (g *fakeConsumerGroup) Pause(map[string][]int32)  {}
func (g *fakeConsumerGroup) Resume(map[string][]int32) {}
func (g *fakeConsumerGroup) PauseAll()                 {}
func (g *fakeConsumerGroup) ResumeAll()                {}

type fakeSession struct {
	marked []*sarama.ConsumerMessage
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "member" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	s.marked = append(s.marked, msg)
}
func (s *fakeSession) Context() context.Context { return context.Background() }

type fakeClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return "users" }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func headerValue(msg *sarama.ProducerMessage, key string) string {
	for _, header := range msg.Headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}

func TestNewBrokerUsesDefaultKafkaConfig(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(&core.Config{})
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	if broker.kafkaConfig == nil || len(broker.kafkaConfig.Brokers) != 1 || broker.kafkaConfig.Brokers[0] != "localhost:9092" {
		t.Fatalf("default Kafka config not applied: %#v", broker.kafkaConfig)
	}
}

func TestBrokerLifecycleAndDisconnectedOperations(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(&core.Config{Kafka: core.DefaultKafkaConfig()})
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)

	if broker.Backend() != backendName {
		t.Fatalf("Backend() = %q, want %q", broker.Backend(), backendName)
	}
	if broker.IsConnected() {
		t.Fatal("new broker should not report connected")
	}

	msg := core.NewTextMessage("payload")
	if err := broker.Publish(context.Background(), "orders", msg); !core.IsNotConnected(err) {
		t.Fatalf("Publish() error = %v, want ErrNotConnected", err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }); !core.IsNotConnected(err) {
		t.Fatalf("Subscribe() error = %v, want ErrNotConnected", err)
	}
	if _, err := broker.Call(context.Background(), "orders", msg); !core.IsNotConnected(err) {
		t.Fatalf("Call() error = %v, want ErrNotConnected", err)
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if broker.IsConnected() {
		t.Fatal("closed broker should not report connected")
	}
	if err := broker.Connect(context.Background()); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Connect() after Close() error = %v, want ErrClosed", err)
	}
}

func TestPublishBuildsKafkaMessageAndErrors(t *testing.T) {
	t.Parallel()

	producer := &fakeSyncProducer{}
	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: core.DefaultKafkaConfig(),
		producer:    producer,
		pending:     make(map[string]chan *core.Message),
		closeChan:   make(chan struct{}),
		connected:   true,
	}

	msg := core.NewTextMessage("payload")
	msg.Subject = "user-42"
	msg.Headers = map[string]string{"trace-id": "abc"}
	msg.CorrelationID = "corr-1"
	msg.ReplyTo = "reply-topic"
	msg.ContentType = "application/json"

	if err := broker.Publish(context.Background(), "users", msg); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if producer.lastMsg == nil {
		t.Fatal("producer did not receive a message")
	}
	if producer.lastMsg.Topic != "users" {
		t.Fatalf("topic = %q, want %q", producer.lastMsg.Topic, "users")
	}
	if key, _ := producer.lastMsg.Key.Encode(); string(key) != "user-42" {
		t.Fatalf("key = %q, want %q", string(key), "user-42")
	}
	if got := headerValue(producer.lastMsg, "trace-id"); got != "abc" {
		t.Fatalf("trace-id header = %q, want %q", got, "abc")
	}
	if got := headerValue(producer.lastMsg, "correlation-id"); got != "corr-1" {
		t.Fatalf("correlation-id header = %q, want %q", got, "corr-1")
	}
	if got := headerValue(producer.lastMsg, "reply-to"); got != "reply-topic" {
		t.Fatalf("reply-to header = %q, want %q", got, "reply-topic")
	}
	if got := headerValue(producer.lastMsg, "content-type"); got != "application/json" {
		t.Fatalf("content-type header = %q, want %q", got, "application/json")
	}

	producer.sendErr = errors.New("send failed")
	if err := broker.Publish(context.Background(), "users", msg); err == nil || !errors.As(err, new(*core.ErrPublishFailed)) {
		t.Fatalf("Publish() error = %v, want ErrPublishFailed", err)
	}
}

func TestSubscribeRequiresConsumerGroup(t *testing.T) {
	t.Parallel()

	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: &core.KafkaConfig{},
		pending:     make(map[string]chan *core.Message),
		closeChan:   make(chan struct{}),
		connected:   true,
	}

	err := broker.Subscribe(context.Background(), "users", func(context.Context, *core.Message) error { return nil })
	var subscribeErr *core.ErrSubscribeFailed
	if !errors.As(err, &subscribeErr) {
		t.Fatalf("Subscribe() error = %v, want ErrSubscribeFailed", err)
	}
}

func TestCallTimeoutAndCorrelationFlow(t *testing.T) {
	t.Run("timeout returns ErrTimeout", func(t *testing.T) {
		producer := &fakeSyncProducer{}
		broker := &Broker{
			config:      &core.Config{},
			kafkaConfig: core.DefaultKafkaConfig(),
			producer:    producer,
			pending:     make(map[string]chan *core.Message),
			closeChan:   make(chan struct{}),
			connected:   true,
			replyTopic:  "reply-topic",
		}

		_, err := broker.Call(context.Background(), "users.get", core.NewTextMessage("42"), core.WithTimeout(1*time.Millisecond))
		if !core.IsTimeout(err) {
			t.Fatalf("Call() error = %v, want ErrTimeout", err)
		}
		if len(broker.pending) != 0 {
			t.Fatalf("pending map length after timeout = %d, want 0", len(broker.pending))
		}
	})

	t.Run("response routed by correlation id", func(t *testing.T) {
		broker := &Broker{
			config:      &core.Config{},
			kafkaConfig: core.DefaultKafkaConfig(),
			pending:     make(map[string]chan *core.Message),
			closeChan:   make(chan struct{}),
			connected:   true,
			replyTopic:  "reply-topic",
		}

		producer := &fakeSyncProducer{}
		producer.afterSend = func(msg *sarama.ProducerMessage) {
			corrID := headerValue(msg, "correlation-id")
			broker.pendingMu.RLock()
			respChan := broker.pending[corrID]
			broker.pendingMu.RUnlock()
			respChan <- core.NewTextMessage("response")
		}
		broker.producer = producer

		response, err := broker.Call(context.Background(), "users.get", core.NewTextMessage("42"), core.WithTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Call() error = %v", err)
		}
		if response.BodyString() != "response" {
			t.Fatalf("response.BodyString() = %q, want %q", response.BodyString(), "response")
		}
	})
}

func TestCancelAllPendingClosesChannels(t *testing.T) {
	t.Parallel()

	broker := &Broker{pending: make(map[string]chan *core.Message)}
	ch1 := make(chan *core.Message)
	ch2 := make(chan *core.Message)
	broker.pending["one"] = ch1
	broker.pending["two"] = ch2

	broker.cancelAllPending()

	if len(broker.pending) != 0 {
		t.Fatalf("pending map length = %d, want 0", len(broker.pending))
	}
	if _, ok := <-ch1; ok {
		t.Fatal("pending channel ch1 should be closed")
	}
	if _, ok := <-ch2; ok {
		t.Fatal("pending channel ch2 should be closed")
	}
}

func TestConsumerGroupHandlerRoutesResponsesAndInvokesHandlers(t *testing.T) {
	t.Run("routes response by correlation id", func(t *testing.T) {
		pending := map[string]chan *core.Message{"corr-1": make(chan *core.Message, 1)}
		handlerCalled := false
		handler := &consumerGroupHandler{
			handler: func(context.Context, *core.Message) error {
				handlerCalled = true
				return nil
			},
			closeChan: make(chan struct{}),
			pending:   pending,
			pendingMu: &sync.RWMutex{},
		}

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{
			Topic:     "users",
			Partition: 0,
			Offset:    3,
			Headers:   []*sarama.RecordHeader{{Key: []byte("correlation-id"), Value: []byte("corr-1")}},
		}
		close(claim.messages)

		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		select {
		case response := <-pending["corr-1"]:
			if response.CorrelationID != "corr-1" {
				t.Fatalf("response.CorrelationID = %q, want %q", response.CorrelationID, "corr-1")
			}
		default:
			t.Fatal("pending response channel did not receive a message")
		}
		if handlerCalled {
			t.Fatal("handler should not be called for pending correlation responses")
		}
		if len(session.marked) != 1 {
			t.Fatalf("marked messages = %d, want 1", len(session.marked))
		}
	})

	t.Run("invokes handler for normal messages", func(t *testing.T) {
		var got *core.Message
		handler := &consumerGroupHandler{
			handler: func(ctx context.Context, msg *core.Message) error {
				got = msg
				return nil
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]chan *core.Message),
			pendingMu: &sync.RWMutex{},
		}

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{
			Topic:     "users",
			Partition: 1,
			Offset:    8,
			Key:       []byte("subject"),
			Value:     []byte("payload"),
			Timestamp: time.Unix(1700000000, 0),
			Headers: []*sarama.RecordHeader{
				{Key: []byte("reply-to"), Value: []byte("reply-topic")},
				{Key: []byte("content-type"), Value: []byte("application/json")},
				{Key: []byte("trace-id"), Value: []byte("abc")},
			},
		}
		close(claim.messages)

		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		if got == nil {
			t.Fatal("handler was not called")
		}
		if got.Subject != "subject" || got.ReplyTo != "reply-topic" || got.ContentType != "application/json" || got.GetHeader("trace-id") != "abc" || got.Partition != 1 || got.Offset != 8 {
			t.Fatalf("unexpected converted message: %#v", got)
		}
		if len(session.marked) != 1 {
			t.Fatalf("marked messages = %d, want 1", len(session.marked))
		}
	})

	t.Run("handler error commits by default no-retry policy", func(t *testing.T) {
		handler := &consumerGroupHandler{
			handler: func(context.Context, *core.Message) error {
				return errors.New("boom")
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]chan *core.Message),
			pendingMu: &sync.RWMutex{},
			policy:    core.HandlerErrorNoRetry,
		}

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 11}
		close(claim.messages)

		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		if len(session.marked) != 1 {
			t.Fatalf("marked messages = %d, want 1", len(session.marked))
		}
	})

	t.Run("handler error does not commit when retry policy enabled", func(t *testing.T) {
		handler := &consumerGroupHandler{
			handler: func(context.Context, *core.Message) error {
				return errors.New("boom")
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]chan *core.Message),
			pendingMu: &sync.RWMutex{},
			policy:    core.HandlerErrorRetry,
		}

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 12}
		close(claim.messages)

		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		if len(session.marked) != 0 {
			t.Fatalf("marked messages = %d, want 0", len(session.marked))
		}
	})
}

func TestPublishReconnectsWhenDisconnected(t *testing.T) {
	producer := &fakeSyncProducer{}
	createProducerCalls := 0

	broker := &Broker{
		config: &core.Config{
			ConnectionRetry: 0,
			RetryDelay:      time.Millisecond,
		},
		kafkaConfig:   &core.KafkaConfig{Brokers: []string{"kafka:9092"}},
		pending:       make(map[string]chan *core.Message),
		closeChan:     make(chan struct{}),
		everConnected: true,
		newSyncProducer: func(_ []string, _ *sarama.Config) (sarama.SyncProducer, error) {
			createProducerCalls++
			return producer, nil
		},
		newConsumerGroup: func(_ []string, _ string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
			return nil, nil
		},
	}

	err := broker.Publish(context.Background(), "users", core.NewTextMessage("payload"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if createProducerCalls != 1 {
		t.Fatalf("producer factory calls = %d, want 1", createProducerCalls)
	}
	if producer.lastMsg == nil || producer.lastMsg.Topic != "users" {
		t.Fatalf("unexpected producer message: %#v", producer.lastMsg)
	}
}

func TestCallReturnsConnectionLostWhenKafkaConnectionDrops(t *testing.T) {
	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: core.DefaultKafkaConfig(),
		pending:     make(map[string]chan *core.Message),
		closeChan:   make(chan struct{}),
		connected:   true,
		replyTopic:  "reply-topic",
	}

	producer := &fakeSyncProducer{}
	producer.afterSend = func(*sarama.ProducerMessage) {
		broker.handleConnectionLoss(errors.New("connection lost"))
	}
	broker.producer = producer

	_, err := broker.Call(context.Background(), "users.get", core.NewTextMessage("42"), core.WithTimeout(500*time.Millisecond))
	if !core.IsConnectionLost(err) {
		t.Fatalf("Call() error = %v, want ErrConnectionLost", err)
	}
}

func TestSubscribeReconnectsAndResumesConsume(t *testing.T) {
	firstProducer := &fakeSyncProducer{}
	secondProducer := &fakeSyncProducer{}
	firstGroup := &fakeConsumerGroup{}
	secondGroup := &fakeConsumerGroup{}

	firstGroup.consumeFn = func(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		return errors.New("broker unavailable")
	}
	secondGroup.consumeFn = func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		<-ctx.Done()
		return nil
	}

	producerQueue := []sarama.SyncProducer{firstProducer, secondProducer}
	consumerQueue := []sarama.ConsumerGroup{firstGroup, secondGroup}

	broker := &Broker{
		config: &core.Config{
			ConnectionRetry: 0,
			RetryDelay:      time.Millisecond,
		},
		kafkaConfig: &core.KafkaConfig{
			Brokers:       []string{"kafka:9092"},
			ConsumerGroup: "workers",
		},
		pending:   make(map[string]chan *core.Message),
		closeChan: make(chan struct{}),
		newSyncProducer: func(_ []string, _ *sarama.Config) (sarama.SyncProducer, error) {
			if len(producerQueue) == 0 {
				return nil, errors.New("no producer available")
			}
			producer := producerQueue[0]
			producerQueue = producerQueue[1:]
			return producer, nil
		},
		newConsumerGroup: func(_ []string, _ string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
			if len(consumerQueue) == 0 {
				return nil, errors.New("no consumer group available")
			}
			consumer := consumerQueue[0]
			consumerQueue = consumerQueue[1:]
			return consumer, nil
		},
	}

	if err := broker.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	if err := broker.Subscribe(context.Background(), "users", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	deadline := time.After(1 * time.Second)
	for {
		firstGroup.mu.Lock()
		firstSeen := len(firstGroup.consumed) > 0
		firstGroup.mu.Unlock()
		secondGroup.mu.Lock()
		secondSeen := len(secondGroup.consumed) > 0
		secondGroup.mu.Unlock()

		if firstSeen && secondSeen {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("subscription did not resume after reconnect: firstSeen=%v secondSeen=%v", firstSeen, secondSeen)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestSubscribeConsumerErrorsTriggerDisconnectHandling(t *testing.T) {
	errorEvents := make(chan core.Event, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group := &fakeConsumerGroup{errorsCh: make(chan error, 1)}
	group.consumeFn = func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		<-ctx.Done()
		return nil
	}

	broker := &Broker{
		config: &core.Config{
			EventHook: func(_ context.Context, event core.Event) {
				select {
				case errorEvents <- event:
				default:
				}
			},
		},
		kafkaConfig: &core.KafkaConfig{ConsumerGroup: "workers"},
		consumer:    group,
		connected:   true,
		pending:     make(map[string]chan *core.Message),
		watching:    make(map[sarama.ConsumerGroup]struct{}),
		closeChan:   make(chan struct{}),
	}

	if err := broker.Subscribe(ctx, "users", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	group.errorsCh <- errors.New("consumer stream broken")

	deadline := time.After(500 * time.Millisecond)
	for broker.IsConnected() {
		select {
		case <-deadline:
			t.Fatal("broker did not handle consumer error and mark connection lost")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	found := false
	for {
		select {
		case event := <-errorEvents:
			if event.Operation == "consumer_errors" && event.Name == core.EventSubscribeFailed {
				found = true
			}
		default:
			if !found {
				t.Fatal("expected subscribe failure event for consumer error")
			}
			return
		}
	}
}

func TestTrackedSubscriptionsRestoreAfterReconnectForMultipleDestinations(t *testing.T) {
	firstProducer := &fakeSyncProducer{}
	secondProducer := &fakeSyncProducer{}
	firstGroup := &fakeConsumerGroup{}
	secondGroup := &fakeConsumerGroup{}

	firstGroup.consumeFn = func(_ context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		return errors.New("broker unavailable")
	}
	secondGroup.consumeFn = func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
		<-ctx.Done()
		return nil
	}

	producerQueue := []sarama.SyncProducer{firstProducer, secondProducer}
	consumerQueue := []sarama.ConsumerGroup{firstGroup, secondGroup}

	broker := &Broker{
		config: &core.Config{
			ConnectionRetry: 0,
			RetryDelay:      time.Millisecond,
		},
		kafkaConfig: &core.KafkaConfig{
			Brokers:       []string{"kafka:9092"},
			ConsumerGroup: "workers",
		},
		pending:   make(map[string]chan *core.Message),
		closeChan: make(chan struct{}),
		newSyncProducer: func(_ []string, _ *sarama.Config) (sarama.SyncProducer, error) {
			if len(producerQueue) == 0 {
				return nil, errors.New("no producer available")
			}
			producer := producerQueue[0]
			producerQueue = producerQueue[1:]
			return producer, nil
		},
		newConsumerGroup: func(_ []string, _ string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
			if len(consumerQueue) == 0 {
				return nil, errors.New("no consumer group available")
			}
			consumer := consumerQueue[0]
			consumerQueue = consumerQueue[1:]
			return consumer, nil
		},
	}

	if err := broker.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	if err := broker.Subscribe(context.Background(), "users", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe(users) error = %v", err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe(orders) error = %v", err)
	}

	deadline := time.After(1 * time.Second)
	for {
		secondGroup.mu.Lock()
		hasUsers := false
		hasOrders := false
		consumedSnapshot := append([]string(nil), secondGroup.consumed...)
		for _, topic := range secondGroup.consumed {
			if topic == "users" {
				hasUsers = true
			}
			if topic == "orders" {
				hasOrders = true
			}
		}
		secondGroup.mu.Unlock()

		if hasUsers && hasOrders {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("tracked subscriptions were not restored after reconnect: consumed=%v", consumedSnapshot)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
