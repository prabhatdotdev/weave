package kafka

import (
	"context"
	"errors"
	"strings"
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
	mu     sync.Mutex
	marked []*sarama.ConsumerMessage
	ctx    context.Context
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "member" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, msg)
}
func (s *fakeSession) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

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
		pending:     make(map[string]*pendingCall),
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
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
		connected:   true,
	}

	err := broker.Subscribe(context.Background(), "users", func(context.Context, *core.Message) error { return nil })
	var subscribeErr *core.ErrSubscribeFailed
	if !errors.As(err, &subscribeErr) {
		t.Fatalf("Subscribe() error = %v, want ErrSubscribeFailed", err)
	}
}

func TestSubscribeRejectsNegativeWorkerCount(t *testing.T) {
	t.Parallel()

	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: core.DefaultKafkaConfig(),
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
	}

	err := broker.Subscribe(context.Background(), "users", func(context.Context, *core.Message) error { return nil }, core.WithWorkerCount(-1))
	if !errors.Is(err, core.ErrInvalidConfig) {
		t.Fatalf("Subscribe() error = %v, want ErrInvalidConfig", err)
	}
}

func TestCallTimeoutAndCorrelationFlow(t *testing.T) {
	t.Run("timeout returns ErrTimeout", func(t *testing.T) {
		producer := &fakeSyncProducer{}
		broker := &Broker{
			config:      &core.Config{},
			kafkaConfig: core.DefaultKafkaConfig(),
			producer:    producer,
			pending:     make(map[string]*pendingCall),
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
			pending:     make(map[string]*pendingCall),
			closeChan:   make(chan struct{}),
			connected:   true,
			replyTopic:  "reply-topic",
		}

		producer := &fakeSyncProducer{}
		producer.afterSend = func(msg *sarama.ProducerMessage) {
			corrID := headerValue(msg, "correlation-id")
			broker.pendingMu.RLock()
			pending := broker.pending[corrID]
			broker.pendingMu.RUnlock()
			pending.complete(core.NewTextMessage("response"))
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

func TestReplyConsumerInitializationRetriesAfterFailure(t *testing.T) {
	producer := &fakeSyncProducer{}
	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: &core.KafkaConfig{ClientID: "test", ReplyTopic: "reply-topic"},
		producer:    producer,
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
		connected:   true,
	}
	defer broker.Close()

	if _, err := broker.Call(context.Background(), "users.get", core.NewTextMessage("first")); err == nil {
		t.Fatal("first Call() error = nil, want reply subscription failure")
	}
	if got := broker.currentReplyTopic(); got != "" {
		t.Fatalf("reply topic after failed initialization = %q, want empty", got)
	}
	broker.subsMu.RLock()
	failedSubscriptions := len(broker.subs)
	broker.subsMu.RUnlock()
	if failedSubscriptions != 0 {
		t.Fatalf("subscriptions after failed initialization = %d, want 0", failedSubscriptions)
	}

	broker.closeMu.Lock()
	broker.consumer = &fakeConsumerGroup{}
	broker.kafkaConfig.ConsumerGroup = "workers"
	broker.closeMu.Unlock()
	producer.afterSend = func(msg *sarama.ProducerMessage) {
		broker.pendingMu.RLock()
		pending := broker.pending[headerValue(msg, "correlation-id")]
		broker.pendingMu.RUnlock()
		pending.complete(core.NewTextMessage("response"))
	}

	response, err := broker.Call(context.Background(), "users.get", core.NewTextMessage("second"), core.WithTimeout(time.Second))
	if err != nil {
		t.Fatalf("second Call() error = %v", err)
	}
	if response.BodyString() != "response" {
		t.Fatalf("second Call() response = %q, want response", response.BodyString())
	}
	if got := broker.currentReplyTopic(); got == "" {
		t.Fatal("reply topic after successful retry is empty")
	}
	broker.subsMu.RLock()
	successfulSubscriptions := len(broker.subs)
	broker.subsMu.RUnlock()
	if successfulSubscriptions != 1 {
		t.Fatalf("subscriptions after successful retry = %d, want 1", successfulSubscriptions)
	}
}

func TestCallRequiresConfiguredReplyTopic(t *testing.T) {
	producer := &fakeSyncProducer{}
	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: &core.KafkaConfig{ConsumerGroup: "workers"},
		producer:    producer,
		consumer:    &fakeConsumerGroup{},
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
		connected:   true,
	}

	response, err := broker.Call(context.Background(), "users.get", core.NewTextMessage("request"))
	if response != nil || !errors.Is(err, core.ErrInvalidConfig) {
		t.Fatalf("Call() = %#v, %v; want nil, ErrInvalidConfig", response, err)
	}
	if producer.lastMsg != nil {
		t.Fatal("Call() published before validating the reply topic")
	}
	broker.subsMu.RLock()
	defer broker.subsMu.RUnlock()
	if len(broker.subs) != 0 {
		t.Fatalf("subscriptions after invalid Call() = %d, want 0", len(broker.subs))
	}
}

func TestSaramaConfigDisablesAutoTopicCreation(t *testing.T) {
	broker := &Broker{kafkaConfig: core.DefaultKafkaConfig()}
	if broker.buildSaramaConfig().Metadata.AllowAutoTopicCreation {
		t.Fatal("Kafka metadata requests allow topic auto-creation")
	}
}

func TestReplyConsumerInitializationIsSerialized(t *testing.T) {
	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: &core.KafkaConfig{ClientID: "test", ConsumerGroup: "workers", ReplyTopic: "reply-topic"},
		consumer:    &fakeConsumerGroup{},
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
		connected:   true,
	}
	defer broker.Close()

	const callers = 20
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- broker.ensureReplyConsumer()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ensureReplyConsumer() error = %v", err)
		}
	}

	broker.subsMu.RLock()
	defer broker.subsMu.RUnlock()
	if len(broker.subs) != 1 {
		t.Fatalf("reply subscriptions = %d, want 1", len(broker.subs))
	}
	if broker.subs[0].destination != broker.currentReplyTopic() {
		t.Fatalf("subscription destination = %q, reply topic = %q", broker.subs[0].destination, broker.currentReplyTopic())
	}
}

func TestCancelAllPendingSignalsConnectionLoss(t *testing.T) {
	t.Parallel()

	broker := &Broker{pending: make(map[string]*pendingCall)}
	call1 := newPendingCall()
	call2 := newPendingCall()
	broker.pending["one"] = call1
	broker.pending["two"] = call2

	broker.cancelAllPending()

	if len(broker.pending) != 0 {
		t.Fatalf("pending map length = %d, want 0", len(broker.pending))
	}
	if response := <-call1.response; response != nil {
		t.Fatalf("pending call one response = %#v, want nil", response)
	}
	if response := <-call2.response; response != nil {
		t.Fatalf("pending call two response = %#v, want nil", response)
	}
}

func TestCallRejectsConcurrentDuplicateCorrelationIDs(t *testing.T) {
	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: core.DefaultKafkaConfig(),
		producer:    &fakeSyncProducer{},
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
		connected:   true,
		replyTopic:  "reply-topic",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type callResult struct {
		response *core.Message
		err      error
	}
	for i := 0; i < 1000; i++ {
		results := make(chan callResult, 2)
		start := make(chan struct{})
		for range 2 {
			go func() {
				<-start
				response, err := broker.Call(ctx, "users.get", core.NewTextMessage("request").WithCorrelationID("shared-id"))
				results <- callResult{response: response, err: err}
			}()
		}
		close(start)

		var duplicate callResult
		select {
		case duplicate = <-results:
		case <-ctx.Done():
			t.Fatal("duplicate call did not return")
		}
		if !errors.Is(duplicate.err, core.ErrDuplicateCorrelationID) {
			t.Fatalf("iteration %d duplicate error = %v, want ErrDuplicateCorrelationID", i, duplicate.err)
		}

		broker.pendingMu.RLock()
		pending := broker.pending["shared-id"]
		broker.pendingMu.RUnlock()
		if pending == nil {
			t.Fatalf("iteration %d winning call was not reserved", i)
		}
		pending.complete(core.NewTextMessage("response").WithCorrelationID("shared-id"))

		select {
		case winner := <-results:
			if winner.err != nil || winner.response.BodyString() != "response" {
				t.Fatalf("iteration %d winning result = %#v, %v", i, winner.response, winner.err)
			}
		case <-ctx.Done():
			t.Fatal("winning call was stranded")
		}
	}
}

func TestConcurrentResponseDeliveryAndDisconnect(t *testing.T) {
	for i := 0; i < 1000; i++ {
		broker := &Broker{pending: make(map[string]*pendingCall)}
		pending := newPendingCall()
		broker.pending["corr-1"] = pending
		handler := &consumerGroupHandler{
			closeChan: make(chan struct{}),
			pending:   broker.pending,
			pendingMu: &broker.pendingMu,
		}
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{
			Topic:   "reply-topic",
			Headers: []*sarama.RecordHeader{{Key: []byte("correlation-id"), Value: []byte("corr-1")}},
		}
		close(claim.messages)

		start := make(chan struct{})
		consumeDone := make(chan error, 1)
		cancelDone := make(chan struct{})
		go func() {
			<-start
			consumeDone <- handler.ConsumeClaim(&fakeSession{}, claim)
		}()
		go func() {
			<-start
			broker.cancelAllPending()
			close(cancelDone)
		}()

		close(start)
		if err := <-consumeDone; err != nil {
			t.Fatalf("iteration %d ConsumeClaim() error = %v", i, err)
		}
		<-cancelDone

		response := <-pending.response
		if response != nil && response.CorrelationID != "corr-1" {
			t.Fatalf("iteration %d correlation ID = %q, want corr-1", i, response.CorrelationID)
		}
		select {
		case duplicate := <-pending.response:
			t.Fatalf("iteration %d delivered a second result: %#v", i, duplicate)
		default:
		}
	}
}

func TestConsumerGroupHandlerRoutesResponsesAndInvokesHandlers(t *testing.T) {
	t.Run("routes messages to handlers by topic", func(t *testing.T) {
		called := make(map[string]int)
		handler := &consumerGroupHandler{
			routes: map[string]subscriptionRoute{
				"users": {
					handler: func(context.Context, *core.Message) error {
						called["users"]++
						return nil
					},
				},
				"orders": {
					handler: func(context.Context, *core.Message) error {
						called["orders"]++
						return nil
					},
				},
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]*pendingCall),
			pendingMu: &sync.RWMutex{},
		}

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users"}
		claim.messages <- &sarama.ConsumerMessage{Topic: "orders"}
		close(claim.messages)
		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		if called["users"] != 1 || called["orders"] != 1 {
			t.Fatalf("handler calls = %v, want one call per topic", called)
		}
		if len(session.marked) != 2 {
			t.Fatalf("marked messages = %d, want 2", len(session.marked))
		}
	})

	t.Run("routes response by correlation id", func(t *testing.T) {
		pending := map[string]*pendingCall{"corr-1": newPendingCall()}
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
		case response := <-pending["corr-1"].response:
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
			pending:   make(map[string]*pendingCall),
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
			pending:   make(map[string]*pendingCall),
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

	t.Run("handler error stops claim without committing when retry policy enabled", func(t *testing.T) {
		handlerCalls := 0
		handler := &consumerGroupHandler{
			handler: func(context.Context, *core.Message) error {
				handlerCalls++
				return errors.New("boom")
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]*pendingCall),
			pendingMu: &sync.RWMutex{},
			policy:    core.HandlerErrorRetry,
		}

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 12}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 13}
		close(claim.messages)

		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err == nil {
			t.Fatal("ConsumeClaim() error = nil, want handler error")
		}
		if len(session.marked) != 0 {
			t.Fatalf("marked messages = %d, want 0", len(session.marked))
		}
		if handlerCalls != 1 {
			t.Fatalf("handler calls = %d, want 1", handlerCalls)
		}
	})

	t.Run("handler panic is contained and committed by no-retry policy", func(t *testing.T) {
		handlerCalls := 0
		events := make(chan core.Event, 1)
		handler := &consumerGroupHandler{
			handler: func(context.Context, *core.Message) error {
				handlerCalls++
				if handlerCalls == 1 {
					panic("boom")
				}
				return nil
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]*pendingCall),
			pendingMu: &sync.RWMutex{},
			config: &core.Config{EventHook: func(_ context.Context, event core.Event) {
				events <- event
			}},
			policy: core.HandlerErrorNoRetry,
		}
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 14}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 15}
		close(claim.messages)

		session := &fakeSession{}
		if err := handler.ConsumeClaim(session, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		if handlerCalls != 2 {
			t.Fatalf("handler calls = %d, want 2", handlerCalls)
		}
		if len(session.marked) != 2 {
			t.Fatalf("marked messages = %d, want 2", len(session.marked))
		}
		select {
		case event := <-events:
			if event.Operation != "handler" || event.Err == nil || !strings.Contains(event.Err.Error(), "handler panic: boom") {
				t.Fatalf("unexpected panic event: %#v", event)
			}
		default:
			t.Fatal("handler panic did not emit a failure event")
		}
	})

	t.Run("handler panic stops claim when retry policy enabled", func(t *testing.T) {
		handlerCalls := 0
		handler := &consumerGroupHandler{
			handler: func(context.Context, *core.Message) error {
				handlerCalls++
				panic("boom")
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]*pendingCall),
			pendingMu: &sync.RWMutex{},
			policy:    core.HandlerErrorRetry,
		}
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 16}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 17}
		close(claim.messages)

		session := &fakeSession{}
		err := handler.ConsumeClaim(session, claim)
		if err == nil || !strings.Contains(err.Error(), "handler panic: boom") {
			t.Fatalf("ConsumeClaim() error = %v, want recovered panic", err)
		}
		if handlerCalls != 1 {
			t.Fatalf("handler calls = %d, want 1", handlerCalls)
		}
		if len(session.marked) != 0 {
			t.Fatalf("marked messages = %d, want 0", len(session.marked))
		}
	})
}

func TestConsumerGroupHandlerPropagatesSubscriptionAndSessionContexts(t *testing.T) {
	t.Run("session cancellation preserves subscription values and deadline", func(t *testing.T) {
		type contextKey struct{}
		key := contextKey{}
		valueCtx := context.WithValue(context.Background(), key, "trace-123")
		subscriptionCtx, cancelSubscription := context.WithTimeout(valueCtx, time.Hour)
		defer cancelSubscription()
		sessionCtx, cancelSession := context.WithCancel(context.Background())
		cancelSession()

		var gotErr error
		var gotValue any
		var gotDeadline bool
		handler := &consumerGroupHandler{
			routes: map[string]subscriptionRoute{
				"users": {
					ctx: subscriptionCtx,
					handler: func(ctx context.Context, _ *core.Message) error {
						gotErr = ctx.Err()
						gotValue = ctx.Value(key)
						_, gotDeadline = ctx.Deadline()
						return nil
					},
				},
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]*pendingCall),
			pendingMu: &sync.RWMutex{},
		}
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users"}
		close(claim.messages)

		if err := handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim); err != nil {
			t.Fatalf("ConsumeClaim() error = %v", err)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", gotErr)
		}
		if gotValue != "trace-123" || !gotDeadline {
			t.Fatalf("handler context value=%v deadline=%v, want trace value and deadline", gotValue, gotDeadline)
		}
	})

	t.Run("subscription cancellation stops active handler", func(t *testing.T) {
		subscriptionCtx, cancelSubscription := context.WithCancel(context.Background())
		started := make(chan struct{})
		stopped := make(chan error, 1)
		handler := &consumerGroupHandler{
			routes: map[string]subscriptionRoute{
				"users": {
					ctx: subscriptionCtx,
					handler: func(ctx context.Context, _ *core.Message) error {
						close(started)
						<-ctx.Done()
						stopped <- ctx.Err()
						return nil
					},
				},
			},
			closeChan: make(chan struct{}),
			pending:   make(map[string]*pendingCall),
			pendingMu: &sync.RWMutex{},
		}
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users"}
		close(claim.messages)

		done := make(chan error, 1)
		go func() { done <- handler.ConsumeClaim(&fakeSession{}, claim) }()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("handler did not start")
		}
		cancelSubscription()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ConsumeClaim() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("handler did not stop after subscription cancellation")
		}
		if err := <-stopped; !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	})
}

func TestConsumerGroupHandlerWorkerPoolLimitsClaims(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	handler := &consumerGroupHandler{
		handler: func(context.Context, *core.Message) error {
			started <- struct{}{}
			<-release
			return nil
		},
		closeChan: make(chan struct{}),
		workers:   make(chan struct{}, 2),
		pending:   make(map[string]*pendingCall),
		pendingMu: &sync.RWMutex{},
	}

	session := &fakeSession{}
	var wg sync.WaitGroup
	for partition := range 3 {
		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "users", Partition: int32(partition)}
		close(claim.messages)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := handler.ConsumeClaim(session, claim); err != nil {
				t.Errorf("ConsumeClaim() error = %v", err)
			}
		}()
	}

	for range 2 {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("configured workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("third claim started before a worker was available")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("third claim did not start after a worker became available")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("claims did not finish")
	}
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
		pending:       make(map[string]*pendingCall),
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
		pending:     make(map[string]*pendingCall),
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

func TestReplyConsumerSurvivesRepeatedReconnects(t *testing.T) {
	producers := []*fakeSyncProducer{{}, {}, {}, {}}
	consumers := []*fakeConsumerGroup{{}, {}, {}, {}}
	producerIndex := 1
	consumerIndex := 1
	broker := &Broker{
		config: &core.Config{
			ConnectionRetry: 1,
			RetryDelay:      time.Millisecond,
		},
		kafkaConfig: &core.KafkaConfig{
			Brokers:       []string{"kafka:9092"},
			ClientID:      "client",
			ConsumerGroup: "workers",
			ReplyTopic:    "reply-topic",
		},
		producer:      producers[0],
		consumer:      consumers[0],
		pending:       make(map[string]*pendingCall),
		closeChan:     make(chan struct{}),
		subsChanged:   make(chan struct{}, 1),
		runner:        true,
		connected:     true,
		everConnected: true,
		newSyncProducer: func(_ []string, _ *sarama.Config) (sarama.SyncProducer, error) {
			producer := producers[producerIndex]
			producerIndex++
			return producer, nil
		},
		newConsumerGroup: func(_ []string, _ string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
			consumer := consumers[consumerIndex]
			consumerIndex++
			return consumer, nil
		},
	}
	defer broker.Close()

	if err := broker.ensureReplyConsumer(); err != nil {
		t.Fatalf("ensureReplyConsumer() error = %v", err)
	}
	replyTopic := broker.currentReplyTopic()

	for cycle := range 3 {
		oldProducer := broker.currentProducer().(*fakeSyncProducer)
		oldConsumer := broker.currentConsumer().(*fakeConsumerGroup)
		broker.handleConnectionLoss(errors.New("connection lost"))
		if !oldProducer.closed || !oldConsumer.closed {
			t.Fatalf("cycle %d did not close the superseded producer and consumer", cycle+1)
		}
		if err := broker.ensureConnected(); err != nil {
			t.Fatalf("cycle %d ensureConnected() error = %v", cycle+1, err)
		}
		if err := broker.ensureReplyConsumer(); err != nil {
			t.Fatalf("cycle %d ensureReplyConsumer() error = %v", cycle+1, err)
		}
		if got := broker.currentReplyTopic(); got != replyTopic {
			t.Fatalf("cycle %d reply topic = %q, want %q", cycle+1, got, replyTopic)
		}
		topics, _ := broker.activeSubscriptionRoutes()
		if len(topics) != 1 || topics[0] != replyTopic {
			t.Fatalf("cycle %d active topics = %v, want [%s]", cycle+1, topics, replyTopic)
		}
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
		pending:   make(map[string]*pendingCall),
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
		pending:     make(map[string]*pendingCall),
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
		pending:   make(map[string]*pendingCall),
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

func TestSubscriptionsUseOneConsumerLoopForAllTopics(t *testing.T) {
	consumeCalls := make(chan []string, 8)
	var consumeMu sync.Mutex
	activeConsumes := 0
	maxActiveConsumes := 0
	group := &fakeConsumerGroup{}
	group.consumeFn = func(ctx context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
		consumeMu.Lock()
		activeConsumes++
		if activeConsumes > maxActiveConsumes {
			maxActiveConsumes = activeConsumes
		}
		consumeMu.Unlock()

		consumeCalls <- append([]string(nil), topics...)
		<-ctx.Done()

		consumeMu.Lock()
		activeConsumes--
		consumeMu.Unlock()
		return nil
	}

	broker := &Broker{
		config:      &core.Config{},
		kafkaConfig: &core.KafkaConfig{ConsumerGroup: "workers"},
		consumer:    group,
		connected:   true,
		pending:     make(map[string]*pendingCall),
		closeChan:   make(chan struct{}),
	}
	defer broker.Close()

	usersCtx, cancelUsers := context.WithCancel(context.Background())
	defer cancelUsers()
	ordersCtx, cancelOrders := context.WithCancel(context.Background())
	defer cancelOrders()
	if err := broker.Subscribe(usersCtx, "users", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe(users) error = %v", err)
	}

	waitForTopics := func(want []string) {
		t.Helper()
		deadline := time.After(time.Second)
		for {
			select {
			case topics := <-consumeCalls:
				if len(topics) == len(want) {
					match := true
					for i := range want {
						if topics[i] != want[i] {
							match = false
							break
						}
					}
					if match {
						return
					}
				}
			case <-deadline:
				t.Fatalf("consumer never received topics %v", want)
			}
		}
	}

	waitForTopics([]string{"users"})
	if err := broker.Subscribe(ordersCtx, "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatalf("Subscribe(orders) error = %v", err)
	}
	waitForTopics([]string{"orders", "users"})
	cancelUsers()
	waitForTopics([]string{"orders"})

	consumeMu.Lock()
	defer consumeMu.Unlock()
	if maxActiveConsumes != 1 {
		t.Fatalf("maximum concurrent Consume calls = %d, want 1", maxActiveConsumes)
	}
}
