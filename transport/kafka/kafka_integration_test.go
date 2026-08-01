package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

func TestMultipleSubscriptionsWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "fix-001", 2)
	usersTopic, ordersTopic := topics[0], topics[1]

	usersReceived := make(chan string, 1)
	ordersReceived := make(chan string, 1)
	if err := broker.Subscribe(ctx, usersTopic, func(_ context.Context, msg *core.Message) error {
		usersReceived <- msg.BodyString()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", usersTopic, err)
	}
	if err := broker.Subscribe(ctx, ordersTopic, func(_ context.Context, msg *core.Message) error {
		ordersReceived <- msg.BodyString()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", ordersTopic, err)
	}

	if err := broker.Publish(ctx, usersTopic, core.NewTextMessage("user-created")); err != nil {
		t.Fatalf("Publish(%s) error = %v", usersTopic, err)
	}
	if err := broker.Publish(ctx, ordersTopic, core.NewTextMessage("order-created")); err != nil {
		t.Fatalf("Publish(%s) error = %v", ordersTopic, err)
	}

	select {
	case got := <-usersReceived:
		if got != "user-created" {
			t.Fatalf("users handler received %q, want %q", got, "user-created")
		}
	case <-ctx.Done():
		t.Fatal("users handler did not receive its message")
	}
	select {
	case got := <-ordersReceived:
		if got != "order-created" {
			t.Fatalf("orders handler received %q, want %q", got, "order-created")
		}
	case <-ctx.Done():
		t.Fatal("orders handler did not receive its message")
	}
}

func TestRetryableHandlerRedeliversBeforeLaterOffsetWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "fix-002", 1)
	topic := topics[0]

	if err := broker.Publish(ctx, topic, core.NewTextMessage("retry-me")); err != nil {
		t.Fatalf("Publish(retry-me) error = %v", err)
	}
	if err := broker.Publish(ctx, topic, core.NewTextMessage("later")); err != nil {
		t.Fatalf("Publish(later) error = %v", err)
	}

	var retryAttempts atomic.Int32
	var laterBeforeRetry atomic.Bool
	laterReceived := make(chan struct{}, 1)
	if err := broker.Subscribe(ctx, topic, func(_ context.Context, msg *core.Message) error {
		switch msg.BodyString() {
		case "retry-me":
			if retryAttempts.Add(1) == 1 {
				return errors.New("transient handler failure")
			}
		case "later":
			if retryAttempts.Load() < 2 {
				laterBeforeRetry.Store(true)
			}
			laterReceived <- struct{}{}
		}
		return nil
	}, core.WithHandlerErrorRetry()); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", topic, err)
	}

	select {
	case <-laterReceived:
	case <-ctx.Done():
		t.Fatal("later message was not received after retry")
	}
	if laterBeforeRetry.Load() {
		t.Fatal("later offset was handled before the failed message was redelivered")
	}
	if retryAttempts.Load() < 2 {
		t.Fatalf("retry attempts = %d, want at least 2", retryAttempts.Load())
	}
}

func TestPanickingHandlerRecoversAndRetriesWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "fix-003", 1)
	topic := topics[0]

	if err := broker.Publish(ctx, topic, core.NewTextMessage("panic-once")); err != nil {
		t.Fatalf("Publish(panic-once) error = %v", err)
	}
	if err := broker.Publish(ctx, topic, core.NewTextMessage("later")); err != nil {
		t.Fatalf("Publish(later) error = %v", err)
	}

	var panicAttempts atomic.Int32
	var laterBeforeRecovery atomic.Bool
	laterReceived := make(chan struct{}, 1)
	if err := broker.Subscribe(ctx, topic, func(_ context.Context, msg *core.Message) error {
		switch msg.BodyString() {
		case "panic-once":
			if panicAttempts.Add(1) == 1 {
				panic("temporary handler panic")
			}
		case "later":
			if panicAttempts.Load() < 2 {
				laterBeforeRecovery.Store(true)
			}
			laterReceived <- struct{}{}
		}
		return nil
	}, core.WithHandlerErrorRetry()); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", topic, err)
	}

	select {
	case <-laterReceived:
	case <-ctx.Done():
		t.Fatal("consumer did not continue after the recovered panic")
	}
	if laterBeforeRecovery.Load() {
		t.Fatal("later message was handled before the panicking message recovered")
	}
	if panicAttempts.Load() < 2 {
		t.Fatalf("panic attempts = %d, want at least 2", panicAttempts.Load())
	}
}

func TestSubscriptionCancellationStopsActiveHandlerWithLiveKafka(t *testing.T) {
	broker, topics, testCtx := newLiveKafkaBroker(t, "fix-004", 1)
	topic := topics[0]
	type contextKey struct{}
	key := contextKey{}
	valueCtx := context.WithValue(testCtx, key, "trace-live")
	subscriptionCtx, cancelSubscription := context.WithCancel(valueCtx)
	defer cancelSubscription()

	started := make(chan struct{})
	stopped := make(chan error, 1)
	if err := broker.Subscribe(subscriptionCtx, topic, func(ctx context.Context, _ *core.Message) error {
		close(started)
		if ctx.Value(key) != "trace-live" {
			stopped <- errors.New("subscription context value was not propagated")
			return nil
		}
		<-ctx.Done()
		stopped <- ctx.Err()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", topic, err)
	}
	if err := broker.Publish(testCtx, topic, core.NewTextMessage("wait-for-cancellation")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case <-started:
	case <-testCtx.Done():
		t.Fatal("handler did not start")
	}
	cancelSubscription()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	case <-testCtx.Done():
		t.Fatal("handler did not stop after subscription cancellation")
	}
}

func TestRPCResponseDeliveryAndCloseWithLiveKafka(t *testing.T) {
	broker, responder, requestTopic, ctx := newLiveKafkaRPCPair(t, "fix-006")

	requestReceived := make(chan *core.Message, 1)
	publishResponse := make(chan struct{})
	responsePublished := make(chan error, 1)
	if err := responder.Subscribe(ctx, requestTopic, func(_ context.Context, request *core.Message) error {
		requestReceived <- request
		<-publishResponse
		response := core.NewTextMessage("response")
		response.CorrelationID = request.CorrelationID
		err := responder.Publish(ctx, request.ReplyTo, response)
		responsePublished <- err
		return err
	}); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", requestTopic, err)
	}

	type callResult struct {
		response *core.Message
		err      error
	}
	result := make(chan callResult, 1)
	go func() {
		response, err := broker.Call(ctx, requestTopic, core.NewTextMessage("request"), core.WithTimeout(10*time.Second))
		result <- callResult{response: response, err: err}
	}()

	select {
	case <-requestReceived:
	case <-ctx.Done():
		t.Fatal("request handler did not receive the RPC call")
	}

	close(publishResponse)
	if err := broker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-responsePublished:
		if err != nil {
			t.Fatalf("publish response: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("responder did not publish the response")
	}

	select {
	case result := <-result:
		if result.err != nil && !core.IsConnectionLost(result.err) {
			t.Fatalf("Call() error = %v, want success or ErrConnectionLost", result.err)
		}
		if result.err == nil && result.response.BodyString() != "response" {
			t.Fatalf("Call() response = %q, want response", result.response.BodyString())
		}
	case <-ctx.Done():
		t.Fatal("Call() did not complete after consumer shutdown")
	}
}

func TestDuplicateCorrelationIDWithLiveKafka(t *testing.T) {
	broker, responder, requestTopic, ctx := newLiveKafkaRPCPair(t, "fix-007")
	requestReceived := make(chan *core.Message, 1)
	publishResponse := make(chan struct{})
	responsePublished := make(chan error, 1)
	if err := responder.Subscribe(ctx, requestTopic, func(_ context.Context, request *core.Message) error {
		requestReceived <- request
		<-publishResponse
		response := core.NewTextMessage("first-response").WithCorrelationID(request.CorrelationID)
		err := responder.Publish(ctx, request.ReplyTo, response)
		responsePublished <- err
		return err
	}); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", requestTopic, err)
	}

	type callResult struct {
		response *core.Message
		err      error
	}
	firstResult := make(chan callResult, 1)
	go func() {
		response, err := broker.Call(ctx, requestTopic, core.NewTextMessage("first").WithCorrelationID("shared-id"), core.WithTimeout(10*time.Second))
		firstResult <- callResult{response: response, err: err}
	}()

	select {
	case <-requestReceived:
	case <-ctx.Done():
		t.Fatal("responder did not receive the first call")
	}

	response, duplicateErr := broker.Call(ctx, requestTopic, core.NewTextMessage("second").WithCorrelationID("shared-id"), core.WithTimeout(2*time.Second))
	close(publishResponse)
	if response != nil || !errors.Is(duplicateErr, core.ErrDuplicateCorrelationID) {
		t.Fatalf("duplicate Call() = %#v, %v; want nil, ErrDuplicateCorrelationID", response, duplicateErr)
	}
	select {
	case err := <-responsePublished:
		if err != nil {
			t.Fatalf("publish first response: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("responder did not publish the first response")
	}
	select {
	case result := <-firstResult:
		if result.err != nil || result.response.BodyString() != "first-response" {
			t.Fatalf("first Call() = %#v, %v", result.response, result.err)
		}
	case <-ctx.Done():
		t.Fatal("first call was stranded by the duplicate")
	}
}

func TestReplyConsumerInitializationRetryWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "fix-008", 1)
	requestTopic := topics[0]

	broker.closeMu.Lock()
	consumer := broker.consumer
	consumerGroup := broker.kafkaConfig.ConsumerGroup
	broker.consumer = nil
	broker.kafkaConfig.ConsumerGroup = ""
	broker.closeMu.Unlock()

	if err := broker.ensureReplyConsumer(); err == nil {
		t.Fatal("first ensureReplyConsumer() error = nil, want failure")
	}
	if got := broker.currentReplyTopic(); got != "" {
		t.Fatalf("reply topic after failed initialization = %q, want empty", got)
	}

	broker.closeMu.Lock()
	broker.consumer = consumer
	broker.kafkaConfig.ConsumerGroup = consumerGroup
	broker.closeMu.Unlock()
	if err := broker.ensureReplyConsumer(); err != nil {
		t.Fatalf("retry ensureReplyConsumer() error = %v", err)
	}

	replyTopic := broker.currentReplyTopic()
	admin, err := sarama.NewClusterAdmin(broker.kafkaConfig.Brokers, sarama.NewConfig())
	if err != nil {
		t.Fatalf("create Kafka admin: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.DeleteTopic(replyTopic)
		_ = admin.Close()
	})
	if err := admin.CreateTopic(replyTopic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		t.Fatalf("create reply topic: %v", err)
	}

	responderConfig := core.DefaultConfig()
	responderConfig.Kafka = core.DefaultKafkaConfig()
	responderConfig.Kafka.Brokers = broker.kafkaConfig.Brokers
	responderConfig.Kafka.ClientID = broker.kafkaConfig.ClientID + "-responder"
	responderConfig.Kafka.ConsumerGroup = broker.kafkaConfig.ConsumerGroup + "-responder"
	responderConfig.Kafka.AutoOffsetReset = "earliest"
	responderAny, err := NewBroker(responderConfig)
	if err != nil {
		t.Fatalf("create responder broker: %v", err)
	}
	responder := responderAny.(*Broker)
	t.Cleanup(func() { _ = responder.Close() })
	if err := responder.Connect(ctx); err != nil {
		t.Fatalf("connect responder broker: %v", err)
	}
	if err := responder.Subscribe(ctx, requestTopic, func(_ context.Context, request *core.Message) error {
		response := core.NewTextMessage("response").WithCorrelationID(request.CorrelationID)
		return responder.Publish(ctx, request.ReplyTo, response)
	}); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", requestTopic, err)
	}

	response, err := broker.Call(ctx, requestTopic, core.NewTextMessage("request"), core.WithTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("Call() after initialization retry error = %v", err)
	}
	if response.BodyString() != "response" {
		t.Fatalf("Call() response = %q, want response", response.BodyString())
	}
}

func newLiveKafkaRPCPair(t *testing.T, name string) (*Broker, *Broker, string, context.Context) {
	t.Helper()
	broker, topics, ctx := newLiveKafkaBroker(t, name, 1)
	if err := broker.ensureReplyConsumer(); err != nil {
		t.Fatalf("initialize reply consumer: %v", err)
	}
	replyTopic := broker.currentReplyTopic()
	admin, err := sarama.NewClusterAdmin(broker.kafkaConfig.Brokers, sarama.NewConfig())
	if err != nil {
		t.Fatalf("create Kafka admin: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.DeleteTopic(replyTopic)
		_ = admin.Close()
	})
	if err := admin.CreateTopic(replyTopic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		t.Fatalf("create reply topic: %v", err)
	}

	responderConfig := core.DefaultConfig()
	responderConfig.Kafka = core.DefaultKafkaConfig()
	responderConfig.Kafka.Brokers = broker.kafkaConfig.Brokers
	responderConfig.Kafka.ClientID = broker.kafkaConfig.ClientID + "-responder"
	responderConfig.Kafka.ConsumerGroup = broker.kafkaConfig.ConsumerGroup + "-responder"
	responderConfig.Kafka.AutoOffsetReset = "earliest"
	responderAny, err := NewBroker(responderConfig)
	if err != nil {
		t.Fatalf("create responder broker: %v", err)
	}
	responder := responderAny.(*Broker)
	t.Cleanup(func() { _ = responder.Close() })
	if err := responder.Connect(ctx); err != nil {
		t.Fatalf("connect responder broker: %v", err)
	}
	return broker, responder, topics[0], ctx
}

func newLiveKafkaBroker(t *testing.T, name string, topicCount int) (*Broker, []string, context.Context) {
	t.Helper()
	if os.Getenv("WEAVE_KAFKA_E2E") == "" {
		t.Skip("set WEAVE_KAFKA_E2E=1 to run the live Kafka test")
	}

	brokers := []string{"localhost:9092"}
	if configured := os.Getenv("WEAVE_KAFKA_BROKERS"); configured != "" {
		brokers = strings.Split(configured, ",")
	}

	suffix := time.Now().UnixNano()
	topics := make([]string, topicCount)
	for i := range topics {
		topics[i] = fmt.Sprintf("weave-%s-%d-%d", name, i, suffix)
	}

	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		t.Fatalf("create Kafka admin: %v", err)
	}
	t.Cleanup(func() {
		for _, topic := range topics {
			_ = admin.DeleteTopic(topic)
		}
		_ = admin.Close()
	})
	for _, topic := range topics {
		if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}

	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.Brokers = brokers
	config.Kafka.ClientID = fmt.Sprintf("weave-%s-%d", name, suffix)
	config.Kafka.ConsumerGroup = fmt.Sprintf("weave-%s-%d", name, suffix)
	config.Kafka.AutoOffsetReset = "earliest"

	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := broker.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return broker, topics, ctx
}
