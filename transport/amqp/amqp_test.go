package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

type fakeAMQPConnection struct {
	mu          sync.RWMutex
	channel     amqpChannel
	notifyClose chan *amqplib.Error
	closed      bool
}

func (c *fakeAMQPConnection) Channel() (amqpChannel, error) { return c.channel, nil }
func (c *fakeAMQPConnection) NotifyClose(receiver chan *amqplib.Error) chan *amqplib.Error {
	c.mu.Lock()
	c.notifyClose = receiver
	c.mu.Unlock()
	return receiver
}
func (c *fakeAMQPConnection) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *fakeAMQPConnection) notifyCloseCh() chan *amqplib.Error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.notifyClose
}

type fakeAMQPChannel struct {
	mu              sync.Mutex
	declares        []string
	bindings        []string
	consumers       []string
	published       []amqplib.Publishing
	consumerStreams map[string]chan amqplib.Delivery
	publishErr      error
	closeErr        error
	closed          bool
}

func newFakeAMQPChannel() *fakeAMQPChannel {
	return &fakeAMQPChannel{consumerStreams: make(map[string]chan amqplib.Delivery)}
}

func (c *fakeAMQPChannel) PublishWithContext(_ context.Context, _ string, _ string, _ bool, _ bool, msg amqplib.Publishing) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.publishErr != nil {
		return c.publishErr
	}
	c.published = append(c.published, msg)
	return nil
}

func (c *fakeAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.declares = append(c.declares, name)
	if name == "" {
		name = fmt.Sprintf("reply-%d", len(c.declares))
	}
	if _, ok := c.consumerStreams[name]; !ok {
		c.consumerStreams[name] = make(chan amqplib.Delivery)
	}
	return amqplib.Queue{Name: name}, nil
}

func (c *fakeAMQPChannel) QueueBind(name, key, exchange string, noWait bool, args amqplib.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings = append(c.bindings, name+":"+key+":"+exchange)
	return nil
}

func (c *fakeAMQPChannel) Qos(int, int, bool) error { return nil }

func (c *fakeAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqplib.Table) (<-chan amqplib.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers = append(c.consumers, queue)
	stream, ok := c.consumerStreams[queue]
	if !ok {
		stream = make(chan amqplib.Delivery)
		c.consumerStreams[queue] = stream
	}
	return stream, nil
}

func (c *fakeAMQPChannel) Close() error {
	c.closed = true
	return c.closeErr
}

type ackRecorder struct {
	ackCalls     int
	nackCalls    int
	rejectCalls  int
	lastMultiple bool
	lastRequeue  bool
}

func (a *ackRecorder) Ack(tag uint64, multiple bool) error {
	a.ackCalls++
	a.lastMultiple = multiple
	return nil
}

func (a *ackRecorder) Nack(tag uint64, multiple, requeue bool) error {
	a.nackCalls++
	a.lastMultiple = multiple
	a.lastRequeue = requeue
	return nil
}

func (a *ackRecorder) Reject(tag uint64, requeue bool) error {
	a.rejectCalls++
	a.lastRequeue = requeue
	return nil
}

func TestNewBrokerAppliesDefaults(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(&core.Config{})
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	if broker.amqpConfig == nil || broker.amqpConfig.Host != "localhost" || broker.amqpConfig.Port != 5672 {
		t.Fatalf("default AMQP config not applied: %#v", broker.amqpConfig)
	}

	shorthandAny, err := NewBroker(&core.Config{Host: "rabbitmq", Username: "user"})
	if err != nil {
		t.Fatalf("NewBroker() with shorthand config error = %v", err)
	}
	shorthand := shorthandAny.(*Broker)
	if shorthand.amqpConfig.Host != "rabbitmq" || shorthand.amqpConfig.Port != 5672 || shorthand.amqpConfig.Username != "user" || shorthand.amqpConfig.Password != "guest" || shorthand.amqpConfig.VHost != "/" {
		t.Fatalf("shorthand AMQP config not normalized: %#v", shorthand.amqpConfig)
	}
}

func TestBrokerLifecycleAndDisconnectedOperations(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(core.DefaultConfig())
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

func TestHandleResponsesRoutesByCorrelationID(t *testing.T) {
	t.Parallel()

	brokerAny, _ := NewBroker(core.DefaultConfig())
	broker := brokerAny.(*Broker)

	responseCh := make(chan *amqplib.Delivery, 1)
	broker.pending["corr-1"] = responseCh

	msgs := make(chan amqplib.Delivery, 1)
	msgs <- amqplib.Delivery{CorrelationId: "corr-1", Body: []byte("reply")}
	close(msgs)
	stop := make(chan struct{})

	done := make(chan struct{})
	go func() {
		broker.handleResponses(msgs, stop)
		close(done)
	}()

	select {
	case delivery := <-responseCh:
		if string(delivery.Body) != "reply" {
			t.Fatalf("delivery body = %q, want %q", string(delivery.Body), "reply")
		}
	case <-done:
		t.Fatal("handleResponses() returned before routing the pending response")
	}
}

func TestCancelAllPendingClosesChannels(t *testing.T) {
	t.Parallel()

	brokerAny, _ := NewBroker(core.DefaultConfig())
	broker := brokerAny.(*Broker)

	ch1 := make(chan *amqplib.Delivery)
	ch2 := make(chan *amqplib.Delivery)
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

func TestReconnectRestoresSubscriptions(t *testing.T) {
	firstChannel := newFakeAMQPChannel()
	secondChannel := newFakeAMQPChannel()
	firstConn := &fakeAMQPConnection{channel: firstChannel}
	secondConn := &fakeAMQPConnection{channel: secondChannel}

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)

	connections := []amqpConnection{firstConn, secondConn}
	var dialMu sync.Mutex
	broker.config.ConnectionRetry = 0
	broker.config.RetryDelay = time.Millisecond
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		dialMu.Lock()
		defer dialMu.Unlock()
		if len(connections) == 0 {
			return nil, errors.New("no more connections")
		}
		conn := connections[0]
		connections = connections[1:]
		return conn, nil
	}

	if err := broker.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }, core.WithQueueBind("events", "orders.*")); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	waitForNotifyClose(t, firstConn)
	firstConn.notifyCloseCh() <- &amqplib.Error{Code: 320, Reason: "connection reset"}

	deadline := time.After(500 * time.Millisecond)
	for {
		secondChannel.mu.Lock()
		declareCount := len(secondChannel.declares)
		consumeCount := len(secondChannel.consumers)
		bindCount := len(secondChannel.bindings)
		secondChannel.mu.Unlock()

		if declareCount > 0 && consumeCount > 0 && bindCount > 0 && broker.IsConnected() {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("reconnect did not restore subscription: declares=%d consumers=%d bindings=%d connected=%v", declareCount, consumeCount, bindCount, broker.IsConnected())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestCallReturnsConnectionLostWhenBrokerDisconnects(t *testing.T) {
	channel := newFakeAMQPChannel()
	conn := &fakeAMQPConnection{channel: channel}

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	broker.config.ConnectionRetry = 0
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		return conn, nil
	}

	if err := broker.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	waitForNotifyClose(t, conn)

	errCh := make(chan error, 1)
	request := core.NewTextMessage("payload").WithHeader("example-kind", "rpc")
	go func() {
		_, err := broker.Call(context.Background(), "orders", request, core.WithTimeout(2*time.Second))
		errCh <- err
	}()

	deadline := time.After(500 * time.Millisecond)
	for {
		broker.pendingMu.RLock()
		pendingCount := len(broker.pending)
		broker.pendingMu.RUnlock()
		if pendingCount == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("pending RPC call was not registered")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	channel.mu.Lock()
	if len(channel.published) != 1 || channel.published[0].Headers["example-kind"] != "rpc" {
		channel.mu.Unlock()
		t.Fatal("Call() did not preserve request headers")
	}
	channel.mu.Unlock()

	conn.notifyCloseCh() <- &amqplib.Error{Code: 320, Reason: "connection reset"}

	select {
	case err := <-errCh:
		if !core.IsConnectionLost(err) {
			t.Fatalf("Call() error = %v, want ErrConnectionLost", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Call() did not unblock after connection loss")
	}
}

func TestPublishReturnsErrNotConnectedWhileReconnectInProgress(t *testing.T) {
	channel := newFakeAMQPChannel()
	conn := &fakeAMQPConnection{channel: channel}

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	broker.config.ConnectionRetry = 0
	broker.config.RetryDelay = time.Millisecond

	dialCalls := 0
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		dialCalls++
		if dialCalls == 1 {
			return conn, nil
		}
		return nil, errors.New("broker unavailable")
	}

	if err := broker.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	waitForNotifyClose(t, conn)
	conn.notifyCloseCh() <- &amqplib.Error{Code: 320, Reason: "connection reset"}

	deadline := time.After(500 * time.Millisecond)
	for {
		if !broker.IsConnected() {
			break
		}

		select {
		case <-deadline:
			t.Fatal("broker did not enter disconnected state after connection loss")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	err = broker.Publish(context.Background(), "orders", core.NewTextMessage("payload"))
	if !core.IsNotConnected(err) {
		t.Fatalf("Publish() error = %v, want ErrNotConnected while reconnecting", err)
	}

	if closeErr := broker.Close(); closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}
}

func waitForNotifyClose(t *testing.T, conn *fakeAMQPConnection) {
	t.Helper()

	deadline := time.After(500 * time.Millisecond)
	for conn.notifyCloseCh() == nil {
		select {
		case <-deadline:
			t.Fatal("connection monitor did not register NotifyClose")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestHandleMessageAckAndNackBehavior(t *testing.T) {
	t.Parallel()

	brokerAny, _ := NewBroker(core.DefaultConfig())
	broker := brokerAny.(*Broker)

	t.Run("success acknowledges message", func(t *testing.T) {
		ack := &ackRecorder{}
		var received *core.Message
		delivery := amqplib.Delivery{
			Acknowledger:  ack,
			DeliveryTag:   1,
			Body:          []byte("payload"),
			CorrelationId: "corr-1",
			ReplyTo:       "reply",
			ContentType:   "application/json",
			MessageId:     "msg-1",
			Headers: amqplib.Table{
				"trace-id": "abc",
				"ignored":  123,
			},
		}

		broker.handleMessage(context.Background(), delivery, func(ctx context.Context, msg *core.Message) error {
			received = msg
			return nil
		}, false, core.HandlerErrorNoRetry)

		if ack.ackCalls != 1 || ack.nackCalls != 0 {
			t.Fatalf("ack=%d nack=%d, want ack=1 nack=0", ack.ackCalls, ack.nackCalls)
		}
		if received == nil || received.GetHeader("trace-id") != "abc" || received.GetHeader("ignored") != "" || received.CorrelationID != "corr-1" || received.ReplyTo != "reply" {
			t.Fatalf("unexpected received message: %#v", received)
		}
	})

	t.Run("handler error nacks without requeue by default", func(t *testing.T) {
		ack := &ackRecorder{}
		delivery := amqplib.Delivery{Acknowledger: ack, DeliveryTag: 2}
		broker.handleMessage(context.Background(), delivery, func(context.Context, *core.Message) error {
			return errors.New("boom")
		}, false, core.HandlerErrorNoRetry)

		if ack.nackCalls != 1 || ack.lastRequeue {
			t.Fatalf("nackCalls=%d requeue=%v, want 1 false", ack.nackCalls, ack.lastRequeue)
		}
	})

	t.Run("handler error nacks with requeue when retry policy enabled", func(t *testing.T) {
		ack := &ackRecorder{}
		delivery := amqplib.Delivery{Acknowledger: ack, DeliveryTag: 22}
		broker.handleMessage(context.Background(), delivery, func(context.Context, *core.Message) error {
			return errors.New("boom")
		}, false, core.HandlerErrorRetry)

		if ack.nackCalls != 1 || !ack.lastRequeue {
			t.Fatalf("nackCalls=%d requeue=%v, want 1 true", ack.nackCalls, ack.lastRequeue)
		}
	})

	t.Run("panic nacks without requeue", func(t *testing.T) {
		ack := &ackRecorder{}
		delivery := amqplib.Delivery{Acknowledger: ack, DeliveryTag: 3}
		broker.handleMessage(context.Background(), delivery, func(context.Context, *core.Message) error {
			panic("boom")
		}, false, core.HandlerErrorRetry)

		if ack.nackCalls != 1 || ack.lastRequeue {
			t.Fatalf("nackCalls=%d requeue=%v, want 1 false", ack.nackCalls, ack.lastRequeue)
		}
	})
}
