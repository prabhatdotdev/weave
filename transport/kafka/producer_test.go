package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

type fakeAsyncProducer struct {
	sarama.AsyncProducer
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	failures  chan *sarama.ProducerError
}

func newFakeAsyncProducer() *fakeAsyncProducer {
	return &fakeAsyncProducer{input: make(chan *sarama.ProducerMessage), successes: make(chan *sarama.ProducerMessage), failures: make(chan *sarama.ProducerError)}
}

func (p *fakeAsyncProducer) Input() chan<- *sarama.ProducerMessage     { return p.input }
func (p *fakeAsyncProducer) Successes() <-chan *sarama.ProducerMessage { return p.successes }
func (p *fakeAsyncProducer) Errors() <-chan *sarama.ProducerError      { return p.failures }
func (p *fakeAsyncProducer) AsyncClose() {
	close(p.successes)
	close(p.failures)
}

func TestPublishWaitsForAcknowledgmentAndReportsRejection(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged", true: "rejected"}[reject], func(t *testing.T) {
			async := newFakeAsyncProducer()
			producer := wrapProducer(async)
			defer producer.Close()
			done := make(chan error, 1)
			go func() {
				_, _, err := producer.SendMessageContext(context.Background(), &sarama.ProducerMessage{Topic: "orders"})
				done <- err
			}()
			message := <-async.input
			select {
			case err := <-done:
				t.Fatalf("publish returned before broker acknowledgment: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			if reject {
				async.failures <- &sarama.ProducerError{Msg: message, Err: sarama.ErrMessageSizeTooLarge}
			} else {
				async.successes <- message
			}
			if err := <-done; reject && !errors.Is(err, sarama.ErrMessageSizeTooLarge) || !reject && err != nil {
				t.Fatalf("SendMessageContext() error = %v", err)
			}
		})
	}
}

func TestPublishTimeoutIncludesQueueingAndAcknowledgment(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(map[bool]string{false: "backpressure", true: "waiting for acknowledgment"}[accept], func(t *testing.T) {
			async := newFakeAsyncProducer()
			producer := wrapProducer(async)
			broker := &Broker{config: core.DefaultConfig(), kafkaConfig: core.DefaultKafkaConfig(), producer: producer, connected: true, closeChan: make(chan struct{})}
			defer broker.Close()
			request := core.NewTextMessage("original").WithHeader("source", "original")
			done := make(chan error, 1)
			go func() {
				done <- broker.Publish(context.Background(), "orders", request, core.WithTimeout(30*time.Millisecond))
			}()
			var message *sarama.ProducerMessage
			if accept {
				select {
				case message = <-async.input:
				case <-time.After(time.Second):
					t.Fatal("producer did not receive the message")
				}
			}
			select {
			case err := <-done:
				if !core.IsTimeout(err) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Publish() = %v, want deadline timeout", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Publish() ignored its timeout")
			}
			if !broker.IsConnected() {
				t.Fatal("a caller timeout disconnected a healthy producer")
			}
			if accept {
				request.Body[0] = 'X'
				request.Headers["source"] = "changed"
				body, err := message.Value.Encode()
				if err != nil || string(body) != "original" || string(message.Headers[0].Value) != "original" {
					t.Fatal("timed-out publish retained caller-owned data")
				}
				// A late acknowledgment must be drained without blocking future sends.
				async.successes <- message
				go func() {
					next := <-async.input
					async.successes <- next
				}()
				if err := broker.Publish(context.Background(), "orders", core.NewTextMessage("next"), core.WithTimeout(time.Second)); err != nil {
					t.Fatalf("publish after late acknowledgment: %v", err)
				}
			}
		})
	}
}

func TestProducerCloseUnblocksQueuedPublish(t *testing.T) {
	async := newFakeAsyncProducer()
	producer := wrapProducer(async)
	done := make(chan error, 1)
	go func() {
		_, _, err := producer.SendMessageContext(context.Background(), &sarama.ProducerMessage{Topic: "orders"})
		done <- err
	}()
	closed := make(chan error, 1)
	go func() { closed <- producer.Close() }()
	select {
	case err := <-done:
		if !core.IsConnectionLost(err) {
			t.Fatalf("publish during close returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer close stranded an input waiter")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestCallTimeoutIncludesPublishingAndCleansPendingRequest(t *testing.T) {
	async := newFakeAsyncProducer()
	producer := wrapProducer(async)
	broker := &Broker{config: core.DefaultConfig(), kafkaConfig: core.DefaultKafkaConfig(), producer: producer, connected: true, closeChan: make(chan struct{}), pending: make(map[string]*pendingCall), replyTopic: "replies"}
	defer broker.Close()
	done := make(chan error, 1)
	go func() {
		_, err := broker.Call(context.Background(), "orders", core.NewTextMessage("order"), core.WithTimeout(30*time.Millisecond))
		done <- err
	}()
	message := <-async.input
	select {
	case err := <-done:
		if !core.IsTimeout(err) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call() = %v, want deadline timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Call() ignored its publish timeout")
	}
	if len(broker.pending) != 0 {
		t.Fatal("timed-out Call retained its pending correlation ID")
	}
	async.successes <- message
}

func TestCallCancellationIsNotReportedAsTimeout(t *testing.T) {
	broker := &Broker{config: core.DefaultConfig(), kafkaConfig: core.DefaultKafkaConfig(), connected: true, closeChan: make(chan struct{}), pending: make(map[string]*pendingCall), replyTopic: "replies"}
	defer broker.Close()
	sent := make(chan struct{})
	broker.producer = &fakeSyncProducer{afterSend: func(*sarama.ProducerMessage) { close(sent) }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := broker.Call(ctx, "orders", core.NewTextMessage("order")); done <- err }()
	<-sent
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || core.IsTimeout(err) {
			t.Fatalf("Call() cancellation returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Call() ignored cancellation")
	}
	if len(broker.pending) != 0 {
		t.Fatal("canceled Call retained its pending correlation ID")
	}
}

func TestMessageRejectionDoesNotInterruptOtherPublishes(t *testing.T) {
	async := newFakeAsyncProducer()
	producer := wrapProducer(async)
	broker := &Broker{config: core.DefaultConfig(), kafkaConfig: core.DefaultKafkaConfig(), producer: producer, connected: true, closeChan: make(chan struct{})}
	defer broker.Close()
	failed, succeeded := make(chan error, 1), make(chan error, 1)
	go func() { failed <- broker.Publish(context.Background(), "orders", core.NewTextMessage("rejected")) }()
	first := <-async.input
	go func() { succeeded <- broker.Publish(context.Background(), "orders", core.NewTextMessage("accepted")) }()
	second := <-async.input
	async.failures <- &sarama.ProducerError{Msg: first, Err: sarama.ErrMessageSizeTooLarge}
	if err := <-failed; !errors.Is(err, sarama.ErrMessageSizeTooLarge) {
		t.Fatalf("rejected publish returned %v", err)
	}
	if !broker.IsConnected() {
		t.Fatal("one rejected message disconnected a healthy producer")
	}
	async.successes <- second
	if err := <-succeeded; err != nil {
		t.Fatalf("unrelated publish was interrupted: %v", err)
	}
}

type blockedCloseProducer struct{ release, closed chan struct{} }

func (p *blockedCloseProducer) SendMessageContext(context.Context, *sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, sarama.ErrOutOfBrokers
}
func (p *blockedCloseProducer) Close() error { <-p.release; close(p.closed); return nil }

func TestFailedProducerCleanupDoesNotBlockPublish(t *testing.T) {
	producer := &blockedCloseProducer{release: make(chan struct{}), closed: make(chan struct{})}
	defer close(producer.release)
	broker := &Broker{config: core.DefaultConfig(), kafkaConfig: core.DefaultKafkaConfig(), producer: producer, connected: true, closeChan: make(chan struct{})}
	defer broker.Close()
	done := make(chan error, 1)
	go func() {
		done <- broker.Publish(context.Background(), "orders", core.NewTextMessage("order"), core.WithTimeout(20*time.Millisecond))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, sarama.ErrOutOfBrokers) || broker.IsConnected() {
			t.Fatalf("failed publish returned %v, connected=%v", err, broker.IsConnected())
		}
	case <-time.After(time.Second):
		t.Fatal("producer cleanup blocked Publish past its deadline")
	}
}
