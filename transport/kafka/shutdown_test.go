package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

func shutdownTestBroker(t *testing.T, group sarama.ConsumerGroup, producer messageProducer) *Broker {
	t.Helper()
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.ConsumerGroup, config.Kafka.ReplyTopic = "workers", "replies"
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	broker.newProducer = func([]string, *sarama.Config) (messageProducer, error) { return producer, nil }
	broker.newConsumerGroup = func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) { return group, nil }
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return broker
}

func TestShutdownDrainsHandlersAndPreservesOffsets(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "canceled"}[force], func(t *testing.T) {
			claims := make(chan *fakeSession, 1)
			group := &fakeConsumerGroup{consumeFn: func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
				messages := make(chan *sarama.ConsumerMessage, 2)
				messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 10}
				messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 11}
				close(messages)
				session := &fakeSession{ctx: ctx}
				err := handler.ConsumeClaim(session, &fakeClaim{messages: messages})
				claims <- session
				<-ctx.Done()
				return err
			}}
			broker := shutdownTestBroker(t, group, &fakeSyncProducer{})
			started := make(chan context.Context, 1)
			release := make(chan struct{})
			var calls atomic.Int32
			if err := broker.Subscribe(context.Background(), "users", func(ctx context.Context, _ *core.Message) error {
				calls.Add(1)
				started <- ctx
				select {
				case <-release:
					return broker.Publish(ctx, "responses", core.NewTextMessage("completed"))
				case <-ctx.Done():
					return ctx.Err()
				}
			}); err != nil {
				t.Fatal(err)
			}
			handlerCtx := <-started
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- broker.Shutdown(ctx) }()
			<-broker.work.Stopping()
			for _, err := range []error{
				broker.Connect(context.Background()),
				broker.Publish(context.Background(), "users", core.NewMessage(nil)),
				broker.Subscribe(handlerCtx, "more", func(context.Context, *core.Message) error { return nil }),
			} {
				if !errors.Is(err, core.ErrShuttingDown) {
					t.Fatalf("new work during drain = %v", err)
				}
			}
			if _, err := broker.Call(context.Background(), "users", core.NewMessage(nil)); !errors.Is(err, core.ErrShuttingDown) {
				t.Fatalf("new Call during shutdown = %v", err)
			}
			if broker.IsConnected() || handlerCtx.Err() != nil {
				t.Fatal("draining broker stayed ready or interrupted its active handler")
			}
			if force {
				cancel()
			} else {
				close(release)
			}
			select {
			case err := <-done:
				if force && !errors.Is(err, context.Canceled) || !force && err != nil {
					t.Fatalf("Shutdown() = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Shutdown() did not return")
			}
			select {
			case session := <-claims:
				session.mu.Lock()
				defer session.mu.Unlock()
				want := 1
				if force {
					want = 0
				}
				if len(session.marked) != want || want == 1 && session.marked[0].Offset != 10 || calls.Load() != 1 {
					t.Fatalf("marked=%v handler calls=%d; want %d marks and one call", session.marked, calls.Load(), want)
				}
			case <-time.After(time.Second):
				t.Fatal("consumer claim did not stop")
			}
		})
	}
}

func TestShutdownWaitsForPublishAcknowledgment(t *testing.T) {
	async := newFakeAsyncProducer()
	broker := shutdownTestBroker(t, &fakeConsumerGroup{}, wrapProducer(async))
	published := make(chan error, 1)
	go func() { published <- broker.Publish(context.Background(), "users", core.NewMessage(nil)) }()
	var message *sarama.ProducerMessage
	select {
	case message = <-async.input:
	case <-time.After(time.Second):
		t.Fatal("publish did not reach producer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before acknowledgment: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	async.successes <- message
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsPendingCall(t *testing.T) {
	reply := make(chan *sarama.ProducerMessage, 1)
	group := &fakeConsumerGroup{consumeFn: func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
		select {
		case request := <-reply:
			messages := make(chan *sarama.ConsumerMessage, 1)
			messages <- &sarama.ConsumerMessage{Topic: "replies", Value: []byte("response"), Headers: []*sarama.RecordHeader{
				{Key: []byte("correlation-id"), Value: []byte(headerValue(request, "correlation-id"))},
			}}
			close(messages)
			if err := handler.ConsumeClaim(&fakeSession{ctx: ctx}, &fakeClaim{messages: messages}); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
		<-ctx.Done()
		return nil
	}}
	sent := make(chan *sarama.ProducerMessage, 1)
	producer := &fakeSyncProducer{afterSend: func(message *sarama.ProducerMessage) { sent <- message }}
	broker := shutdownTestBroker(t, group, producer)
	callDone := make(chan error, 1)
	go func() {
		response, err := broker.Call(context.Background(), "users", core.NewMessage(nil))
		if err == nil && response.BodyString() != "response" {
			err = errors.New("incorrect response")
		}
		callDone <- err
	}()
	request := <-sent
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	reply <- request
	if err := <-callDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineBoundsClientClose(t *testing.T) {
	producer := &blockedCloseProducer{release: make(chan struct{}), closed: make(chan struct{})}
	broker := shutdownTestBroker(t, &fakeConsumerGroup{}, producer)
	t.Cleanup(func() { close(producer.release) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := broker.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() = %v, want deadline", err)
	}
	select {
	case <-producer.closed:
		t.Fatal("blocked client close finished before it was released")
	default:
	}
	if err := broker.Publish(context.Background(), "users", core.NewMessage(nil)); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("publish after forced closure = %v", err)
	}
}
