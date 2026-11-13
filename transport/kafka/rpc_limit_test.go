package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

func limitedTestBroker(t *testing.T, maximum int, publishErr error) (*Broker, <-chan string) {
	t.Helper()
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.ConsumerGroup, config.Kafka.ReplyTopic = "limits", "replies"
	config.MaxPendingCalls = maximum
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxPendingCalls = maximum + 10
	broker := brokerAny.(*Broker)
	if broker.config.MaxPendingCalls != maximum {
		t.Fatal("constructor retained caller configuration")
	}
	sent := make(chan string, 16)
	producer := &fakeSyncProducer{sendErr: publishErr, afterSend: func(msg *sarama.ProducerMessage) { sent <- headerValue(msg, "correlation-id") }}
	broker.newProducer = func([]string, *sarama.Config) (messageProducer, error) { return producer, nil }
	broker.newConsumerGroup = func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) { return &fakeConsumerGroup{}, nil }
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return broker, sent
}

func startLimitedCall(broker *Broker, ctx context.Context, id string, opts ...core.PublishOption) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := broker.Call(ctx, "rpc."+id, core.NewTextMessage("request").WithCorrelationID(id), opts...)
		done <- err
	}()
	return done
}

func awaitLimitedCall(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("RPC was stranded")
		return nil
	}
}

func awaitLimitedPublish(t *testing.T, sent <-chan string, want string) {
	t.Helper()
	select {
	case id := <-sent:
		if id != want {
			t.Fatalf("published correlation ID=%q, want %q", id, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("RPC %q did not publish", want)
	}
}

func completeLimitedCall(t *testing.T, broker *Broker, id string) {
	t.Helper()
	broker.pendingMu.RLock()
	pending := broker.pending[id]
	broker.pendingMu.RUnlock()
	if pending == nil {
		t.Fatalf("RPC %q is not registered", id)
	}
	pending.complete(core.NewTextMessage("response"))
}

func TestPendingCallLimitAndUnlimitedDefault(t *testing.T) {
	for _, maximum := range []int{0, 2} {
		t.Run(fmt.Sprint(maximum), func(t *testing.T) {
			broker, sent := limitedTestBroker(t, maximum, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			first := startLimitedCall(broker, ctx, "first")
			awaitLimitedPublish(t, sent, "first")
			second := startLimitedCall(broker, ctx, "second")
			awaitLimitedPublish(t, sent, "second")
			third := startLimitedCall(broker, ctx, "third")
			if maximum != 0 {
				select {
				case id := <-sent:
					t.Fatalf("over-capacity RPC was published: %s", id)
				case <-time.After(20 * time.Millisecond):
				}
				broker.pendingMu.RLock()
				count, queued := len(broker.pending), broker.pending["third"]
				broker.pendingMu.RUnlock()
				if count != maximum || queued != nil {
					t.Fatalf("pending=%d queued=%v", count, queued)
				}
			}
			completeLimitedCall(t, broker, "first")
			if err := awaitLimitedCall(t, first); err != nil {
				t.Fatal(err)
			}
			select {
			case id := <-sent:
				if id != "third" {
					t.Fatal(id)
				}
			case <-ctx.Done():
				t.Fatal("released capacity did not wake a waiter")
			}
			completeLimitedCall(t, broker, "second")
			completeLimitedCall(t, broker, "third")
			for _, done := range []<-chan error{second, third} {
				if err := awaitLimitedCall(t, done); err != nil {
					t.Fatal(err)
				}
			}
			broker.pendingMu.RLock()
			remaining := len(broker.pending)
			broker.pendingMu.RUnlock()
			if remaining != 0 {
				t.Fatalf("leaked pending calls=%d", remaining)
			}
		})
	}
}

func TestPendingCallLimitWaitingHonorsCancellationAndTimeout(t *testing.T) {
	for _, outcome := range []string{"canceled", "context deadline", "publish timeout"} {
		t.Run(outcome, func(t *testing.T) {
			broker, sent := limitedTestBroker(t, 1, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			first := startLimitedCall(broker, ctx, "first")
			awaitLimitedPublish(t, sent, "first")
			waitCtx, stopWaiting := context.WithCancel(ctx)
			defer stopWaiting()
			var opts []core.PublishOption
			if outcome == "context deadline" {
				waitCtx, stopWaiting = context.WithTimeout(ctx, 20*time.Millisecond)
				defer stopWaiting()
			}
			if outcome == "publish timeout" {
				opts = []core.PublishOption{core.WithTimeout(20 * time.Millisecond)}
			}
			queued := startLimitedCall(broker, waitCtx, "queued", opts...)
			if outcome == "canceled" {
				select {
				case id := <-sent:
					t.Fatalf("queued call published: %s", id)
				case <-time.After(20 * time.Millisecond):
				}
				stopWaiting()
			}
			err := awaitLimitedCall(t, queued)
			want := error(context.DeadlineExceeded)
			if outcome == "canceled" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || (outcome != "canceled") != core.IsTimeout(err) {
				t.Fatalf("waiting call=%v", err)
			}
			broker.pendingMu.RLock()
			count := len(broker.pending)
			broker.pendingMu.RUnlock()
			if count != 1 {
				t.Fatalf("expired waiter registered a pending call: %d", count)
			}
			select {
			case id := <-sent:
				t.Fatalf("expired waiter published: %s", id)
			default:
			}
			completeLimitedCall(t, broker, "first")
			if err := awaitLimitedCall(t, first); err != nil {
				t.Fatal(err)
			}
			next := startLimitedCall(broker, ctx, "next")
			select {
			case <-sent:
			case <-ctx.Done():
				t.Fatal("expired waiter leaked RPC capacity")
			}
			completeLimitedCall(t, broker, "next")
			if err := awaitLimitedCall(t, next); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPendingCallLimitShutdownReleasesWaiters(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprint(force), func(t *testing.T) {
			broker, sent := limitedTestBroker(t, 1, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			first := startLimitedCall(broker, ctx, "first")
			awaitLimitedPublish(t, sent, "first")
			queued := startLimitedCall(broker, ctx, "queued")
			select {
			case id := <-sent:
				t.Fatalf("queued call published: %s", id)
			case <-time.After(20 * time.Millisecond):
			}
			shutdownDone := make(chan error, 1)
			if force {
				_ = broker.Close()
			} else {
				go func() { shutdownDone <- broker.Shutdown(ctx) }()
			}
			want := core.ErrShuttingDown
			if force {
				want = core.ErrClosed
			}
			if err := awaitLimitedCall(t, queued); !errors.Is(err, want) {
				t.Fatalf("queued call=%v, want %v", err, want)
			}
			if !force {
				completeLimitedCall(t, broker, "first")
			}
			err := awaitLimitedCall(t, first)
			if !force && err != nil || force && !core.IsConnectionLost(err) {
				t.Fatalf("admitted call=%v", err)
			}
			if !force {
				if err := awaitLimitedCall(t, shutdownDone); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case id := <-sent:
				t.Fatalf("stopped waiter published: %s", id)
			default:
			}
		})
	}
}

func TestPendingCallLimitReleasesPublishFailure(t *testing.T) {
	failure := errors.New("publish failed")
	broker, _ := limitedTestBroker(t, 1, failure)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, id := range []string{"first", "next"} {
		if err := awaitLimitedCall(t, startLimitedCall(broker, ctx, id)); !errors.Is(err, failure) {
			t.Fatalf("publish failure leaked capacity: %v", err)
		}
	}
}

func TestPendingCallLimitReleasesSetupFailure(t *testing.T) {
	broker, sent := limitedTestBroker(t, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	broker.kafkaConfig.ReplyTopic = ""
	if _, err := broker.Call(ctx, "orders", core.NewMessage(nil)); !errors.Is(err, core.ErrInvalidConfig) {
		t.Fatalf("setup failure=%v", err)
	}
	broker.kafkaConfig.ReplyTopic = "replies"
	next := startLimitedCall(broker, ctx, "next")
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("setup failure leaked capacity")
	}
	completeLimitedCall(t, broker, "next")
	if err := awaitLimitedCall(t, next); err != nil {
		t.Fatal(err)
	}
}
