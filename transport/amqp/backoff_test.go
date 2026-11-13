package amqp

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestConnectionBackoffOverridesDelayAndResets(t *testing.T) {
	config := core.DefaultConfig()
	config.ConnectionRetry, config.RetryDelay = 2, time.Hour
	seen := make(chan int, 16)
	jitter := core.JitteredBackoff(core.FixedBackoff(time.Millisecond))
	config.ConnectionBackoff = func(attempt int) time.Duration { seen <- attempt; return jitter(attempt) }
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	var dials atomic.Int32
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		if dials.Add(1)%3 != 0 {
			return nil, errors.New("offline")
		}
		return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for cycle := 0; cycle < 2; cycle++ {
		if err := broker.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if cycle == 0 {
			broker.handleConnectionLoss(broker.conn, nil)
		}
	}
	var attempts []int
	for len(seen) > 0 {
		attempts = append(attempts, <-seen)
	}
	if !reflect.DeepEqual(attempts, []int{1, 2, 1, 2}) || dials.Load() != 6 {
		t.Fatalf("attempts=%v dials=%d", attempts, dials.Load())
	}
}

func TestConnectionBackoffStopsOnCancellationCloseAndShutdown(t *testing.T) {
	for _, outcome := range []string{"canceled", "closed", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var broker *Broker
			shutdown := make(chan error, 1)
			config := core.DefaultConfig()
			config.ConnectionBackoff = func(int) time.Duration {
				switch outcome {
				case "canceled":
					cancel()
				case "closed":
					_ = broker.Close()
				case "shutdown":
					go func() { shutdown <- broker.Shutdown(context.Background()) }()
					<-broker.work.Stopping()
				}
				if outcome == "shutdown" {
					return time.Hour
				}
				return 0
			}
			brokerAny, _ := NewBroker(config)
			broker = brokerAny.(*Broker)
			t.Cleanup(func() { _ = broker.Close() })
			var dials atomic.Int32
			broker.dial = func(string, amqplib.Config) (amqpConnection, error) { dials.Add(1); return nil, errors.New("offline") }
			done := make(chan error, 1)
			go func() { done <- broker.Connect(ctx) }()
			want := error(context.Canceled)
			if outcome == "closed" {
				want = core.ErrClosed
			}
			if outcome == "shutdown" {
				want = core.ErrShuttingDown
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("Connect=%v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("backoff did not stop")
			}
			if dials.Load() != 1 {
				t.Fatalf("retried after interruption: %d dials", dials.Load())
			}
			if outcome == "shutdown" {
				if err := <-shutdown; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestReconnectBackoffCoversRepeatedConnectFailures(t *testing.T) {
	config := core.DefaultConfig()
	config.ConnectionRetry, config.RetryDelay = 1, time.Hour
	seen := make(chan int, 16)
	config.ConnectionBackoff = func(attempt int) time.Duration { seen <- attempt; return 0 }
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	var dials atomic.Int32
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		if dials.Add(1) <= 4 {
			return nil, errors.New("offline")
		}
		return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
	}
	done := make(chan struct{})
	go func() { broker.reconnectLoop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconnect ignored configured backoff")
	}
	var attempts []int
	for len(seen) > 0 {
		attempts = append(attempts, <-seen)
	}
	if !reflect.DeepEqual(attempts, []int{1, 1, 1, 2}) || !broker.IsConnected() {
		t.Fatalf("attempts=%v connected=%v", attempts, broker.IsConnected())
	}
}

type failedRestoreChannel struct{ *fakeAMQPChannel }

func (c *failedRestoreChannel) QueueDeclare(string, bool, bool, bool, bool, amqplib.Table) (amqplib.Queue, error) {
	return amqplib.Queue{}, errors.New("temporary topology failure")
}

func TestReconnectBackoffCoversSubscriptionRestoreFailure(t *testing.T) {
	config := core.DefaultConfig()
	config.RetryDelay = time.Hour
	seen := make(chan int, 4)
	config.ConnectionBackoff = func(attempt int) time.Duration { seen <- attempt; return 0 }
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	clients := []*fakeAMQPConnection{
		{channel: newFakeAMQPChannel()},
		{channel: &failedRestoreChannel{newFakeAMQPChannel()}},
		{channel: newFakeAMQPChannel()},
	}
	var dials atomic.Int32
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) { return clients[int(dials.Add(1))-1], nil }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(ctx, "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	broker.handleConnectionLoss(clients[0], nil)
	done := make(chan struct{})
	go func() { broker.reconnectLoop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("restore retry ignored backoff strategy")
	}
	if !broker.IsReady() || len(seen) != 1 || <-seen != 1 {
		t.Fatalf("restore recovery: ready=%v backoff calls=%d", broker.IsReady(), len(seen))
	}
}
