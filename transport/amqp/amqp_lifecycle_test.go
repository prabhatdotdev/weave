package amqp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

func TestLifecycleEventHooksCanInspectAndCloseBroker(t *testing.T) {
	for _, scenario := range []string{"success", "failure", "close from connect"} {
		t.Run(scenario, func(t *testing.T) {
			config := core.DefaultConfig()
			config.ConnectionRetry, config.RetryDelay = 1, time.Millisecond
			var broker *Broker
			config.EventHook = func(_ context.Context, event core.Event) {
				if event.Name == core.EventConnect && event.Operation == "connect" && event.Duration <= 0 {
					t.Errorf("connection duration missing: %+v", event)
				}
				_ = broker.IsConnected()
				_ = broker.IsRecovering()
				if event.Operation == "close" || scenario == "close from connect" && event.Name == core.EventConnect {
					if err := broker.Close(); err != nil {
						t.Errorf("Close() from hook: %v", err)
					}
				}
			}
			brokerAny, _ := NewBroker(config)
			broker = brokerAny.(*Broker)
			broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
				if scenario == "failure" {
					return nil, errors.New("dial failed")
				}
				return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
			}
			done := make(chan error, 1)
			go func() { done <- broker.Connect(context.Background()) }()
			select {
			case err := <-done:
				if (err != nil) != (scenario == "failure") {
					t.Fatalf("Connect() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Connect() deadlocked in its event hook")
			}
			go func() { done <- broker.Close() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close() deadlocked in its event hook")
			}
		})
	}
}

func TestReconnectEventHookCanCloseBroker(t *testing.T) {
	brokerAny, _ := NewBroker(core.DefaultConfig())
	broker := brokerAny.(*Broker)
	broker.config.EventHook = func(_ context.Context, event core.Event) {
		_ = broker.IsConnected()
		if event.Name == "reconnect_attempt" {
			_ = broker.Close()
		}
	}
	done := make(chan struct{})
	go func() { broker.reconnectLoop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconnect hook could not close the broker")
	}
}

func TestConnectCancellation(t *testing.T) {
	for _, scenario := range []string{"before connect", "retry wait", "in-flight dial", "waiting for connect", "broker close"} {
		t.Run(scenario, func(t *testing.T) {
			config := core.DefaultConfig()
			config.RetryDelay = time.Hour
			brokerAny, _ := NewBroker(config)
			broker := brokerAny.(*Broker)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			var attempts atomic.Int32
			broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
				if attempts.Add(1) == 1 {
					close(entered)
				}
				if scenario != "retry wait" {
					<-release
				}
				return nil, errors.New("dial failed")
			}
			if scenario == "before connect" {
				cancel()
			}
			done := make(chan error, 1)
			go func() { done <- broker.Connect(ctx) }()
			if scenario != "before connect" {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("dial did not start")
				}
				if scenario == "waiting for connect" {
					waitingCtx, cancelWaiting := context.WithCancel(context.Background())
					waiting := make(chan error, 1)
					go func() { waiting <- broker.Connect(waitingCtx) }()
					cancelWaiting()
					select {
					case err := <-waiting:
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("waiting Connect() error = %v", err)
						}
					case <-time.After(time.Second):
						t.Fatal("waiting Connect() ignored cancellation")
					}
				}
				if scenario == "broker close" {
					if err := broker.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
			}
			want := error(context.Canceled)
			if scenario == "broker close" {
				want = core.ErrClosed
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("Connect() error = %v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("Connect() did not stop promptly")
			}
			if scenario == "before connect" && attempts.Load() != 0 {
				t.Fatal("pre-canceled Connect() attempted a connection")
			}
			if broker.IsConnected() {
				t.Fatal("canceled Connect() marked the broker connected")
			}
			_ = broker.Close()
		})
	}
}
