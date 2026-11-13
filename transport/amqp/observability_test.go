package amqp

import (
	"context"
	"errors"
	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
	"testing"
	"time"
)

func metricsTestBroker(t *testing.T, hook core.EventHook, publishErr error) (*Broker, <-chan struct{}) {
	t.Helper()
	config := core.DefaultConfig()
	config.EventHook = hook
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	channel := &signaledPublishChannel{fakeAMQPChannel: newFakeAMQPChannel(), sent: make(chan struct{})}
	channel.publishErr = publishErr
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		return &fakeAMQPConnection{channel: channel}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return broker, channel.sent
}

func TestRPCOperationalMetrics(t *testing.T) {
	for _, outcome := range []string{"success", "publish failure", "canceled", "timeout", "closed"} {
		t.Run(outcome, func(t *testing.T) {
			events := make(chan core.Event, 64)
			var broker *Broker
			hook := func(ctx context.Context, event core.Event) {
				if event.Name == core.EventCallStarted {
					broker.pendingMu.RLock()
					registered := broker.pending[event.CorrelationID] != nil
					broker.pendingMu.RUnlock()
					if !registered {
						t.Error("RPC start event preceded registration")
					}
					_, err := broker.Call(ctx, "orders", core.NewMessage(nil).WithCorrelationID(event.CorrelationID))
					if !errors.Is(err, core.ErrDuplicateCorrelationID) {
						t.Errorf("reentrant hook error=%v", err)
					}
				}
				if event.Name == core.EventCallFinished {
					broker.pendingMu.RLock()
					remaining := len(broker.pending)
					broker.pendingMu.RUnlock()
					if remaining != 0 {
						t.Errorf("RPC finish preceded cleanup: pending=%d", remaining)
					}
				}
				events <- event
			}
			publishErr := error(nil)
			if outcome == "publish failure" {
				publishErr = errors.New("destination failed")
			}
			var sent <-chan struct{}
			broker, sent = metricsTestBroker(t, hook, publishErr)
			timeout := time.Second
			if outcome == "timeout" {
				timeout = 10 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := broker.Call(ctx, "orders", core.NewTextMessage("request").WithCorrelationID("rpc-1"))
				done <- err
			}()
			select {
			case <-sent:
			case <-time.After(time.Second):
				t.Fatal("RPC publish did not begin")
			}
			switch outcome {
			case "success":
				broker.pendingMu.RLock()
				pending := broker.pending["rpc-1"]
				broker.pendingMu.RUnlock()
				pending.complete(&amqplib.Delivery{Body: []byte("response")})
			case "canceled":
				cancel()
			case "closed":
				_ = broker.Close()
			}
			var result error
			select {
			case result = <-done:
			case <-time.After(time.Second):
				t.Fatal("RPC did not finish")
			}
			if (result == nil) != (outcome == "success") {
				t.Fatalf("RPC result=%v for %s", result, outcome)
			}
			if outcome == "canceled" && !errors.Is(result, context.Canceled) || outcome == "timeout" && !errors.Is(result, context.DeadlineExceeded) || outcome == "publish failure" && !errors.Is(result, publishErr) {
				t.Fatalf("lost RPC error: %v", result)
			}
			starts, finishes, published := 0, 0, 0
			for len(events) > 0 {
				event := <-events
				if event.Name == core.EventCallStarted {
					starts++
					if event.PendingRPCs != 1 || event.Duration != 0 {
						t.Errorf("start event=%+v", event)
					}
				}
				if event.Name == core.EventCallFinished {
					finishes++
					if event.PendingRPCs != 0 || event.Duration <= 0 || (event.Err == nil) != (result == nil) || event.CorrelationID != "rpc-1" {
						t.Errorf("finish event=%+v", event)
					}
				}
				if event.Name == core.EventPublishSucceeded {
					published++
					if event.PendingRPCs != 1 || event.Duration <= 0 {
						t.Errorf("publish event=%+v", event)
					}
				}
			}
			if starts != 1 || finishes != 1 {
				t.Fatalf("RPC starts=%d finishes=%d", starts, finishes)
			}
			if outcome == "publish failure" && published != 0 {
				t.Fatal("failed publish emitted success")
			}
		})
	}
}

func TestSubscriptionOperationalMetrics(t *testing.T) {
	events := make(chan core.Event, 32)
	broker, _ := metricsTestBroker(t, func(_ context.Context, event core.Event) { events <- event }, nil)
	ctx := context.Background()
	if err := broker.Subscribe(ctx, "orders", func(ctx context.Context, _ *core.Message) error {
		return broker.Publish(ctx, "replies", core.NewTextMessage("reply").WithCorrelationID("handler-1"))
	}); err != nil {
		t.Fatal(err)
	}
	broker.subsMu.RLock()
	handler := broker.subs[0].handler
	broker.subsMu.RUnlock()
	if err := handler(ctx, core.NewTextMessage("request").WithCorrelationID("handler-1")); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]core.Event)
	for len(events) > 0 {
		event := <-events
		seen[event.Name] = event
		if event.Backend != backendName || event.Timestamp.IsZero() || event.ActiveHandlers < 0 || event.PendingRPCs != 0 {
			t.Errorf("event=%+v", event)
		}
	}
	for _, name := range []string{core.EventConnect, core.EventSubscribeSucceeded, core.EventPublishSucceeded, core.EventHandlerFinished} {
		if seen[name].Duration <= 0 {
			t.Errorf("%s duration missing: %+v", name, seen[name])
		}
	}
	if seen[core.EventHandlerStarted].ActiveHandlers != 1 || seen[core.EventPublishSucceeded].ActiveHandlers != 1 || seen[core.EventHandlerFinished].ActiveHandlers != 0 {
		t.Errorf("active handler events=%+v", seen)
	}
}
