package testkit

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestMockBrokerLifecycleAndMessaging(t *testing.T) {
	t.Parallel()

	broker := NewMockBroker()
	ctx := context.Background()

	if err := broker.Publish(ctx, "orders", core.NewTextMessage("payload")); !core.IsNotConnected(err) {
		t.Fatalf("Publish() before Connect() error = %v, want ErrNotConnected", err)
	}

	if err := broker.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if !broker.IsConnected() {
		t.Fatal("broker should report connected after Connect()")
	}

	if err := broker.Publish(ctx, "orders", core.NewTextMessage("created")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	broker.AssertPublished(t, "orders")
	broker.AssertPublishCount(t, "orders", 1)
	broker.AssertNotPublished(t, "users")

	seen := ""
	if err := broker.Subscribe(ctx, "orders", func(ctx context.Context, msg *core.Message) error {
		seen = msg.BodyString()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	if err := broker.SimulateMessage(ctx, "orders", core.NewTextMessage("simulated")); err != nil {
		t.Fatalf("SimulateMessage() error = %v", err)
	}
	if seen != "simulated" {
		t.Fatalf("handler saw %q, want %q", seen, "simulated")
	}

	broker.SetCallResponse("users.get", core.NewTextMessage("user"))
	response, err := broker.Call(ctx, "users.get", core.NewTextMessage("42"))
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if response.BodyString() != "user" {
		t.Fatalf("response.BodyString() = %q, want %q", response.BodyString(), "user")
	}

	want := errors.New("rpc failed")
	broker.SetCallError("users.fail", want)
	if _, err := broker.Call(ctx, "users.fail", core.NewTextMessage("42")); !errors.Is(err, want) {
		t.Fatalf("Call() error = %v, want %v", err, want)
	}

	if subs := broker.Subscriptions(); len(subs) != 1 || subs[0] != "orders" {
		t.Fatalf("Subscriptions() = %v, want [orders]", subs)
	}

	broker.Reset()
	if got := broker.PublishedMessages(); len(got) != 0 {
		t.Fatalf("PublishedMessages() after Reset() = %v, want empty", got)
	}
	if _, err := broker.Call(ctx, "users.get", core.NewTextMessage("42")); !core.IsTimeout(err) {
		t.Fatalf("Call() after Reset() error = %v, want ErrTimeout", err)
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if broker.IsConnected() {
		t.Fatal("broker should report disconnected after Close()")
	}
	if err := broker.Connect(ctx); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Connect() after Close() error = %v, want ErrClosed", err)
	}
}

func TestRecordedMessageInspectionReturnsOwnedCopies(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	msg := core.NewTextMessage("body").WithHeader("trace", "original")
	if err := broker.Publish(context.Background(), "orders", msg); err != nil {
		t.Fatal(err)
	}
	for _, records := range []func() []PublishedMessage{broker.PublishedMessages, func() []PublishedMessage { return broker.PublishedTo("orders") }} {
		returned := records()
		returned[0].Message.Body[0] = 'X'
		returned[0].Message.Headers["trace"] = "changed"
		returned[0].Destination = "changed"
		if next := records(); len(next) != 1 || next[0].Destination != "orders" || !reflect.DeepEqual(next[0].Message, msg) {
			t.Fatalf("inspection mutated recorded messages: %+v", next)
		}
	}
}

func TestNilCallResponseClearsConfiguredReply(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	broker.SetCallResponse("orders", core.NewTextMessage("reply"))
	broker.SetCallResponse("orders", nil)
	if _, err := broker.Call(context.Background(), "orders", core.NewMessage(nil)); !core.IsTimeout(err) {
		t.Fatalf("cleared reply returned %v", err)
	}
}

func TestSimulationContextCancelsActiveHandler(t *testing.T) {
	broker := NewMockBroker()
	defer broker.Close()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	if err := broker.Subscribe(context.Background(), "orders", func(ctx context.Context, _ *core.Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- broker.SimulateMessage(ctx, "orders", core.NewMessage(nil)) }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("simulation cancellation did not reach the handler")
	}
}
