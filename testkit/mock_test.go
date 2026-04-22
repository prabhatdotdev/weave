package testkit

import (
	"context"
	"errors"
	"testing"

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
