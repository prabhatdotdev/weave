// Package brokercontract contains shared tests for MessageBroker implementations.
package brokercontract

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

// Fixture supplies only broker I/O; all contract assertions live in Run.
// New fixtures start disconnected. Deliver creates an isolated incoming packet.
type Fixture struct {
	Broker    core.MessageBroker
	Published func() []*core.Message
	Deliver   func(context.Context, string, *core.Message) error
	Reply     func(*core.Message)
}

func sample() *core.Message {
	return &core.Message{
		Body: []byte("original body"), Headers: map[string]string{"trace-id": "original-trace"},
		CorrelationID: "contract-correlation", ReplyTo: "contract.reply", ContentType: "text/plain",
		MessageID: "contract-message", Timestamp: time.Unix(1700000000, 0).UTC(),
	}
}

func mutate(msg *core.Message) {
	msg.Body[0] = 'X'
	msg.Headers["trace-id"] = "changed"
	msg.MessageID = "changed"
}

func metadata(t *testing.T, got, want *core.Message) {
	t.Helper()
	if got == nil || !bytes.Equal(got.Body, want.Body) || !maps.Equal(got.Headers, want.Headers) ||
		got.CorrelationID != want.CorrelationID || got.ReplyTo != want.ReplyTo || got.ContentType != want.ContentType ||
		got.MessageID != want.MessageID || !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("portable metadata changed:\n got: %+v\nwant: %+v", got, want)
	}
}

// Run exercises the same ownership, metadata, cancellation, and closure contract
// with each backend's real public operations and a deterministic I/O fixture.
func Run(t *testing.T, newFixture func(*testing.T) Fixture) {
	run := func(name string, test func(*testing.T, context.Context, Fixture)) {
		t.Run(name, func(t *testing.T) {
			h := newFixture(t)
			t.Cleanup(func() { _ = h.Broker.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.Cleanup(cancel)
			if h.Broker.IsConnected() {
				t.Fatal("new broker is already connected")
			}
			if err := h.Broker.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			if !h.Broker.IsConnected() {
				t.Fatal("connected broker is not ready")
			}
			test(t, ctx, h)
		})
	}
	run("publish ownership and metadata", func(t *testing.T, ctx context.Context, h Fixture) {
		msg := sample()
		original := msg.Clone()
		if err := h.Broker.Publish(ctx, "contract", msg); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(msg, original) {
			t.Fatal("Publish mutated its input")
		}
		mutate(msg)
		published := h.Published()
		if len(published) != 1 {
			t.Fatalf("published=%d, want 1", len(published))
		}
		metadata(t, published[0], original)
	})
	run("consumer readiness", func(t *testing.T, ctx context.Context, h Fixture) {
		ready, ok := h.Broker.(core.ConsumerReadiness)
		if !ok {
			t.Fatal("broker does not implement ConsumerReadiness")
		}
		if !ready.IsReady() {
			t.Fatal("connected publisher-only broker is not ready")
		}
		if err := ready.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		if err := ready.WaitReady(nil); !errors.Is(err, core.ErrInvalidConfig) {
			t.Fatalf("nil wait context = %v", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := ready.WaitReady(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled readiness wait = %v", err)
		}
		expired, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer stop()
		if err := ready.WaitReady(expired); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired readiness wait = %v", err)
		}
		subCtx, unsubscribe := context.WithCancel(ctx)
		if err := h.Broker.Subscribe(subCtx, "contract", func(context.Context, *core.Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := ready.WaitReady(ctx); err != nil {
			t.Fatalf("consumer never became ready: %v", err)
		}
		unsubscribe()
		if err := ready.WaitReady(ctx); err != nil || !ready.IsReady() {
			t.Fatalf("canceled subscription prevented publisher readiness: %v", err)
		}
		if err := h.Broker.(core.Shutdowner).Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		if ready.IsReady() {
			t.Fatal("closed broker reports ready")
		}
		if err := ready.WaitReady(ctx); !errors.Is(err, core.ErrClosed) {
			t.Fatalf("closed readiness wait = %v", err)
		}
	})
	run("RPC ownership and reply metadata", func(t *testing.T, ctx context.Context, h Fixture) {
		response := sample()
		response.Body, response.MessageID = []byte("reply body"), "reply-message"
		expected := response.Clone()
		h.Reply(response)
		mutate(response)
		request := sample()
		original := request.Clone()
		for range 2 {
			reply, err := h.Broker.Call(ctx, "contract", request, core.WithTimeout(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(request, original) {
				t.Fatal("Call mutated its request")
			}
			metadata(t, reply, expected)
			mutate(reply)
		}
	})
	run("delivery ownership and metadata", func(t *testing.T, ctx context.Context, h Fixture) {
		incoming := sample()
		original := incoming.Clone()
		seen := make(chan *core.Message, 1)
		if err := h.Broker.Subscribe(ctx, "contract", func(_ context.Context, msg *core.Message) error {
			seen <- msg.Clone()
			mutate(msg)
			return nil
		}, core.WithWorkerCount(1)); err != nil {
			t.Fatal(err)
		}
		if err := h.Deliver(ctx, "contract", incoming); err != nil {
			t.Fatal(err)
		}
		select {
		case msg := <-seen:
			metadata(t, msg, original)
		case <-ctx.Done():
			t.Fatal("delivery did not reach handler")
		}
		if !reflect.DeepEqual(incoming, original) {
			t.Fatal("handler mutated caller-owned delivery data")
		}
	})
	for _, expired := range []bool{false, true} {
		name := "canceled operations"
		if expired {
			name = "expired operations"
		}
		run(name, func(t *testing.T, ctx context.Context, h Fixture) {
			stopped, cancel := context.WithCancel(ctx)
			cancel()
			want := error(context.Canceled)
			if expired {
				stopped, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			}
			h.Reply(sample())
			input := sample()
			original := input.Clone()
			_, callErr := h.Broker.Call(stopped, "contract", input)
			for _, err := range []error{
				h.Broker.Connect(stopped), h.Broker.Publish(stopped, "contract", input), callErr,
				h.Broker.Subscribe(stopped, "contract", func(context.Context, *core.Message) error { t.Error("canceled subscription ran"); return nil }),
			} {
				if !errors.Is(err, want) {
					t.Errorf("operation error=%v, want %v", err, want)
				}
			}
			if expired && !core.IsTimeout(callErr) || !expired && core.IsTimeout(callErr) {
				t.Errorf("wrong RPC timeout classification: %v", callErr)
			}
			if len(h.Published()) != 0 {
				t.Fatal("canceled operation published a message")
			}
			if !reflect.DeepEqual(input, original) {
				t.Fatal("canceled operation mutated input")
			}
			if !h.Broker.IsConnected() {
				t.Fatal("operation cancellation closed the connection")
			}
		})
	}
	run("closed state", func(t *testing.T, ctx context.Context, h Fixture) {
		if err := h.Broker.Close(); err != nil {
			t.Fatal(err)
		}
		if err := h.Broker.Close(); err != nil {
			t.Fatalf("repeated Close: %v", err)
		}
		if h.Broker.IsConnected() || h.Broker.IsRecovering() {
			t.Fatal("closed broker reports connected or recovering")
		}
		_, callErr := h.Broker.Call(ctx, "contract", sample())
		for _, err := range []error{
			h.Broker.Connect(ctx), h.Broker.Publish(ctx, "contract", sample()), callErr,
			h.Broker.Subscribe(ctx, "contract", func(context.Context, *core.Message) error { return nil }),
		} {
			if !errors.Is(err, core.ErrClosed) {
				t.Errorf("closed operation=%v, want ErrClosed", err)
			}
		}
		if len(h.Published()) != 0 {
			t.Fatal("closed operation published")
		}
	})
	for _, closeBroker := range []bool{false, true} {
		name := "subscription cancellation"
		if closeBroker {
			name = "closure cancels handler"
		}
		run(name, func(t *testing.T, ctx context.Context, h Fixture) {
			type traceKey struct{}
			subCtx, cancel := context.WithCancel(context.WithValue(ctx, traceKey{}, "subscription-trace"))
			defer cancel()
			started, exited := make(chan context.Context, 1), make(chan error, 1)
			if err := h.Broker.Subscribe(subCtx, "contract", func(handlerCtx context.Context, _ *core.Message) error {
				started <- handlerCtx
				<-handlerCtx.Done()
				exited <- handlerCtx.Err()
				return handlerCtx.Err()
			}, core.WithWorkerCount(1)); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- h.Deliver(ctx, "contract", sample()) }()
			select {
			case handlerCtx := <-started:
				if handlerCtx.Value(traceKey{}) != "subscription-trace" {
					t.Error("subscription context values were lost")
				}
			case <-ctx.Done():
				t.Fatal("handler did not start")
			}
			if closeBroker {
				if err := h.Broker.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case err := <-exited:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("handler error=%v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("handler ignored subscription cancellation/closure")
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("delivery did not complete")
			}
			if !closeBroker && !h.Broker.IsConnected() {
				t.Fatal("subscription cancellation closed the broker")
			}
		})
	}
}
