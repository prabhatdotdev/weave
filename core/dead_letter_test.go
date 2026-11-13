package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type deadLetterPublisherFunc func(context.Context, string, *Message, ...PublishOption) error

func (f deadLetterPublisherFunc) Publish(ctx context.Context, destination string, msg *Message, opts ...PublishOption) error {
	return f(ctx, destination, msg, opts...)
}

func TestDeadLetterHandlerRetriesThenPublishesOriginal(t *testing.T) {
	failure := errors.New("invalid order")
	for _, maxAttempts := range []int{0, 3} {
		t.Run(fmt.Sprint(maxAttempts), func(t *testing.T) {
			calls, publishes := 0, 0
			details := map[string]any{"nested": map[string]string{"reason": "original"}}
			opts := []PublishOption{WithTimeout(time.Second), func(o *PublishOptions) { o.Mandatory = false }}
			msg := NewTextMessage("original").WithHeader("trace", "original").WithCorrelationID("correlation")
			msg.MessageID, msg.Subject, msg.Partition, msg.Offset = "message", "orders", 2, 42
			msg.ReplyTo, msg.ContentType = "replies", "text/plain"
			publisher := deadLetterPublisherFunc(func(ctx context.Context, destination string, dead *Message, options ...PublishOption) error {
				publishes++
				if destination != "orders.dead" || ctx != context.Background() {
					t.Fatalf("unexpected destination or context: %q %v", destination, ctx)
				}
				p := ApplyPublishOptions(options...)
				if !p.Mandatory || p.DeliveryMode != 2 || p.Timeout != time.Second {
					t.Errorf("publish options = %+v", p)
				}
				envelope, err := DecodeDeadLetterMessage(dead)
				if err != nil {
					t.Fatal(err)
				}
				if dead.ContentType != DeadLetterContentType || dead.CorrelationID != "correlation" || envelope.Attempt != calls || envelope.Error.Message != failure.Error() {
					t.Errorf("unexpected envelope: %+v", envelope)
				}
				if envelope.Backend != "kafka" || envelope.Destination != "orders" || envelope.Handler != "validate" || envelope.Error.Code != "invalid_order" || envelope.FailedAt.IsZero() {
					t.Errorf("missing failure metadata: %+v", envelope)
				}
				original := envelope.Original
				if string(original.Body) != "original" || original.Headers["trace"] != "original" || original.MessageID != "message" || original.Subject != "orders" || original.Partition != 2 || original.Offset != 42 || original.ReplyTo != "replies" || original.ContentType != "text/plain" {
					t.Errorf("original delivery changed: %+v", original)
				}
				if envelope.Error.Details["nested"].(map[string]any)["reason"] != "original" {
					t.Errorf("details were not snapshotted: %v", envelope.Error.Details)
				}
				return nil
			})
			handler, err := DeadLetterHandler(func(ctx context.Context, msg *Message) error {
				calls++
				if RetryAttempt(ctx) != calls {
					t.Errorf("retry attempt = %d, want %d", RetryAttempt(ctx), calls)
				}
				msg.Body[0], msg.Headers["trace"] = 'X', "changed"
				return failure
			}, RetryPolicy{MaxAttempts: maxAttempts}, publisher, "orders.dead", DeadLetterOptions{
				Backend: "kafka", Destination: "orders", Handler: "validate", Attempt: 999, Code: "invalid_order", Details: details,
			}, opts...)
			if err != nil {
				t.Fatal(err)
			}
			details["nested"].(map[string]string)["reason"] = "changed"
			opts[0] = WithTimeout(time.Hour)
			if err := handler(context.Background(), msg); err != nil {
				t.Fatal(err)
			}
			if calls != max(1, maxAttempts) || publishes != 1 {
				t.Fatalf("handler calls=%d publishes=%d", calls, publishes)
			}
		})
	}
}

func TestDeadLetterHandlerSuccessAndNonRetryableFailure(t *testing.T) {
	for _, successfulAttempt := range []int{1, 2, 0} {
		t.Run(fmt.Sprint(successfulAttempt), func(t *testing.T) {
			calls, publishes := 0, 0
			handler, err := DeadLetterHandler(func(context.Context, *Message) error {
				calls++
				if calls == successfulAttempt {
					return nil
				}
				return errors.New("failed")
			}, RetryPolicy{MaxAttempts: 3, Retryable: func(error) bool { return successfulAttempt != 0 }}, deadLetterPublisherFunc(func(_ context.Context, _ string, msg *Message, _ ...PublishOption) error {
				publishes++
				envelope, err := DecodeDeadLetterMessage(msg)
				if err != nil || envelope.Attempt != 1 {
					t.Errorf("non-retryable failure: attempt=%d err=%v", envelope.Attempt, err)
				}
				return nil
			}), "dead", DeadLetterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := handler(context.Background(), NewTextMessage("body")); err != nil {
				t.Fatal(err)
			}
			wantPublishes := 0
			if successfulAttempt == 0 {
				wantPublishes = 1
			}
			if calls != max(1, successfulAttempt) || publishes != wantPublishes {
				t.Fatalf("calls=%d publishes=%d", calls, publishes)
			}
		})
	}
}

func TestDeadLetterHandlerPreservesFailedHandoffs(t *testing.T) {
	businessErr, destinationErr := errors.New("business failure"), errors.New("destination unavailable")
	for _, test := range []string{"publish failure", "publish panic", "handler panic", "canceled before", "canceled during", "handler cancellation", "handler deadline", "nil context", "nil message", "encoding failure"} {
		t.Run(test, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			msg := NewTextMessage("body")
			if test == "canceled before" {
				cancel()
			}
			if test == "nil context" {
				ctx = nil
			}
			if test == "nil message" {
				msg = nil
			}
			// An out-of-range timestamp fails envelope encoding.
			if test == "encoding failure" {
				msg.Timestamp = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			calls, publishes := 0, 0
			handler, err := DeadLetterHandler(func(context.Context, *Message) error {
				calls++
				switch test {
				case "handler panic":
					panic("handler broke")
				case "canceled during":
					cancel()
				case "handler cancellation":
					return context.Canceled
				case "handler deadline":
					return context.DeadlineExceeded
				}
				return businessErr
			}, RetryPolicy{MaxAttempts: 3}, deadLetterPublisherFunc(func(context.Context, string, *Message, ...PublishOption) error {
				publishes++
				if test == "publish panic" {
					panic("publisher broke")
				}
				return destinationErr
			}), "dead", DeadLetterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			err = handler(ctx, msg)
			if !IsDeadLetterFailed(fmt.Errorf("wrapped: %w", err)) {
				t.Fatalf("error=%v, want ErrDeadLetterFailed", err)
			}
			var failed *ErrDeadLetterFailed
			if !errors.As(err, &failed) || failed.Destination != "dead" {
				t.Fatalf("error=%v", err)
			}
			if test == "publish failure" && (!errors.Is(err, destinationErr) || !errors.Is(err, businessErr)) {
				t.Errorf("lost error cause: %v", err)
			}
			wantPublishes := 0
			if test == "publish failure" || test == "publish panic" {
				wantPublishes = 1
			}
			if publishes != wantPublishes {
				t.Errorf("publishes=%d, want %d", publishes, wantPublishes)
			}
			if test == "canceled before" && (calls != 0 || !errors.Is(err, context.Canceled)) {
				t.Errorf("calls=%d error=%v", calls, err)
			}
			if (test == "canceled during" || test == "handler cancellation" || test == "handler deadline") && calls != 1 {
				t.Errorf("interrupted handler called %d times", calls)
			}
		})
	}
}

func TestDeadLetterHandlerValidatesConfiguration(t *testing.T) {
	valid := func(context.Context, *Message) error { return nil }
	publisher := deadLetterPublisherFunc(func(context.Context, string, *Message, ...PublishOption) error { return nil })
	var nilPublisher deadLetterPublisherFunc
	for _, test := range []struct {
		name        string
		handler     Handler
		publisher   Publisher
		destination string
		metadata    DeadLetterOptions
		options     []PublishOption
	}{
		{name: "nil handler", publisher: publisher, destination: "dead"},
		{name: "nil publisher", handler: valid, destination: "dead"},
		{name: "typed nil publisher", handler: valid, publisher: nilPublisher, destination: "dead"},
		{name: "blank target", handler: valid, publisher: publisher, destination: " \t"},
		{name: "nil option", handler: valid, publisher: publisher, destination: "dead", options: []PublishOption{nil}},
		{name: "invalid details", handler: valid, publisher: publisher, destination: "dead", metadata: DeadLetterOptions{Details: map[string]any{"unsupported": make(chan int)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, err := DeadLetterHandler(test.handler, RetryPolicy{}, test.publisher, test.destination, test.metadata, test.options...)
			if handler != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("handler is nil=%v err=%v", handler == nil, err)
			}
		})
	}
}

func TestDeadLetterHandlerConcurrentDeliveries(t *testing.T) {
	const deliveries = 20
	var mu sync.Mutex
	seen := make(map[string]int)
	handler, err := DeadLetterHandler(func(ctx context.Context, msg *Message) error {
		if RetryAttempt(ctx) < int(msg.Offset) {
			return errors.New("retry")
		}
		return errors.New("terminal")
	}, RetryPolicy{MaxAttempts: 3, Retryable: func(err error) bool { return err.Error() != "terminal" }}, deadLetterPublisherFunc(func(_ context.Context, _ string, msg *Message, _ ...PublishOption) error {
		envelope, err := DecodeDeadLetterMessage(msg)
		if err != nil {
			return err
		}
		mu.Lock()
		seen[envelope.Original.MessageID] = envelope.Attempt
		mu.Unlock()
		return nil
	}), "dead", DeadLetterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := NewTextMessage("body")
			msg.MessageID, msg.Offset = fmt.Sprint(i), int64(i%3+1)
			if err := handler(context.Background(), msg); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for i := range deliveries {
		if seen[fmt.Sprint(i)] != i%3+1 {
			t.Errorf("delivery %d attempt=%d", i, seen[fmt.Sprint(i)])
		}
	}
}
