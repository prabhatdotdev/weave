package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMessageClone(t *testing.T) {
	msg := NewMessage([]byte("hello")).
		WithCorrelationID("corr-1").
		WithHeader("trace-id", "abc").
		WithContentType("application/json")

	clone := msg.Clone()
	clone.Body[0] = 'H'
	clone.Headers["trace-id"] = "changed"

	if msg.BodyString() != "hello" || msg.GetHeader("trace-id") != "abc" {
		t.Fatal("Clone mutated the original message")
	}
}

func TestOptions(t *testing.T) {
	publish := ApplyPublishOptions(
		WithTimeout(time.Second),
		WithMandatory(),
		WithPersistent(),
		WithPartition(2),
		WithKey("key"),
	)
	if publish.Timeout != time.Second || !publish.Mandatory || publish.DeliveryMode != 2 || publish.Partition != 2 || publish.Key != "key" {
		t.Fatalf("unexpected publish options: %#v", publish)
	}

	subscribe := ApplySubscribeOptions(
		WithAutoAck(),
		WithPrefetchCount(10),
		WithWorkerCount(4),
		WithHandlerErrorRetry(),
	)
	if !subscribe.AutoAck || subscribe.PrefetchCount != 10 || subscribe.WorkerCount != 4 || subscribe.HandlerErrorPolicy != HandlerErrorRetry {
		t.Fatalf("unexpected subscribe options: %#v", subscribe)
	}
}

func TestRetryHandler(t *testing.T) {
	attempts := 0
	handler := RetryHandler(func(_ context.Context, _ *Message) error {
		attempts++
		if attempts < 3 {
			return errors.New("retry")
		}
		return nil
	}, RetryPolicy{MaxAttempts: 3, Backoff: FixedBackoff(0)})

	if err := handler(context.Background(), NewTextMessage("work")); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryHandlerStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handler := RetryHandler(func(_ context.Context, _ *Message) error {
		cancel()
		return errors.New("retry")
	}, RetryPolicy{MaxAttempts: 3, Backoff: FixedBackoff(time.Second)})

	if err := handler(ctx, NewTextMessage("work")); err == nil {
		t.Fatal("expected handler error")
	}
}

func TestErrorAndDeadLetterMessages(t *testing.T) {
	msg, err := NewErrorMessage(NewErrorPayload("invalid", errors.New("bad input")))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := DecodeErrorMessage(msg)
	if err != nil || payload.Code != "invalid" || payload.Message != "bad input" {
		t.Fatalf("unexpected error payload: %#v, %v", payload, err)
	}

	original := NewTextMessage("body").WithCorrelationID("corr-1").WithHeader("key", "value")
	deadLetter := NewDeadLetterEnvelope(original, errors.New("failed"), DeadLetterOptions{Backend: "amqp"})
	deadLetterMessage, err := deadLetter.Message()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDeadLetterMessage(deadLetterMessage)
	if err != nil || decoded.Original.Headers["key"] != "value" || decoded.Error.CorrelationID != "corr-1" {
		t.Fatalf("unexpected dead-letter payload: %#v, %v", decoded, err)
	}
}

func TestEmitEvent(t *testing.T) {
	fields := map[string]any{"key": "value"}
	var received Event
	config := &Config{EventHook: func(_ context.Context, event Event) {
		received = event
		event.Fields["key"] = "changed"
	}}

	config.EmitEvent(context.Background(), Event{Name: EventConnect, Fields: fields})
	if received.Timestamp.IsZero() || fields["key"] != "value" {
		t.Fatalf("event was not timestamped and cloned: %#v", received)
	}
}

func TestErrorHelpers(t *testing.T) {
	tests := []struct {
		err   error
		check func(error) bool
	}{
		{&ErrNotConnected{}, IsNotConnected},
		{&ErrConnectionLost{}, IsConnectionLost},
		{&ErrTimeout{}, IsTimeout},
		{&ErrCircuitOpen{}, IsCircuitOpen},
		{&ErrUnsupportedOperation{}, IsUnsupportedOperation},
	}
	for _, test := range tests {
		if !test.check(test.err) {
			t.Fatalf("helper did not match %T", test.err)
		}
	}
}

func TestDefaultsAndEdgeCases(t *testing.T) {
	config := DefaultConfig()
	if config.ConnectionRetry != 3 || config.RetryDelay != 2*time.Second {
		t.Fatalf("unexpected connection defaults: %#v", config)
	}
	if amqp := DefaultAMQPConfig(); amqp.Host != "localhost" || amqp.Port != 5672 {
		t.Fatalf("unexpected AMQP defaults: %#v", amqp)
	}
	if kafka := DefaultKafkaConfig(); len(kafka.Brokers) != 1 || kafka.AutoOffsetReset != "latest" {
		t.Fatalf("unexpected Kafka defaults: %#v", kafka)
	}

	if delay := FixedBackoff(-time.Second)(1); delay != 0 {
		t.Fatalf("negative fixed backoff = %s", delay)
	}
	backoff := ExponentialBackoff(time.Second, 3*time.Second, 2)
	if backoff(1) != time.Second || backoff(2) != 2*time.Second || backoff(3) != 3*time.Second {
		t.Fatal("unexpected exponential backoff")
	}
	if RetryAttempt(nil) != 1 {
		t.Fatal("missing retry attempt should be one")
	}

	var nilConfig *Config
	nilConfig.EmitEvent(context.Background(), Event{Name: EventConnect})
	if _, err := DecodeErrorMessage(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeDeadLetterMessage(nil); err != nil {
		t.Fatal(err)
	}
}
