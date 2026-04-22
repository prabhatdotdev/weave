package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type captureLogger struct {
	events []Event
}

func (l *captureLogger) LogEvent(_ context.Context, event Event) {
	l.events = append(l.events, event)
}

type captureMetrics struct {
	counters []struct {
		name   string
		value  float64
		labels map[string]string
	}
	durations []struct {
		name   string
		value  time.Duration
		labels map[string]string
	}
}

type captureTracer struct {
	starts   []TraceSpanStart
	finishes []TraceSpanFinish
}

func (t *captureTracer) StartSpan(ctx context.Context, span TraceSpanStart) (context.Context, TraceSpan) {
	t.starts = append(t.starts, span)
	return context.WithValue(ctx, captureTracerKey{}, span.Name), captureTraceSpan{tracer: t}
}

type captureTraceSpan struct {
	tracer *captureTracer
}

func (s captureTraceSpan) Finish(finish TraceSpanFinish) {
	s.tracer.finishes = append(s.tracer.finishes, finish)
}

type captureTracerKey struct{}

type captureHealthReporter struct {
	reports []HealthReport
}

func (m *captureMetrics) IncrementCounter(name string, value float64, labels map[string]string) {
	m.counters = append(m.counters, struct {
		name   string
		value  float64
		labels map[string]string
	}{name: name, value: value, labels: labels})
}

func (m *captureMetrics) ObserveDuration(name string, value time.Duration, labels map[string]string) {
	m.durations = append(m.durations, struct {
		name   string
		value  time.Duration
		labels map[string]string
	}{name: name, value: value, labels: labels})
}

func (r *captureHealthReporter) ReportHealth(_ context.Context, report HealthReport) {
	r.reports = append(r.reports, report)
}

type stubBroker struct{}

func (stubBroker) Connect(context.Context) error { return nil }
func (stubBroker) Close() error                  { return nil }
func (stubBroker) IsConnected() bool             { return true }
func (stubBroker) IsRecovering() bool            { return false }
func (stubBroker) Backend() string               { return "stub" }
func (stubBroker) Publish(context.Context, string, *Message, ...PublishOption) error {
	return nil
}
func (stubBroker) Call(context.Context, string, *Message, ...PublishOption) (*Message, error) {
	return NewTextMessage("ok"), nil
}
func (stubBroker) Subscribe(context.Context, string, Handler, ...SubscribeOption) error {
	return nil
}

func registerTestBackend(name string) {
	Register(name, func(config *Config) (MessageBroker, error) {
		return stubBroker{}, nil
	})
}

func TestMessageHelpersAndClone(t *testing.T) {
	t.Parallel()

	msg := NewMessage([]byte("hello")).
		WithCorrelationID("corr-1").
		WithReplyTo("reply.queue").
		WithHeader("trace-id", "abc").
		WithContentType("application/json").
		WithSubject("users.get")
	msg.MessageID = "msg-1"
	msg.Partition = 3
	msg.Offset = 42

	clone := msg.Clone()
	clone.Body[0] = 'H'
	clone.Headers["trace-id"] = "mutated"

	if got := msg.BodyString(); got != "hello" {
		t.Fatalf("BodyString() = %q, want %q", got, "hello")
	}
	if got := msg.GetHeader("trace-id"); got != "abc" {
		t.Fatalf("GetHeader() = %q, want %q", got, "abc")
	}
	if got := msg.GetHeader("missing"); got != "" {
		t.Fatalf("GetHeader() for missing header = %q, want empty string", got)
	}
	if string(msg.Body) != "hello" {
		t.Fatalf("original body mutated: got %q", string(msg.Body))
	}
	if msg.Headers["trace-id"] != "abc" {
		t.Fatalf("original headers mutated: got %q", msg.Headers["trace-id"])
	}
	if clone.CorrelationID != msg.CorrelationID || clone.ReplyTo != msg.ReplyTo || clone.ContentType != msg.ContentType || clone.Subject != msg.Subject || clone.MessageID != msg.MessageID || clone.Partition != msg.Partition || clone.Offset != msg.Offset {
		t.Fatal("clone did not preserve message metadata")
	}
	if clone.Timestamp != msg.Timestamp {
		t.Fatal("clone did not preserve timestamp")
	}
}

func TestApplyPublishOptions(t *testing.T) {
	t.Parallel()

	options := ApplyPublishOptions(
		WithTimeout(5*time.Second),
		WithMandatory(),
		WithExchange("events"),
		WithPartition(2),
		WithKey("user-1"),
		WithPersistent(),
		WithPriority(7),
		WithExpiration("10s"),
	)

	if options.Timeout != 5*time.Second || !options.Mandatory || options.Exchange != "events" || options.Partition != 2 || options.Key != "user-1" || options.DeliveryMode != 2 || options.Priority != 7 || options.Expiration != "10s" {
		t.Fatalf("unexpected publish options: %#v", options)
	}
}

func TestApplySubscribeOptions(t *testing.T) {
	t.Parallel()

	options := ApplySubscribeOptions(
		WithAutoAck(),
		WithExclusive(),
		WithConsumerTag("consumer-1"),
		WithPrefetchCount(10),
		WithQueueBind("events", "users.*"),
		WithConsumerGroup("workers"),
		WithStartFromBeginning(),
		WithHandlerErrorRetry(),
	)

	if !options.AutoAck || !options.Exclusive || options.ConsumerTag != "consumer-1" || options.PrefetchCount != 10 || !options.QueueBind || options.Exchange != "events" || options.RoutingKey != "users.*" || options.ConsumerGroup != "workers" || !options.StartFromBeginning || options.HandlerErrorPolicy != HandlerErrorRetry {
		t.Fatalf("unexpected subscribe options: %#v", options)
	}

	defaultOptions := ApplySubscribeOptions()
	if defaultOptions.HandlerErrorPolicy != HandlerErrorNoRetry {
		t.Fatalf("default HandlerErrorPolicy = %q, want %q", defaultOptions.HandlerErrorPolicy, HandlerErrorNoRetry)
	}
}

func TestDefaultConfigAndFluentHelpers(t *testing.T) {
	t.Parallel()

	config := DefaultConfig()
	if config.Backend != "amqp" || config.AMQP == nil {
		t.Fatalf("DefaultConfig() = %#v, want AMQP defaults", config)
	}

	amqpConfig := DefaultAMQPConfig()
	kafkaConfig := DefaultKafkaConfig()

	config.WithBackend("custom").WithAMQP(amqpConfig).WithKafka(kafkaConfig)
	if config.Backend != "kafka" {
		t.Fatalf("WithKafka should set backend to kafka, got %q", config.Backend)
	}
	if config.AMQP != amqpConfig || config.Kafka != kafkaConfig {
		t.Fatal("WithAMQP/WithKafka should store provided config pointers")
	}
}

func TestRegistryFactoryLifecycle(t *testing.T) {
	backend := fmt.Sprintf("test-backend-%d", time.Now().UnixNano())
	registerTestBackend(backend)

	if !IsBackendAvailable(backend) {
		t.Fatalf("backend %q should be registered", backend)
	}

	broker, err := NewWithBackend(backend, &Config{})
	if err != nil {
		t.Fatalf("NewWithBackend() error = %v", err)
	}
	if broker.Backend() != "stub" {
		t.Fatalf("broker.Backend() = %q, want %q", broker.Backend(), "stub")
	}

	available := AvailableBackends()
	found := false
	for _, name := range available {
		if name == backend {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("AvailableBackends() missing %q: %v", backend, available)
	}

	if got := MustNew(&Config{Backend: backend}); got.Backend() != "stub" {
		t.Fatalf("MustNew() backend = %q, want %q", got.Backend(), "stub")
	}
}

func TestErrorHelpers(t *testing.T) {
	t.Parallel()

	notConnected := &ErrNotConnected{Backend: "amqp"}
	if !IsNotConnected(notConnected) {
		t.Fatal("IsNotConnected should detect ErrNotConnected")
	}

	lost := &ErrConnectionLost{Backend: "kafka", Cause: errors.New("boom")}
	if !IsConnectionLost(lost) {
		t.Fatal("IsConnectionLost should detect ErrConnectionLost")
	}
	if !errors.Is(lost, lost.Cause) {
		t.Fatal("ErrConnectionLost should unwrap its cause")
	}

	timeout := &ErrTimeout{Operation: "Call", Duration: "1s"}
	if !IsTimeout(timeout) {
		t.Fatal("IsTimeout should detect ErrTimeout")
	}

	circuitOpen := &ErrCircuitOpen{Operation: "Call", RetryAfter: "1s"}
	if !IsCircuitOpen(circuitOpen) {
		t.Fatal("IsCircuitOpen should detect ErrCircuitOpen")
	}

	unsupported := &ErrUnsupportedOperation{Backend: "amqp", Operation: "seek"}
	if !IsUnsupportedOperation(unsupported) {
		t.Fatal("IsUnsupportedOperation should detect ErrUnsupportedOperation")
	}

	unknown := &ErrUnknownBackend{Backend: "missing"}
	if !IsUnknownBackend(unknown) {
		t.Fatal("IsUnknownBackend should detect ErrUnknownBackend")
	}
	if got := unknown.Error(); got == "" {
		t.Fatal("ErrUnknownBackend.Error() should not be empty")
	}
}

func TestRetryHandlerRetriesWithBackoffAndAttemptContext(t *testing.T) {
	t.Parallel()

	attempts := 0
	var seenAttempts []int
	handler := RetryHandler(func(ctx context.Context, msg *Message) error {
		attempts++
		seenAttempts = append(seenAttempts, RetryAttempt(ctx))
		if msg.BodyString() != "payload" {
			t.Fatalf("message body = %q, want payload", msg.BodyString())
		}
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	}, RetryPolicy{
		MaxAttempts: 3,
		Backoff:     FixedBackoff(0),
	})

	if err := handler(context.Background(), NewTextMessage("payload")); err != nil {
		t.Fatalf("RetryHandler() error = %v, want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(seenAttempts) != 3 || seenAttempts[0] != 1 || seenAttempts[1] != 2 || seenAttempts[2] != 3 {
		t.Fatalf("seen attempts = %v, want [1 2 3]", seenAttempts)
	}
}

func TestRetryHandlerStopsWhenRetryPredicateRejects(t *testing.T) {
	t.Parallel()

	attempts := 0
	want := errors.New("permanent")
	handler := RetryHandler(func(context.Context, *Message) error {
		attempts++
		return want
	}, RetryPolicy{
		MaxAttempts: 5,
		Retryable: func(err error) bool {
			return false
		},
	})

	if err := handler(context.Background(), NewTextMessage("payload")); !errors.Is(err, want) {
		t.Fatalf("RetryHandler() error = %v, want %v", err, want)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestStructuredErrorPayloadAndDeadLetterHelpers(t *testing.T) {
	t.Parallel()

	payload := NewErrorPayload("validation_failed", errors.New("missing email"))
	payload.Retryable = false
	payload.CorrelationID = "corr-1"
	payload.Details = map[string]any{"field": "email"}

	msg, err := NewErrorMessage(payload)
	if err != nil {
		t.Fatalf("NewErrorMessage() error = %v", err)
	}
	if msg.ContentType != ErrorContentType {
		t.Fatalf("error content type = %q, want %q", msg.ContentType, ErrorContentType)
	}

	decodedPayload, err := DecodeErrorMessage(msg)
	if err != nil {
		t.Fatalf("DecodeErrorMessage() error = %v", err)
	}
	if decodedPayload.Code != payload.Code || decodedPayload.Message != payload.Message || decodedPayload.CorrelationID != payload.CorrelationID {
		t.Fatalf("decoded payload = %#v, want %#v", decodedPayload, payload)
	}

	original := NewTextMessage("order-1").WithCorrelationID("corr-1").WithHeader("trace-id", "abc")
	envelope := NewDeadLetterEnvelope(original, errors.New("validation failed"), DeadLetterOptions{
		Backend:     "amqp",
		Destination: "orders",
		Handler:     "orders.create",
		Attempt:     2,
		Code:        "validation_failed",
		Details:     map[string]any{"field": "email"},
	})

	deadLetterMsg, err := envelope.Message()
	if err != nil {
		t.Fatalf("DeadLetterEnvelope.Message() error = %v", err)
	}
	if deadLetterMsg.ContentType != DeadLetterContentType {
		t.Fatalf("dead-letter content type = %q, want %q", deadLetterMsg.ContentType, DeadLetterContentType)
	}

	decodedEnvelope, err := DecodeDeadLetterMessage(deadLetterMsg)
	if err != nil {
		t.Fatalf("DecodeDeadLetterMessage() error = %v", err)
	}
	if decodedEnvelope.Destination != "orders" || decodedEnvelope.Handler != "orders.create" || decodedEnvelope.Attempt != 2 {
		t.Fatalf("decoded envelope = %#v", decodedEnvelope)
	}
	if decodedEnvelope.Original.Body == nil || string(decodedEnvelope.Original.Body) != "order-1" {
		t.Fatalf("decoded original body = %q, want %q", string(decodedEnvelope.Original.Body), "order-1")
	}
	if decodedEnvelope.Error.CorrelationID != "corr-1" {
		t.Fatalf("decoded correlation id = %q, want corr-1", decodedEnvelope.Error.CorrelationID)
	}
}

func TestEmitHealthClonesDetailsAndReports(t *testing.T) {
	t.Parallel()

	reporter := &captureHealthReporter{}
	var hooked HealthReport
	config := &Config{
		HealthReporter: reporter,
		HealthHook: func(_ context.Context, report HealthReport) {
			hooked = report
		},
	}

	details := map[string]any{"state": "ready"}
	config.EmitHealth(context.Background(), HealthReport{
		Status:    HealthStatusHealthy,
		Backend:   "amqp",
		Component: "runtime.client",
		Details:   details,
	})
	details["state"] = "mutated"

	if len(reporter.reports) != 1 {
		t.Fatalf("health reports = %d, want 1", len(reporter.reports))
	}
	if reporter.reports[0].Details["state"] != "ready" {
		t.Fatalf("reported details = %v, want cloned details", reporter.reports[0].Details)
	}
	if hooked.Details["state"] != "ready" {
		t.Fatalf("hooked details = %v, want cloned details", hooked.Details)
	}
	if reporter.reports[0].Timestamp.IsZero() {
		t.Fatal("reported health timestamp should be populated")
	}
}

func TestEmitEventDispatchesLoggerAndHook(t *testing.T) {
	t.Parallel()

	logger := &captureLogger{}
	hookCalled := 0
	var hookEvent Event

	config := &Config{
		Logger: logger,
		EventHook: func(_ context.Context, event Event) {
			hookCalled++
			hookEvent = event
		},
	}

	fields := map[string]any{"stage": "connect"}
	config.EmitEvent(context.Background(), Event{Name: EventConnect, Level: EventLevelInfo, Fields: fields})
	fields["stage"] = "mutated"

	if len(logger.events) != 1 {
		t.Fatalf("logger events = %d, want 1", len(logger.events))
	}
	if logger.events[0].Timestamp.IsZero() {
		t.Fatal("event timestamp should be set")
	}
	if logger.events[0].Fields["stage"] != "connect" {
		t.Fatalf("logger field stage = %v, want %q", logger.events[0].Fields["stage"], "connect")
	}
	if hookCalled != 1 {
		t.Fatalf("hook called %d times, want 1", hookCalled)
	}
	if hookEvent.Name != EventConnect {
		t.Fatalf("hook event name = %q, want %q", hookEvent.Name, EventConnect)
	}
}

func TestEmitMetricsDispatchesWithClonedLabels(t *testing.T) {
	t.Parallel()

	metrics := &captureMetrics{}
	config := &Config{Metrics: metrics}

	labels := map[string]string{"backend": "amqp"}
	config.EmitCounter("weave.test.counter", 2, labels)
	config.EmitDuration("weave.test.duration", time.Second, labels)
	labels["backend"] = "mutated"

	if len(metrics.counters) != 1 {
		t.Fatalf("counter calls = %d, want 1", len(metrics.counters))
	}
	if len(metrics.durations) != 1 {
		t.Fatalf("duration calls = %d, want 1", len(metrics.durations))
	}
	if metrics.counters[0].labels["backend"] != "amqp" {
		t.Fatalf("counter labels backend = %q, want %q", metrics.counters[0].labels["backend"], "amqp")
	}
	if metrics.durations[0].labels["backend"] != "amqp" {
		t.Fatalf("duration labels backend = %q, want %q", metrics.durations[0].labels["backend"], "amqp")
	}
}

func TestStartSpanDispatchesTracingHook(t *testing.T) {
	t.Parallel()

	tracer := &captureTracer{}
	config := &Config{Tracing: tracer}

	startAttrs := map[string]string{"backend": "amqp"}
	ctx, span := config.StartSpan(context.Background(), TraceSpanStart{
		Name:       "weave.runtime.client.call",
		Backend:    "amqp",
		Attributes: startAttrs,
	})
	startAttrs["backend"] = "mutated"

	if got := ctx.Value(captureTracerKey{}); got != "weave.runtime.client.call" {
		t.Fatalf("context trace value = %v, want span name", got)
	}
	if len(tracer.starts) != 1 {
		t.Fatalf("start calls = %d, want 1", len(tracer.starts))
	}
	if tracer.starts[0].Attributes["backend"] != "amqp" {
		t.Fatalf("start attributes backend = %q, want %q", tracer.starts[0].Attributes["backend"], "amqp")
	}

	finishAttrs := map[string]string{"outcome": "success"}
	span.Finish(TraceSpanFinish{
		Err:        errors.New("boom"),
		Attributes: finishAttrs,
	})
	finishAttrs["outcome"] = "mutated"

	if len(tracer.finishes) != 1 {
		t.Fatalf("finish calls = %d, want 1", len(tracer.finishes))
	}
	if tracer.finishes[0].Err == nil {
		t.Fatal("finish error should be recorded")
	}
	if tracer.finishes[0].Attributes["outcome"] != "success" {
		t.Fatalf("finish attributes outcome = %q, want %q", tracer.finishes[0].Attributes["outcome"], "success")
	}
}
