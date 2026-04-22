// Copyright 2025 PrabhatDotDev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/codec"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/testkit"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type runtimeEventRecorder struct {
	mu     sync.Mutex
	events []core.Event
}

func (r *runtimeEventRecorder) Hook(_ context.Context, event core.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *runtimeEventRecorder) Events() []core.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make([]core.Event, len(r.events))
	copy(events, r.events)
	return events
}

type runtimeMetricsRecorder struct {
	mu        sync.Mutex
	counters  []runtimeCounterMetric
	durations []runtimeDurationMetric
}

type runtimeCounterMetric struct {
	name   string
	value  float64
	labels map[string]string
}

type runtimeDurationMetric struct {
	name   string
	value  time.Duration
	labels map[string]string
}

type runtimeTraceRecorder struct {
	mu       sync.Mutex
	starts   []core.TraceSpanStart
	finishes []core.TraceSpanFinish
}

type runtimeHealthRecorder struct {
	mu      sync.Mutex
	reports []core.HealthReport
}

func (r *runtimeTraceRecorder) StartSpan(ctx context.Context, span core.TraceSpanStart) (context.Context, core.TraceSpan) {
	r.mu.Lock()
	r.starts = append(r.starts, core.TraceSpanStart{
		Name:          span.Name,
		Backend:       span.Backend,
		Component:     span.Component,
		Operation:     span.Operation,
		Destination:   span.Destination,
		CorrelationID: span.CorrelationID,
		Attributes:    cloneMetricLabels(span.Attributes),
	})
	r.mu.Unlock()
	return ctx, runtimeTraceSpan{recorder: r}
}

func (r *runtimeTraceRecorder) Starts() []core.TraceSpanStart {
	r.mu.Lock()
	defer r.mu.Unlock()
	spans := make([]core.TraceSpanStart, len(r.starts))
	copy(spans, r.starts)
	return spans
}

func (r *runtimeTraceRecorder) Finishes() []core.TraceSpanFinish {
	r.mu.Lock()
	defer r.mu.Unlock()
	spans := make([]core.TraceSpanFinish, len(r.finishes))
	copy(spans, r.finishes)
	return spans
}

func (r *runtimeHealthRecorder) Hook(_ context.Context, report core.HealthReport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report)
}

func (r *runtimeHealthRecorder) Reports() []core.HealthReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	reports := make([]core.HealthReport, len(r.reports))
	copy(reports, r.reports)
	return reports
}

type runtimeTraceSpan struct {
	recorder *runtimeTraceRecorder
}

func (s runtimeTraceSpan) Finish(finish core.TraceSpanFinish) {
	s.recorder.mu.Lock()
	defer s.recorder.mu.Unlock()
	s.recorder.finishes = append(s.recorder.finishes, core.TraceSpanFinish{
		Err:        finish.Err,
		Attributes: cloneMetricLabels(finish.Attributes),
	})
}

func (r *runtimeMetricsRecorder) IncrementCounter(name string, value float64, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = append(r.counters, runtimeCounterMetric{
		name:   name,
		value:  value,
		labels: cloneMetricLabels(labels),
	})
}

func (r *runtimeMetricsRecorder) ObserveDuration(name string, value time.Duration, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.durations = append(r.durations, runtimeDurationMetric{
		name:   name,
		value:  value,
		labels: cloneMetricLabels(labels),
	})
}

func (r *runtimeMetricsRecorder) Counters() []runtimeCounterMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics := make([]runtimeCounterMetric, len(r.counters))
	copy(metrics, r.counters)
	return metrics
}

func (r *runtimeMetricsRecorder) Durations() []runtimeDurationMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics := make([]runtimeDurationMetric, len(r.durations))
	copy(metrics, r.durations)
	return metrics
}

func cloneMetricLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for k, v := range labels {
		cloned[k] = v
	}
	return cloned
}

func registerRuntimeTestBackend(t *testing.T, broker core.MessageBroker) string {
	t.Helper()

	backend := fmt.Sprintf("runtime-test-%d", time.Now().UnixNano())
	core.Register(backend, func(*core.Config) (core.MessageBroker, error) {
		return broker, nil
	})
	return backend
}

type connectFailBroker struct {
	err error
}

type callSequenceBroker struct {
	callErrs  []error
	callCount int
}

func (b *connectFailBroker) Connect(context.Context) error { return b.err }
func (b *connectFailBroker) Close() error                  { return nil }
func (b *connectFailBroker) IsConnected() bool             { return false }
func (b *connectFailBroker) IsRecovering() bool            { return false }
func (b *connectFailBroker) Backend() string               { return "failing" }
func (b *connectFailBroker) Publish(context.Context, string, *core.Message, ...core.PublishOption) error {
	return nil
}
func (b *connectFailBroker) Call(context.Context, string, *core.Message, ...core.PublishOption) (*core.Message, error) {
	return nil, nil
}
func (b *connectFailBroker) Subscribe(context.Context, string, core.Handler, ...core.SubscribeOption) error {
	return nil
}

func (b *callSequenceBroker) Connect(context.Context) error { return nil }
func (b *callSequenceBroker) Close() error                  { return nil }
func (b *callSequenceBroker) IsConnected() bool             { return true }
func (b *callSequenceBroker) IsRecovering() bool            { return false }
func (b *callSequenceBroker) Backend() string               { return "sequence" }
func (b *callSequenceBroker) Publish(context.Context, string, *core.Message, ...core.PublishOption) error {
	return nil
}
func (b *callSequenceBroker) Call(context.Context, string, *core.Message, ...core.PublishOption) (*core.Message, error) {
	err := b.callErrs[b.callCount]
	b.callCount++
	if err != nil {
		return nil, err
	}
	return core.NewTextMessage("ok"), nil
}
func (b *callSequenceBroker) Subscribe(context.Context, string, core.Handler, ...core.SubscribeOption) error {
	return nil
}

func TestNewClientUsesRegisteredBackend(t *testing.T) {
	broker := testkit.NewMockBroker()
	backend := registerRuntimeTestBackend(t, broker)
	config := &core.Config{Backend: backend}

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	if client.Broker() != broker {
		t.Fatal("Broker() should return the registered broker instance")
	}
	if client.Config() != config {
		t.Fatal("Config() should return the original config")
	}
}

func TestClientLifecycleAndMessaging(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	client := NewClientWithBroker(broker, core.DefaultConfig())
	ctx := context.Background()

	if err := client.Publish(ctx, "orders", core.NewTextMessage("payload")); !core.IsNotConnected(err) {
		t.Fatalf("Publish() before Connect() error = %v, want ErrNotConnected", err)
	}

	if _, err := client.Call(ctx, "users.get", core.NewTextMessage("1")); !core.IsNotConnected(err) {
		t.Fatalf("Call() before Connect() error = %v, want ErrNotConnected", err)
	}

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if !client.IsConnected() {
		t.Fatal("client should report connected after Connect()")
	}
	if client.Backend() != "mock" {
		t.Fatalf("Backend() = %q, want %q", client.Backend(), "mock")
	}

	if err := client.Connect(ctx); !errors.Is(err, core.ErrAlreadyConnected) {
		t.Fatalf("second Connect() error = %v, want ErrAlreadyConnected", err)
	}

	message := core.NewTextMessage("created")
	if err := client.Publish(ctx, "orders", message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	broker.AssertPublished(t, "orders")

	broker.SetCallResponse("users.get", core.NewTextMessage("user-json"))
	response, err := client.Call(ctx, "users.get", core.NewTextMessage("42"))
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if response.BodyString() != "user-json" {
		t.Fatalf("response.BodyString() = %q, want %q", response.BodyString(), "user-json")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if client.IsConnected() {
		t.Fatal("client should report disconnected after Close()")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
}

func TestClientPublishWithCodecEncodesPayload(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	client := NewClientWithBroker(broker, core.DefaultConfig())

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	payload := wrapperspb.String("42")
	if err := client.PublishWithCodec(context.Background(), "users.events", payload, codec.Protobuf); err != nil {
		t.Fatalf("PublishWithCodec() error = %v", err)
	}

	published := broker.PublishedTo("users.events")
	if len(published) != 1 {
		t.Fatalf("published count = %d, want 1", len(published))
	}
	if published[0].Message.ContentType != codec.Protobuf.ContentType() {
		t.Fatalf("ContentType = %q, want %q", published[0].Message.ContentType, codec.Protobuf.ContentType())
	}

	var decoded wrapperspb.StringValue
	if err := codec.UnmarshalMessage(codec.Protobuf, published[0].Message, &decoded); err != nil {
		t.Fatalf("UnmarshalMessage() error = %v", err)
	}
	if decoded.Value != payload.Value {
		t.Fatalf("decoded.Value = %q, want %q", decoded.Value, payload.Value)
	}
}

func TestClientCallWithCodecDecodesResponse(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	client := NewClientWithBroker(broker, core.DefaultConfig())

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	replyPayload := wrapperspb.String("Ada")
	replyMessage, err := codec.MarshalMessage(codec.Protobuf, replyPayload)
	if err != nil {
		t.Fatalf("MarshalMessage() error = %v", err)
	}
	broker.SetCallResponse("users.get", replyMessage)

	var decoded wrapperspb.StringValue
	response, err := client.CallWithCodec(
		context.Background(),
		"users.get",
		wrapperspb.String("42"),
		&decoded,
		codec.Protobuf,
	)
	if err != nil {
		t.Fatalf("CallWithCodec() error = %v", err)
	}

	if response.ContentType != codec.Protobuf.ContentType() {
		t.Fatalf("ContentType = %q, want %q", response.ContentType, codec.Protobuf.ContentType())
	}
	if decoded.Value != "Ada" {
		t.Fatalf("decoded response value = %q, want %q", decoded.Value, "Ada")
	}
}

func TestClientConnectPropagatesBrokerErrors(t *testing.T) {
	t.Parallel()

	want := errors.New("connect failed")
	client := NewClientWithBroker(&connectFailBroker{err: want}, nil)

	if err := client.Connect(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Connect() error = %v, want %v", err, want)
	}
	if client.IsConnected() {
		t.Fatal("client should not report connected after failed Connect()")
	}
}

func TestClientConnectFailureEmitsObservability(t *testing.T) {
	t.Parallel()

	want := errors.New("connect failed")
	events := &runtimeEventRecorder{}
	metrics := &runtimeMetricsRecorder{}
	config := &core.Config{
		EventHook: events.Hook,
		Metrics:   metrics,
	}
	client := NewClientWithBroker(&connectFailBroker{err: want}, config)

	if err := client.Connect(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Connect() error = %v, want %v", err, want)
	}

	recordedEvents := events.Events()
	if len(recordedEvents) != 1 {
		t.Fatalf("event count = %d, want 1", len(recordedEvents))
	}
	if recordedEvents[0].Name != core.EventConnect || recordedEvents[0].Level != core.EventLevelError {
		t.Fatalf("unexpected event = %#v", recordedEvents[0])
	}
	if recordedEvents[0].Fields["outcome"] != "failure" {
		t.Fatalf("event outcome = %v, want %q", recordedEvents[0].Fields["outcome"], "failure")
	}

	counters := metrics.Counters()
	if len(counters) != 1 {
		t.Fatalf("counter count = %d, want 1", len(counters))
	}
	if counters[0].name != "weave.runtime.connect.failures" {
		t.Fatalf("counter name = %q, want connect failure metric", counters[0].name)
	}
	if counters[0].labels["backend"] != "failing" || counters[0].labels["role"] != "client" {
		t.Fatalf("counter labels = %v, want backend=failing role=client", counters[0].labels)
	}
}

func TestClientCallTimeoutEmitsObservability(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	events := &runtimeEventRecorder{}
	metrics := &runtimeMetricsRecorder{}
	config := &core.Config{
		EventHook: events.Hook,
		Metrics:   metrics,
	}
	client := NewClientWithBroker(broker, config)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	timeout := 25 * time.Millisecond
	if _, err := client.Call(context.Background(), "users.timeout", core.NewTextMessage("42"), core.WithTimeout(timeout)); !core.IsTimeout(err) {
		t.Fatalf("Call() error = %v, want ErrTimeout", err)
	}

	recordedEvents := events.Events()
	if len(recordedEvents) < 2 {
		t.Fatalf("expected connect and timeout events, got %d", len(recordedEvents))
	}
	timeoutEvent := recordedEvents[len(recordedEvents)-1]
	if timeoutEvent.Name != core.EventTimeout || timeoutEvent.Level != core.EventLevelWarn {
		t.Fatalf("unexpected timeout event = %#v", timeoutEvent)
	}
	if timeoutEvent.Destination != "users.timeout" {
		t.Fatalf("timeout event destination = %q, want %q", timeoutEvent.Destination, "users.timeout")
	}
	if timeoutEvent.Fields["timeout"] != timeout.String() {
		t.Fatalf("timeout field = %v, want %q", timeoutEvent.Fields["timeout"], timeout.String())
	}

	counters := metrics.Counters()
	if len(counters) < 2 {
		t.Fatalf("expected connect and timeout counters, got %d", len(counters))
	}
	timeoutCounter := counters[len(counters)-1]
	if timeoutCounter.name != "weave.runtime.call.timeouts" {
		t.Fatalf("timeout counter name = %q, want call timeouts metric", timeoutCounter.name)
	}
	if timeoutCounter.labels["destination"] != "users.timeout" || timeoutCounter.labels["role"] != "client" {
		t.Fatalf("timeout counter labels = %v", timeoutCounter.labels)
	}

	durations := metrics.Durations()
	if len(durations) != 1 {
		t.Fatalf("duration count = %d, want 1", len(durations))
	}
	if durations[0].name != "weave.runtime.call.duration" || durations[0].labels["outcome"] != "timeout" {
		t.Fatalf("duration metric = %#v, want timeout call duration", durations[0])
	}
}

func TestClientCallWithPolicyRetriesTransientFailures(t *testing.T) {
	t.Parallel()

	broker := &callSequenceBroker{
		callErrs: []error{
			&core.ErrTimeout{Operation: "Call", Duration: "1s"},
			nil,
		},
	}
	client := NewClientWithBroker(broker, core.DefaultConfig())
	client.connected = true

	var retries []int
	response, err := client.CallWithPolicy(context.Background(), "users.get", core.NewTextMessage("1"), CallPolicy{
		MaxAttempts: 2,
		Backoff:     core.FixedBackoff(0),
		OnRetry: func(_ context.Context, attempt int, err error, delay time.Duration) {
			retries = append(retries, attempt)
		},
	})
	if err != nil {
		t.Fatalf("CallWithPolicy() error = %v", err)
	}
	if response.BodyString() != "ok" {
		t.Fatalf("response.BodyString() = %q, want %q", response.BodyString(), "ok")
	}
	if broker.callCount != 2 {
		t.Fatalf("call count = %d, want 2", broker.callCount)
	}
	if len(retries) != 1 || retries[0] != 2 {
		t.Fatalf("retries = %v, want [2]", retries)
	}
}

func TestClientCallWithPolicyCircuitBreakerOpens(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(CircuitBreakerOptions{
		FailureThreshold: 1,
		ResetAfter:       time.Minute,
	})
	broker := &callSequenceBroker{
		callErrs: []error{
			&core.ErrTimeout{Operation: "Call", Duration: "1s"},
		},
	}
	client := NewClientWithBroker(broker, core.DefaultConfig())
	client.connected = true

	_, err := client.CallWithPolicy(context.Background(), "users.get", core.NewTextMessage("1"), CallPolicy{
		MaxAttempts:    1,
		CircuitBreaker: breaker,
	})
	if !core.IsTimeout(err) {
		t.Fatalf("first CallWithPolicy() error = %v, want timeout", err)
	}

	_, err = client.CallWithPolicy(context.Background(), "users.get", core.NewTextMessage("2"), CallPolicy{
		MaxAttempts:    1,
		CircuitBreaker: breaker,
	})
	if !core.IsCircuitOpen(err) {
		t.Fatalf("second CallWithPolicy() error = %v, want circuit open", err)
	}
	if breaker.State() != CircuitOpen {
		t.Fatalf("circuit state = %q, want %q", breaker.State(), CircuitOpen)
	}
}

func TestClientHealthHooksReceiveLifecycleSnapshots(t *testing.T) {
	t.Parallel()

	health := &runtimeHealthRecorder{}
	config := &core.Config{HealthHook: health.Hook}
	client := NewClientWithBroker(testkit.NewMockBroker(), config)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reports := health.Reports()
	if len(reports) != 2 {
		t.Fatalf("health reports = %d, want 2", len(reports))
	}
	if reports[0].Status != core.HealthStatusHealthy || !reports[0].Connected {
		t.Fatalf("connect report = %#v", reports[0])
	}
	if reports[1].Status != core.HealthStatusUnhealthy || reports[1].Connected {
		t.Fatalf("close report = %#v", reports[1])
	}
}

func TestClientCallEmitsTracingSpan(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	tracing := &runtimeTraceRecorder{}
	config := &core.Config{Tracing: tracing}
	client := NewClientWithBroker(broker, config)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	request := core.NewTextMessage("42").WithCorrelationID("corr-42")
	broker.SetCallResponse("users.get", core.NewTextMessage("ok"))

	if _, err := client.Call(context.Background(), "users.get", request); err != nil {
		t.Fatalf("Call() error = %v", err)
	}

	starts := tracing.Starts()
	if len(starts) != 2 {
		t.Fatalf("trace starts = %d, want 2 (connect + call)", len(starts))
	}
	callSpan := starts[1]
	if callSpan.Name != "weave.runtime.client.call" || callSpan.Destination != "users.get" || callSpan.CorrelationID != "corr-42" {
		t.Fatalf("unexpected call span start = %#v", callSpan)
	}

	finishes := tracing.Finishes()
	if len(finishes) != 2 {
		t.Fatalf("trace finishes = %d, want 2 (connect + call)", len(finishes))
	}
	callFinish := finishes[1]
	if callFinish.Err != nil {
		t.Fatalf("call finish error = %v, want nil", callFinish.Err)
	}
	if callFinish.Attributes["outcome"] != "success" || callFinish.Attributes["destination"] != "users.get" {
		t.Fatalf("unexpected call finish attributes = %v", callFinish.Attributes)
	}
}
