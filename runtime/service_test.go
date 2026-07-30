package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/prabhatdotdev/weave/codec"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/testkit"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type startFailBroker struct {
	connectErr error
	closeCalls int
}

func (b *startFailBroker) Connect(context.Context) error { return b.connectErr }
func (b *startFailBroker) Close() error {
	b.closeCalls++
	return nil
}
func (b *startFailBroker) IsConnected() bool  { return false }
func (b *startFailBroker) IsRecovering() bool { return false }
func (b *startFailBroker) Backend() string    { return "failing" }
func (b *startFailBroker) Publish(context.Context, string, *core.Message, ...core.PublishOption) error {
	return nil
}
func (b *startFailBroker) Call(context.Context, string, *core.Message, ...core.PublishOption) (*core.Message, error) {
	return core.NewTextMessage("ok"), nil
}
func (b *startFailBroker) Subscribe(context.Context, string, core.Handler, ...core.SubscribeOption) error {
	return nil
}

type subscribeFailBroker struct {
	subscribeErr error
	connected    bool
	options      *core.SubscribeOptions
}

func (b *subscribeFailBroker) Connect(context.Context) error {
	b.connected = true
	return nil
}
func (b *subscribeFailBroker) Close() error {
	b.connected = false
	return nil
}
func (b *subscribeFailBroker) IsConnected() bool  { return b.connected }
func (b *subscribeFailBroker) IsRecovering() bool { return false }
func (b *subscribeFailBroker) Backend() string    { return "subscribe-failing" }
func (b *subscribeFailBroker) Publish(context.Context, string, *core.Message, ...core.PublishOption) error {
	return nil
}
func (b *subscribeFailBroker) Call(context.Context, string, *core.Message, ...core.PublishOption) (*core.Message, error) {
	return core.NewTextMessage("ok"), nil
}
func (b *subscribeFailBroker) Subscribe(_ context.Context, _ string, _ core.Handler, opts ...core.SubscribeOption) error {
	b.options = core.ApplySubscribeOptions(opts...)
	return b.subscribeErr
}

func TestNewServerUsesRegisteredBackend(t *testing.T) {
	broker := testkit.NewMockBroker()
	backend := registerRuntimeTestBackend(t, broker)
	config := &core.Config{Backend: backend}

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	if server.Broker() != broker {
		t.Fatal("Broker() should return the registered broker instance")
	}
	if server.Config() != config {
		t.Fatal("Config() should return the original config")
	}
}

func TestServerStartSubscribesHandlersAndDelegatesMessaging(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	server := NewServerWithBroker(broker, core.DefaultConfig())

	handled := make(chan string, 2)
	server.Handle("orders", func(ctx context.Context, msg *core.Message) error {
		handled <- msg.BodyString()
		return nil
	})
	server.Handle("users.get", func(ctx context.Context, msg *core.Message) error {
		handled <- msg.BodyString()
		return nil
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !server.IsStarted() {
		t.Fatal("server should report started after Start()")
	}
	if !broker.HasSubscription("orders") || !broker.HasSubscription("users.get") {
		t.Fatalf("expected subscriptions to be registered, got %v", broker.Subscriptions())
	}

	if err := broker.SimulateMessage(context.Background(), "orders", core.NewTextMessage("created")); err != nil {
		t.Fatalf("SimulateMessage() error = %v", err)
	}
	if got := <-handled; got != "created" {
		t.Fatalf("handler received %q, want %q", got, "created")
	}

	if err := server.Publish(context.Background(), "audit", core.NewTextMessage("event")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	broker.AssertPublished(t, "audit")

	broker.SetCallResponse("users.get", core.NewTextMessage("response"))
	response, err := server.Call(context.Background(), "users.get", core.NewTextMessage("request"))
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if response.BodyString() != "response" {
		t.Fatalf("response.BodyString() = %q, want %q", response.BodyString(), "response")
	}

	if err := server.Start(context.Background()); err == nil {
		t.Fatal("second Start() should return an error")
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v, want nil", err)
	}
}

func TestServerHandlePassesSubscriptionOptions(t *testing.T) {
	t.Parallel()

	broker := &subscribeFailBroker{}
	server := NewServerWithBroker(broker, core.DefaultConfig())
	server.Handle("orders", func(context.Context, *core.Message) error { return nil }, core.WithWorkerCount(3))

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if broker.options == nil || broker.options.WorkerCount != 3 {
		t.Fatalf("worker count = %#v, want 3", broker.options)
	}
}

func TestServerPublishWithCodecEncodesPayload(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	server := NewServerWithBroker(broker, core.DefaultConfig())

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	payload := wrapperspb.String("pay-42")
	if err := server.PublishWithCodec(context.Background(), "users.events", payload, codec.Protobuf); err != nil {
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

func TestServerCallWithCodecDecodesResponse(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	server := NewServerWithBroker(broker, core.DefaultConfig())

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	replyPayload := wrapperspb.Bool(true)
	replyMessage, err := codec.MarshalMessage(codec.Protobuf, replyPayload)
	if err != nil {
		t.Fatalf("MarshalMessage() error = %v", err)
	}
	broker.SetCallResponse("payments.get", replyMessage)

	var decoded wrapperspb.BoolValue
	response, err := server.CallWithCodec(
		context.Background(),
		"payments.get",
		wrapperspb.String("pay-42"),
		&decoded,
		codec.Protobuf,
	)
	if err != nil {
		t.Fatalf("CallWithCodec() error = %v", err)
	}

	if response.ContentType != codec.Protobuf.ContentType() {
		t.Fatalf("ContentType = %q, want %q", response.ContentType, codec.Protobuf.ContentType())
	}
	if !decoded.Value {
		t.Fatalf("decoded response value = %t, want true", decoded.Value)
	}
}

func TestServerStartFailureDoesNotLeaveStartedState(t *testing.T) {
	t.Parallel()

	want := errors.New("connect failed")
	server := NewServerWithBroker(&startFailBroker{connectErr: want}, nil)
	server.Handle("orders", func(context.Context, *core.Message) error { return nil })

	if err := server.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Start() error = %v, want %v", err, want)
	}
	if server.IsStarted() {
		t.Fatal("server should not remain started after Start() failure")
	}
}

func TestServerStartSubscribeFailureEmitsObservability(t *testing.T) {
	t.Parallel()

	want := errors.New("subscribe failed")
	events := &runtimeEventRecorder{}
	metrics := &runtimeMetricsRecorder{}
	config := &core.Config{
		EventHook: events.Hook,
		Metrics:   metrics,
	}
	server := NewServerWithBroker(&subscribeFailBroker{subscribeErr: want}, config)
	server.Handle("orders", func(context.Context, *core.Message) error { return nil })

	if err := server.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Start() error = %v, want %v", err, want)
	}
	if server.IsStarted() {
		t.Fatal("server should not report started after subscribe failure")
	}

	recordedEvents := events.Events()
	if len(recordedEvents) < 2 {
		t.Fatalf("expected connect and subscribe failure events, got %d", len(recordedEvents))
	}
	subscribeEvent := recordedEvents[len(recordedEvents)-1]
	if subscribeEvent.Name != core.EventSubscribeFailed || subscribeEvent.Level != core.EventLevelError {
		t.Fatalf("unexpected subscribe failure event = %#v", subscribeEvent)
	}
	if subscribeEvent.Destination != "orders" {
		t.Fatalf("subscribe failure destination = %q, want %q", subscribeEvent.Destination, "orders")
	}

	counters := metrics.Counters()
	if len(counters) < 2 {
		t.Fatalf("expected connect and subscribe counters, got %d", len(counters))
	}
	subscribeCounter := counters[len(counters)-1]
	if subscribeCounter.name != "weave.runtime.subscribe.failures" {
		t.Fatalf("counter name = %q, want subscribe failure metric", subscribeCounter.name)
	}
	if subscribeCounter.labels["backend"] != "subscribe-failing" || subscribeCounter.labels["destination"] != "orders" || subscribeCounter.labels["role"] != "server" {
		t.Fatalf("counter labels = %v", subscribeCounter.labels)
	}
}

func TestServerStartAndStopEmitTracingSpans(t *testing.T) {
	t.Parallel()

	broker := testkit.NewMockBroker()
	tracing := &runtimeTraceRecorder{}
	server := NewServerWithBroker(broker, &core.Config{Tracing: tracing})
	server.Handle("orders", func(context.Context, *core.Message) error { return nil })

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	starts := tracing.Starts()
	if len(starts) != 2 {
		t.Fatalf("trace starts = %d, want 2 (start + stop)", len(starts))
	}
	if starts[0].Name != "weave.runtime.server.start" || starts[1].Name != "weave.runtime.server.stop" {
		t.Fatalf("unexpected trace starts = %#v", starts)
	}

	finishes := tracing.Finishes()
	if len(finishes) != 2 {
		t.Fatalf("trace finishes = %d, want 2 (start + stop)", len(finishes))
	}
	if finishes[0].Attributes["outcome"] != "success" || finishes[1].Attributes["outcome"] != "success" {
		t.Fatalf("unexpected trace finish attributes = %#v", finishes)
	}
}

func TestServerHealthHooksReceiveLifecycleSnapshots(t *testing.T) {
	t.Parallel()

	health := &runtimeHealthRecorder{}
	server := NewServerWithBroker(testkit.NewMockBroker(), &core.Config{HealthHook: health.Hook})
	server.Handle("orders", func(context.Context, *core.Message) error { return nil })

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	reports := health.Reports()
	if len(reports) != 3 {
		t.Fatalf("health reports = %d, want 3", len(reports))
	}
	if reports[0].Status != core.HealthStatusDegraded || reports[0].Started {
		t.Fatalf("connect health report = %#v", reports[0])
	}
	if reports[1].Status != core.HealthStatusHealthy || !reports[1].Started {
		t.Fatalf("started health report = %#v", reports[1])
	}
	if reports[2].Status != core.HealthStatusUnhealthy || reports[2].Connected {
		t.Fatalf("stop health report = %#v", reports[2])
	}
}
