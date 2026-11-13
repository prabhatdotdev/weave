package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

type readinessSession struct {
	fakeSession
	generation int32
	claims     map[string][]int32
}

func (s *readinessSession) GenerationID() int32        { return s.generation }
func (s *readinessSession) Claims() map[string][]int32 { return s.claims }

type readinessInvocation struct {
	ctx     context.Context
	group   string
	handler sarama.ConsumerGroupHandler
}

func readinessKafkaBroker(t *testing.T, mutate bool) (*Broker, <-chan readinessInvocation, <-chan core.Event) {
	t.Helper()
	var broker *Broker
	events := make(chan core.Event, 128)
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.ConsumerGroup = "workers"
	config.EventHook = func(_ context.Context, event core.Event) {
		_ = broker.IsReady()
		if event.Name == core.EventConsumerAssigned || event.Name == core.EventConsumerRevoked || event.Name == core.EventRebalanceStarted || event.Name == core.EventRebalanceCompleted {
			events <- event
			if mutate && (event.Name == core.EventConsumerAssigned || event.Name == core.EventRebalanceCompleted) {
				if partitions, ok := event.Fields["partitions"].([]int32); ok && len(partitions) > 0 {
					partitions[0] = 99
				}
				if claims, ok := event.Fields["assignments"].(map[string][]int32); ok {
					delete(claims, "orders")
				}
			}
		}
	}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker = brokerAny.(*Broker)
	invocations := make(chan readinessInvocation, 32)
	broker.newProducer = func([]string, *sarama.Config) (messageProducer, error) { return &fakeSyncProducer{}, nil }
	broker.newConsumerGroup = func(_ []string, group string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
		return &fakeConsumerGroup{consumeFn: func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
			invocations <- readinessInvocation{ctx, group, handler}
			<-ctx.Done()
			return nil
		}}, nil
	}
	t.Cleanup(func() { _ = broker.Close() })
	return broker, invocations, events
}

func nextReadinessInvocation(t *testing.T, invocations <-chan readinessInvocation) readinessInvocation {
	t.Helper()
	for {
		select {
		case invocation := <-invocations:
			if invocation.ctx.Err() == nil {
				return invocation
			}
		case <-time.After(time.Second):
			t.Fatal("consumer group did not start")
		}
	}
}

func TestConsumerReadinessWaitsForAssignmentAndRebalance(t *testing.T) {
	broker, invocations, events := readinessKafkaBroker(t, false)
	if broker.IsReady() || !core.IsNotConnected(broker.WaitReady(context.Background())) {
		t.Fatal("unconnected broker reports ready")
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	invocation := nextReadinessInvocation(t, invocations)
	if broker.IsReady() || !broker.IsConnected() {
		t.Fatal("registration must not imply assignment readiness")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := broker.WaitReady(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unassigned wait = %v", err)
	}
	firstCtx, cancelFirst := context.WithCancel(invocation.ctx)
	defer cancelFirst()
	first := &readinessSession{fakeSession: fakeSession{ctx: firstCtx}, generation: 1, claims: map[string][]int32{"orders": {0, 2}}}
	if err := invocation.handler.Setup(first); err != nil {
		t.Fatal(err)
	}
	if err := broker.WaitReady(context.Background()); err != nil || !broker.IsReady() {
		t.Fatalf("assigned consumer readiness = %v", err)
	}
	cancelFirst()
	if broker.IsReady() {
		t.Fatal("canceled session still reports ready before Cleanup")
	}
	if err := invocation.handler.Cleanup(first); err != nil {
		t.Fatal(err)
	}
	second := &readinessSession{fakeSession: fakeSession{ctx: invocation.ctx}, generation: 2}
	if err := invocation.handler.Setup(second); err != nil {
		t.Fatal(err)
	}
	if !broker.IsReady() {
		t.Fatal("joined member with zero partitions is not ready")
	}
	_ = invocation.handler.Cleanup(first)
	if !broker.IsReady() {
		t.Fatal("old Cleanup revoked the new session")
	}
	assigned, revoked, started, completed := 0, 0, 0, 0
	limit := time.After(time.Second)
	for assigned < 2 || revoked < 1 || started < 2 || completed < 2 {
		var event core.Event
		select {
		case event = <-events:
		case <-limit:
			t.Fatalf("missing lifecycle events: assigned=%d revoked=%d started=%d completed=%d", assigned, revoked, started, completed)
		}
		if event.Fields["consumer_group"] != "workers" {
			t.Fatalf("missing group metadata: %+v", event)
		}
		switch event.Name {
		case core.EventConsumerAssigned:
			assigned++
			if event.Fields["generation_id"] == int32(1) && !reflect.DeepEqual(event.Fields["partitions"], []int32{0, 2}) {
				t.Fatal("wrong assignment")
			}
		case core.EventConsumerRevoked:
			revoked++
		case core.EventRebalanceStarted:
			started++
		case core.EventRebalanceCompleted:
			completed++
		}
	}
	if assigned != 2 || revoked != 1 || started != 2 || completed != 2 {
		t.Fatalf("assigned=%d revoked=%d started=%d completed=%d", assigned, revoked, started, completed)
	}
}

func TestConsumerReadinessIncludesEveryConsumerGroup(t *testing.T) {
	broker, invocations, _ := readinessKafkaBroker(t, false)
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"a", "b"} {
		if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }, core.WithConsumerGroup(group)); err != nil {
			t.Fatal(err)
		}
	}
	active := make(map[string]readinessInvocation)
	for len(active) < 2 {
		invocation := nextReadinessInvocation(t, invocations)
		active[invocation.group] = invocation
		for group, current := range active {
			if current.ctx.Err() != nil {
				delete(active, group)
			}
		}
	}
	for i, group := range []string{"a", "b"} {
		invocation := active[group]
		if err := invocation.handler.Setup(&readinessSession{fakeSession: fakeSession{ctx: invocation.ctx}, generation: 1}); err != nil {
			t.Fatal(err)
		}
		if broker.IsReady() != (i == 1) {
			t.Fatalf("readiness after assigning group %s = %v", group, broker.IsReady())
		}
	}
}

func TestReadinessWaitDoesNotDelayShutdown(t *testing.T) {
	broker, invocations, _ := readinessKafkaBroker(t, false)
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	_ = nextReadinessInvocation(t, invocations)
	_, release, err := broker.work.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	wait := make(chan error, 1)
	go func() { wait <- broker.WaitReady(context.Background()) }()
	shutdown := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { shutdown <- broker.Shutdown(ctx) }()
	<-broker.work.Stopping()
	select {
	case err := <-wait:
		if !errors.Is(err, core.ErrShuttingDown) {
			t.Fatalf("wait error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("readiness wait prevented shutdown")
	}
	release()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}

func TestAssignmentEventsOwnTheirMetadata(t *testing.T) {
	broker, invocations, events := readinessKafkaBroker(t, true)
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	invocation := nextReadinessInvocation(t, invocations)
	claims := map[string][]int32{"orders": {0, 2}}
	session := &readinessSession{fakeSession: fakeSession{ctx: invocation.ctx}, generation: 1, claims: claims}
	if err := invocation.handler.Setup(session); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(claims, map[string][]int32{"orders": {0, 2}}) {
		t.Fatalf("hook mutated native claims: %v", claims)
	}
	observer := invocation.handler.(*consumerGroupHandler).onCleanup
	if err := observer(session); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(claims, map[string][]int32{"orders": {0, 2}}) {
		t.Fatal("cleanup mutated native claims")
	}
	for {
		select {
		case event := <-events:
			if event.Name == core.EventConsumerRevoked {
				if !reflect.DeepEqual(event.Fields["partitions"], []int32{0, 2}) {
					t.Fatalf("assigned hook corrupted revocation metadata: %+v", event)
				}
				return
			}
		case <-time.After(time.Second):
			t.Fatal("missing revocation event")
		}
	}
}

func TestReadinessRecoveryRejectsOldConsumerCallbacks(t *testing.T) {
	broker, invocations, _ := readinessKafkaBroker(t, false)
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := broker.Subscribe(context.Background(), "orders", func(context.Context, *core.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	old := nextReadinessInvocation(t, invocations)
	session := &readinessSession{fakeSession: fakeSession{ctx: old.ctx}, generation: 1}
	_ = old.handler.Setup(session)
	broker.handleConnectionLoss(errors.New("lost broker"))
	if broker.IsReady() {
		t.Fatal("connection loss left readiness true")
	}
	_ = old.handler.Setup(&readinessSession{fakeSession: fakeSession{ctx: context.Background()}, generation: 2})
	if broker.IsReady() {
		t.Fatal("old Setup restored readiness after connection loss")
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	broker.restoreTrackedSubscriptions()
	fresh := nextReadinessInvocation(t, invocations)
	_ = fresh.handler.Setup(&readinessSession{fakeSession: fakeSession{ctx: fresh.ctx}, generation: 1})
	_ = old.handler.Cleanup(session)
	if !broker.IsReady() {
		t.Fatal("old consumer cleanup cleared recovered readiness")
	}
}
