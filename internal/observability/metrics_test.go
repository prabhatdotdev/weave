package observability

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestConcurrentHandlersAndCallsBalanceMetrics(t *testing.T) {
	var metrics Metrics
	var mu sync.Mutex
	var events []core.Event
	emit := func(_ context.Context, event core.Event) {
		metrics.Snapshot(&event)
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	const count = 20
	started, release := make(chan struct{}, count), make(chan struct{})
	handler := metrics.WrapHandler(func(ctx context.Context, msg *core.Message) error {
		finish := metrics.StartCall(ctx, "rpc", msg.CorrelationID, time.Now(), emit)
		defer finish(nil)
		started <- struct{}{}
		<-release
		return nil
	}, "orders", emit)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := handler(context.Background(), core.NewTextMessage("body").WithCorrelationID("correlation")); err != nil {
				t.Error(err)
			}
		}()
	}
	for range count {
		<-started
	}
	var current core.Event
	metrics.Snapshot(&current)
	if current.ActiveHandlers != count || current.PendingRPCs != count {
		t.Errorf("active snapshot=%+v", current)
	}
	close(release)
	wg.Wait()
	metrics.Snapshot(&current)
	if current.ActiveHandlers != 0 || current.PendingRPCs != 0 {
		t.Errorf("leaked work: %+v", current)
	}
	names := make(map[string]int)
	for _, event := range events {
		names[event.Name]++
		if event.ActiveHandlers < 0 || event.PendingRPCs < 0 || event.Err != nil || event.CorrelationID != "correlation" {
			t.Errorf("invalid event=%+v", event)
		}
		finished := event.Name == core.EventCallFinished || event.Name == core.EventHandlerFinished
		if finished && event.Duration <= 0 || !finished && event.Duration != 0 {
			t.Errorf("invalid duration=%+v", event)
		}
	}
	for _, name := range []string{core.EventHandlerStarted, core.EventHandlerFinished, core.EventCallStarted, core.EventCallFinished} {
		if names[name] != count {
			t.Errorf("%s events=%d, want %d", name, names[name], count)
		}
	}
}

func TestHandlerMetricsPreserveErrorsAndPanics(t *testing.T) {
	for _, scenario := range []string{"error", "panic", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			var metrics Metrics
			var events []core.Event
			failure := errors.New("handler failed")
			emit := func(_ context.Context, event core.Event) { metrics.Snapshot(&event); events = append(events, event) }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handler := metrics.WrapHandler(func(ctx context.Context, _ *core.Message) error {
				if scenario == "panic" {
					panic("handler panic")
				}
				if scenario == "canceled" {
					cancel()
					return ctx.Err()
				}
				return failure
			}, "orders", emit)
			func() {
				defer func() {
					got := recover()
					if scenario == "panic" && got != "handler panic" || scenario != "panic" && got != nil {
						t.Errorf("panic changed: %v", got)
					}
				}()
				err := handler(ctx, core.NewTextMessage("body"))
				want := failure
				if scenario == "canceled" {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Errorf("handler error=%v, want %v", err, want)
				}
			}()
			if len(events) != 2 || events[0].ActiveHandlers != 1 || events[1].ActiveHandlers != 0 || events[1].Err == nil || events[1].Duration <= 0 {
				t.Fatalf("events=%+v", events)
			}
		})
	}
}

func TestNilHandlerRemainsInvalid(t *testing.T) {
	var metrics Metrics
	if metrics.WrapHandler(nil, "orders", nil) != nil {
		t.Fatal("nil handler became valid")
	}
}
