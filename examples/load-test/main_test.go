package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestPercentileNearestRank(t *testing.T) {
	values := make([]time.Duration, 100)
	for i := range values {
		values[i] = time.Duration(i+1) * time.Millisecond
	}

	tests := []struct {
		name   string
		values []time.Duration
		p      float64
		want   time.Duration
	}{
		{name: "empty", p: 0.95},
		{name: "single", values: values[:1], p: 0.99, want: time.Millisecond},
		{name: "p95", values: values, p: 0.95, want: 95 * time.Millisecond},
		{name: "p99", values: values, p: 0.99, want: 99 * time.Millisecond},
		{name: "lower boundary", values: values, p: 0, want: time.Millisecond},
		{name: "upper boundary", values: values, p: 1, want: 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := percentile(tt.values, tt.p); got != tt.want {
				t.Fatalf("percentile() = %s, want %s", got, tt.want)
			}
		})
	}
}

type callerFunc func(context.Context, *core.Message) (*core.Message, error)

func (call callerFunc) Call(ctx context.Context, _ string, msg *core.Message, _ ...core.PublishOption) (*core.Message, error) {
	return call(ctx, msg)
}

func TestRunStageStopsWhenCanceled(t *testing.T) {
	for _, canceledBeforeStart := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		if canceledBeforeStart {
			cancel()
		}
		caller := callerFunc(func(ctx context.Context, _ *core.Message) (*core.Message, error) {
			calls++
			cancel()
			return nil, ctx.Err()
		})
		got := runStage(ctx, caller, "rpc", []byte("payload"), 1, time.Second, time.Second)
		cancel()
		want := 1
		if canceledBeforeStart {
			want = 0
		}
		if calls != want || got.attempts != want || got.failures != want {
			t.Fatalf("pre-canceled=%v calls=%d result=%+v", canceledBeforeStart, calls, got)
		}
	}
}

func TestCallRejectsCorruptResponse(t *testing.T) {
	caller := callerFunc(func(context.Context, *core.Message) (*core.Message, error) {
		return core.NewTextMessage("changed payload"), nil
	})
	if err := call(context.Background(), caller, "rpc", []byte("original"), time.Second); err == nil {
		t.Fatal("corrupt response counted as success")
	}
	want := errors.New("broker unavailable")
	caller = func(context.Context, *core.Message) (*core.Message, error) { return nil, want }
	if err := call(context.Background(), caller, "rpc", nil, time.Second); !errors.Is(err, want) {
		t.Fatalf("lost broker error: %v", err)
	}
}
