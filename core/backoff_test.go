package core

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestJitteredBackoffBoundsAndAttemptForwarding(t *testing.T) {
	for _, maximum := range []time.Duration{1, time.Millisecond, time.Duration(math.MaxInt64)} {
		for _, upper := range []bool{false, true} {
			backoff := jitteredBackoff(func(attempt int) time.Duration {
				if attempt != 7 {
					t.Errorf("base received attempt %d", attempt)
				}
				return maximum
			}, func(bound uint64) uint64 {
				if bound != uint64(maximum)+1 {
					t.Errorf("sampling bound=%d", bound)
				}
				if upper {
					return bound - 1
				}
				return 0
			})
			want := time.Duration(0)
			if upper {
				want = maximum
			}
			if got := backoff(7); got != want {
				t.Fatalf("jitter=%s, want %s", got, want)
			}
		}
	}
	for _, base := range []BackoffStrategy{nil, FixedBackoff(0), func(int) time.Duration { return -time.Second }} {
		backoff := jitteredBackoff(base, func(uint64) uint64 { t.Fatal("sampled a nonpositive delay"); return 0 })
		if got := backoff(1); got != 0 {
			t.Fatalf("nonpositive jitter = %s", got)
		}
	}
}

func TestJitteredBackoffConcurrentUse(t *testing.T) {
	backoff := JitteredBackoff(ExponentialBackoff(time.Millisecond, time.Second, 2))
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for attempt := 1; attempt <= 100; attempt++ {
				if delay := backoff(attempt); delay < 0 || delay > time.Second {
					t.Errorf("jitter out of range: %s", delay)
				}
			}
		}()
	}
	workers.Wait()
}

func TestJitteredRetryStopsAfterCancellation(t *testing.T) {
	for _, upper := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		failure := errors.New("handler failed")
		attempts := 0
		handler := RetryHandler(func(context.Context, *Message) error { attempts++; return failure }, RetryPolicy{
			MaxAttempts: 3,
			Backoff: jitteredBackoff(FixedBackoff(time.Hour), func(bound uint64) uint64 {
				if upper {
					return bound - 1
				}
				return 0
			}),
			OnRetry: func(_ context.Context, _ int, _ error, _ time.Duration) { cancel() },
		})
		if err := handler(ctx, NewTextMessage("work")); !errors.Is(err, failure) || attempts != 1 {
			t.Fatalf("attempts=%d error=%v", attempts, err)
		}
	}
}

func TestExponentialBackoffSaturatesWithoutOverflow(t *testing.T) {
	maximum := time.Duration(math.MaxInt64)
	for _, multiplier := range []float64{2, math.Inf(1), math.NaN()} {
		backoff := ExponentialBackoff(time.Second, maximum, multiplier)
		if got := backoff(math.MaxInt); got != maximum {
			t.Fatalf("multiplier=%v delay=%s, want cap", multiplier, got)
		}
	}
	initial := maximum - 1
	if got := ExponentialBackoff(initial, maximum, 1)(math.MaxInt); got != initial {
		t.Fatalf("constant backoff overflowed: %s", got)
	}
	if got := ExponentialBackoff(time.Second, time.Hour, 2)(0); got != time.Second {
		t.Fatal("nonpositive attempt changed initial delay")
	}
}

func TestBackoffDefaultsRemainOptIn(t *testing.T) {
	if DefaultConfig().ConnectionBackoff != nil {
		t.Fatal("connection jitter enabled by default")
	}
	policy := DefaultRetryPolicy()
	for range 10 {
		if policy.Backoff(1) != 100*time.Millisecond || policy.Backoff(2) != 200*time.Millisecond {
			t.Fatal("default handler delay changed")
		}
	}
}
