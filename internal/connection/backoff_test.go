package connection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestConnectionBackoffOverridesFixedDelay(t *testing.T) {
	config := &core.Config{RetryDelay: time.Hour}
	if got := Backoff(config, 1, time.Second); got != time.Hour {
		t.Fatal(got)
	}
	config.RetryDelay = 0
	if got := Backoff(config, 1, 200*time.Millisecond); got != 200*time.Millisecond {
		t.Fatal(got)
	}
	config.ConnectionBackoff = func(attempt int) time.Duration { return time.Duration(attempt) * time.Millisecond }
	if got := Backoff(config, 3, time.Hour); got != 3*time.Millisecond {
		t.Fatal(got)
	}
	config.ConnectionBackoff = func(int) time.Duration { return -time.Second }
	if got := Backoff(config, 1, time.Hour); got != 0 {
		t.Fatal(got)
	}
}

func TestZeroBackoffPreservesCancellationAndLifecycleErrors(t *testing.T) {
	closed, stopping := make(chan struct{}), make(chan struct{})
	close(stopping)
	for _, delay := range []time.Duration{0, -time.Second, time.Hour} {
		if err := Wait(context.Background(), nil, stopping, delay); !errors.Is(err, core.ErrShuttingDown) {
			t.Fatalf("shutdown wait=%v", err)
		}
	}
	close(closed)
	for range 100 {
		if err := Wait(context.Background(), closed, stopping, 0); !errors.Is(err, core.ErrClosed) {
			t.Fatalf("closed wait=%v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Wait(ctx, closed, stopping, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait=%v", err)
	}
}

func TestShutdownInterruptsPendingBackoff(t *testing.T) {
	stopping := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- Wait(context.Background(), nil, stopping, time.Hour) }()
	close(stopping)
	select {
	case err := <-done:
		if !errors.Is(err, core.ErrShuttingDown) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not interrupt backoff")
	}
}
