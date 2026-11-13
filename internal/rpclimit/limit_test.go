package rpclimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/lifecycle"
)

type waitingContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *waitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestWaitingCallCancelsWithoutTakingCapacity(t *testing.T) {
	var work lifecycle.Group
	limit := New(1)
	release, err := limit.Acquire(context.Background(), &work)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	waiter := &waitingContext{Context: ctx, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := limit.Acquire(waiter, &work); done <- err }()
	<-waiter.waiting
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter=%v", err)
	}
	release()
	release, err = limit.Acquire(context.Background(), &work)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestWaitingCallsStopWithBroker(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "close"}[force], func(t *testing.T) {
			var work lifecycle.Group
			_, finish, err := work.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			limit := New(1)
			release, err := limit.Acquire(context.Background(), &work)
			if err != nil {
				t.Fatal(err)
			}
			ctx := &waitingContext{Context: context.Background(), waiting: make(chan struct{})}
			done := make(chan error, 1)
			go func() { _, err := limit.Acquire(ctx, &work); done <- err }()
			<-ctx.waiting
			shutdownDone := make(chan error, 1)
			if force {
				work.Close()
			} else {
				go func() { shutdownDone <- work.Shutdown(context.Background(), nil, func() error { return nil }) }()
			}
			want := core.ErrShuttingDown
			if force {
				want = core.ErrClosed
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("waiter=%v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("broker stop stranded a queued call")
			}
			release()
			finish()
			if !force {
				if err := <-shutdownDone; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCanceledOrClosedAcquireDoesNotLeakAvailableSlot(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		var work lifecycle.Group
		limit := New(1)
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		} else {
			work.Close()
		}
		if release, err := limit.Acquire(ctx, &work); err == nil {
			release()
			t.Fatal("acquired capacity after cancellation/close")
		}
		cancel()
		if len(limit.slots) != 0 {
			t.Fatal("failed acquire leaked capacity")
		}
	}
}

func TestUnlimitedAndNestedShutdownCapacity(t *testing.T) {
	var work lifecycle.Group
	unlimited := New(0)
	for range 3 {
		release, err := unlimited.Acquire(context.Background(), &work)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	ctx, finish, err := work.Handler(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- work.Shutdown(context.Background(), nil, func() error { return nil }) }()
	<-work.Stopping()
	ctx, nestedFinish, err := work.Continue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	limit := New(1)
	release, err := limit.Acquire(ctx, &work)
	if err != nil {
		t.Fatal(err)
	}
	release()
	nestedFinish()
	finish()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
