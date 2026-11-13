package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestShutdownDrainsAdmittedAndNestedWork(t *testing.T) {
	var group, bridge Group
	handlerCtx, finishHandler, err := group.Handler(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finishHandler()
	bridgeCtx, finishBridge, err := bridge.Continue(handlerCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer finishBridge()
	done := make(chan error, 1)
	go func() { done <- group.Shutdown(context.Background(), nil, func() error { return nil }) }()
	<-group.Stopping()
	if _, _, err := group.Begin(context.Background()); !errors.Is(err, core.ErrShuttingDown) {
		t.Fatalf("new work = %v, want ErrShuttingDown", err)
	}
	_, finishPublish, err := group.Continue(bridgeCtx)
	if err != nil {
		t.Fatalf("admitted handler's publish = %v", err)
	}
	defer finishPublish()
	finishHandler()
	if _, _, err := group.Continue(context.WithoutCancel(handlerCtx)); !errors.Is(err, core.ErrShuttingDown) {
		t.Fatalf("finished handler context admitted work: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before nested publish finished: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	finishPublish()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after work drained")
	}
	if _, _, err := group.Continue(context.Background()); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("work after shutdown = %v, want ErrClosed", err)
	}
}

func TestShutdownDeadlineInterruptsHandlersAndBoundsClientClose(t *testing.T) {
	var group Group
	handlerCtx, finish, err := group.Handler(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	releaseClose := make(chan struct{})
	defer close(releaseClose)
	closing := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- group.Shutdown(ctx, nil, func() error { close(closing); <-releaseClose; return nil })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown() = %v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for a blocked handler or client close")
	}
	select {
	case <-handlerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("deadline did not cancel handler context")
	}
	select {
	case <-closing:
	case <-time.After(time.Second):
		t.Fatal("deadline did not start closing clients")
	}
}

func TestShutdownIsConcurrentAndPreservesErrors(t *testing.T) {
	var group Group
	failure := errors.New("flush failed")
	closeFailure := errors.New("close failed")
	var closes atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 8)
	for range cap(done) {
		go func() {
			done <- group.Shutdown(ctx, func(context.Context) error { return failure }, func() error { closes.Add(1); return closeFailure })
		}()
	}
	for range cap(done) {
		if err := <-done; !errors.Is(err, failure) || !errors.Is(err, closeFailure) {
			t.Fatalf("shutdown error = %v, want flush and close failures", err)
		}
	}
	if closes.Load() != 1 {
		t.Fatalf("client closes = %d, want 1", closes.Load())
	}
}

func TestShutdownValidatesContextAndRejectsForeignLeases(t *testing.T) {
	var group, other Group
	if err := group.Shutdown(nil, nil, func() error { return nil }); !errors.Is(err, core.ErrInvalidConfig) || group.IsStopping() {
		t.Fatalf("Shutdown(nil) = %v, stopping=%v", err, group.IsStopping())
	}
	ctx, finish, _ := other.Begin(context.Background())
	defer finish()
	_, hold, _ := group.Begin(context.Background())
	defer hold()
	deadline, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- group.Shutdown(deadline, nil, func() error { return nil }) }()
	<-group.Stopping()
	if _, _, err := group.Continue(ctx); !errors.Is(err, core.ErrShuttingDown) {
		t.Fatalf("foreign lease bypassed shutdown: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() = %v, want cancellation", err)
	}
}
