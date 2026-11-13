package readiness

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestOldCleanupCannotRevokeReplacement(t *testing.T) {
	var state State
	var revoked atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	old, previous := state.Assign(ctx, func(uint64) { revoked.Add(1) })
	previous()
	newAssignment, previous := state.Assign(context.Background(), func(uint64) { revoked.Add(1) })
	previous()
	cancel()
	old.Release()
	if !state.Ready() || revoked.Load() != 1 {
		t.Fatalf("replacement revoked: ready=%v revoked=%d", state.Ready(), revoked.Load())
	}
	if newAssignment.Generation != old.Generation+1 {
		t.Fatal("assignment generation was not advanced")
	}
	state.Invalidate()()
	newAssignment.Release()
	if state.Ready() || revoked.Load() != 2 {
		t.Fatal("revocation was lost or duplicated")
	}
}

func TestCancellationInvalidatesReadinessBeforeNotification(t *testing.T) {
	var state State
	ctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan struct{})
	defer close(blocked)
	_, previous := state.Assign(ctx, func(uint64) { <-blocked })
	previous()
	cancel()
	if state.Ready() {
		t.Fatal("canceled assignment still reports ready")
	}
}

func TestWaitDoesNotLoseNotificationsAndHonorsCancellation(t *testing.T) {
	for range 100 {
		var signal Signal
		var ready atomic.Bool
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		done := make(chan error, 1)
		go func() { done <- signal.Wait(ctx, nil, func() (bool, error) { return ready.Load(), nil }) }()
		ready.Store(true)
		signal.Notify()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	var signal Signal
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := signal.Wait(ctx, nil, func() (bool, error) { return true, nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWaitStopsWithoutHoldingUpLifecycle(t *testing.T) {
	var signal Signal
	var stopped atomic.Bool
	stopping, entered := make(chan struct{}), make(chan struct{}, 1)
	want := errors.New("stopped")
	done := make(chan error, 1)
	go func() {
		done <- signal.Wait(context.Background(), stopping, func() (bool, error) {
			if stopped.Load() {
				return false, want
			}
			select {
			case entered <- struct{}{}:
			default:
			}
			return false, nil
		})
	}()
	<-entered
	stopped.Store(true)
	close(stopping)
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("lifecycle stop did not interrupt waiter")
	}
	if err := signal.Wait(nil, nil, func() (bool, error) { return true, nil }); err == nil {
		t.Fatal("nil wait context accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := signal.Wait(ctx, nil, func() (bool, error) { return false, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestCanceledAssignmentCannotBecomeReady(t *testing.T) {
	var state State
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assignment, notify := state.Assign(ctx, nil)
	notify()
	if assignment != nil || state.Ready() {
		t.Fatal("canceled context created a ready assignment")
	}
	assignment.Release()
	var absent *State
	absent.Invalidate()()
	if absent.Ready() {
		t.Fatal("absent subscription reports ready")
	}
}
