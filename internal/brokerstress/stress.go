// Package brokerstress exercises live transports with concurrent traffic and broker outages.
package brokerstress

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

type Fixture struct {
	Broker           core.MessageBroker
	Events, Requests string
	Idle             func() bool
	// DrainTimeout bounds delivery of confirmed events; zero uses 30 seconds.
	DrainTimeout time.Duration
}

func Enabled(t testing.TB) {
	t.Helper()
	if os.Getenv("WEAVE_STRESS_E2E") != "1" {
		t.Skip("run scripts/test_stress.sh to enable isolated live-broker stress tests")
	}
}

func Sustained(t *testing.T, f Fixture) {
	t.Helper()
	duration := 30 * time.Second
	if value := os.Getenv("WEAVE_STRESS_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration < time.Second {
			t.Fatalf("WEAVE_STRESS_DURATION must be a duration of at least 1s: %q", value)
		}
	}
	w := start(t, f, duration+time.Minute, false)
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case err := <-w.unexpected:
		t.Fatalf("healthy broker failed under load: %v", err)
	case <-w.ctx.Done():
		t.Fatal(w.ctx.Err())
	}
	w.finish(t)
}

func RepeatedFailures(t *testing.T, f Fixture, service string) {
	t.Helper()
	cycles := 3
	if value := os.Getenv("WEAVE_STRESS_CYCLES"); value != "" {
		var err error
		cycles, err = strconv.Atoi(value)
		if err != nil || cycles < 2 {
			t.Fatalf("WEAVE_STRESS_CYCLES must be at least 2: %q", value)
		}
	}
	broker := newContainer(t, service)
	w := start(t, f, time.Duration(cycles)*90*time.Second+time.Minute, true)
	for cycle := 1; cycle <= cycles; cycle++ {
		resume, entered := w.pause()
		t.Cleanup(resume)
		marker, err := w.publish()
		if err != nil {
			t.Fatalf("cycle %d: publish durable marker: %v", cycle, err)
		}
		select {
		case <-entered:
		case <-w.ctx.Done():
			t.Fatal("consumer did not enter the paused handler")
		}
		before := w.failures.Load()
		broker.kill(t, w.ctx)
		started := time.Now()
		if err := w.call(time.Second); err == nil {
			t.Fatal("RPC unexpectedly succeeded while the broker was stopped")
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("RPC ignored its 1s deadline during outage: %s", elapsed)
		}
		select {
		case <-time.After(2 * time.Second):
		case <-w.ctx.Done():
			t.Fatal(w.ctx.Err())
		}
		broker.start(t, w.ctx)
		resume()
		wait(t, w.ctx, 30*time.Second, "RPC recovery", func() bool { return w.call(time.Second) == nil })
		readyCtx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
		if err := f.Broker.(core.ConsumerReadiness).WaitReady(readyCtx); err != nil {
			cancel()
			t.Fatalf("consumer readiness after recovery: %v", err)
		}
		cancel()
		wait(t, w.ctx, 30*time.Second, "confirmed marker completion after recovery", func() bool { return !w.ledger.contains(marker) })
		if w.failures.Load() == before {
			t.Fatal("background traffic did not observe the broker outage")
		}
		t.Logf("cycle=%d recovered=%s confirmed_marker_delivered=true failures=%d", cycle, time.Since(started).Round(time.Millisecond), w.failures.Load()-before)
	}
	w.finish(t)
}

type delivery struct{ confirmed, seen bool }

// The ledger retains only unfinished attempts, so long runs do not retain every message ID.
type ledger struct {
	mu      sync.Mutex
	pending map[string]delivery
}

func (l *ledger) register(id string) {
	l.mu.Lock()
	l.pending[id] = delivery{}
	l.mu.Unlock()
}

func (l *ledger) confirm(id string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	d := l.pending[id]
	if err != nil || d.seen {
		delete(l.pending, id)
		return
	}
	d.confirmed = true
	l.pending[id] = d
}

func (l *ledger) see(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	d, exists := l.pending[id]
	if !exists { // Late duplicates of completed or uncertain attempts need no new entry.
		return
	}
	if d.confirmed {
		delete(l.pending, id)
		return
	}
	d.seen = true
	l.pending[id] = d
}

func (l *ledger) contains(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, exists := l.pending[id]
	return exists
}

func (l *ledger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pending)
}

type workload struct {
	Fixture
	ctx        context.Context
	stop       chan struct{}
	stopOnce   sync.Once
	done       chan struct{}
	unexpected chan error
	ledger     ledger
	sequence   atomic.Uint64
	published  atomic.Uint64
	rpcs       atomic.Uint64
	failures   atomic.Uint64
	run        string
	started    time.Time
	pauseMu    sync.Mutex
	gate       chan struct{}
	entered    chan struct{}
}

var padding = bytes.Repeat([]byte("x"), 1024)

func body(kind, id string) []byte { return append([]byte(kind+":"+id+":"), padding...) }

func start(t *testing.T, f Fixture, timeout time.Duration, tolerateFailures bool) *workload {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	w := &workload{Fixture: f, ctx: ctx, stop: make(chan struct{}), done: make(chan struct{}), unexpected: make(chan error, 1), run: rand.Text(), ledger: ledger{pending: make(map[string]delivery)}}
	t.Cleanup(func() {
		cancel()
		w.stopOnce.Do(func() { close(w.stop) })
		_ = f.Broker.Close()
	})
	check := func(kind string, msg *core.Message) error {
		if msg.MessageID == "" || msg.Headers["stress-run"] != w.run || !bytes.Equal(msg.Body, body(kind, msg.MessageID)) {
			return fmt.Errorf("corrupted %s: id=%q body_bytes=%d headers=%v", kind, msg.MessageID, len(msg.Body), msg.Headers)
		}
		return nil
	}
	if err := f.Broker.Subscribe(ctx, f.Events, func(handlerCtx context.Context, msg *core.Message) error {
		w.pauseMu.Lock()
		gate, entered := w.gate, w.entered
		w.pauseMu.Unlock()
		if gate != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		}
		if err := check("event", msg); err != nil {
			w.report(err)
			return err
		}
		w.ledger.see(msg.MessageID)
		return nil
	}, core.WithWorkerCount(8), core.WithPrefetchCount(16), core.WithHandlerErrorRetry()); err != nil {
		t.Fatal(err)
	}
	if err := f.Broker.Subscribe(ctx, f.Requests, func(handlerCtx context.Context, msg *core.Message) error {
		if err := check("request", msg); err != nil {
			w.report(err)
			return err
		}
		response := core.NewMessage(body("response", msg.MessageID)).WithCorrelationID(msg.CorrelationID)
		response.MessageID = msg.MessageID
		return f.Broker.Publish(handlerCtx, msg.ReplyTo, response, core.WithTimeout(2*time.Second))
	}, core.WithWorkerCount(8), core.WithHandlerErrorRetry()); err != nil {
		t.Fatal(err)
	}
	if err := f.Broker.(core.ConsumerReadiness).WaitReady(ctx); err != nil {
		t.Fatalf("consumer setup failed: %v", err)
	}
	if err := w.call(15 * time.Second); err != nil {
		t.Fatalf("warm-up RPC failed: %v", err)
	}
	if _, err := w.publish(); err != nil {
		t.Fatalf("warm-up publish failed: %v", err)
	}
	wait(t, ctx, 15*time.Second, "warm-up delivery", func() bool { return w.ledger.count() == 0 })
	w.published.Store(0)
	w.rpcs.Store(0)
	w.started = time.Now()
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		for _, rpc := range []bool{false, true} {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					select {
					case <-w.stop:
						return
					case <-ctx.Done():
						return
					default:
					}
					var err error
					if rpc {
						err = w.call(2 * time.Second)
					} else {
						_, err = w.publish()
					}
					if err != nil {
						w.failures.Add(1)
						if !tolerateFailures {
							w.report(err)
						}
						select {
						case <-time.After(10 * time.Millisecond):
						case <-ctx.Done():
							return
						}
					}
				}
			}()
		}
	}
	go func() { workers.Wait(); close(w.done) }()
	return w
}

func (w *workload) report(err error) {
	select {
	case w.unexpected <- err:
	default:
	}
}

func (w *workload) publish() (string, error) {
	id := w.run + "-" + strconv.FormatUint(w.sequence.Add(1), 10)
	msg := core.NewMessage(body("event", id)).WithHeader("stress-run", w.run)
	msg.MessageID = id
	w.ledger.register(id)
	err := w.Broker.Publish(w.ctx, w.Events, msg, core.WithPersistent(), core.WithMandatory(), core.WithTimeout(2*time.Second))
	w.ledger.confirm(id, err)
	if err == nil {
		w.published.Add(1)
	}
	return id, err
}

func (w *workload) call(timeout time.Duration) error {
	id := w.run + "-" + strconv.FormatUint(w.sequence.Add(1), 10)
	msg := core.NewMessage(body("request", id)).WithHeader("stress-run", w.run)
	msg.MessageID = id
	response, err := w.Broker.Call(w.ctx, w.Requests, msg, core.WithPersistent(), core.WithTimeout(timeout))
	if err != nil {
		return err
	}
	if response == nil || response.MessageID != id || response.CorrelationID == "" || !bytes.Equal(response.Body, body("response", id)) {
		err := fmt.Errorf("RPC returned the wrong response for %q: %#v", id, response)
		w.report(err)
		return err
	}
	w.rpcs.Add(1)
	return nil
}

func (w *workload) pause() (func(), <-chan struct{}) {
	w.pauseMu.Lock()
	defer w.pauseMu.Unlock()
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	w.gate, w.entered = gate, entered
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }, entered
}

func (w *workload) drain(t *testing.T) {
	t.Helper()
	w.stopOnce.Do(func() { close(w.stop) })
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		t.Fatal("traffic workers did not finish within operation deadlines")
	}
	drainTimeout := w.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	wait(t, w.ctx, drainTimeout, "delivery of all confirmed publishes", func() bool { return w.ledger.count() == 0 })
	wait(t, w.ctx, 15*time.Second, "pending RPC and handler cleanup before shutdown", w.Idle)
	select {
	case err := <-w.unexpected:
		t.Fatal(err)
	default:
	}
}

func (w *workload) finish(t *testing.T) {
	t.Helper()
	w.drain(t)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := w.Broker.(core.Shutdowner).Shutdown(shutdownCtx); err != nil {
		t.Fatalf("drain failed: %v", err)
	}
	if !w.Idle() {
		t.Fatal("work remained after graceful shutdown")
	}
	if w.published.Load() < 10 || w.rpcs.Load() < 10 {
		t.Fatal("insufficient completed traffic")
	}
	t.Logf("elapsed=%s publishers=4 callers=4 confirmed=%d rpc_successes=%d errors=%d pending_deliveries=%d", time.Since(w.started).Round(time.Millisecond), w.published.Load(), w.rpcs.Load(), w.failures.Load(), w.ledger.count())
}

func wait(t *testing.T, ctx context.Context, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
		}
	}
}
