// Package lifecycle coordinates admission and deadline-bounded broker shutdown.
package lifecycle

import (
	"context"
	"errors"
	"sync"

	"github.com/prabhatdotdev/weave/core"
)

// Group tracks admitted work. Its zero value is ready to use.
type Group struct {
	mu           sync.Mutex
	active       int
	stopping     bool
	closed       bool
	stop         chan struct{}
	idle         chan struct{}
	forced       context.Context
	force        context.CancelFunc
	forceErr     error
	shutdownDone chan struct{}
	shutdownErr  error
}

type leaseKey struct{ owner *Group }
type lease struct {
	active bool // protected by its group's mutex
}

func (g *Group) initLocked() {
	if g.stop == nil {
		g.stop = make(chan struct{})
		g.idle = make(chan struct{})
		close(g.idle)
		g.forced, g.force = context.WithCancel(context.Background())
	}
}

// Begin admits fresh work, rejecting it once shutdown starts.
func (g *Group) Begin(ctx context.Context) (context.Context, func(), error) {
	return g.begin(ctx, false)
}

// Continue also admits work descended from an active lease in this group,
// allowing admitted calls and handlers to publish their remaining messages.
func (g *Group) Continue(ctx context.Context) (context.Context, func(), error) {
	return g.begin(ctx, true)
}

func (g *Group) begin(ctx context.Context, nested bool) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	if g.closed {
		return nil, nil, core.ErrClosed
	}
	parent, _ := ctx.Value(leaseKey{g}).(*lease)
	if g.stopping && (!nested || parent == nil || !parent.active) {
		return nil, nil, core.ErrShuttingDown
	}
	if g.active == 0 {
		g.idle = make(chan struct{})
	}
	g.active++
	work := &lease{active: true}
	var once sync.Once
	return context.WithValue(ctx, leaseKey{g}, work), func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			work.active = false
			g.active--
			if g.active == 0 {
				close(g.idle)
			}
		})
	}, nil
}

// Handler admits a handler and cancels its context on forced closure.
func (g *Group) Handler(ctx context.Context) (context.Context, func(), error) {
	ctx, finish, err := g.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.forced, cancel)
	if g.forced.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel(); finish() }, nil
}

// Stopping is closed as soon as draining or immediate closure begins.
func (g *Group) Stopping() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	return g.stop
}

func (g *Group) IsStopping() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopping
}

func (g *Group) IsClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

func (g *Group) abort(err error) {
	g.mu.Lock()
	g.initLocked()
	if !g.stopping {
		g.stopping = true
		close(g.stop)
	}
	g.closed = true
	if g.forceErr == nil {
		g.forceErr = err
	}
	force := g.force
	g.mu.Unlock()
	force()
}

// Close stops admission and interrupts handlers without waiting for them.
func (g *Group) Close() { g.abort(core.ErrClosed) }

func (g *Group) wait() error {
	g.mu.Lock()
	if g.active == 0 {
		g.mu.Unlock()
		return nil
	}
	idle, forced := g.idle, g.forced
	g.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-forced.Done():
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.forceErr
	}
}

// Shutdown coordinates one drain and close. Each caller has its own deadline;
// an expired deadline forces closure without waiting for blocking client code.
func (g *Group) Shutdown(ctx context.Context, flush func(context.Context) error, closeBroker func() error) error {
	if ctx == nil {
		return core.ErrInvalidConfig
	}
	g.mu.Lock()
	g.initLocked()
	if !g.stopping {
		g.stopping = true
		close(g.stop)
	}
	if g.shutdownDone == nil {
		g.shutdownDone = make(chan struct{})
		go func() {
			err := g.wait()
			if err == nil && flush != nil {
				err = flush(g.forced)
			}
			if err != nil {
				g.mu.Lock()
				if g.forceErr != nil {
					err = g.forceErr
				}
				g.mu.Unlock()
			}
			g.Close()
			err = errors.Join(err, closeBroker())
			g.mu.Lock()
			g.shutdownErr = err
			close(g.shutdownDone)
			g.mu.Unlock()
		}()
	}
	done := g.shutdownDone
	g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		g.abort(err)
		return err
	}
	select {
	case <-done:
		return g.shutdownErr
	case <-ctx.Done():
		g.abort(ctx.Err())
		return ctx.Err()
	}
}
