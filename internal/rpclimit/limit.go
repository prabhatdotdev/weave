// Package rpclimit bounds calls before reply setup and correlation registration.
package rpclimit

import (
	"context"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/lifecycle"
)

// Limiter reserves RPC capacity. Its zero value leaves calls unlimited.
type Limiter struct{ slots chan struct{} }

func New(max int) Limiter {
	if max == 0 {
		return Limiter{}
	}
	return Limiter{slots: make(chan struct{}, max)}
}

// Acquire waits for capacity or cancellation. Shutdown rejects queued calls;
// admitted handlers can still take immediately available capacity while draining.
func (l *Limiter) Acquire(ctx context.Context, work *lifecycle.Group) (func(), error) {
	if l.slots == nil {
		return func() {}, nil
	}
	waited := false
	select {
	case l.slots <- struct{}{}:
	default:
		waited = true
		select {
		case l.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-work.Stopping():
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if work.IsClosed() {
				return nil, core.ErrClosed
			}
			return nil, core.ErrShuttingDown
		}
	}
	release := func() { <-l.slots }
	var err error
	switch {
	case ctx.Err() != nil:
		err = ctx.Err()
	case work.IsClosed():
		err = core.ErrClosed
	case waited && work.IsStopping():
		err = core.ErrShuttingDown
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}
