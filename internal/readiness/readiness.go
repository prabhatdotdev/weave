// Package readiness coordinates consumer readiness notifications and assignment lifetimes.
package readiness

import (
	"context"
	"sync"

	"github.com/prabhatdotdev/weave/internal/validation"
)

// Signal wakes waiters after a broker or consumer changes state. Its zero value is usable.
type Signal struct {
	mu      sync.Mutex
	changed chan struct{}
}

func (s *Signal) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
}

func (s *Signal) Wait(ctx context.Context, stopping <-chan struct{}, check func() (bool, error)) error {
	if err := validation.Context(ctx); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.changed == nil {
			s.changed = make(chan struct{})
		}
		changed := s.changed
		s.mu.Unlock()
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-stopping:
			_, err := check()
			return err
		}
	}
}

// State belongs to one registered subscription and survives reconnections.
type State struct {
	mu         sync.Mutex
	generation uint64
	current    *Assignment
}

// Assignment belongs to one native consumer/session. Old cleanup cannot revoke its replacement.
type Assignment struct {
	state      *State
	ctx        context.Context
	Generation uint64
	onRevoked  func(uint64)
	stop       func() bool
	once       sync.Once
}

func (s *State) Ready() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current != nil && s.current.ctx.Err() == nil
}

// Assign returns a callback for the previous assignment. Invoke it outside broker locks.
func (s *State) Assign(ctx context.Context, revoked func(uint64)) (*Assignment, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return nil, func() {}
	}
	previous := s.current
	s.generation++
	a := &Assignment{state: s, ctx: ctx, Generation: s.generation, onRevoked: revoked}
	s.current = a
	a.stop = context.AfterFunc(ctx, a.Release)
	return a, func() {
		if previous != nil {
			previous.revoked()
		}
	}
}

// Invalidate updates readiness immediately and defers callbacks until locks are released.
func (s *State) Invalidate() func() {
	if s == nil {
		return func() {}
	}
	s.mu.Lock()
	previous := s.current
	s.current = nil
	s.mu.Unlock()
	return func() {
		if previous != nil {
			previous.revoked()
		}
	}
}

func (a *Assignment) Release() {
	if a == nil {
		return
	}
	a.state.mu.Lock()
	current := a.state.current == a
	if current {
		a.state.current = nil
	}
	a.state.mu.Unlock()
	if current {
		a.revoked()
	}
}

func (a *Assignment) revoked() {
	a.once.Do(func() {
		a.stop()
		if a.onRevoked != nil {
			a.onRevoked(a.Generation)
		}
	})
}
