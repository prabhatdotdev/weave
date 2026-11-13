package testkit

import (
	"context"

	"github.com/prabhatdotdev/weave/core"
)

func (m *MockBroker) consumerReady() (bool, error) {
	if m.work.IsClosed() {
		return false, core.ErrClosed
	}
	if m.work.IsStopping() {
		return false, core.ErrShuttingDown
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connected {
		return false, &core.ErrNotConnected{Backend: "mock"}
	}
	for _, sub := range m.subscriptions {
		if sub.ctx.Err() == nil && !sub.readiness.Ready() {
			return false, nil
		}
	}
	return true, nil
}

// IsReady reports connectivity and setup of current mock subscriptions.
func (m *MockBroker) IsReady() bool { ready, _ := m.consumerReady(); return ready }

// WaitReady observes the same context and lifecycle contract as live transports.
func (m *MockBroker) WaitReady(ctx context.Context) error {
	return m.ready.Wait(ctx, m.work.Stopping(), m.consumerReady)
}

// SetEventHook installs an event collector. Hooks run outside mock locks.
func (m *MockBroker) SetEventHook(hook core.EventHook) {
	m.mu.Lock()
	m.eventHook = hook
	m.mu.Unlock()
}

func (m *MockBroker) emitEvent(ctx context.Context, event core.Event) {
	m.mu.Lock()
	hook := m.eventHook
	m.mu.Unlock()
	event.Backend, event.Component, event.Level = "mock", "transport", core.EventLevelInfo
	(&core.Config{EventHook: hook}).EmitEvent(ctx, event)
}

func (m *MockBroker) invalidateConsumersLocked() func() {
	var revoked []func()
	for _, sub := range m.subscriptions {
		revoked = append(revoked, sub.readiness.Invalidate())
	}
	return func() {
		m.ready.Notify()
		for _, notify := range revoked {
			notify()
		}
	}
}

var _ core.ConsumerReadiness = (*MockBroker)(nil)
