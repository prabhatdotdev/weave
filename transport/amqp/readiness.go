package amqp

import (
	"context"

	"github.com/prabhatdotdev/weave/core"
)

func (b *Broker) consumerReady() (bool, error) {
	if b.work.IsClosed() {
		return false, core.ErrClosed
	}
	if b.work.IsStopping() {
		return false, core.ErrShuttingDown
	}
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	if !b.connected && !b.everConnected {
		return false, &core.ErrNotConnected{Backend: backendName}
	}
	if !b.connected || b.recovering {
		return false, nil
	}
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()
	for _, sub := range b.subs {
		if sub.ctx.Err() == nil && !sub.readiness.Ready() {
			return false, nil
		}
	}
	return true, nil
}

// IsReady reports connection and active business-consumer readiness.
func (b *Broker) IsReady() bool { ready, _ := b.consumerReady(); return ready }

// WaitReady waits for connection recovery and active business-consumer setup.
func (b *Broker) WaitReady(ctx context.Context) error {
	return b.ready.Wait(ctx, b.work.Stopping(), b.consumerReady)
}

// Call the returned event notifications after releasing broker locks.
func (b *Broker) invalidateConsumers() func() {
	b.subsMu.RLock()
	var revoked []func()
	for _, sub := range b.subs {
		revoked = append(revoked, sub.readiness.Invalidate())
	}
	b.subsMu.RUnlock()
	return func() {
		b.ready.Notify()
		for _, notify := range revoked {
			notify()
		}
	}
}

var _ core.ConsumerReadiness = (*Broker)(nil)
