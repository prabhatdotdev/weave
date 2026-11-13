package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/readiness"
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
	type routeKey struct {
		consumerKey
		topic string
	}
	active := make(map[routeKey]subscriptionRegistration)
	for _, sub := range b.subs {
		if sub.ctx.Err() == nil {
			active[routeKey{sub.consumerKey, sub.destination}] = sub
		}
	}
	for _, sub := range active {
		if sub.tracked && !sub.readiness.Ready() {
			return false, nil
		}
	}
	return true, nil
}

// IsReady includes group setup for all active business subscriptions, even members with no claims.
func (b *Broker) IsReady() bool { ready, _ := b.consumerReady(); return ready }

// WaitReady waits through connection recovery and consumer group setup.
func (b *Broker) WaitReady(ctx context.Context) error {
	return b.ready.Wait(ctx, b.work.Stopping(), b.consumerReady)
}

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

type readySession struct {
	member      string
	generation  int32
	claims      map[string][]int32
	assignments []*readiness.Assignment
	cancel      context.CancelFunc
	stop        func() bool
}

func (s *readySession) release() {
	if s == nil {
		return
	}
	s.stop()
	s.cancel()
	for _, assignment := range s.assignments {
		assignment.Release()
	}
}

type consumerReadiness struct {
	broker      *Broker
	consumer    sarama.ConsumerGroup
	key         consumerKey
	routes      map[string]subscriptionRoute
	ctx         context.Context
	mu          sync.Mutex
	session     *readySession
	rebalancing bool
}

func copyClaims(claims map[string][]int32) map[string][]int32 {
	result := make(map[string][]int32, len(claims))
	for topic, partitions := range claims {
		result[topic] = append([]int32{}, partitions...)
	}
	return result
}

func (o *consumerReadiness) emitRebalance(name string, session *readySession) {
	fields := map[string]any{"consumer_group": o.key.group}
	if session != nil {
		fields["member_id"], fields["generation_id"] = session.member, session.generation
		fields["assignments"] = copyClaims(session.claims)
	}
	o.broker.emitEvent(o.ctx, core.Event{Level: core.EventLevelInfo, Name: name, Operation: "rebalance", Destination: "*", Fields: fields})
}

func (o *consumerReadiness) current() bool {
	b := o.broker
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return o.ctx.Err() == nil && b.connected && !b.closed && !b.work.IsStopping() && b.consumerForLocked(o.key) == o.consumer
}

func (o *consumerReadiness) beginRebalance(session *readySession) {
	if !o.current() {
		return
	}
	o.mu.Lock()
	begin := !o.rebalancing
	o.rebalancing = true
	o.mu.Unlock()
	if begin {
		o.emitRebalance(core.EventRebalanceStarted, session)
	}
}

func (o *consumerReadiness) setup(session sarama.ConsumerGroupSession) error {
	b := o.broker
	readyCtx, cancel := context.WithCancel(session.Context())
	stop := context.AfterFunc(o.ctx, cancel)
	record := &readySession{member: session.MemberID(), generation: session.GenerationID(), claims: copyClaims(session.Claims()), cancel: cancel, stop: stop}
	var previous []func()
	var assigned []func()
	b.closeMu.RLock()
	if o.ctx.Err() != nil || readyCtx.Err() != nil || !b.connected || b.work.IsStopping() || b.consumerForLocked(o.key) != o.consumer {
		b.closeMu.RUnlock()
		record.release()
		return nil
	}
	for topic, route := range o.routes {
		if route.readiness == nil || route.ctx.Err() != nil {
			continue
		}
		partitions := append([]int32{}, record.claims[topic]...)
		emit := func(name string, generation uint64) {
			b.ready.Notify()
			b.emitEvent(route.ctx, core.Event{Level: core.EventLevelInfo, Name: name, Operation: "consume", Destination: topic, Fields: map[string]any{
				"consumer_group": o.key.group, "member_id": record.member, "generation_id": record.generation,
				"consumer_generation": generation, "partitions": append([]int32{}, partitions...),
			}})
		}
		assignment, old := route.readiness.Assign(readyCtx, func(generation uint64) { emit(core.EventConsumerRevoked, generation) })
		previous = append(previous, old)
		if assignment != nil {
			record.assignments = append(record.assignments, assignment)
			assigned = append(assigned, func() { emit(core.EventConsumerAssigned, assignment.Generation) })
		}
	}
	b.closeMu.RUnlock()
	o.mu.Lock()
	oldSession := o.session
	o.session, o.rebalancing = record, false
	o.mu.Unlock()
	oldSession.release()
	for _, notify := range previous {
		notify()
	}
	b.ready.Notify()
	for _, notify := range assigned {
		notify()
	}
	o.emitRebalance(core.EventRebalanceCompleted, record)
	return nil
}

func (o *consumerReadiness) cleanup(session sarama.ConsumerGroupSession) error {
	o.mu.Lock()
	record := o.session
	if record == nil || record.member != session.MemberID() || record.generation != session.GenerationID() {
		o.mu.Unlock()
		return nil
	}
	o.session = nil
	o.mu.Unlock()
	record.release()
	o.beginRebalance(record)
	return nil
}

func (o *consumerReadiness) stop() {
	o.mu.Lock()
	record := o.session
	o.session = nil
	o.mu.Unlock()
	record.release()
}
