package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

// CallRetryHook observes each scheduled RPC retry.
type CallRetryHook func(ctx context.Context, attempt int, err error, delay time.Duration)

// CallPolicy controls retry and circuit-breaking behavior for RPC-style calls.
type CallPolicy struct {
	// MaxAttempts is the total number of attempts including the initial call.
	MaxAttempts int
	// Backoff calculates the delay before each retry attempt.
	Backoff core.BackoffStrategy
	// Retryable decides whether a failed attempt should be retried.
	Retryable core.RetryPredicate
	// OnRetry is called whenever another retry attempt is scheduled.
	OnRetry CallRetryHook
	// CircuitBreaker blocks calls while the circuit is open.
	CircuitBreaker *CircuitBreaker
}

// DefaultCallPolicy returns a conservative retry policy for transient RPC failures.
func DefaultCallPolicy() CallPolicy {
	return CallPolicy{
		MaxAttempts: 3,
		Backoff:     core.ExponentialBackoff(100*time.Millisecond, 2*time.Second, 2),
		Retryable:   defaultCallRetryable,
	}
}

// CircuitState describes the current state of a circuit breaker.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

// CircuitBreakerOptions configures a circuit breaker.
type CircuitBreakerOptions struct {
	FailureThreshold int
	ResetAfter       time.Duration
}

// CircuitBreaker implements a simple consecutive-failure circuit breaker.
type CircuitBreaker struct {
	mu sync.Mutex

	options       CircuitBreakerOptions
	state         CircuitState
	failures      int
	openedAt      time.Time
	probeInFlight bool
	now           func() time.Time
}

// NewCircuitBreaker creates a simple circuit breaker.
func NewCircuitBreaker(options CircuitBreakerOptions) *CircuitBreaker {
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 5
	}
	if options.ResetAfter <= 0 {
		options.ResetAfter = 30 * time.Second
	}

	return &CircuitBreaker{
		options: options,
		state:   CircuitClosed,
		now:     time.Now,
	}
}

// State returns the current circuit breaker state.
func (b *CircuitBreaker) State() CircuitState {
	if b == nil {
		return CircuitClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether the next call may proceed.
func (b *CircuitBreaker) Allow(operation string) error {
	if b == nil {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	switch b.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		retryAfter := b.options.ResetAfter - now.Sub(b.openedAt)
		if retryAfter > 0 {
			return &core.ErrCircuitOpen{Operation: operation, RetryAfter: retryAfter.String()}
		}
		b.state = CircuitHalfOpen
		b.probeInFlight = true
		return nil
	case CircuitHalfOpen:
		if b.probeInFlight {
			return &core.ErrCircuitOpen{Operation: operation}
		}
		b.probeInFlight = true
		return nil
	default:
		return nil
	}
}

// MarkSuccess closes the circuit and clears accumulated failures.
func (b *CircuitBreaker) MarkSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = CircuitClosed
	b.openedAt = time.Time{}
	b.probeInFlight = false
}

// MarkFailure records a failed call and opens the circuit if needed.
func (b *CircuitBreaker) MarkFailure() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures++
	if b.state == CircuitHalfOpen || b.failures >= b.options.FailureThreshold {
		b.state = CircuitOpen
		b.openedAt = b.now()
	}
	b.probeInFlight = false
}

// CallWithPolicy performs a policy-controlled RPC call using any core.Caller.
func CallWithPolicy(ctx context.Context, caller core.Caller, destination string, msg *core.Message, policy CallPolicy, opts ...core.PublishOption) (*core.Message, error) {
	normalized := normalizeCallPolicy(policy)
	var lastErr error

	for attempt := 1; attempt <= normalized.MaxAttempts; attempt++ {
		if normalized.CircuitBreaker != nil {
			if err := normalized.CircuitBreaker.Allow("Call"); err != nil {
				return nil, err
			}
		}

		request := cloneRequest(msg)
		response, err := caller.Call(ctx, destination, request, opts...)
		if err == nil {
			if normalized.CircuitBreaker != nil {
				normalized.CircuitBreaker.MarkSuccess()
			}
			return response, nil
		}

		lastErr = err
		if normalized.CircuitBreaker != nil {
			normalized.CircuitBreaker.MarkFailure()
		}
		if attempt == normalized.MaxAttempts || !normalized.Retryable(err) {
			return nil, err
		}

		delay := time.Duration(0)
		if normalized.Backoff != nil {
			delay = normalized.Backoff(attempt)
		}
		if normalized.OnRetry != nil {
			normalized.OnRetry(ctx, attempt+1, err, delay)
		}
		if sleepErr := sleepWithContext(ctx, delay); sleepErr != nil {
			return nil, lastErr
		}
	}

	return nil, lastErr
}

func normalizeCallPolicy(policy CallPolicy) CallPolicy {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Retryable == nil {
		policy.Retryable = defaultCallRetryable
	}
	return policy
}

func defaultCallRetryable(err error) bool {
	if err == nil {
		return false
	}
	if core.IsTimeout(err) || core.IsConnectionLost(err) || core.IsNotConnected(err) {
		return true
	}
	var publishErr *core.ErrPublishFailed
	return errors.As(err, &publishErr)
}

func cloneRequest(msg *core.Message) *core.Message {
	if msg == nil {
		return nil
	}

	cloned := msg.Clone()
	if msg.CorrelationID == "" {
		cloned.CorrelationID = ""
	}
	return cloned
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
