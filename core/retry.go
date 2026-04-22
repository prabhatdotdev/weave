package core

import (
	"context"
	"time"
)

type retryAttemptKey struct{}

// BackoffStrategy returns the delay to wait before the next retry attempt.
// The attempt argument is one-based and represents the retry count after the
// initial attempt has already failed.
type BackoffStrategy func(attempt int) time.Duration

// RetryPredicate decides whether another retry should be attempted.
type RetryPredicate func(error) bool

// RetryHook observes each scheduled retry.
type RetryHook func(ctx context.Context, attempt int, err error, delay time.Duration)

// RetryPolicy controls transport-agnostic in-process retry behavior.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts including the initial call.
	MaxAttempts int
	// Backoff calculates the delay before each retry attempt.
	Backoff BackoffStrategy
	// Retryable decides whether an error is retryable.
	Retryable RetryPredicate
	// OnRetry is called whenever another retry attempt is scheduled.
	OnRetry RetryHook
}

// DefaultRetryPolicy returns a conservative retry policy suitable for transient
// handler failures.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Backoff:     ExponentialBackoff(100*time.Millisecond, 2*time.Second, 2),
	}
}

// FixedBackoff returns a backoff strategy that always returns the same delay.
func FixedBackoff(delay time.Duration) BackoffStrategy {
	return func(int) time.Duration {
		if delay < 0 {
			return 0
		}
		return delay
	}
}

// ExponentialBackoff returns an exponential backoff strategy capped at maxDelay.
func ExponentialBackoff(initialDelay, maxDelay time.Duration, multiplier float64) BackoffStrategy {
	if initialDelay <= 0 {
		initialDelay = 100 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = initialDelay
	}
	if multiplier < 1 {
		multiplier = 2
	}

	return func(attempt int) time.Duration {
		if attempt <= 1 {
			if initialDelay > maxDelay {
				return maxDelay
			}
			return initialDelay
		}

		delay := float64(initialDelay)
		for i := 1; i < attempt; i++ {
			delay *= multiplier
			if time.Duration(delay) >= maxDelay {
				return maxDelay
			}
		}
		if time.Duration(delay) > maxDelay {
			return maxDelay
		}
		return time.Duration(delay)
	}
}

// RetryAttempt returns the current attempt number from context. The first
// attempt is 1. A missing value also reports 1.
func RetryAttempt(ctx context.Context) int {
	if ctx == nil {
		return 1
	}
	if attempt, ok := ctx.Value(retryAttemptKey{}).(int); ok && attempt > 0 {
		return attempt
	}
	return 1
}

// RetryHandler wraps a handler with bounded in-process retries. Use it when you
// want consistent application-level retry semantics across transports.
func RetryHandler(handler Handler, policy RetryPolicy) Handler {
	normalized := normalizeRetryPolicy(policy)

	return func(ctx context.Context, msg *Message) error {
		var lastErr error
		for attempt := 1; attempt <= normalized.MaxAttempts; attempt++ {
			attemptCtx := context.WithValue(ctx, retryAttemptKey{}, attempt)
			lastErr = handler(attemptCtx, msg)
			if lastErr == nil {
				return nil
			}
			if attempt == normalized.MaxAttempts || (normalized.Retryable != nil && !normalized.Retryable(lastErr)) {
				return lastErr
			}

			delay := time.Duration(0)
			if normalized.Backoff != nil {
				delay = normalized.Backoff(attempt)
			}
			if normalized.OnRetry != nil {
				normalized.OnRetry(attemptCtx, attempt+1, lastErr, delay)
			}
			if err := sleepWithContext(attemptCtx, delay); err != nil {
				return lastErr
			}
		}

		return lastErr
	}
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	return policy
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
