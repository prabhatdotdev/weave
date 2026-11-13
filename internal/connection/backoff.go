package connection

import (
	"time"

	"github.com/prabhatdotdev/weave/core"
)

// Backoff applies the configured strategy, retaining each loop's legacy fallback.
func Backoff(config *core.Config, attempt int, fallback time.Duration) time.Duration {
	if config.ConnectionBackoff != nil {
		delay := config.ConnectionBackoff(attempt)
		if delay < 0 {
			return 0
		}
		return delay
	}
	if config.RetryDelay > 0 {
		return config.RetryDelay
	}
	return fallback
}
