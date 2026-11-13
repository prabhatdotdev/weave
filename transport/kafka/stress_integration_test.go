package kafka

import (
	"testing"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokerstress"
)

func newStressKafkaFixture(t *testing.T) brokerstress.Fixture {
	t.Helper()
	brokerstress.Enabled(t)
	broker, topics, _ := newLiveKafkaBrokerWithConfig(t, "stress", 2, 1, func(config *core.Config) { config.MaxPendingCalls = 16 })
	return brokerstress.Fixture{
		Broker: broker, Events: topics[0], Requests: topics[1],
		Idle: func() bool {
			broker.pendingMu.RLock()
			pending := len(broker.pending)
			broker.pendingMu.RUnlock()
			var snapshot core.Event
			broker.metrics.Snapshot(&snapshot)
			return pending == 0 && snapshot.PendingRPCs == 0 && snapshot.ActiveHandlers == 0
		},
	}
}

func TestSustainedLoadWithLiveKafka(t *testing.T) {
	brokerstress.Sustained(t, newStressKafkaFixture(t))
}

func TestRepeatedBrokerFailuresWithLiveKafka(t *testing.T) {
	brokerstress.RepeatedFailures(t, newStressKafkaFixture(t), "kafka")
}
