package amqp

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokerstress"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func stressAMQPConfig(t testing.TB) *core.Config {
	t.Helper()
	brokerstress.Enabled(t)
	address := os.Getenv("WEAVE_STRESS_AMQP_ADDRESS")
	if address == "" {
		address = "127.0.0.1:5674"
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	config := core.DefaultConfig()
	config.MaxPendingCalls = 16
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.Host, config.AMQP.Port = host, portNumber
	config.AMQP.QueueDurable = true
	return config
}

func stressAMQPAdmin(t testing.TB, config *core.Config) *amqplib.Channel {
	t.Helper()
	address := net.JoinHostPort(config.AMQP.Host, strconv.Itoa(config.AMQP.Port))
	conn, err := amqplib.DialConfig("amqp://guest:guest@"+address+"/", amqplib.Config{Dial: func(network, address string) (net.Conn, error) {
		return net.DialTimeout(network, address, 5*time.Second)
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel
}

func newStressAMQPFixture(t *testing.T) brokerstress.Fixture {
	t.Helper()
	config := stressAMQPConfig(t)
	prefix := "weave-stress-" + rand.Text()
	fixture := brokerstress.Fixture{Events: prefix + ".events", Requests: prefix + ".requests"}
	// Reopen the admin connection after crashes; the original socket cannot clean up queues.
	t.Cleanup(func() {
		admin := stressAMQPAdmin(t, config)
		for _, queue := range []string{fixture.Events, fixture.Requests} {
			if _, err := admin.QueueDelete(queue, false, false, false); err != nil {
				t.Error(err)
			}
		}
	})
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.Broker = broker
	fixture.Idle = func() bool {
		broker.pendingMu.RLock()
		pending := len(broker.pending)
		broker.pendingMu.RUnlock()
		var snapshot core.Event
		broker.metrics.Snapshot(&snapshot)
		return pending == 0 && snapshot.PendingRPCs == 0 && snapshot.ActiveHandlers == 0
	}
	return fixture
}

func TestSustainedLoadWithLiveRabbitMQ(t *testing.T) {
	brokerstress.Sustained(t, newStressAMQPFixture(t))
}

func TestRepeatedBrokerFailuresWithLiveRabbitMQ(t *testing.T) {
	brokerstress.RepeatedFailures(t, newStressAMQPFixture(t), "rabbitmq")
}

func BenchmarkConfirmedPublishWithLiveRabbitMQ(b *testing.B) {
	brokerstress.Enabled(b)
	for _, scenario := range []struct{ bytes, concurrency, connections int }{
		{1024, 1, 1}, {1024, 8, 1}, {1024, 32, 1}, {1024, 32, 4}, {65536, 8, 1},
	} {
		b.Run(fmt.Sprintf("bytes=%d/concurrency=%d/connections=%d", scenario.bytes, scenario.concurrency, scenario.connections), func(b *testing.B) {
			benchmarkConfirmedPublish(b, scenario.bytes, scenario.concurrency, scenario.connections)
		})
	}
}
