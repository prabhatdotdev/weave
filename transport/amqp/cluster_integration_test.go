package amqp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokerstress"
	amqplib "github.com/rabbitmq/amqp091-go"
)

type quorumState struct {
	Type, Leader    string
	Members, Online []string
}

func clusterQuorumState(ctx context.Context, queue string) (quorumState, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:25682/api/queues/%2F/"+url.PathEscape(queue), nil)
	if err != nil {
		return quorumState{}, err
	}
	request.SetBasicAuth("guest", "guest")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return quorumState{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return quorumState{}, fmt.Errorf("queue status: HTTP %d", response.StatusCode)
	}
	var state quorumState
	err = json.NewDecoder(response.Body).Decode(&state)
	return state, err
}

func testReplicatedRabbitMQ(t *testing.T, partition bool) {
	t.Helper()
	brokerstress.ClusterEnabled(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.Host, config.AMQP.Port = "127.0.0.1", 25672
	config.AMQP.QueueDurable, config.AMQP.Heartbeat = true, time.Second
	config.MaxPendingCalls = 16
	config.ConnectionBackoff = core.JitteredBackoff(core.ExponentialBackoff(50*time.Millisecond, time.Second, 2))
	prefix := "weave-cluster-" + rand.Text()
	queues := []string{prefix + ".events", prefix + ".requests", prefix + ".minority"}
	// Declare through node 1 so the test removes the actual quorum leader, not a follower.
	leaderConfig := *config
	leaderAMQP := *config.AMQP
	leaderAMQP.Port = 25671
	leaderConfig.AMQP = &leaderAMQP
	admin := stressAMQPAdmin(t, &leaderConfig)
	for _, queue := range queues {
		if _, err := admin.QueueDeclare(queue, true, false, false, false, amqplib.Table{"x-queue-type": "quorum", "x-quorum-initial-group-size": int32(3)}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		admin := stressAMQPAdmin(t, config)
		for _, queue := range queues {
			if _, err := admin.QueueDelete(queue, false, false, false); err != nil {
				t.Error(err)
			}
		}
	})
	checkQueues := func(full bool) bool {
		for _, queue := range queues {
			state, err := clusterQuorumState(ctx, queue)
			if err != nil || state.Type != "quorum" || len(state.Members) != 3 {
				return false
			}
			if full {
				if len(state.Online) != 3 {
					return false
				}
			} else if state.Leader == "" || state.Leader == "rabbit@rabbit1-peer" || len(state.Online) != 2 || slices.Contains(state.Online, "rabbit@rabbit1-peer") {
				return false
			}
		}
		return true
	}
	brokerstress.Eventually(t, ctx, "three online replicas for every quorum queue", func() bool { return checkQueues(true) })
	state, err := clusterQuorumState(ctx, queues[0])
	if err != nil || state.Leader != "rabbit@rabbit1-peer" {
		t.Fatalf("expected node 1 to lead the event queue: %+v, %v", state, err)
	}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	node := brokerstress.NewClusterNode(t, "rabbitmq", 1)
	fixture := brokerstress.Fixture{Broker: broker, Events: queues[0], Requests: queues[1], Idle: func() bool {
		broker.pendingMu.RLock()
		pending := len(broker.pending)
		broker.pendingMu.RUnlock()
		var event core.Event
		broker.metrics.Snapshot(&event)
		return pending == 0 && event.PendingRPCs == 0 && event.ActiveHandlers == 0
	}}
	brokerstress.ReplicatedFailure(t, fixture, func(ctx context.Context) {
		var err error
		if partition {
			err = node.Partition(ctx)
		} else {
			err = node.Kill(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		brokerstress.Eventually(t, ctx, "quorum leaders on the surviving majority", func() bool { return checkQueues(false) })
		if partition {
			connection, err := net.DialTimeout("tcp", "127.0.0.1:25671", time.Second)
			if err != nil {
				t.Fatalf("partition also removed the client network: %v", err)
			}
			_ = connection.Close()
			minority, err := NewBroker(&leaderConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer minority.Close()
			attemptCtx, stop := context.WithTimeout(ctx, 3*time.Second)
			defer stop()
			if err := minority.Publish(attemptCtx, queues[2], core.NewTextMessage("uncertain minority write"), core.WithPersistent()); err == nil {
				t.Fatal("isolated minority confirmed a quorum write")
			}
		}
	}, func(ctx context.Context) {
		if err := node.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		brokerstress.Eventually(t, ctx, "all quorum replicas rejoining after healing", func() bool { return checkQueues(true) })
	})
}

func TestReplicatedFailoverWithClusterRabbitMQ(t *testing.T) { testReplicatedRabbitMQ(t, false) }
func TestNetworkPartitionWithClusterRabbitMQ(t *testing.T)   { testReplicatedRabbitMQ(t, true) }
