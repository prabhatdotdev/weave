package kafka

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokerstress"
)

var clusterKafkaAddresses = []string{"127.0.0.1:29091", "127.0.0.1:29092", "127.0.0.1:29093"}

// Observe a surviving broker directly; admin controller caches can retain the killed node.
func clusterKafkaMetadata(topics []string, config *sarama.Config) ([]*sarama.TopicMetadata, error) {
	broker := sarama.NewBroker(clusterKafkaAddresses[1])
	if err := broker.Open(config); err != nil {
		return nil, err
	}
	defer broker.Close()
	response, err := broker.GetMetadata(sarama.NewMetadataRequest(config.Version, topics))
	if err != nil {
		return nil, err
	}
	return response.Topics, nil
}

// Bypass metadata routing so the minority probe cannot silently move to a majority broker.
func directClusterProduce(address, topic string) error {
	config := sarama.NewConfig()
	config.Net.DialTimeout, config.Net.ReadTimeout, config.Net.WriteTimeout = time.Second, 2*time.Second, 2*time.Second
	broker := sarama.NewBroker(address)
	if err := broker.Open(config); err != nil {
		return err
	}
	defer broker.Close()
	request := &sarama.ProduceRequest{Version: 7, RequiredAcks: sarama.WaitForAll, Timeout: 1000}
	now := time.Now()
	request.AddBatch(topic, 0, &sarama.RecordBatch{
		Version: 2, PartitionLeaderEpoch: -1, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		FirstTimestamp: now, MaxTimestamp: now, Records: []*sarama.Record{{Value: []byte("minority probe")}},
	})
	response, err := broker.Produce(request)
	if err != nil {
		return err
	}
	block := response.GetBlock(topic, 0)
	if block == nil {
		return fmt.Errorf("missing probe partition response")
	}
	if block.Err != sarama.ErrNoError {
		return block.Err
	}
	return nil
}

func testReplicatedKafka(t *testing.T, partition bool) {
	t.Helper()
	brokerstress.ClusterEnabled(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V3_6_0_0
	adminConfig.Net.DialTimeout, adminConfig.Net.ReadTimeout, adminConfig.Net.WriteTimeout = 2*time.Second, 2*time.Second, 2*time.Second
	adminConfig.Admin.Timeout = 5 * time.Second
	admin, err := sarama.NewClusterAdmin(clusterKafkaAddresses, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "weave-cluster-" + rand.Text()
	topics := []string{prefix + ".events", prefix + ".requests", prefix + ".replies", prefix + ".minority"}
	t.Cleanup(func() {
		_ = admin.Close()
		// Cleanup must discover the current controller rather than reuse a pre-fault cache.
		cleanupAdmin, err := sarama.NewClusterAdmin(clusterKafkaAddresses[1:], adminConfig)
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanupAdmin.Close()
		for _, topic := range topics {
			if err := cleanupAdmin.DeleteTopic(topic); err != nil {
				t.Error(err)
			}
		}
	})
	minimum := "2"
	for _, topic := range topics {
		if err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions: -1, ReplicationFactor: -1, ReplicaAssignment: map[int32][]int32{0: {1, 2, 3}},
			ConfigEntries: map[string]*string{"min.insync.replicas": &minimum},
		}, false); err != nil {
			t.Fatal(err)
		}
	}
	checkReplicas := func(full bool) bool {
		metadata, err := clusterKafkaMetadata(topics, adminConfig)
		if err != nil || len(metadata) != len(topics) {
			return false
		}
		for _, topic := range metadata {
			if topic.Err != sarama.ErrNoError || len(topic.Partitions) != 1 {
				return false
			}
			p := topic.Partitions[0]
			if p.Err != sarama.ErrNoError || len(p.Replicas) != 3 {
				return false
			}
			if full {
				if len(p.Isr) != 3 {
					return false
				}
			} else if p.Leader < 2 || len(p.Isr) != 2 || slices.Contains(p.Isr, 1) {
				return false
			}
		}
		return true
	}
	brokerstress.Eventually(t, ctx, "three in-sync replicas for every topic", func() bool { return checkReplicas(true) })
	preferred := make(map[string][]int32, len(topics))
	for _, topic := range topics {
		preferred[topic] = []int32{0}
	}
	if _, err := admin.ElectLeaders(sarama.PreferredElection, preferred); err != nil {
		t.Fatal(err)
	}
	brokerstress.Eventually(t, ctx, "preferred leaders on node 1 before disruption", func() bool {
		metadata, err := clusterKafkaMetadata(topics, adminConfig)
		if err != nil || len(metadata) != len(topics) {
			return false
		}
		for _, topic := range metadata {
			if topic.Err != sarama.ErrNoError || len(topic.Partitions) != 1 || topic.Partitions[0].Leader != 1 {
				return false
			}
		}
		return true
	})
	if err := directClusterProduce(clusterKafkaAddresses[0], topics[3]); err != nil {
		t.Fatalf("healthy direct-produce baseline failed: %v", err)
	}
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.Brokers = slices.Clone(clusterKafkaAddresses)
	config.Kafka.ConsumerGroup, config.Kafka.ReplyTopic = prefix, topics[2]
	config.Kafka.RequiredAcks, config.Kafka.AutoOffsetReset = -1, "earliest"
	config.MaxPendingCalls = 16
	config.ConnectionBackoff = core.JitteredBackoff(core.ExponentialBackoff(50*time.Millisecond, time.Second, 2))
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	node := brokerstress.NewClusterNode(t, "kafka", 1)
	fixture := brokerstress.Fixture{Broker: broker, Events: topics[0], Requests: topics[1], Idle: func() bool {
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
		brokerstress.Eventually(t, ctx, "topic leaders and two in-sync replicas on the majority", func() bool { return checkReplicas(false) })
		if partition {
			connection, err := net.DialTimeout("tcp", clusterKafkaAddresses[0], time.Second)
			if err != nil {
				t.Fatalf("partition also removed the client network: %v", err)
			}
			_ = connection.Close()
			err = directClusterProduce(clusterKafkaAddresses[0], topics[3])
			var networkError net.Error
			if err == nil {
				t.Fatal("isolated minority confirmed an all-replica write")
			}
			if !errors.Is(err, sarama.ErrNotLeaderForPartition) && !errors.Is(err, sarama.ErrNotEnoughReplicas) && !errors.Is(err, sarama.ErrNotEnoughReplicasAfterAppend) && !errors.Is(err, sarama.ErrRequestTimedOut) && !(errors.As(err, &networkError) && networkError.Timeout()) {
				t.Fatalf("unexpected minority write failure: %v", err)
			}
		}
	}, func(ctx context.Context) {
		if err := node.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		brokerstress.Eventually(t, ctx, "all replicas returning to the ISR after healing", func() bool { return checkReplicas(true) })
	})
}

func TestReplicatedFailoverWithClusterKafka(t *testing.T) { testReplicatedKafka(t, false) }
func TestNetworkPartitionWithClusterKafka(t *testing.T)   { testReplicatedKafka(t, true) }
