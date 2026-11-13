package amqp

import (
	"context"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func benchmarkConfirmedPublish(b *testing.B, payloadBytes, concurrency, connections int) {
	b.Helper()
	b.StopTimer()
	config := stressAMQPConfig(b)
	admin := stressAMQPAdmin(b, config)
	queue := "weave-benchmark-" + rand.Text()
	if _, err := admin.QueueDeclare(queue, true, false, false, false, amqplib.Table{"x-queue-type": "classic"}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _, _ = admin.QueueDelete(queue, false, false, false) })
	brokers := make([]core.MessageBroker, connections)
	for i := range brokers {
		broker, err := NewBroker(config)
		if err != nil {
			b.Fatal(err)
		}
		brokers[i] = broker
		b.Cleanup(func() { _ = broker.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err = broker.Connect(ctx)
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}
	msg := core.NewMessage(make([]byte, payloadBytes))
	opts := []core.PublishOption{core.WithPersistent(), core.WithMandatory(), core.WithTimeout(5 * time.Second)}
	var next atomic.Int64
	var workers sync.WaitGroup
	start := make(chan struct{})
	errors := make(chan error, concurrency)
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for next.Add(1) <= int64(b.N) {
				if err := brokers[worker%connections].Publish(context.Background(), queue, msg, opts...); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	b.SetBytes(int64(payloadBytes))
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	workers.Wait()
	b.StopTimer()
	close(errors)
	for err := range errors {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
	state, err := admin.QueueInspect(queue)
	if err != nil || state.Messages != b.N {
		b.Fatalf("confirmed publishes=%d, queued=%d, inspect error=%v", b.N, state.Messages, err)
	}
}
