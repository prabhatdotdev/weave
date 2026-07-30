package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
)

type config struct {
	mode        string
	host        string
	port        int
	queue       string
	workers     int
	concurrency int
	duration    time.Duration
	warmup      time.Duration
	payloadSize int
	timeout     time.Duration
}

type result struct {
	attempts  int
	failures  int
	latencies []time.Duration
	elapsed   time.Duration
}

func main() {
	log.SetFlags(0)

	cfg := config{}
	flag.StringVar(&cfg.mode, "mode", "client", "server or client")
	flag.StringVar(&cfg.host, "host", "localhost", "RabbitMQ host")
	flag.IntVar(&cfg.port, "port", 5672, "RabbitMQ port")
	flag.StringVar(&cfg.queue, "queue", "weave.load-test.rpc", "RPC queue")
	flag.IntVar(&cfg.workers, "workers", 256, "server worker count")
	flag.IntVar(&cfg.concurrency, "concurrency", 1, "client concurrency")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "measured duration")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Second, "warm-up duration")
	flag.IntVar(&cfg.payloadSize, "payload-bytes", 1024, "echo payload size")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		log.Fatal(err)
	}

	var err error
	switch cfg.mode {
	case "server":
		err = runServer(cfg)
	case "client":
		err = runClient(cfg)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func (c config) validate() error {
	if c.mode != "server" && c.mode != "client" {
		return fmt.Errorf("mode must be server or client")
	}
	if c.host == "" || c.queue == "" {
		return fmt.Errorf("host and queue must not be empty")
	}
	if c.port < 1 || c.port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if c.workers < 1 || c.concurrency < 1 {
		return fmt.Errorf("workers and concurrency must be positive")
	}
	if c.duration <= 0 || c.warmup < 0 || c.timeout <= 0 {
		return fmt.Errorf("duration and timeout must be positive; warmup must not be negative")
	}
	if c.payloadSize < 1 {
		return fmt.Errorf("payload-bytes must be positive")
	}
	return nil
}

func amqpConfig(cfg config) *core.Config {
	brokerConfig := core.DefaultConfig()
	brokerConfig.AMQP = core.DefaultAMQPConfig()
	brokerConfig.AMQP.Host = cfg.host
	brokerConfig.AMQP.Port = cfg.port
	return brokerConfig
}

func runServer(cfg config) error {
	brokerConfig := amqpConfig(cfg)
	brokerConfig.AMQP.QueueAutoDelete = true
	brokerConfig.AMQP.QueueExclusive = true

	broker, err := amqp.NewBroker(brokerConfig)
	if err != nil {
		return err
	}
	defer broker.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := broker.Connect(ctx); err != nil {
		return err
	}

	if err := broker.Subscribe(ctx, cfg.queue, func(ctx context.Context, request *core.Message) error {
		if request.ReplyTo == "" {
			return core.ErrNoReplyTo
		}
		response := core.NewMessage(request.Body)
		response.CorrelationID = request.CorrelationID
		return broker.Publish(ctx, request.ReplyTo, response)
	}, core.WithWorkerCount(cfg.workers)); err != nil {
		return err
	}
	fmt.Printf("ready queue=%s workers=%d\n", cfg.queue, cfg.workers)
	<-ctx.Done()
	return nil
}

func runClient(cfg config) error {
	client, err := amqp.NewBroker(amqpConfig(cfg))
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		return err
	}

	payload := bytes.Repeat([]byte("x"), cfg.payloadSize)
	if err := call(ctx, client, cfg.queue, payload, cfg.timeout); err != nil {
		return fmt.Errorf("prime request failed: %w", err)
	}

	if cfg.warmup > 0 {
		warmup := runStage(ctx, client, cfg.queue, payload, cfg.concurrency, cfg.warmup, cfg.timeout)
		if warmup.failures > 0 {
			return fmt.Errorf("warm-up failed: %d of %d requests", warmup.failures, warmup.attempts)
		}
	}

	measured := runStage(ctx, client, cfg.queue, payload, cfg.concurrency, cfg.duration, cfg.timeout)
	slices.Sort(measured.latencies)
	successes := len(measured.latencies)
	rps := float64(successes) / measured.elapsed.Seconds()

	fmt.Printf(
		"concurrency=%d attempts=%d successes=%d failures=%d rps=%.2f p95=%s p99=%s elapsed=%s\n",
		cfg.concurrency,
		measured.attempts,
		successes,
		measured.failures,
		rps,
		percentile(measured.latencies, 0.95),
		percentile(measured.latencies, 0.99),
		measured.elapsed.Round(time.Millisecond),
	)
	return nil
}

func runStage(
	ctx context.Context,
	client core.Caller,
	queue string,
	payload []byte,
	concurrency int,
	duration time.Duration,
	timeout time.Duration,
) result {
	stopAt := time.Now().Add(duration)
	workers := make([]result, concurrency)
	started := time.Now()

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func(worker *result) {
			defer wg.Done()
			for time.Now().Before(stopAt) {
				start := time.Now()
				err := call(ctx, client, queue, payload, timeout)
				worker.attempts++
				if err != nil {
					worker.failures++
					continue
				}
				worker.latencies = append(worker.latencies, time.Since(start))
			}
		}(&workers[i])
	}
	wg.Wait()

	combined := result{elapsed: time.Since(started)}
	for _, worker := range workers {
		combined.attempts += worker.attempts
		combined.failures += worker.failures
		combined.latencies = append(combined.latencies, worker.latencies...)
	}
	return combined
}

func call(ctx context.Context, client core.Caller, queue string, payload []byte, timeout time.Duration) error {
	response, err := client.Call(ctx, queue, core.NewMessage(payload), core.WithTimeout(timeout))
	if err != nil {
		return err
	}
	if !bytes.Equal(response.Body, payload) {
		return fmt.Errorf("response payload mismatch")
	}
	return nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	return sorted[int(math.Ceil(p*float64(len(sorted))))-1]
}
