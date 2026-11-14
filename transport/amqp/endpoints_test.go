package amqp

import (
	"context"
	"crypto/tls"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestEndpointInitialFailoverSnapshotsConfiguration(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672", "third:5672"}}
	config.ConnectionRetry = 1
	config.ConnectionBackoff = func(int) time.Duration { t.Error("waited before trying all endpoints"); return 0 }
	var addresses []string
	config.EventHook = func(_ context.Context, event core.Event) {
		if event.Name == core.EventConnect && event.Err == nil && event.Fields["address"] != "third:5672" {
			t.Errorf("successful connect reported address %v", event.Fields["address"])
		}
	}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	config.AMQP.Endpoints[0] = "changed:5672"
	broker.dial = func(address string, _ amqplib.Config) (amqpConnection, error) {
		uri, err := amqplib.ParseURI(address)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, uri.Host)
		if uri.Host != "third" {
			return nil, errors.New("offline")
		}
		return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(addresses, []string{"first", "second", "third"}) {
		t.Fatalf("dialed %v", addresses)
	}
	if err := broker.Connect(context.Background()); err != nil || len(addresses) != 3 {
		t.Fatalf("healthy Connect changed endpoints: %v, %v", addresses, err)
	}
}

func TestEndpointRetryBudgetAndFailureCause(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672", "third:5672"}}
	config.ConnectionRetry = 1
	var retries []int
	config.ConnectionBackoff = func(attempt int) time.Duration { retries = append(retries, attempt); return 0 }
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	failure := errors.New("all offline")
	var addresses []string
	broker.dial = func(address string, _ amqplib.Config) (amqpConnection, error) {
		uri, _ := amqplib.ParseURI(address)
		addresses = append(addresses, uri.Host)
		return nil, failure
	}
	err := broker.Connect(context.Background())
	var connectionError *core.ErrConnectionFailed
	if !errors.As(err, &connectionError) || !errors.Is(err, failure) || connectionError.Address != strings.Join(config.AMQP.Endpoints, ", ") {
		t.Fatalf("Connect returned %v", err)
	}
	if !reflect.DeepEqual(addresses, []string{"first", "second", "third", "first", "second", "third"}) || !reflect.DeepEqual(retries, []int{1}) {
		t.Fatalf("addresses=%v backoff attempts=%v", addresses, retries)
	}
}

func TestEndpointReconnectRotatesAndRestoresSubscriptions(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672", "third:5672"}}
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	attempts := make(chan string, 8)
	broker.dial = func(address string, _ amqplib.Config) (amqpConnection, error) {
		uri, _ := amqplib.ParseURI(address)
		attempts <- uri.Host
		return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if first := <-attempts; first != "first" {
		t.Fatal(first)
	}
	if err := broker.Subscribe(ctx, "orders", func(context.Context, *core.Message) error { return nil }, core.WithQueueBind("events", "orders.*")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"second", "third", "first"} {
		pending := newPendingCall()
		broker.pendingMu.Lock()
		broker.pending["inflight"] = pending
		broker.pendingMu.Unlock()
		broker.closeMu.RLock()
		old := broker.conn.(*fakeAMQPConnection)
		broker.closeMu.RUnlock()
		waitForNotifyClose(t, old)
		old.notifyCloseCh() <- &amqplib.Error{Code: 320, Reason: "connected endpoint stopped"}
		select {
		case address := <-attempts:
			if address != want {
				t.Fatalf("dialed %s, want %s", address, want)
			}
		case <-ctx.Done():
			t.Fatal("did not rotate after connection loss")
		}
		if err := broker.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case response := <-pending.response:
			if response != nil {
				t.Fatal("old pending call received a response after connection loss")
			}
		case <-ctx.Done():
			t.Fatal("failover left an old pending call stranded")
		}
		broker.closeMu.RLock()
		channel := broker.channel.(*fakeAMQPChannel)
		oldReply := broker.replyQueue
		broker.closeMu.RUnlock()
		if oldReply != "" {
			t.Fatal("failover retained the dead endpoint's reply queue")
		}
		channel.mu.Lock()
		consumers, bindings := append([]string(nil), channel.consumers...), append([]string(nil), channel.bindings...)
		channel.mu.Unlock()
		if !reflect.DeepEqual(consumers, []string{"orders"}) || !reflect.DeepEqual(bindings, []string{"orders:orders.*:events"}) {
			t.Fatalf("failed over without restoring the subscription: consumers=%v bindings=%v", consumers, bindings)
		}
		if err := broker.ensureReplyQueue(ctx, channel); err != nil {
			t.Fatal(err)
		}
		channel.mu.Lock()
		replies := len(channel.consumers) == 2 && channel.consumers[1] != "orders"
		channel.mu.Unlock()
		if !replies {
			t.Fatal("new endpoint did not create its own reply consumer")
		}
	}
}

type endpointChannelFailure struct{ *fakeAMQPConnection }

func (*endpointChannelFailure) Channel() (amqpChannel, error) {
	return nil, errors.New("channel setup failed")
}

func TestEndpointChannelFailureClosesClientAndContinues(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672"}}
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	failed := &endpointChannelFailure{&fakeAMQPConnection{channel: newFakeAMQPChannel()}}
	var dials atomic.Int32
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		if dials.Add(1) == 1 {
			return failed, nil
		}
		return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
	}
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed.mu.RLock()
	closed := failed.closed
	failed.mu.RUnlock()
	if !closed || dials.Load() != 2 {
		t.Fatal("failed setup retained a connection or skipped failover")
	}
}

func TestEndpointTLSAndIdentityRemainIsolated(t *testing.T) {
	for _, serverName := range []string{"", "shared.example"} {
		t.Run(serverName, func(t *testing.T) {
			config := core.DefaultConfig()
			config.AMQP = &core.AMQPConfig{Endpoints: []string{"[::1]:5671", "second:5671"}, Username: "user:@/", Password: "pass:@/#", VHost: "tenant/one", TLS: &tls.Config{ServerName: serverName}}
			brokerAny, err := NewBroker(config)
			if err != nil {
				t.Fatal(err)
			}
			broker := brokerAny.(*Broker)
			t.Cleanup(func() { _ = broker.Close() })
			var dials atomic.Int32
			broker.dial = func(address string, dialConfig amqplib.Config) (amqpConnection, error) {
				uri, err := amqplib.ParseURI(address)
				if err != nil {
					return nil, err
				}
				if uri.Scheme != "amqps" || uri.Username != config.AMQP.Username || uri.Password != config.AMQP.Password || uri.Vhost != config.AMQP.VHost || dialConfig.TLSClientConfig == nil {
					return nil, errors.New("TLS or escaped credentials/vhost lost during failover")
				}
				if dialConfig.TLSClientConfig.ServerName != serverName || dialConfig.TLSClientConfig.MinVersion != tls.VersionTLS12 || dialConfig.TLSClientConfig.InsecureSkipVerify {
					return nil, errors.New("TLS mutation leaked between endpoints")
				}
				if dials.Add(1) == 1 {
					// amqp091-go fills ServerName during dialing; isolate all such mutations.
					dialConfig.TLSClientConfig.ServerName = uri.Host
					dialConfig.TLSClientConfig.InsecureSkipVerify = true
					return nil, errors.New("first endpoint failed")
				}
				return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
			}
			if err := broker.Connect(context.Background()); err != nil || dials.Load() != 2 {
				t.Fatalf("TLS failover: dials=%d error=%v", dials.Load(), err)
			}
			if config.AMQP.TLS.ServerName != serverName || config.AMQP.TLS.MinVersion != 0 || broker.amqpConfig.TLS.ServerName != serverName {
				t.Fatal("dialing mutated caller or shared TLS settings")
			}
		})
	}
}

func TestEndpointConcurrentConnectCoalesces(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672"}}
	brokerAny, _ := NewBroker(config)
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	releaseDial := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseDial)
	var dials atomic.Int32
	broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
		if dials.Add(1) == 1 {
			close(entered)
		}
		<-release
		return &fakeAMQPConnection{channel: newFakeAMQPChannel()}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var callers sync.WaitGroup
	for range 16 {
		callers.Go(func() {
			if err := broker.Connect(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("connection attempt did not start")
	}
	releaseDial()
	callers.Wait()
	if dials.Load() != 1 {
		t.Fatalf("coalesced callers dialed %d endpoints", dials.Load())
	}
}

func TestEndpointFailoverStopsBeforeDialingAnotherEndpoint(t *testing.T) {
	for _, outcome := range []string{"canceled", "closed", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			config := core.DefaultConfig()
			config.AMQP = &core.AMQPConfig{Endpoints: []string{"first:5672", "second:5672"}}
			config.ConnectionBackoff = func(int) time.Duration { t.Error("backoff ran after interruption"); return 0 }
			brokerAny, _ := NewBroker(config)
			broker := brokerAny.(*Broker)
			t.Cleanup(func() { _ = broker.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			shutdown := make(chan error, 1)
			var dials atomic.Int32
			broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
				dials.Add(1)
				switch outcome {
				case "canceled":
					cancel()
				case "closed":
					_ = broker.Close()
				case "shutdown":
					go func() { shutdown <- broker.Shutdown(context.Background()) }()
					<-broker.work.Stopping()
				}
				return nil, errors.New("offline")
			}
			done := make(chan error, 1)
			go func() { done <- broker.Connect(ctx) }()
			want := error(context.Canceled)
			if outcome == "closed" {
				want = core.ErrClosed
			}
			if outcome == "shutdown" {
				want = core.ErrShuttingDown
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) || dials.Load() != 1 {
					t.Fatalf("Connect=%v dials=%d, want %v and one dial", err, dials.Load(), want)
				}
			case <-time.After(time.Second):
				t.Fatal("endpoint iteration ignored interruption")
			}
			if outcome == "shutdown" {
				select {
				case err := <-shutdown:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("shutdown was blocked by endpoint iteration")
				}
			}
		})
	}
}
