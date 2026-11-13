// Copyright 2025 PrabhatDotDev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package kafka provides an Apache Kafka transport implementation for Weave.
package kafka

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/connection"
	"github.com/prabhatdotdev/weave/internal/lifecycle"
	"github.com/prabhatdotdev/weave/internal/observability"
	"github.com/prabhatdotdev/weave/internal/readiness"
	"github.com/prabhatdotdev/weave/internal/rpclimit"
	"github.com/prabhatdotdev/weave/internal/validation"
)

const backendName = "kafka"

type subscriptionRegistration struct {
	id          uint64
	ctx         context.Context
	destination string
	handler     core.Handler
	opts        []core.SubscribeOption
	tracked     bool
	consumerKey consumerKey
	readiness   *readiness.State
}

type consumerKey struct {
	group    string
	earliest bool
}

type subscriptionRoute struct {
	ctx       context.Context
	handler   core.Handler
	workers   chan struct{}
	policy    core.HandlerErrorPolicy
	readiness *readiness.State
}

type pendingCall struct {
	once     sync.Once
	response chan *core.Message
}

type explicitPartitioner struct {
	fallback sarama.Partitioner
}

func newExplicitPartitioner(topic string) sarama.Partitioner {
	return &explicitPartitioner{fallback: sarama.NewHashPartitioner(topic)}
}

func (p *explicitPartitioner) Partition(message *sarama.ProducerMessage, partitions int32) (int32, error) {
	if message.Partition >= 0 {
		if message.Partition >= partitions {
			return -1, sarama.ErrInvalidPartition
		}
		return message.Partition, nil
	}
	return p.fallback.Partition(message, partitions)
}

func (p *explicitPartitioner) RequiresConsistency() bool {
	return true
}

func (p *explicitPartitioner) MessageRequiresConsistency(message *sarama.ProducerMessage) bool {
	return message.Partition >= 0 || message.Key != nil
}

func newPendingCall() *pendingCall {
	return &pendingCall{response: make(chan *core.Message, 1)}
}

func (p *pendingCall) complete(response *core.Message) {
	p.once.Do(func() { p.response <- response })
}

// Broker implements the core.MessageBroker interface for Apache Kafka.
type Broker struct {
	work        lifecycle.Group
	ready       readiness.Signal
	metrics     observability.Metrics
	rpcSlots    rpclimit.Limiter
	config      *core.Config
	kafkaConfig *core.KafkaConfig

	producer         messageProducer
	consumer         sarama.ConsumerGroup
	consumers        map[consumerKey]sarama.ConsumerGroup
	newProducer      func([]string, *sarama.Config) (messageProducer, error)
	newConsumerGroup func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error)

	replyGate   chan struct{}
	replyTopic  string
	pending     map[string]*pendingCall
	pendingMu   sync.RWMutex
	subs        []subscriptionRegistration
	subsMu      sync.RWMutex
	nextSubID   uint64
	runnersMu   sync.Mutex
	runner      bool
	subsChanged chan struct{}
	watchMu     sync.Mutex
	watching    map[sarama.ConsumerGroup]struct{}

	connected     bool
	everConnected bool
	recovering    bool
	closed        bool
	connecting    chan struct{}
	closeMu       sync.RWMutex
	closeOnce     sync.Once
	closeChan     chan struct{}
}

func (b *Broker) emitEvent(ctx context.Context, event core.Event) {
	if b.config == nil {
		return
	}
	if event.Backend == "" {
		event.Backend = backendName
	}
	if event.Component == "" {
		event.Component = "transport"
	}
	b.metrics.Snapshot(&event)
	b.config.EmitEvent(ctx, event)
}

// NewBroker creates a new Kafka broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	var err error
	config, err = validation.Config(config)
	if err != nil {
		return nil, err
	}
	if config.Kafka == nil {
		config.Kafka = core.DefaultKafkaConfig()
	}
	if err := validation.Kafka(config.Kafka); err != nil {
		return nil, err
	}
	if sasl := config.Kafka.SASL; sasl != nil && sasl.Enable {
		switch sasl.Mechanism {
		case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		default:
			return nil, fmt.Errorf("%w: unsupported Kafka SASL mechanism %q", core.ErrInvalidConfig, sasl.Mechanism)
		}
	}

	broker := &Broker{
		config:           config,
		rpcSlots:         rpclimit.New(config.MaxPendingCalls),
		kafkaConfig:      config.Kafka,
		newProducer:      newProducer,
		newConsumerGroup: sarama.NewConsumerGroup,
		pending:          make(map[string]*pendingCall),
		subsChanged:      make(chan struct{}, 1),
		watching:         make(map[sarama.ConsumerGroup]struct{}),
		closeChan:        make(chan struct{}),
	}
	if err := broker.buildSaramaConfig().Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrInvalidConfig, err)
	}
	return broker, nil
}

// Backend returns the name of this backend.
func (b *Broker) Backend() string {
	return backendName
}

// Connect establishes a connection to the Kafka cluster.
func (b *Broker) Connect(ctx context.Context) error {
	started := time.Now()
	if err := validation.Context(ctx); err != nil {
		return err
	}
	ctx, finish, err := b.work.Continue(ctx)
	if err != nil {
		return err
	}
	defer finish()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.closeMu.Lock()
		if b.closed {
			b.closeMu.Unlock()
			return core.ErrClosed
		}
		if b.connected {
			b.closeMu.Unlock()
			return nil
		}
		if done := b.connecting; done != nil {
			b.closeMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-b.closeChan:
				return core.ErrClosed
			case <-done:
				continue
			}
		}
		b.connecting = make(chan struct{})
		b.closeMu.Unlock()
		break
	}

	b.closeMu.RLock()
	keys := make([]consumerKey, 0, len(b.consumers))
	for key := range b.consumers {
		keys = append(keys, key)
	}
	b.closeMu.RUnlock()
	client, err := b.open(ctx, keys)
	b.closeMu.Lock()
	if b.closed {
		err = core.ErrClosed
	}
	var previous *openedConnection
	if err == nil {
		previous = &openedConnection{producer: b.producer, consumer: b.consumer, consumers: b.consumers}
		b.producer, b.consumer, b.consumers = client.producer, client.consumer, client.consumers
		b.connected, b.everConnected, b.recovering = true, true, false
	}
	close(b.connecting)
	b.connecting = nil
	b.closeMu.Unlock()
	b.ready.Notify()
	if previous != nil {
		_ = previous.Close()
	}
	if err != nil && client != nil {
		_ = client.Close()
	}
	event := core.Event{
		Level:     core.EventLevelInfo,
		Name:      core.EventConnect,
		Operation: "connect",
		Duration:  time.Since(started),
		Err:       err,
		Fields:    map[string]any{"outcome": "success"},
	}
	if err != nil {
		event.Level = core.EventLevelError
		event.Fields["outcome"] = "failure"
	}
	b.emitEvent(ctx, event)
	return err
}

type openedConnection struct {
	producer  messageProducer
	consumer  sarama.ConsumerGroup
	consumers map[consumerKey]sarama.ConsumerGroup
}

func (c *openedConnection) Close() error {
	var errs []error
	if c.producer != nil {
		errs = append(errs, c.producer.Close())
	}
	if c.consumer != nil {
		errs = append(errs, c.consumer.Close())
	}
	for _, consumer := range c.consumers {
		if consumer != nil {
			errs = append(errs, consumer.Close())
		}
	}
	return errors.Join(errs...)
}

func (b *Broker) open(ctx context.Context, keys []consumerKey) (*openedConnection, error) {
	saramaConfig := b.buildSaramaConfig()
	retries := b.config.ConnectionRetry
	if retries <= 0 {
		retries = 3
	}

	var lastErr error
	for i := 0; i <= retries; i++ {
		client, err := connection.Open(ctx, b.closeChan, net.Dialer{Timeout: saramaConfig.Net.DialTimeout, KeepAlive: saramaConfig.Net.KeepAlive}, func(dialer *connection.Dialer) (*openedConnection, error) {
			config := *saramaConfig
			config.Net.Proxy.Enable, config.Net.Proxy.Dialer = true, dialer
			producer, err := b.newProducer(b.kafkaConfig.Brokers, &config)
			if err != nil {
				return nil, err
			}
			client := &openedConnection{producer: producer, consumers: make(map[consumerKey]sarama.ConsumerGroup, len(keys))}
			if group := b.kafkaConfig.ConsumerGroup; group != "" {
				client.consumer, err = b.newConsumerGroup(b.kafkaConfig.Brokers, group, &config)
				if err != nil {
					_ = client.Close()
					return nil, err
				}
			}
			for _, key := range keys {
				consumerConfig := config
				if key.earliest {
					consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
				}
				consumer, err := b.newConsumerGroup(b.kafkaConfig.Brokers, key.group, &consumerConfig)
				if err != nil {
					_ = client.Close()
					return nil, err
				}
				client.consumers[key] = consumer
			}
			return client, nil
		})
		if err == nil {
			return client, nil
		}
		lastErr = err

		if i < retries {
			delay := connection.Backoff(b.config, i+1, 2*time.Second)
			if err := connection.Wait(ctx, b.closeChan, b.work.Stopping(), delay); err != nil {
				return nil, err
			}
		}
	}

	return nil, &core.ErrConnectionFailed{
		Backend: backendName,
		Address: fmt.Sprintf("%v", b.kafkaConfig.Brokers),
		Cause:   lastErr,
	}
}

func (b *Broker) buildSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	if b.kafkaConfig.ClientID != "" {
		saramaConfig.ClientID = b.kafkaConfig.ClientID
	}
	if b.kafkaConfig.TLS != nil {
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = b.kafkaConfig.TLS.Clone()
	}
	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(b.kafkaConfig.RequiredAcks)
	saramaConfig.Producer.Retry.Max = b.kafkaConfig.MaxRetries
	saramaConfig.Producer.Retry.Backoff = b.kafkaConfig.RetryBackoff
	saramaConfig.Producer.Partitioner = newExplicitPartitioner
	saramaConfig.Metadata.AllowAutoTopicCreation = false

	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Session.Timeout = b.kafkaConfig.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = b.kafkaConfig.HeartbeatInterval

	if b.kafkaConfig.AutoOffsetReset == "earliest" {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if b.kafkaConfig.SASL != nil && b.kafkaConfig.SASL.Enable {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = b.kafkaConfig.SASL.Username
		saramaConfig.Net.SASL.Password = b.kafkaConfig.SASL.Password
		switch b.kafkaConfig.SASL.Mechanism {
		case "SCRAM-SHA-256":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{hash: scram.SHA256}
			}
		case "SCRAM-SHA-512":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{hash: scram.SHA512}
			}
		default:
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	return saramaConfig
}

// IsConnected returns true if the broker is connected.
func (b *Broker) IsConnected() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.connected && !b.recovering && !b.closed && !b.work.IsStopping()
}

// IsRecovering returns true if the broker is actively recovering from a connection loss.
func (b *Broker) IsRecovering() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.recovering
}

// Shutdown drains admitted operations and handlers before closing the clients.
// A deadline interrupts handlers and bounds the caller's wait for client closure.
func (b *Broker) Shutdown(ctx context.Context) error {
	return b.work.Shutdown(ctx, func(context.Context) error { b.invalidateConsumers()(); return nil }, b.Close)
}

var _ core.Shutdowner = (*Broker)(nil)

// Close closes clients and interrupts handlers without draining work.
func (b *Broker) Close() error {
	b.work.Close()
	var err error
	closed := false
	revoked := func() {}
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		b.closed = true
		b.connected = false
		b.recovering = false
		client := &openedConnection{producer: b.producer, consumer: b.consumer, consumers: b.consumers}
		b.producer, b.consumer, b.consumers = nil, nil, nil
		revoked = b.invalidateConsumers()
		b.closeMu.Unlock()
		b.ready.Notify()

		close(b.closeChan)
		b.cancelAllPending()

		err = client.Close()
		closed = true
	})
	revoked()
	if closed {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      core.EventDisconnect,
			Operation: "close",
			Fields:    map[string]any{"outcome": "success", "reason": "close"},
		})
	}
	return err
}

// Publish sends a message to the specified Kafka topic.
func (b *Broker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	started := time.Now()
	options, err := validation.Publish(ctx, destination, msg, opts...)
	if err != nil {
		return validation.ContextError(err, "Publish", "context deadline")
	}
	if err := validation.KafkaTopic(destination); err != nil {
		return err
	}
	ctx, finish, err := b.work.Continue(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}
	if err := b.ensureConnected(ctx); err != nil {
		return validation.ContextError(err, "Publish", options.Timeout.String())
	}

	msg = msg.Clone()

	kafkaMsg := &sarama.ProducerMessage{
		Topic:     destination,
		Partition: -1,
		Value:     sarama.ByteEncoder(msg.Body),
		Timestamp: msg.Timestamp,
	}

	if options.Partition >= 0 {
		kafkaMsg.Partition = int32(options.Partition)
	}

	if options.Key != "" {
		kafkaMsg.Key = sarama.StringEncoder(options.Key)
	} else if msg.Subject != "" {
		kafkaMsg.Key = sarama.StringEncoder(msg.Subject)
	}

	if len(msg.Headers) > 0 {
		for k, v := range msg.Headers {
			if k == "message-id" && msg.MessageID != "" {
				continue
			}
			kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	if msg.MessageID != "" {
		kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
			Key:   []byte("message-id"),
			Value: []byte(msg.MessageID),
		})
	}

	if msg.CorrelationID != "" {
		kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
			Key:   []byte("correlation-id"),
			Value: []byte(msg.CorrelationID),
		})
	}

	if msg.ReplyTo != "" {
		kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
			Key:   []byte("reply-to"),
			Value: []byte(msg.ReplyTo),
		})
	}

	if msg.ContentType != "" {
		kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
			Key:   []byte("content-type"),
			Value: []byte(msg.ContentType),
		})
	}

	producer := b.currentProducer()
	if producer == nil {
		return &core.ErrNotConnected{Backend: backendName}
	}

	_, _, err = producer.SendMessageContext(ctx, kafkaMsg)
	if err != nil {
		err = validation.ContextError(err, "Publish", options.Timeout.String())
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventPublishFailed,
			Operation:   "publish",
			Destination: destination,
			Err:         err,
		})
		if producerUnavailable(err) {
			b.handleProducerLoss(producer, err)
		}
		return &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	b.emitEvent(ctx, core.Event{Level: core.EventLevelInfo, Name: core.EventPublishSucceeded, Operation: "publish", Destination: destination, CorrelationID: msg.CorrelationID, Duration: time.Since(started)})
	return nil
}

// Subscribe registers a handler for messages from the specified Kafka topic.
func (b *Broker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	started := time.Now()
	handler = b.metrics.WrapHandler(handler, destination, b.emitEvent)
	if err := b.subscribe(ctx, destination, handler, true, opts...); err != nil {
		return err
	}

	b.emitEvent(ctx, core.Event{Level: core.EventLevelInfo, Name: core.EventSubscribeSucceeded, Operation: "subscribe", Destination: destination, Duration: time.Since(started)})
	return nil
}

func (b *Broker) subscribe(ctx context.Context, destination string, handler core.Handler, track bool, opts ...core.SubscribeOption) error {
	return b.subscribeWithSetup(ctx, ctx, destination, handler, track, opts...)
}

func (b *Broker) subscribeWithSetup(setupCtx, ctx context.Context, destination string, handler core.Handler, track bool, opts ...core.SubscribeOption) error {
	options, err := validation.Subscribe(ctx, destination, handler, opts...)
	if err != nil {
		return err
	}
	if err := validation.KafkaTopic(destination); err != nil {
		return err
	}
	var finish func()
	if track {
		setupCtx, finish, err = b.work.Begin(setupCtx)
	} else {
		setupCtx, finish, err = b.work.Continue(setupCtx)
	}
	if err != nil {
		return err
	}
	defer finish()

	if err := b.ensureConnected(setupCtx); err != nil {
		return err
	}

	if err := b.startSubscriptionConsumer(setupCtx, destination, options); err != nil {
		return err
	}
	reg := b.registerSubscription(ctx, destination, handler, opts, track)
	if !b.work.IsStopping() || !track {
		b.restartSubscriptionRunner()
	}

	go func() {
		select {
		case <-ctx.Done():
			b.unregisterSubscription(reg.id)
			if !b.work.IsStopping() {
				b.restartSubscriptionRunner()
			}
		case <-b.closeChan:
		}
	}()

	return nil
}

func (b *Broker) startSubscriptionConsumer(ctx context.Context, destination string, options *core.SubscribeOptions) error {
	if err := b.ensureConnected(ctx); err != nil {
		return err
	}

	key := b.subscriptionConsumerKey(options)
	if b.currentConsumerFor(key) == nil && key.group == "" {
		err := fmt.Errorf("consumer group not configured")
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventSubscribeFailed,
			Operation:   "subscribe",
			Destination: destination,
			Err:         err,
		})
		return &core.ErrSubscribeFailed{
			Backend:     backendName,
			Destination: destination,
			Cause:       err,
		}
	}
	if b.currentConsumerFor(key) == nil {
		config := b.buildSaramaConfig()
		if key.earliest {
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		}
		producer := b.currentProducer()
		consumer, err := connection.Open(ctx, b.closeChan, net.Dialer{Timeout: config.Net.DialTimeout, KeepAlive: config.Net.KeepAlive}, func(dialer *connection.Dialer) (sarama.ConsumerGroup, error) {
			config.Net.Proxy.Enable, config.Net.Proxy.Dialer = true, dialer
			return b.newConsumerGroup(b.kafkaConfig.Brokers, key.group, config)
		})
		if err != nil {
			return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
		}
		b.closeMu.Lock()
		if b.closed || !b.connected || b.producer != producer {
			b.closeMu.Unlock()
			_ = consumer.Close()
			return &core.ErrNotConnected{Backend: backendName}
		}
		previous := b.consumerForLocked(key)
		if previous == nil {
			if key == b.subscriptionConsumerKey(&core.SubscribeOptions{}) {
				b.consumer = consumer
			} else {
				if b.consumers == nil {
					b.consumers = make(map[consumerKey]sarama.ConsumerGroup)
				}
				b.consumers[key] = consumer
			}
		}
		b.closeMu.Unlock()
		if previous != nil {
			_ = consumer.Close()
		}
	}

	return nil
}

func (b *Broker) subscriptionConsumerKey(options *core.SubscribeOptions) consumerKey {
	group := options.ConsumerGroup
	if group == "" {
		group = b.kafkaConfig.ConsumerGroup
	}
	return consumerKey{group: group, earliest: options.StartFromBeginning || b.kafkaConfig.AutoOffsetReset == "earliest"}
}

func (b *Broker) consumerForLocked(key consumerKey) sarama.ConsumerGroup {
	if key == b.subscriptionConsumerKey(&core.SubscribeOptions{}) {
		return b.consumer
	}
	return b.consumers[key]
}

func (b *Broker) currentConsumerFor(key consumerKey) sarama.ConsumerGroup {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.consumerForLocked(key)
}

func (b *Broker) restartSubscriptionRunner() {
	b.invalidateConsumers()()
	b.runnersMu.Lock()
	if b.subsChanged == nil {
		b.subsChanged = make(chan struct{}, 1)
	}
	changed := b.subsChanged
	if !b.runner {
		b.runner = true
		go b.runSubscriptionConsumer(changed)
		b.runnersMu.Unlock()
		return
	}
	b.runnersMu.Unlock()

	select {
	case changed <- struct{}{}:
	default:
	}
}

func (b *Broker) runSubscriptionConsumer(changed <-chan struct{}) {
	defer func() {
		b.runnersMu.Lock()
		b.runner = false
		b.runnersMu.Unlock()
	}()

	failedConnections := 0
	for {
		groups := b.activeSubscriptionGroups()
		if len(groups) == 0 {
			select {
			case <-changed:
				continue
			case <-b.closeChan:
				return
			}
		}

		if err := b.ensureConnected(context.Background()); err != nil {
			if b.work.IsStopping() {
				return
			}
			b.emitEvent(context.Background(), core.Event{
				Level:       core.EventLevelWarn,
				Name:        core.EventConnect,
				Operation:   "reconnect",
				Destination: "*",
				Err:         err,
				Fields:      map[string]any{"outcome": "failure"},
			})
			failedConnections++
			delay := 200 * time.Millisecond
			if b.config.ConnectionBackoff != nil {
				delay = connection.Backoff(b.config, failedConnections, delay)
			}
			if connection.Wait(context.Background(), b.closeChan, b.work.Stopping(), delay) != nil {
				return
			}
			continue
		}
		failedConnections = 0

		consumeCtx, cancel := context.WithCancel(context.Background())
		type result struct {
			consumer sarama.ConsumerGroup
			err      error
		}
		results := make(chan result, len(groups))
		for key, routes := range groups {
			consumer := b.currentConsumerFor(key)
			go func() {
				if consumer == nil {
					results <- result{err: &core.ErrNotConnected{Backend: backendName}}
					return
				}
				b.watchConsumerErrors(consumeCtx, "*", consumer)
				observer := &consumerReadiness{broker: b, consumer: consumer, key: key, routes: routes, ctx: consumeCtx}
				defer observer.stop()
				handler := &consumerGroupHandler{
					routes: routes, closeChan: b.closeChan, replyTopic: b.kafkaConfig.ReplyTopic,
					pending: b.pending, pendingMu: &b.pendingMu, config: b.config, work: &b.work, metrics: &b.metrics,
					onSetup: observer.setup, onCleanup: observer.cleanup,
				}
				topics := make([]string, 0, len(routes))
				for topic := range routes {
					topics = append(topics, topic)
				}
				sort.Strings(topics)
				var err error
				observer.beginRebalance(nil)
				for consumeCtx.Err() == nil {
					err = consumer.Consume(consumeCtx, topics, handler)
					if err != nil {
						break
					}
				}
				results <- result{consumer, err}
			}()
		}
		var failed result
		remaining := len(groups)
		select {
		case <-changed:
		case <-b.closeChan:
		case failed = <-results:
			remaining--
		}
		cancel()
		for range remaining {
			<-results
		}
		if failed.err == nil || b.isClosed() {
			continue
		}

		b.emitEvent(context.Background(), core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventSubscribeFailed,
			Operation:   "consume",
			Destination: "*",
			Err:         failed.err,
		})
		b.handleConsumerLoss(failed.consumer, failed.err)
	}
}

func (b *Broker) activeSubscriptionGroups() map[consumerKey]map[string]subscriptionRoute {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()

	groups := make(map[consumerKey]map[string]subscriptionRoute)
	for _, sub := range b.subs {
		if sub.ctx.Err() != nil {
			continue
		}
		options := core.ApplySubscribeOptions(sub.opts...)
		route := subscriptionRoute{
			ctx:       sub.ctx,
			handler:   sub.handler,
			policy:    options.HandlerErrorPolicy,
			readiness: sub.readiness,
		}
		if options.WorkerCount > 0 {
			route.workers = make(chan struct{}, options.WorkerCount)
		}
		if groups[sub.consumerKey] == nil {
			groups[sub.consumerKey] = make(map[string]subscriptionRoute)
		}
		groups[sub.consumerKey][sub.destination] = route
	}
	return groups
}

func (b *Broker) activeSubscriptionRoutes() ([]string, map[string]subscriptionRoute) {
	routes := make(map[string]subscriptionRoute)
	for _, group := range b.activeSubscriptionGroups() {
		for topic, route := range group {
			routes[topic] = route
		}
	}
	topics := make([]string, 0, len(routes))
	for topic := range routes {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics, routes
}

// Call implements request-response pattern using correlation IDs and reply topics.
func (b *Broker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (_ *core.Message, resultErr error) {
	started := time.Now()
	options, err := validation.Publish(ctx, destination, msg, opts...)
	if err != nil {
		return nil, validation.ContextError(err, "Call", "context deadline")
	}
	if err := validation.KafkaTopic(destination); err != nil {
		return nil, err
	}
	ctx, finish, err := b.work.Continue(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}
	if msg.CorrelationID != "" {
		b.pendingMu.RLock()
		_, duplicate := b.pending[msg.CorrelationID]
		b.pendingMu.RUnlock()
		if duplicate {
			return nil, fmt.Errorf("%w: %q", core.ErrDuplicateCorrelationID, msg.CorrelationID)
		}
	}
	releaseSlot, err := b.rpcSlots.Acquire(ctx, &b.work)
	if err != nil {
		return nil, validation.ContextError(err, "Call", options.Timeout.String())
	}
	defer releaseSlot()
	if err := b.ensureConnected(ctx); err != nil {
		return nil, validation.ContextError(err, "Call", options.Timeout.String())
	}

	if err := b.ensureReplyConsumer(ctx); err != nil {
		return nil, validation.ContextError(err, "Call", options.Timeout.String())
	}

	request := msg.Clone()
	corrID := request.CorrelationID
	if corrID == "" {
		corrID = rand.Text()
	}

	pending := newPendingCall()
	b.pendingMu.Lock()
	if _, exists := b.pending[corrID]; exists {
		b.pendingMu.Unlock()
		return nil, fmt.Errorf("%w: %q", core.ErrDuplicateCorrelationID, corrID)
	}
	b.pending[corrID] = pending
	b.pendingMu.Unlock()

	finishCall := b.metrics.StartCall(ctx, destination, corrID, started, b.emitEvent)
	defer func() {
		b.pendingMu.Lock()
		// A reconnect may have registered a newer call with the same ID.
		if b.pending[corrID] == pending {
			delete(b.pending, corrID)
		}
		b.pendingMu.Unlock()
		finishCall(resultErr)
	}()

	request.CorrelationID = corrID
	request.ReplyTo = b.currentReplyTopic()

	if err := b.Publish(ctx, destination, request, opts...); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelWarn,
			Name:        core.EventTimeout,
			Operation:   "call",
			Destination: destination,
			Fields: map[string]any{
				"timeout": options.Timeout.String(),
			},
		})
		return nil, validation.ContextError(ctx.Err(), "Call", options.Timeout.String())
	case response := <-pending.response:
		if response == nil {
			return nil, &core.ErrConnectionLost{Backend: backendName}
		}
		return response, nil
	}
}

func (b *Broker) ensureReplyConsumer(ctx context.Context) error {
	b.closeMu.Lock()
	if b.replyGate == nil {
		b.replyGate = make(chan struct{}, 1)
	}
	gate := b.replyGate
	b.closeMu.Unlock()
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closeChan:
		return core.ErrClosed
	}

	if b.currentReplyTopic() != "" {
		return nil
	}

	replyTopic := b.kafkaConfig.ReplyTopic
	if replyTopic == "" {
		return fmt.Errorf("%w: Kafka reply topic must be configured", core.ErrInvalidConfig)
	}
	if err := b.subscribeWithSetup(ctx, context.Background(), replyTopic, func(ctx context.Context, msg *core.Message) error {
		return nil
	}, false); err != nil {
		return err
	}

	b.closeMu.Lock()
	b.replyTopic = replyTopic
	b.closeMu.Unlock()
	return nil
}

func (b *Broker) cancelAllPending() {
	b.pendingMu.Lock()
	pending := make([]*pendingCall, 0, len(b.pending))
	for key, call := range b.pending {
		pending = append(pending, call)
		delete(b.pending, key)
	}
	b.pendingMu.Unlock()

	for _, call := range pending {
		call.complete(nil)
	}
}

func (b *Broker) ensureConnected(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.closeMu.Lock()

	if b.closed {
		b.closeMu.Unlock()
		return core.ErrClosed
	}

	if b.connected {
		b.closeMu.Unlock()
		return nil
	}

	if !b.everConnected {
		b.closeMu.Unlock()
		return &core.ErrNotConnected{Backend: backendName}
	}
	b.closeMu.Unlock()

	// Emit reconnect started event
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelInfo,
		Name:      "reconnect_started",
		Operation: "ensure_connected",
	})

	err := b.Connect(ctx)
	if err != nil {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelError,
			Name:      "reconnect_failed",
			Operation: "ensure_connected",
			Err:       err,
		})
		return err
	}

	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelInfo,
		Name:      "reconnect_succeeded",
		Operation: "ensure_connected",
	})

	b.restoreTrackedSubscriptions()
	return nil
}

func (b *Broker) handleConnectionLoss(cause error) {
	if client := b.handleConnectionLossFor(nil, nil, cause); client != nil {
		_ = client.Close()
	}
}

func (b *Broker) handleProducerLoss(producer messageProducer, cause error) {
	if client := b.handleConnectionLossFor(producer, nil, cause); client != nil {
		go func() { _ = client.Close() }()
	}
}

func (b *Broker) handleConsumerLoss(consumer sarama.ConsumerGroup, cause error) {
	if client := b.handleConnectionLossFor(nil, consumer, cause); client != nil {
		_ = client.Close()
	}
}

func producerUnavailable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var networkError net.Error
	return core.IsConnectionLost(err) || errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrClosedClient) || errors.Is(err, sarama.ErrShuttingDown) || errors.As(err, &networkError)
}

func (b *Broker) handleConnectionLossFor(expectedProducer messageProducer, expectedConsumer sarama.ConsumerGroup, cause error) *openedConnection {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()
		return nil
	}
	if expectedProducer != nil && b.producer != expectedProducer {
		b.closeMu.Unlock()
		return nil
	}
	if expectedConsumer != nil && b.consumer != expectedConsumer {
		found := false
		for _, consumer := range b.consumers {
			found = found || consumer == expectedConsumer
		}
		if !found {
			b.closeMu.Unlock()
			return nil
		}
	}
	if !b.connected && b.recovering {
		b.closeMu.Unlock()
		return nil
	}
	b.connected = false
	b.recovering = true
	client := &openedConnection{producer: b.producer, consumer: b.consumer, consumers: b.consumers}
	b.producer = nil
	b.consumer = nil
	b.consumers = make(map[consumerKey]sarama.ConsumerGroup, len(client.consumers))
	b.watchMu.Lock()
	delete(b.watching, client.consumer)
	for key, consumer := range client.consumers {
		b.consumers[key] = nil
		delete(b.watching, consumer)
	}
	b.watchMu.Unlock()
	revoked := b.invalidateConsumers()
	b.closeMu.Unlock()
	b.ready.Notify()
	revoked()

	b.cancelAllPending()
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelWarn,
		Name:      core.EventDisconnect,
		Operation: "connection_lost",
		Err:       cause,
		Fields:    map[string]any{"outcome": "failure"},
	})
	return client
}

func (b *Broker) registerSubscription(ctx context.Context, destination string, handler core.Handler, opts []core.SubscribeOption, tracked bool) subscriptionRegistration {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	b.nextSubID++
	reg := subscriptionRegistration{
		id:          b.nextSubID,
		ctx:         ctx,
		destination: destination,
		handler:     handler,
		opts:        append([]core.SubscribeOption(nil), opts...),
		tracked:     tracked,
		consumerKey: b.subscriptionConsumerKey(core.ApplySubscribeOptions(opts...)),
	}
	if tracked {
		reg.readiness = &readiness.State{}
	}
	b.subs = append(b.subs, reg)
	b.ready.Notify()
	return reg
}

func (b *Broker) unregisterSubscription(id uint64) {
	if id == 0 {
		return
	}
	b.subsMu.Lock()
	var revoked func()
	for i, sub := range b.subs {
		if sub.id == id {
			revoked = sub.readiness.Invalidate()
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			break
		}
	}
	b.subsMu.Unlock()
	if revoked != nil {
		revoked()
	}
	b.ready.Notify()
}

func (b *Broker) restoreTrackedSubscriptions() {
	b.subsMu.RLock()
	tracked := 0
	for _, sub := range b.subs {
		if sub.tracked && sub.ctx.Err() == nil {
			tracked++
		}
	}
	b.subsMu.RUnlock()

	if tracked > 0 {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      "subscription_restore_started",
			Operation: "restore_subscriptions",
			Fields: map[string]any{
				"subscription_count": tracked,
			},
		})
	}

	b.restartSubscriptionRunner()

	if tracked > 0 {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      "subscription_restore_completed",
			Operation: "restore_subscriptions",
			Fields: map[string]any{
				"subscription_count": tracked,
			},
		})
	}
}

func (b *Broker) currentProducer() messageProducer {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.producer
}

func (b *Broker) currentConsumer() sarama.ConsumerGroup {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.consumer
}

func (b *Broker) currentReplyTopic() string {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.replyTopic
}

func (b *Broker) isClosed() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.closed
}

func (b *Broker) watchConsumerErrors(ctx context.Context, destination string, consumer sarama.ConsumerGroup) {
	b.watchMu.Lock()
	if b.watching == nil {
		b.watching = make(map[sarama.ConsumerGroup]struct{})
	}
	if _, exists := b.watching[consumer]; exists {
		b.watchMu.Unlock()
		return
	}
	b.watching[consumer] = struct{}{}
	b.watchMu.Unlock()

	errCh := consumer.Errors()
	if errCh == nil {
		b.watchMu.Lock()
		delete(b.watching, consumer)
		b.watchMu.Unlock()
		return
	}

	go func() {
		defer func() {
			b.watchMu.Lock()
			delete(b.watching, consumer)
			b.watchMu.Unlock()
		}()

		for {
			select {
			case <-b.closeChan:
				return
			case <-ctx.Done():
				return
			case err, ok := <-errCh:
				if !ok {
					return
				}
				if err == nil {
					continue
				}
				b.emitEvent(ctx, core.Event{
					Level:       core.EventLevelError,
					Name:        core.EventSubscribeFailed,
					Operation:   "consumer_errors",
					Destination: destination,
					Err:         err,
				})
				if !b.isClosed() {
					b.handleConsumerLoss(consumer, err)
					b.restartSubscriptionRunner()
				}
			}
		}
	}()
}

type consumerGroupHandler struct {
	metrics    *observability.Metrics
	work       *lifecycle.Group
	handler    core.Handler
	routes     map[string]subscriptionRoute
	closeChan  chan struct{}
	replyTopic string
	workers    chan struct{}
	pending    map[string]*pendingCall
	pendingMu  *sync.RWMutex
	config     *core.Config
	policy     core.HandlerErrorPolicy
	onSetup    func(sarama.ConsumerGroupSession) error
	onCleanup  func(sarama.ConsumerGroupSession) error
}

func (h *consumerGroupHandler) route(topic string) (subscriptionRoute, bool) {
	if len(h.routes) > 0 {
		route, ok := h.routes[topic]
		if route.ctx == nil {
			route.ctx = context.Background()
		}
		return route, ok
	}
	return subscriptionRoute{
		ctx:     context.Background(),
		handler: h.handler,
		workers: h.workers,
		policy:  h.policy,
	}, h.handler != nil
}

func (h *consumerGroupHandler) emitSubscribeFailure(destination string, err error) {
	if h.config == nil {
		return
	}
	event := core.Event{
		Level:       core.EventLevelError,
		Name:        core.EventSubscribeFailed,
		Backend:     backendName,
		Component:   "transport",
		Operation:   "handler",
		Destination: destination,
		Err:         err,
	}
	if h.metrics != nil {
		h.metrics.Snapshot(&event)
	}
	h.config.EmitEvent(context.Background(), event)
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	if h.onSetup != nil {
		return h.onSetup(session)
	}
	return nil
}

func (h *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	if h.onCleanup != nil {
		return h.onCleanup(session)
	}
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-h.closeChan:
			return nil
		case kafkaMsg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			msg := &core.Message{
				Body:      kafkaMsg.Value,
				Subject:   string(kafkaMsg.Key),
				Partition: kafkaMsg.Partition,
				Offset:    kafkaMsg.Offset,
				Timestamp: kafkaMsg.Timestamp,
				Headers:   make(map[string]string),
			}

			var correlationID string
			for _, header := range kafkaMsg.Headers {
				key := string(header.Key)
				value := string(header.Value)
				switch key {
				case "message-id":
					msg.MessageID = value
				case "correlation-id":
					correlationID = value
					msg.CorrelationID = value
				case "reply-to":
					msg.ReplyTo = value
				case "content-type":
					msg.ContentType = value
				default:
					msg.Headers[key] = value
				}
			}

			if h.replyTopic != "" && kafkaMsg.Topic == h.replyTopic && correlationID != "" {
				h.pendingMu.RLock()
				pending, exists := h.pending[correlationID]
				h.pendingMu.RUnlock()
				if exists {
					pending.complete(msg)
					session.MarkMessage(kafkaMsg, "")
					continue
				}
			}

			route, ok := h.route(kafkaMsg.Topic)
			if !ok {
				session.MarkMessage(kafkaMsg, "")
				continue
			}

			if route.workers != nil {
				select {
				case route.workers <- struct{}{}:
				case <-h.closeChan:
					return nil
				case <-session.Context().Done():
					return nil
				}
			}
			handlerCtx, release := mergeHandlerContexts(session.Context(), route.ctx)
			var finishHandler func()
			if h.work != nil {
				var err error
				handlerCtx, finishHandler, err = h.work.Handler(handlerCtx)
				if err != nil {
					release()
					if route.workers != nil {
						<-route.workers
					}
					if h.work.IsStopping() {
						select {
						case <-session.Context().Done():
						case <-h.closeChan:
						}
					}
					return nil
				}
			}
			interrupted := false
			err := func() error {
				if route.workers != nil {
					defer func() { <-route.workers }()
				}
				defer release()
				err := callSubscriptionHandler(handlerCtx, route.handler, msg)
				interrupted = handlerCtx.Err() != nil
				return err
			}()
			if err != nil {
				// Interrupted work must remain uncommitted even under the no-retry policy.
				if interrupted || session.Context().Err() != nil || route.ctx.Err() != nil || h.work != nil && h.work.IsClosed() {
					if finishHandler != nil {
						finishHandler()
					}
					return err
				}
				h.emitSubscribeFailure(kafkaMsg.Topic, err)
				if route.policy == core.HandlerErrorRetry || core.IsDeadLetterFailed(err) {
					if finishHandler != nil {
						finishHandler()
					}
					return err
				}
			}

			session.MarkMessage(kafkaMsg, "")
			if finishHandler != nil {
				finishHandler()
			}
		}
	}
}

func mergeHandlerContexts(sessionCtx, subscriptionCtx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(subscriptionCtx)
	stop := context.AfterFunc(sessionCtx, cancel)
	if sessionCtx.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stop()
		cancel()
	}
}

func callSubscriptionHandler(ctx context.Context, handler core.Handler, msg *core.Message) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, msg)
}
