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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

const backendName = "kafka"

type subscriptionRegistration struct {
	id          uint64
	ctx         context.Context
	destination string
	handler     core.Handler
	opts        []core.SubscribeOption
	tracked     bool
}

type subscriptionRoute struct {
	ctx     context.Context
	handler core.Handler
	workers chan struct{}
	policy  core.HandlerErrorPolicy
}

type pendingCall struct {
	once     sync.Once
	response chan *core.Message
}

func newPendingCall() *pendingCall {
	return &pendingCall{response: make(chan *core.Message, 1)}
}

func (p *pendingCall) complete(response *core.Message) {
	p.once.Do(func() { p.response <- response })
}

// Broker implements the core.MessageBroker interface for Apache Kafka.
type Broker struct {
	config      *core.Config
	kafkaConfig *core.KafkaConfig

	producer         sarama.SyncProducer
	consumer         sarama.ConsumerGroup
	newSyncProducer  func([]string, *sarama.Config) (sarama.SyncProducer, error)
	newConsumerGroup func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error)

	replyMu     sync.Mutex
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
	b.config.EmitEvent(ctx, event)
}

// NewBroker creates a new Kafka broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	if config == nil {
		config = core.DefaultConfig()
	}
	if config.Kafka == nil {
		config.Kafka = core.DefaultKafkaConfig()
	}

	return &Broker{
		config:           config,
		kafkaConfig:      config.Kafka,
		newSyncProducer:  sarama.NewSyncProducer,
		newConsumerGroup: sarama.NewConsumerGroup,
		pending:          make(map[string]*pendingCall),
		subsChanged:      make(chan struct{}, 1),
		watching:         make(map[sarama.ConsumerGroup]struct{}),
		closeChan:        make(chan struct{}),
	}, nil
}

// Backend returns the name of this backend.
func (b *Broker) Backend() string {
	return backendName
}

// Connect establishes a connection to the Kafka cluster.
func (b *Broker) Connect(ctx context.Context) error {
	_ = ctx

	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return core.ErrClosed
	}
	if b.connected {
		return nil
	}

	return b.connectLocked()
}

func (b *Broker) connectLocked() error {
	saramaConfig := b.buildSaramaConfig()
	retries := b.config.ConnectionRetry
	if retries <= 0 {
		retries = 3
	}

	var lastErr error
	for i := 0; i <= retries; i++ {
		producer, err := b.newSyncProducer(b.kafkaConfig.Brokers, saramaConfig)
		if err != nil {
			lastErr = err
		} else {
			var consumer sarama.ConsumerGroup
			consumerGroup := b.kafkaConfig.ConsumerGroup
			if consumerGroup != "" {
				consumer, err = b.newConsumerGroup(b.kafkaConfig.Brokers, consumerGroup, saramaConfig)
				if err != nil {
					_ = producer.Close()
					lastErr = err
				} else {
					lastErr = nil
				}
			} else {
				lastErr = nil
			}

			if lastErr == nil {
				if b.producer != nil {
					_ = b.producer.Close()
				}
				if b.consumer != nil {
					_ = b.consumer.Close()
				}

				b.producer = producer
				b.consumer = consumer
				b.connected = true
				b.everConnected = true
				b.recovering = false
				b.emitEvent(context.Background(), core.Event{
					Level:     core.EventLevelInfo,
					Name:      core.EventConnect,
					Operation: "connect",
					Fields:    map[string]any{"outcome": "success"},
				})
				return nil
			}
		}

		if i < retries {
			delay := b.config.RetryDelay
			if delay == 0 {
				delay = 2 * time.Second
			}
			time.Sleep(delay)
		}
	}

	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelError,
		Name:      core.EventConnect,
		Operation: "connect",
		Err:       lastErr,
		Fields:    map[string]any{"outcome": "failure"},
	})

	return &core.ErrConnectionFailed{
		Backend: backendName,
		Address: fmt.Sprintf("%v", b.kafkaConfig.Brokers),
		Cause:   lastErr,
	}
}

func (b *Broker) buildSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(b.kafkaConfig.RequiredAcks)
	saramaConfig.Producer.Retry.Max = b.kafkaConfig.MaxRetries
	saramaConfig.Producer.Retry.Backoff = b.kafkaConfig.RetryBackoff
	saramaConfig.Metadata.AllowAutoTopicCreation = false

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
		case "SCRAM-SHA-512":
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
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
	return b.connected && !b.recovering && !b.closed
}

// IsRecovering returns true if the broker is actively recovering from a connection loss.
func (b *Broker) IsRecovering() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.recovering
}

// Close gracefully shuts down the Kafka connections.
func (b *Broker) Close() error {
	var errs []error
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		b.closed = true
		b.connected = false
		b.recovering = false
		b.closeMu.Unlock()

		close(b.closeChan)
		b.cancelAllPending()

		if b.producer != nil {
			if err := b.producer.Close(); err != nil {
				errs = append(errs, err)
			}
			b.producer = nil
		}
		if b.consumer != nil {
			if err := b.consumer.Close(); err != nil {
				errs = append(errs, err)
			}
			b.consumer = nil
		}

		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      core.EventDisconnect,
			Operation: "close",
			Fields:    map[string]any{"outcome": "success", "reason": "close"},
		})
	})

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Publish sends a message to the specified Kafka topic.
func (b *Broker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	if err := b.ensureConnected(); err != nil {
		return err
	}

	options := core.ApplyPublishOptions(opts...)

	kafkaMsg := &sarama.ProducerMessage{
		Topic: destination,
		Value: sarama.ByteEncoder(msg.Body),
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
			kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
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

	_, _, err := producer.SendMessage(kafkaMsg)
	if err != nil {
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventPublishFailed,
			Operation:   "publish",
			Destination: destination,
			Err:         err,
		})
		b.handleProducerLoss(producer, err)
		return &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	return nil
}

// Subscribe registers a handler for messages from the specified Kafka topic.
func (b *Broker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	if err := b.subscribe(ctx, destination, handler, true, opts...); err != nil {
		return err
	}

	return nil
}

func (b *Broker) subscribe(ctx context.Context, destination string, handler core.Handler, track bool, opts ...core.SubscribeOption) error {
	options := core.ApplySubscribeOptions(opts...)
	if options.WorkerCount < 0 {
		return fmt.Errorf("%w: worker count must not be negative", core.ErrInvalidConfig)
	}

	if err := b.ensureConnected(); err != nil {
		return err
	}

	reg := b.registerSubscription(ctx, destination, handler, opts, track)

	if err := b.startSubscriptionConsumer(ctx, destination, options); err != nil {
		b.unregisterSubscription(reg.id)
		return err
	}

	go func() {
		select {
		case <-ctx.Done():
			b.unregisterSubscription(reg.id)
			b.restartSubscriptionRunner()
		case <-b.closeChan:
		}
	}()

	return nil
}

func (b *Broker) startSubscriptionConsumer(ctx context.Context, destination string, options *core.SubscribeOptions) error {
	if err := b.ensureConnected(); err != nil {
		return err
	}

	consumerGroup := options.ConsumerGroup
	if consumerGroup == "" {
		consumerGroup = b.kafkaConfig.ConsumerGroup
	}

	if b.currentConsumer() == nil && consumerGroup == "" {
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

	b.restartSubscriptionRunner()
	return nil
}

func (b *Broker) restartSubscriptionRunner() {
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

	for {
		topics, routes := b.activeSubscriptionRoutes()
		if len(topics) == 0 {
			select {
			case <-changed:
				continue
			case <-b.closeChan:
				return
			}
		}

		if err := b.ensureConnected(); err != nil {
			b.emitEvent(context.Background(), core.Event{
				Level:       core.EventLevelWarn,
				Name:        core.EventConnect,
				Operation:   "reconnect",
				Destination: "*",
				Err:         err,
				Fields:      map[string]any{"outcome": "failure"},
			})
			select {
			case <-time.After(200 * time.Millisecond):
			case <-b.closeChan:
				return
			}
			continue
		}

		consumer := b.currentConsumer()
		if consumer == nil {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-b.closeChan:
				return
			}
			continue
		}

		consumeCtx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-changed:
				cancel()
			case <-consumeCtx.Done():
			case <-b.closeChan:
				cancel()
			}
		}()

		b.watchConsumerErrors(consumeCtx, "*", consumer)
		handler := &consumerGroupHandler{
			routes:    routes,
			closeChan: b.closeChan,
			pending:   b.pending,
			pendingMu: &b.pendingMu,
			config:    b.config,
		}
		err := consumer.Consume(consumeCtx, topics, handler)
		changedTopics := consumeCtx.Err() != nil
		cancel()
		if err == nil || changedTopics || b.isClosed() {
			continue
		}

		b.emitEvent(context.Background(), core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventSubscribeFailed,
			Operation:   "consume",
			Destination: "*",
			Err:         err,
		})
		b.handleConsumerLoss(consumer, err)
	}
}

func (b *Broker) activeSubscriptionRoutes() ([]string, map[string]subscriptionRoute) {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()

	routes := make(map[string]subscriptionRoute, len(b.subs))
	for _, sub := range b.subs {
		if sub.ctx.Err() != nil {
			continue
		}
		options := core.ApplySubscribeOptions(sub.opts...)
		route := subscriptionRoute{
			ctx:     sub.ctx,
			handler: sub.handler,
			policy:  options.HandlerErrorPolicy,
		}
		if options.WorkerCount > 0 {
			route.workers = make(chan struct{}, options.WorkerCount)
		}
		routes[sub.destination] = route
	}

	topics := make([]string, 0, len(routes))
	for topic := range routes {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics, routes
}

// Call implements request-response pattern using correlation IDs and reply topics.
func (b *Broker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	if err := b.ensureConnected(); err != nil {
		return nil, err
	}

	options := core.ApplyPublishOptions(opts...)

	if err := b.ensureReplyConsumer(); err != nil {
		return nil, err
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

	defer func() {
		b.pendingMu.Lock()
		delete(b.pending, corrID)
		b.pendingMu.Unlock()
	}()

	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}

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
		return nil, &core.ErrTimeout{Operation: "Call", Duration: options.Timeout.String()}
	case response := <-pending.response:
		if response == nil {
			return nil, &core.ErrConnectionLost{Backend: backendName}
		}
		return response, nil
	}
}

func (b *Broker) ensureReplyConsumer() error {
	b.replyMu.Lock()
	defer b.replyMu.Unlock()

	if b.currentReplyTopic() != "" {
		return nil
	}

	replyTopic := b.kafkaConfig.ReplyTopic
	if replyTopic == "" {
		return fmt.Errorf("%w: Kafka reply topic must be configured", core.ErrInvalidConfig)
	}
	if err := b.subscribe(context.Background(), replyTopic, func(ctx context.Context, msg *core.Message) error {
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

func (b *Broker) ensureConnected() error {
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

	// Emit reconnect started event
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelInfo,
		Name:      "reconnect_started",
		Operation: "ensure_connected",
	})

	err := b.connectLocked()
	b.closeMu.Unlock()
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
	b.handleConnectionLossFor(nil, nil, cause)
}

func (b *Broker) handleProducerLoss(producer sarama.SyncProducer, cause error) {
	b.handleConnectionLossFor(producer, nil, cause)
}

func (b *Broker) handleConsumerLoss(consumer sarama.ConsumerGroup, cause error) {
	b.handleConnectionLossFor(nil, consumer, cause)
}

func (b *Broker) handleConnectionLossFor(expectedProducer sarama.SyncProducer, expectedConsumer sarama.ConsumerGroup, cause error) {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()
		return
	}
	if expectedProducer != nil && b.producer != expectedProducer {
		b.closeMu.Unlock()
		return
	}
	if expectedConsumer != nil && b.consumer != expectedConsumer {
		b.closeMu.Unlock()
		return
	}
	if !b.connected && b.recovering {
		b.closeMu.Unlock()
		return
	}
	b.connected = false
	b.recovering = true
	producer := b.producer
	consumer := b.consumer
	b.producer = nil
	b.consumer = nil
	b.watchMu.Lock()
	delete(b.watching, consumer)
	b.watchMu.Unlock()
	b.closeMu.Unlock()

	if producer != nil {
		_ = producer.Close()
	}
	if consumer != nil {
		_ = consumer.Close()
	}

	b.cancelAllPending()
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelWarn,
		Name:      core.EventDisconnect,
		Operation: "connection_lost",
		Err:       cause,
		Fields:    map[string]any{"outcome": "failure"},
	})
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
	}
	b.subs = append(b.subs, reg)
	return reg
}

func (b *Broker) unregisterSubscription(id uint64) {
	if id == 0 {
		return
	}
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	for i, sub := range b.subs {
		if sub.id == id {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			return
		}
	}
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

func (b *Broker) currentProducer() sarama.SyncProducer {
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
				}
			}
		}
	}()
}

type consumerGroupHandler struct {
	handler   core.Handler
	routes    map[string]subscriptionRoute
	closeChan chan struct{}
	workers   chan struct{}
	pending   map[string]*pendingCall
	pendingMu *sync.RWMutex
	config    *core.Config
	policy    core.HandlerErrorPolicy
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
	h.config.EmitEvent(context.Background(), core.Event{
		Level:       core.EventLevelError,
		Name:        core.EventSubscribeFailed,
		Backend:     backendName,
		Component:   "transport",
		Operation:   "handler",
		Destination: destination,
		Err:         err,
	})
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
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

			if correlationID != "" {
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
			err := func() error {
				if route.workers != nil {
					defer func() { <-route.workers }()
				}
				handlerCtx, release := mergeHandlerContexts(session.Context(), route.ctx)
				defer release()
				return callSubscriptionHandler(handlerCtx, route.handler, msg)
			}()
			if err != nil {
				h.emitSubscribeFailure(kafkaMsg.Topic, err)
				if route.policy == core.HandlerErrorRetry {
					return err
				}
			}

			session.MarkMessage(kafkaMsg, "")
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
