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
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/prabhatdotdev/weave/core"
)

const backendName = "kafka"

type subscriptionRegistration struct {
	id          uint64
	ctx         context.Context
	destination string
	handler     core.Handler
	opts        []core.SubscribeOption
}

func init() {
	core.Register(backendName, NewBroker)
}

// Broker implements the core.MessageBroker interface for Apache Kafka.
type Broker struct {
	config      *core.Config
	kafkaConfig *core.KafkaConfig

	producer         sarama.SyncProducer
	consumer         sarama.ConsumerGroup
	newSyncProducer  func([]string, *sarama.Config) (sarama.SyncProducer, error)
	newConsumerGroup func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error)

	replyTopic string
	pending    map[string]chan *core.Message
	pendingMu  sync.RWMutex
	subs       []subscriptionRegistration
	subsMu     sync.RWMutex
	nextSubID  uint64
	runnersMu  sync.Mutex
	runners    map[uint64]struct{}
	watchMu    sync.Mutex
	watching   map[sarama.ConsumerGroup]struct{}

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

func (b *Broker) emitCounter(name string, labels map[string]string) {
	if b.config == nil {
		return
	}
	b.config.EmitCounter(name, 1, labels)
}

func (b *Broker) emitDuration(name string, value time.Duration, labels map[string]string) {
	if b.config == nil {
		return
	}
	b.config.EmitDuration(name, value, labels)
}

// NewBroker creates a new Kafka broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	if config.Kafka == nil {
		config.Kafka = core.DefaultKafkaConfig()
	}

	return &Broker{
		config:           config,
		kafkaConfig:      config.Kafka,
		newSyncProducer:  sarama.NewSyncProducer,
		newConsumerGroup: sarama.NewConsumerGroup,
		pending:          make(map[string]chan *core.Message),
		runners:          make(map[uint64]struct{}),
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
	start := time.Now()
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
				b.emitCounter("weave.transport.connect.success", map[string]string{"backend": backendName})
				b.emitDuration("weave.transport.connect.duration", time.Since(start), map[string]string{"backend": backendName, "outcome": "success"})
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
	b.emitCounter("weave.transport.connect.failures", map[string]string{"backend": backendName})
	b.emitDuration("weave.transport.connect.duration", time.Since(start), map[string]string{"backend": backendName, "outcome": "failure"})

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
		b.emitCounter("weave.transport.disconnect.events", map[string]string{"backend": backendName, "reason": "close"})
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
		b.emitCounter("weave.transport.publish.failures", map[string]string{"backend": backendName, "destination": destination})
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

	var reg subscriptionRegistration
	if track {
		reg = b.registerSubscription(ctx, destination, handler, opts)
	}

	if err := b.startSubscriptionConsumer(ctx, destination, handler, options, reg.id); err != nil {
		if track {
			b.unregisterSubscription(reg.id)
		}
		return err
	}

	return nil
}

func (b *Broker) startSubscriptionConsumer(ctx context.Context, destination string, handler core.Handler, options *core.SubscribeOptions, subID uint64) error {
	if err := b.ensureConnected(); err != nil {
		return err
	}

	if subID != 0 && !b.startRunner(subID) {
		return nil
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
		b.emitCounter("weave.transport.subscribe.failures", map[string]string{"backend": backendName, "destination": destination, "stage": "config"})
		return &core.ErrSubscribeFailed{
			Backend:     backendName,
			Destination: destination,
			Cause:       err,
		}
	}

	cgHandler := &consumerGroupHandler{
		handler:   handler,
		closeChan: b.closeChan,
		pending:   b.pending,
		pendingMu: &b.pendingMu,
		config:    b.config,
		policy:    options.HandlerErrorPolicy,
	}
	if options.WorkerCount > 0 {
		cgHandler.workers = make(chan struct{}, options.WorkerCount)
	}

	go func() {
		defer b.stopRunner(subID)
		for {
			select {
			case <-b.closeChan:
				return
			case <-ctx.Done():
				return
			default:
				consumer := b.currentConsumer()
				if consumer == nil {
					if err := b.ensureConnected(); err != nil {
						b.emitEvent(ctx, core.Event{
							Level:       core.EventLevelWarn,
							Name:        core.EventConnect,
							Operation:   "reconnect",
							Destination: destination,
							Err:         err,
							Fields:      map[string]any{"outcome": "failure"},
						})
						b.emitCounter("weave.transport.connect.failures", map[string]string{"backend": backendName, "stage": "reconnect"})
						time.Sleep(200 * time.Millisecond)
						continue
					}
					consumer = b.currentConsumer()
					if consumer == nil {
						time.Sleep(200 * time.Millisecond)
						continue
					}
				}

				b.watchConsumerErrors(ctx, destination, consumer)

				if err := consumer.Consume(ctx, []string{destination}, cgHandler); err != nil {
					b.emitEvent(ctx, core.Event{
						Level:       core.EventLevelError,
						Name:        core.EventSubscribeFailed,
						Operation:   "consume",
						Destination: destination,
						Err:         err,
					})
					b.emitCounter("weave.transport.subscribe.failures", map[string]string{"backend": backendName, "destination": destination, "stage": "consume"})
					if ctx.Err() != nil || b.isClosed() {
						return
					}
					b.handleConsumerLoss(consumer, err)
				}
			}
		}
	}()

	return nil
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

	corrID := msg.CorrelationID
	if corrID == "" {
		corrID = uuid.New().String()
	}

	respChan := make(chan *core.Message, 1)
	b.pendingMu.Lock()
	b.pending[corrID] = respChan
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
	callStart := time.Now()

	msg.CorrelationID = corrID
	msg.ReplyTo = b.currentReplyTopic()

	if err := b.Publish(ctx, destination, msg, opts...); err != nil {
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
		b.emitCounter("weave.transport.call.timeouts", map[string]string{"backend": backendName, "destination": destination})
		b.emitDuration("weave.transport.call.duration", time.Since(callStart), map[string]string{"backend": backendName, "destination": destination, "outcome": "timeout"})
		return nil, &core.ErrTimeout{Operation: "Call", Duration: options.Timeout.String()}
	case response := <-respChan:
		if response == nil {
			return nil, &core.ErrConnectionLost{Backend: backendName}
		}
		b.emitDuration("weave.transport.call.duration", time.Since(callStart), map[string]string{"backend": backendName, "destination": destination, "outcome": "success"})
		return response, nil
	}
}

func (b *Broker) ensureReplyConsumer() error {
	b.closeMu.Lock()
	if b.replyTopic != "" {
		b.closeMu.Unlock()
		return nil
	}

	replyTopic := fmt.Sprintf("reply-%s-%s", b.kafkaConfig.ClientID, uuid.New().String()[:8])
	b.replyTopic = replyTopic
	b.closeMu.Unlock()

	return b.subscribe(context.Background(), replyTopic, func(ctx context.Context, msg *core.Message) error {
		return nil
	}, false)
}

func (b *Broker) cancelAllPending() {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for key, ch := range b.pending {
		close(ch)
		delete(b.pending, key)
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
	b.emitCounter("weave.transport.reconnect.started", map[string]string{"backend": backendName})

	err := b.connectLocked()
	b.closeMu.Unlock()
	if err != nil {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelError,
			Name:      "reconnect_failed",
			Operation: "ensure_connected",
			Err:       err,
		})
		b.emitCounter("weave.transport.reconnect.attempt_failures", map[string]string{"backend": backendName})
		return err
	}

	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelInfo,
		Name:      "reconnect_succeeded",
		Operation: "ensure_connected",
	})
	b.emitCounter("weave.transport.reconnect.succeeded", map[string]string{"backend": backendName})

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
	b.replyTopic = ""
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
	b.emitCounter("weave.transport.disconnect.events", map[string]string{"backend": backendName, "reason": "connection_lost"})
}

func (b *Broker) registerSubscription(ctx context.Context, destination string, handler core.Handler, opts []core.SubscribeOption) subscriptionRegistration {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	b.nextSubID++
	reg := subscriptionRegistration{
		id:          b.nextSubID,
		ctx:         ctx,
		destination: destination,
		handler:     handler,
		opts:        append([]core.SubscribeOption(nil), opts...),
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
	subscriptions := append([]subscriptionRegistration(nil), b.subs...)
	b.subsMu.RUnlock()

	if len(subscriptions) > 0 {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      "subscription_restore_started",
			Operation: "restore_subscriptions",
			Fields: map[string]any{
				"subscription_count": len(subscriptions),
			},
		})
	}

	for _, sub := range subscriptions {
		if sub.ctx.Err() != nil {
			continue
		}
		options := core.ApplySubscribeOptions(sub.opts...)
		err := b.startSubscriptionConsumer(sub.ctx, sub.destination, sub.handler, options, sub.id)
		if err != nil {
			b.emitEvent(context.Background(), core.Event{
				Level:       core.EventLevelError,
				Name:        "subscription_restore_failed",
				Operation:   "restore_subscriptions",
				Destination: sub.destination,
				Err:         err,
			})
			b.emitCounter("weave.transport.subscription.restore.failures", map[string]string{"backend": backendName, "destination": sub.destination})
		} else {
			b.emitCounter("weave.transport.subscription.restore.success", map[string]string{"backend": backendName, "destination": sub.destination})
		}
	}

	if len(subscriptions) > 0 {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      "subscription_restore_completed",
			Operation: "restore_subscriptions",
			Fields: map[string]any{
				"subscription_count": len(subscriptions),
			},
		})
	}
}

func (b *Broker) startRunner(id uint64) bool {
	if id == 0 {
		return true
	}
	b.runnersMu.Lock()
	defer b.runnersMu.Unlock()
	if b.runners == nil {
		b.runners = make(map[uint64]struct{})
	}
	if _, exists := b.runners[id]; exists {
		return false
	}
	b.runners[id] = struct{}{}
	return true
}

func (b *Broker) stopRunner(id uint64) {
	if id == 0 {
		return
	}
	b.runnersMu.Lock()
	defer b.runnersMu.Unlock()
	delete(b.runners, id)
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
				b.emitCounter("weave.transport.subscribe.failures", map[string]string{"backend": backendName, "destination": destination, "stage": "consumer_errors"})
				if !b.isClosed() {
					b.handleConsumerLoss(consumer, err)
				}
			}
		}
	}()
}

type consumerGroupHandler struct {
	handler   core.Handler
	closeChan chan struct{}
	workers   chan struct{}
	pending   map[string]chan *core.Message
	pendingMu *sync.RWMutex
	config    *core.Config
	policy    core.HandlerErrorPolicy
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
	h.config.EmitCounter("weave.transport.subscribe.failures", 1, map[string]string{"backend": backendName, "destination": destination, "stage": "handler"})
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

			ctx := context.Background()

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
				respChan, exists := h.pending[correlationID]
				h.pendingMu.RUnlock()
				if exists {
					respChan <- msg
					session.MarkMessage(kafkaMsg, "")
					continue
				}
			}

			if h.workers != nil {
				select {
				case h.workers <- struct{}{}:
				case <-h.closeChan:
					return nil
				case <-session.Context().Done():
					return nil
				}
			}
			err := func() error {
				if h.workers != nil {
					defer func() { <-h.workers }()
				}
				return h.handler(ctx, msg)
			}()
			if err != nil {
				h.emitSubscribeFailure(kafkaMsg.Topic, err)
				if h.policy == core.HandlerErrorRetry {
					continue
				}
			}

			session.MarkMessage(kafkaMsg, "")
		}
	}
}
