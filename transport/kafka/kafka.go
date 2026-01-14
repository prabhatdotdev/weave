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

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/prabhatdotdev/weave/core"
)

const backendName = "kafka"

func init() {
	core.Register(backendName, NewBroker)
}

// Broker implements the core.MessageBroker interface for Apache Kafka.
type Broker struct {
	config      *core.Config
	kafkaConfig *core.KafkaConfig

	producer sarama.SyncProducer
	consumer sarama.ConsumerGroup

	replyTopic string
	pending    map[string]chan *core.Message
	pendingMu  sync.RWMutex

	connected bool
	closed    bool
	closeMu   sync.RWMutex
	closeOnce sync.Once
	closeChan chan struct{}
}

// NewBroker creates a new Kafka broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	if config.Kafka == nil {
		config.Kafka = core.DefaultKafkaConfig()
	}

	return &Broker{
		config:      config,
		kafkaConfig: config.Kafka,
		pending:     make(map[string]chan *core.Message),
		closeChan:   make(chan struct{}),
	}, nil
}

// Backend returns the name of this backend.
func (b *Broker) Backend() string {
	return backendName
}

// Connect establishes a connection to the Kafka cluster.
func (b *Broker) Connect(ctx context.Context) error {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return core.ErrClosed
	}
	if b.connected {
		return nil
	}

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

	producer, err := sarama.NewSyncProducer(b.kafkaConfig.Brokers, saramaConfig)
	if err != nil {
		return &core.ErrConnectionFailed{
			Backend: backendName,
			Address: fmt.Sprintf("%v", b.kafkaConfig.Brokers),
			Cause:   err,
		}
	}
	b.producer = producer

	if b.kafkaConfig.ConsumerGroup != "" {
		consumer, err := sarama.NewConsumerGroup(b.kafkaConfig.Brokers, b.kafkaConfig.ConsumerGroup, saramaConfig)
		if err != nil {
			producer.Close()
			return &core.ErrConnectionFailed{
				Backend: backendName,
				Address: fmt.Sprintf("%v", b.kafkaConfig.Brokers),
				Cause:   err,
			}
		}
		b.consumer = consumer
	}

	b.connected = true
	return nil
}

// IsConnected returns true if the broker is connected.
func (b *Broker) IsConnected() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.connected && !b.closed
}

// Close gracefully shuts down the Kafka connections.
func (b *Broker) Close() error {
	var errs []error
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		b.closed = true
		b.connected = false
		b.closeMu.Unlock()

		close(b.closeChan)
		b.cancelAllPending()

		if b.producer != nil {
			if err := b.producer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if b.consumer != nil {
			if err := b.consumer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	})

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Publish sends a message to the specified Kafka topic.
func (b *Broker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	if !b.IsConnected() {
		return &core.ErrNotConnected{Backend: backendName}
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

	_, _, err := b.producer.SendMessage(kafkaMsg)
	if err != nil {
		return &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	return nil
}

// Subscribe registers a handler for messages from the specified Kafka topic.
func (b *Broker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	if !b.IsConnected() {
		return &core.ErrNotConnected{Backend: backendName}
	}

	options := core.ApplySubscribeOptions(opts...)

	consumerGroup := options.ConsumerGroup
	if consumerGroup == "" {
		consumerGroup = b.kafkaConfig.ConsumerGroup
	}

	if b.consumer == nil && consumerGroup == "" {
		return &core.ErrSubscribeFailed{
			Backend:     backendName,
			Destination: destination,
			Cause:       fmt.Errorf("consumer group not configured"),
		}
	}

	cgHandler := &consumerGroupHandler{
		handler:   handler,
		closeChan: b.closeChan,
		pending:   b.pending,
		pendingMu: &b.pendingMu,
	}

	go func() {
		for {
			select {
			case <-b.closeChan:
				return
			case <-ctx.Done():
				return
			default:
				if err := b.consumer.Consume(ctx, []string{destination}, cgHandler); err != nil {
					fmt.Printf("[kafka] Consumer error: %v\n", err)
				}
			}
		}
	}()

	return nil
}

// Call implements request-response pattern using correlation IDs and reply topics.
func (b *Broker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	if !b.IsConnected() {
		return nil, &core.ErrNotConnected{Backend: backendName}
	}

	options := core.ApplyPublishOptions(opts...)

	if err := b.ensureReplyConsumer(ctx); err != nil {
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
		close(respChan)
	}()

	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}

	msg.CorrelationID = corrID
	msg.ReplyTo = b.replyTopic

	if err := b.Publish(ctx, destination, msg, opts...); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, &core.ErrTimeout{Operation: "Call", Duration: options.Timeout.String()}
	case response := <-respChan:
		if response == nil {
			return nil, &core.ErrConnectionLost{Backend: backendName}
		}
		return response, nil
	}
}

func (b *Broker) ensureReplyConsumer(ctx context.Context) error {
	if b.replyTopic != "" {
		return nil
	}

	b.replyTopic = fmt.Sprintf("reply-%s-%s", b.kafkaConfig.ClientID, uuid.New().String()[:8])

	return b.Subscribe(ctx, b.replyTopic, func(ctx context.Context, msg *core.Message) error {
		return nil
	})
}

func (b *Broker) cancelAllPending() {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for _, ch := range b.pending {
		close(ch)
	}
	b.pending = make(map[string]chan *core.Message)
}

type consumerGroupHandler struct {
	handler   core.Handler
	closeChan chan struct{}
	pending   map[string]chan *core.Message
	pendingMu *sync.RWMutex
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

			err := h.handler(ctx, msg)
			if err != nil {
				fmt.Printf("[kafka] Handler error: %v\n", err)
			}

			session.MarkMessage(kafkaMsg, "")
		}
	}
}
