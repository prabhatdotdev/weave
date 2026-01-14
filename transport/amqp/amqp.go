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

// Package amqp provides an AMQP/RabbitMQ transport implementation for Weave.
package amqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

const backendName = "amqp"

func init() {
	core.Register(backendName, NewBroker)
}

// Broker implements the core.MessageBroker interface for AMQP/RabbitMQ.
type Broker struct {
	config     *core.Config
	amqpConfig *core.AMQPConfig
	conn       *amqplib.Connection
	channel    *amqplib.Channel

	replyQueue string
	pending    map[string]chan *amqplib.Delivery
	pendingMu  sync.RWMutex

	connected bool
	closed    bool
	closeMu   sync.RWMutex
	closeOnce sync.Once
	closeChan chan struct{}
}

// NewBroker creates a new AMQP broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	if config.AMQP == nil {
		if config.Host != "" {
			config.AMQP = &core.AMQPConfig{
				Host:     config.Host,
				Port:     config.Port,
				Username: config.Username,
				Password: config.Password,
				VHost:    config.VHost,
			}
			if config.Port == 0 {
				config.AMQP.Port = 5672
			}
			if config.Username == "" {
				config.AMQP.Username = "guest"
			}
			if config.Password == "" {
				config.AMQP.Password = "guest"
			}
			if config.VHost == "" {
				config.AMQP.VHost = "/"
			}
		} else {
			config.AMQP = core.DefaultAMQPConfig()
		}
	}

	return &Broker{
		config:     config,
		amqpConfig: config.AMQP,
		pending:    make(map[string]chan *amqplib.Delivery),
		closeChan:  make(chan struct{}),
	}, nil
}

// Backend returns the name of this backend.
func (b *Broker) Backend() string {
	return backendName
}

// Connect establishes a connection to the AMQP broker.
func (b *Broker) Connect(ctx context.Context) error {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	if b.closed {
		return core.ErrClosed
	}
	if b.connected {
		return nil
	}

	if err := b.connect(); err != nil {
		return err
	}

	b.connected = true
	return nil
}

func (b *Broker) connect() error {
	var conn *amqplib.Connection
	var err error

	connString := b.connectionString()
	retries := b.config.ConnectionRetry
	if retries <= 0 {
		retries = 3
	}

	for i := 0; i <= retries; i++ {
		conn, err = amqplib.DialConfig(connString, amqplib.Config{
			Heartbeat: b.amqpConfig.Heartbeat,
			Properties: amqplib.Table{
				"connection_name": b.config.ConnectionName,
			},
		})
		if err == nil {
			break
		}
		if i < retries {
			delay := b.config.RetryDelay
			if delay == 0 {
				delay = 2 * time.Second
			}
			time.Sleep(delay)
		}
	}

	if err != nil {
		return &core.ErrConnectionFailed{
			Backend: backendName,
			Address: fmt.Sprintf("%s:%d", b.amqpConfig.Host, b.amqpConfig.Port),
			Cause:   err,
		}
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	b.conn = conn
	b.channel = channel

	go b.monitorConnection()

	return nil
}

func (b *Broker) connectionString() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		b.amqpConfig.Username,
		b.amqpConfig.Password,
		b.amqpConfig.Host,
		b.amqpConfig.Port,
		b.amqpConfig.VHost,
	)
}

func (b *Broker) monitorConnection() {
	select {
	case connErr := <-b.conn.NotifyClose(make(chan *amqplib.Error)):
		b.closeMu.Lock()
		if !b.closed {
			b.connected = false
			fmt.Printf("[amqp] Connection lost: %v\n", connErr)
			b.cancelAllPending()
		}
		b.closeMu.Unlock()
	case <-b.closeChan:
		return
	}
}

// IsConnected returns true if the broker is connected.
func (b *Broker) IsConnected() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.connected && !b.closed
}

// Close gracefully shuts down the broker connection.
func (b *Broker) Close() error {
	var err error
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		b.closed = true
		b.connected = false
		b.closeMu.Unlock()

		close(b.closeChan)
		b.cancelAllPending()

		if b.channel != nil {
			if e := b.channel.Close(); e != nil {
				err = e
			}
		}
		if b.conn != nil {
			if e := b.conn.Close(); e != nil && err == nil {
				err = e
			}
		}
	})
	return err
}

// Publish sends a message to the specified queue.
func (b *Broker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	if !b.IsConnected() {
		return &core.ErrNotConnected{Backend: backendName}
	}

	options := core.ApplyPublishOptions(opts...)

	publishing := amqplib.Publishing{
		ContentType:   msg.ContentType,
		CorrelationId: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		Body:          msg.Body,
		Timestamp:     msg.Timestamp,
		MessageId:     msg.MessageID,
		DeliveryMode:  options.DeliveryMode,
		Priority:      options.Priority,
		Expiration:    options.Expiration,
	}

	if publishing.ContentType == "" {
		publishing.ContentType = "application/octet-stream"
	}

	if len(msg.Headers) > 0 {
		publishing.Headers = make(amqplib.Table)
		for k, v := range msg.Headers {
			publishing.Headers[k] = v
		}
	}

	exchange := options.Exchange
	if exchange == "" {
		exchange = b.amqpConfig.Exchange
	}

	routingKey := destination
	if msg.Subject != "" {
		routingKey = msg.Subject
	}

	err := b.channel.PublishWithContext(ctx, exchange, routingKey, options.Mandatory, options.Immediate, publishing)
	if err != nil {
		return &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	return nil
}

// Subscribe registers a handler for messages from the specified queue.
func (b *Broker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	if !b.IsConnected() {
		return &core.ErrNotConnected{Backend: backendName}
	}

	options := core.ApplySubscribeOptions(opts...)

	q, err := b.channel.QueueDeclare(
		destination,
		b.amqpConfig.QueueDurable,
		b.amqpConfig.QueueAutoDelete,
		options.Exclusive || b.amqpConfig.QueueExclusive,
		false,
		nil,
	)
	if err != nil {
		return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	if options.QueueBind && options.Exchange != "" {
		routingKey := options.RoutingKey
		if routingKey == "" {
			routingKey = destination
		}
		if err := b.channel.QueueBind(q.Name, routingKey, options.Exchange, false, nil); err != nil {
			return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
		}
	}

	prefetch := options.PrefetchCount
	if prefetch == 0 {
		prefetch = 1
	}
	if err := b.channel.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := b.channel.Consume(q.Name, options.ConsumerTag, options.AutoAck, options.Exclusive, false, false, nil)
	if err != nil {
		return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	go func() {
		for {
			select {
			case <-b.closeChan:
				return
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				go b.handleMessage(ctx, msg, handler, options.AutoAck)
			}
		}
	}()

	return nil
}

func (b *Broker) handleMessage(ctx context.Context, msg amqplib.Delivery, handler core.Handler, autoAck bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[amqp] Panic in handler: %v\n", r)
			if !autoAck {
				msg.Nack(false, false)
			}
		}
	}()

	coreMsg := &core.Message{
		Body:          msg.Body,
		CorrelationID: msg.CorrelationId,
		ReplyTo:       msg.ReplyTo,
		ContentType:   msg.ContentType,
		MessageID:     msg.MessageId,
		Timestamp:     msg.Timestamp,
	}

	if msg.Headers != nil {
		coreMsg.Headers = make(map[string]string)
		for k, v := range msg.Headers {
			if s, ok := v.(string); ok {
				coreMsg.Headers[k] = s
			}
		}
	}

	err := handler(ctx, coreMsg)

	if !autoAck {
		if err != nil {
			msg.Nack(false, true)
		} else {
			msg.Ack(false)
		}
	}
}

// Call implements the request-response pattern.
func (b *Broker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	if !b.IsConnected() {
		return nil, &core.ErrNotConnected{Backend: backendName}
	}

	options := core.ApplyPublishOptions(opts...)

	if err := b.ensureReplyQueue(); err != nil {
		return nil, err
	}

	corrID := msg.CorrelationID
	if corrID == "" {
		corrID = uuid.New().String()
	}

	respChan := make(chan *amqplib.Delivery, 1)
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

	exchange := options.Exchange
	if exchange == "" {
		exchange = b.amqpConfig.Exchange
	}

	err := b.channel.PublishWithContext(ctx, exchange, destination, false, false,
		amqplib.Publishing{
			ContentType:   msg.ContentType,
			CorrelationId: corrID,
			ReplyTo:       b.replyQueue,
			Body:          msg.Body,
		},
	)
	if err != nil {
		return nil, &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	select {
	case <-ctx.Done():
		return nil, &core.ErrTimeout{Operation: "Call", Duration: options.Timeout.String()}
	case response := <-respChan:
		if response == nil {
			return nil, &core.ErrConnectionLost{Backend: backendName}
		}
		return &core.Message{
			Body:          response.Body,
			CorrelationID: response.CorrelationId,
			ContentType:   response.ContentType,
			Timestamp:     response.Timestamp,
		}, nil
	}
}

func (b *Broker) ensureReplyQueue() error {
	if b.replyQueue != "" {
		return nil
	}

	q, err := b.channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare reply queue: %w", err)
	}

	b.replyQueue = q.Name

	msgs, err := b.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume reply queue: %w", err)
	}

	go b.handleResponses(msgs)

	return nil
}

func (b *Broker) handleResponses(msgs <-chan amqplib.Delivery) {
	for msg := range msgs {
		corrID := msg.CorrelationId
		b.pendingMu.RLock()
		respChan, exists := b.pending[corrID]
		b.pendingMu.RUnlock()

		if exists {
			respChan <- &msg
		}
	}
}

func (b *Broker) cancelAllPending() {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for _, ch := range b.pending {
		close(ch)
	}
	b.pending = make(map[string]chan *amqplib.Delivery)
}
