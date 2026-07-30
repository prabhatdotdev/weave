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
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

const backendName = "amqp"

type amqpConnection interface {
	Channel() (amqpChannel, error)
	NotifyClose(chan *amqplib.Error) chan *amqplib.Error
	Close() error
}

type amqpChannel interface {
	PublishWithContext(context.Context, string, string, bool, bool, amqplib.Publishing) error
	QueueDeclare(string, bool, bool, bool, bool, amqplib.Table) (amqplib.Queue, error)
	QueueBind(string, string, string, bool, amqplib.Table) error
	Qos(int, int, bool) error
	Consume(string, string, bool, bool, bool, bool, amqplib.Table) (<-chan amqplib.Delivery, error)
	Close() error
}

type amqpConnectionAdapter struct {
	conn *amqplib.Connection
}

func (a *amqpConnectionAdapter) Channel() (amqpChannel, error) {
	channel, err := a.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &amqpChannelAdapter{channel: channel}, nil
}

func (a *amqpConnectionAdapter) NotifyClose(ch chan *amqplib.Error) chan *amqplib.Error {
	return a.conn.NotifyClose(ch)
}

func (a *amqpConnectionAdapter) Close() error {
	return a.conn.Close()
}

type amqpChannelAdapter struct {
	channel *amqplib.Channel
}

func (a *amqpChannelAdapter) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqplib.Publishing) error {
	return a.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (a *amqpChannelAdapter) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error) {
	return a.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (a *amqpChannelAdapter) QueueBind(name, key, exchange string, noWait bool, args amqplib.Table) error {
	return a.channel.QueueBind(name, key, exchange, noWait, args)
}

func (a *amqpChannelAdapter) Qos(prefetchCount, prefetchSize int, global bool) error {
	return a.channel.Qos(prefetchCount, prefetchSize, global)
}

func (a *amqpChannelAdapter) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqplib.Table) (<-chan amqplib.Delivery, error) {
	return a.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (a *amqpChannelAdapter) Close() error {
	return a.channel.Close()
}

type subscriptionRegistration struct {
	ctx         context.Context
	destination string
	handler     core.Handler
	opts        []core.SubscribeOption
}

// Broker implements the core.MessageBroker interface for AMQP/RabbitMQ.
type Broker struct {
	config     *core.Config
	amqpConfig *core.AMQPConfig
	conn       amqpConnection
	channel    amqpChannel
	dial       func(string, amqplib.Config) (amqpConnection, error)

	replyQueue        string
	replyConsumerStop chan struct{}
	pending           map[string]chan *amqplib.Delivery
	pendingMu         sync.RWMutex
	subs              []subscriptionRegistration
	subsMu            sync.RWMutex

	connected  bool
	recovering bool
	closed     bool
	closeMu    sync.RWMutex
	closeOnce  sync.Once
	closeChan  chan struct{}
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

// NewBroker creates a new AMQP broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	if config == nil {
		config = core.DefaultConfig()
	}
	if config.AMQP == nil {
		config.AMQP = core.DefaultAMQPConfig()
	}

	return &Broker{
		config:     config,
		amqpConfig: config.AMQP,
		dial: func(connString string, config amqplib.Config) (amqpConnection, error) {
			conn, err := amqplib.DialConfig(connString, config)
			if err != nil {
				return nil, err
			}
			return &amqpConnectionAdapter{conn: conn}, nil
		},
		pending:   make(map[string]chan *amqplib.Delivery),
		closeChan: make(chan struct{}),
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
	return nil
}

func (b *Broker) connect() error {
	var conn amqpConnection
	var err error

	connString := b.connectionString()
	retries := b.config.ConnectionRetry
	if retries <= 0 {
		retries = 3
	}

	for i := 0; i <= retries; i++ {
		conn, err = b.dial(connString, amqplib.Config{
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

	if err != nil || conn == nil {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelError,
			Name:      core.EventConnect,
			Operation: "connect",
			Err:       err,
			Fields: map[string]any{
				"address": fmt.Sprintf("%s:%d", b.amqpConfig.Host, b.amqpConfig.Port),
				"outcome": "failure",
			},
		})
		return &core.ErrConnectionFailed{
			Backend: backendName,
			Address: fmt.Sprintf("%s:%d", b.amqpConfig.Host, b.amqpConfig.Port),
			Cause:   err,
		}
	}

	channel, err := conn.Channel()
	if err != nil {
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelError,
			Name:      core.EventConnect,
			Operation: "connect",
			Err:       err,
			Fields:    map[string]any{"outcome": "failure", "stage": "open_channel"},
		})
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	b.conn = conn
	b.channel = channel
	b.connected = true
	b.replyQueue = ""
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelInfo,
		Name:      core.EventConnect,
		Operation: "connect",
		Fields:    map[string]any{"outcome": "success"},
	})

	go b.monitorConnection(conn)

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

func (b *Broker) monitorConnection(conn amqpConnection) {
	notifyClose := conn.NotifyClose(make(chan *amqplib.Error, 1))

	select {
	case connErr, ok := <-notifyClose:
		if !ok {
			return
		}
		if b.handleConnectionLoss(conn, connErr) {
			b.emitEvent(context.Background(), core.Event{
				Level:     core.EventLevelWarn,
				Name:      core.EventDisconnect,
				Operation: "connection_lost",
				Err:       connErr,
				Fields:    map[string]any{"outcome": "failure"},
			})
			b.reconnectLoop()
		}
	case <-b.closeChan:
		return
	}
}

func (b *Broker) handleConnectionLoss(conn amqpConnection, connErr *amqplib.Error) bool {
	b.closeMu.Lock()
	if b.closed || b.conn != conn {
		b.closeMu.Unlock()
		return false
	}
	b.stopReplyConsumerLocked()
	b.connected = false
	b.recovering = true
	b.conn = nil
	b.channel = nil
	b.replyQueue = ""
	b.closeMu.Unlock()

	b.cancelAllPending()

	// Emit connection loss event
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelWarn,
		Name:      core.EventDisconnect,
		Operation: "handle_connection_loss",
		Err:       connErr,
	})

	return true
}

func (b *Broker) reconnectLoop() {
	// Emit reconnect started event
	b.emitEvent(context.Background(), core.Event{
		Level:     core.EventLevelInfo,
		Name:      "reconnect_started",
		Operation: "reconnect_loop",
	})

	attempt := 0
	for {
		b.closeMu.RLock()
		if b.closed || b.connected {
			b.closeMu.RUnlock()
			return
		}
		b.closeMu.RUnlock()

		attempt++
		b.closeMu.Lock()
		if b.closed || b.connected {
			b.closeMu.Unlock()
			return
		}

		// Emit reconnect attempt event
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelDebug,
			Name:      "reconnect_attempt",
			Operation: "connect",
			Fields: map[string]any{
				"attempt": attempt,
			},
		})

		err := b.connect()
		b.closeMu.Unlock()
		if err == nil {
			if restoreErr := b.restoreSubscriptions(); restoreErr != nil {
				b.emitEvent(context.Background(), core.Event{
					Level:       core.EventLevelError,
					Name:        core.EventSubscribeFailed,
					Operation:   "restore_subscriptions",
					Destination: "*",
					Err:         restoreErr,
				})
				b.closeMu.RLock()
				conn := b.conn
				b.closeMu.RUnlock()
				if conn != nil {
					b.handleConnectionLoss(conn, nil)
				}
				continue
			}
			b.closeMu.Lock()
			b.recovering = false
			b.closeMu.Unlock()

			// Emit reconnect success event
			b.emitEvent(context.Background(), core.Event{
				Level:     core.EventLevelInfo,
				Name:      "reconnect_succeeded",
				Operation: "reconnect_loop",
				Fields: map[string]any{
					"attempts": attempt,
				},
			})
			return
		}

		// Emit reconnect attempt failure event
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelWarn,
			Name:      "reconnect_attempt_failed",
			Operation: "connect",
			Err:       err,
			Fields: map[string]any{
				"attempt": attempt,
			},
		})

		delay := b.config.RetryDelay
		if delay == 0 {
			delay = 2 * time.Second
		}

		select {
		case <-time.After(delay):
		case <-b.closeChan:
			return
		}
	}
}

func (b *Broker) restoreSubscriptions() error {
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
		if err := b.subscribe(sub.ctx, sub.destination, sub.handler, false, true, sub.opts...); err != nil {
			b.emitEvent(context.Background(), core.Event{
				Level:       core.EventLevelError,
				Name:        "subscription_restore_failed",
				Operation:   "restore_subscriptions",
				Destination: sub.destination,
				Err:         err,
			})
			return err
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

	return nil
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

// Close gracefully shuts down the broker connection.
func (b *Broker) Close() error {
	var err error
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		b.stopReplyConsumerLocked()
		b.closed = true
		b.connected = false
		b.recovering = false
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

		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelInfo,
			Name:      core.EventDisconnect,
			Operation: "close",
			Fields:    map[string]any{"outcome": "success", "reason": "close"},
		})
	})
	return err
}

// Publish sends a message to the specified queue.
func (b *Broker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	channel, err := b.currentChannel(false)
	if err != nil {
		return err
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

	err = channel.PublishWithContext(ctx, exchange, routingKey, options.Mandatory, options.Immediate, publishing)
	if err != nil {
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventPublishFailed,
			Operation:   "publish",
			Destination: destination,
			Err:         err,
		})
		return &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	return nil
}

// Subscribe registers a handler for messages from the specified queue.
func (b *Broker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	if err := b.subscribe(ctx, destination, handler, true, false, opts...); err != nil {
		return err
	}

	return nil
}

func (b *Broker) subscribe(ctx context.Context, destination string, handler core.Handler, track bool, allowRecovering bool, opts ...core.SubscribeOption) error {
	options := core.ApplySubscribeOptions(opts...)
	if options.WorkerCount < 0 {
		return fmt.Errorf("%w: worker count must not be negative", core.ErrInvalidConfig)
	}

	channel, err := b.currentChannel(allowRecovering)
	if err != nil {
		return err
	}

	q, err := channel.QueueDeclare(
		destination,
		b.amqpConfig.QueueDurable,
		b.amqpConfig.QueueAutoDelete,
		options.Exclusive || b.amqpConfig.QueueExclusive,
		false,
		nil,
	)
	if err != nil {
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventSubscribeFailed,
			Operation:   "queue_declare",
			Destination: destination,
			Err:         err,
		})
		return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	if options.QueueBind && options.Exchange != "" {
		routingKey := options.RoutingKey
		if routingKey == "" {
			routingKey = destination
		}
		if err := channel.QueueBind(q.Name, routingKey, options.Exchange, false, nil); err != nil {
			b.emitEvent(ctx, core.Event{
				Level:       core.EventLevelError,
				Name:        core.EventSubscribeFailed,
				Operation:   "queue_bind",
				Destination: destination,
				Err:         err,
			})
			return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
		}
	}

	prefetch := options.PrefetchCount
	if prefetch == 0 {
		prefetch = 1
		if options.WorkerCount > 0 {
			prefetch = options.WorkerCount
		}
	}
	if err := channel.Qos(prefetch, 0, false); err != nil {
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventSubscribeFailed,
			Operation:   "qos",
			Destination: destination,
			Err:         err,
		})
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := channel.Consume(q.Name, options.ConsumerTag, options.AutoAck, options.Exclusive, false, false, nil)
	if err != nil {
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventSubscribeFailed,
			Operation:   "consume",
			Destination: destination,
			Err:         err,
		})
		return &core.ErrSubscribeFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	if track {
		b.subsMu.Lock()
		b.subs = append(b.subs, subscriptionRegistration{ctx: ctx, destination: destination, handler: handler, opts: append([]core.SubscribeOption(nil), opts...)})
		b.subsMu.Unlock()
	}

	consume := func(concurrent bool) {
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
				if ctx.Err() != nil {
					return
				}
				if concurrent {
					go b.handleMessage(ctx, msg, handler, options.AutoAck, options.HandlerErrorPolicy)
				} else {
					b.handleMessage(ctx, msg, handler, options.AutoAck, options.HandlerErrorPolicy)
				}
			}
		}
	}

	if options.WorkerCount == 0 {
		go consume(true)
	} else {
		for range options.WorkerCount {
			go consume(false)
		}
	}

	return nil
}

func (b *Broker) handleMessage(ctx context.Context, msg amqplib.Delivery, handler core.Handler, autoAck bool, policy core.HandlerErrorPolicy) {
	defer func() {
		if r := recover(); r != nil {
			b.emitEvent(ctx, core.Event{
				Level:     core.EventLevelError,
				Name:      core.EventSubscribeFailed,
				Operation: "handler_panic",
				Err:       fmt.Errorf("panic: %v", r),
			})
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
			requeue := policy == core.HandlerErrorRetry
			msg.Nack(false, requeue)
		} else {
			msg.Ack(false)
		}
	}
}

// Call implements the request-response pattern.
func (b *Broker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	channel, err := b.currentChannel(false)
	if err != nil {
		return nil, err
	}

	options := core.ApplyPublishOptions(opts...)

	if err := b.ensureReplyQueue(channel); err != nil {
		return nil, err
	}

	corrID := msg.CorrelationID
	if corrID == "" {
		corrID = rand.Text()
	}

	respChan := make(chan *amqplib.Delivery, 1)
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

	exchange := options.Exchange
	if exchange == "" {
		exchange = b.amqpConfig.Exchange
	}

	publishing := amqplib.Publishing{
		ContentType:   msg.ContentType,
		CorrelationId: corrID,
		ReplyTo:       b.currentReplyQueue(),
		Body:          msg.Body,
	}
	if len(msg.Headers) > 0 {
		publishing.Headers = make(amqplib.Table, len(msg.Headers))
		for key, value := range msg.Headers {
			publishing.Headers[key] = value
		}
	}
	err = channel.PublishWithContext(ctx, exchange, destination, false, false, publishing)
	if err != nil {
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventPublishFailed,
			Operation:   "call_publish",
			Destination: destination,
			Err:         err,
		})
		return nil, &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
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

func (b *Broker) ensureReplyQueue(channel amqpChannel) error {
	b.closeMu.RLock()
	if b.replyQueue != "" {
		b.closeMu.RUnlock()
		return nil
	}
	b.closeMu.RUnlock()

	q, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare reply queue: %w", err)
	}

	msgs, err := channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume reply queue: %w", err)
	}

	stop := make(chan struct{})
	b.closeMu.Lock()
	if b.replyQueue != "" {
		b.closeMu.Unlock()
		close(stop)
		return nil
	}
	b.replyQueue = q.Name
	b.replyConsumerStop = stop
	b.closeMu.Unlock()

	go b.handleResponses(msgs, stop)

	return nil
}

func (b *Broker) handleResponses(msgs <-chan amqplib.Delivery, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			corrID := msg.CorrelationId
			b.pendingMu.RLock()
			respChan, exists := b.pending[corrID]
			b.pendingMu.RUnlock()

			if exists {
				respChan <- &msg
			}
		}
	}
}

func (b *Broker) cancelAllPending() {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	for key, ch := range b.pending {
		close(ch)
		delete(b.pending, key)
	}
}

func (b *Broker) currentChannel(allowRecovering bool) (amqpChannel, error) {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()

	if b.closed || (!allowRecovering && b.recovering) || !b.connected || b.channel == nil {
		return nil, &core.ErrNotConnected{Backend: backendName}
	}

	return b.channel, nil
}

func (b *Broker) currentReplyQueue() string {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.replyQueue
}

func (b *Broker) stopReplyConsumerLocked() {
	if b.replyConsumerStop != nil {
		close(b.replyConsumerStop)
		b.replyConsumerStop = nil
	}
}
