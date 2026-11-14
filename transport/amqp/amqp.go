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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/connection"
	"github.com/prabhatdotdev/weave/internal/lifecycle"
	"github.com/prabhatdotdev/weave/internal/observability"
	"github.com/prabhatdotdev/weave/internal/readiness"
	"github.com/prabhatdotdev/weave/internal/rpclimit"
	"github.com/prabhatdotdev/weave/internal/validation"
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
	QueueDelete(string, bool, bool, bool) (int, error)
	QueueBind(string, string, string, bool, amqplib.Table) error
	Qos(int, int, bool) error
	Consume(string, string, bool, bool, bool, bool, amqplib.Table) (<-chan amqplib.Delivery, error)
	Cancel(string, bool) error
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
	publisher, err := newConfirmedPublisher(channel)
	if err != nil {
		_ = channel.Close()
		return nil, err
	}
	return &amqpChannelAdapter{channel: channel, publisher: publisher}, nil
}

func (a *amqpConnectionAdapter) NotifyClose(ch chan *amqplib.Error) chan *amqplib.Error {
	return a.conn.NotifyClose(ch)
}

func (a *amqpConnectionAdapter) Close() error {
	return a.conn.CloseDeadline(time.Now().Add(5 * time.Second))
}

type amqpChannelAdapter struct {
	channel   *amqplib.Channel
	publisher *confirmedPublisher
}

func (a *amqpChannelAdapter) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqplib.Publishing) error {
	return a.publisher.publish(ctx, exchange, key, mandatory, immediate, msg)
}

func (a *amqpChannelAdapter) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error) {
	a.publisher.wireMu.Lock()
	defer a.publisher.wireMu.Unlock()
	return a.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (a *amqpChannelAdapter) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	a.publisher.wireMu.Lock()
	defer a.publisher.wireMu.Unlock()
	return a.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (a *amqpChannelAdapter) QueueBind(name, key, exchange string, noWait bool, args amqplib.Table) error {
	a.publisher.wireMu.Lock()
	defer a.publisher.wireMu.Unlock()
	return a.channel.QueueBind(name, key, exchange, noWait, args)
}

func (a *amqpChannelAdapter) Qos(prefetchCount, prefetchSize int, global bool) error {
	a.publisher.wireMu.Lock()
	defer a.publisher.wireMu.Unlock()
	return a.channel.Qos(prefetchCount, prefetchSize, global)
}

func (a *amqpChannelAdapter) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqplib.Table) (<-chan amqplib.Delivery, error) {
	a.publisher.wireMu.Lock()
	defer a.publisher.wireMu.Unlock()
	return a.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (a *amqpChannelAdapter) Cancel(consumer string, noWait bool) error {
	a.publisher.wireMu.Lock()
	defer a.publisher.wireMu.Unlock()
	return a.channel.Cancel(consumer, noWait)
}

func (a *amqpChannelAdapter) Flush(ctx context.Context) error { return a.publisher.flush(ctx) }

func (a *amqpChannelAdapter) Close() error {
	return a.channel.Close()
}

type subscriptionRegistration struct {
	ctx         context.Context
	destination string
	handler     core.Handler
	opts        []core.SubscribeOption
	readiness   *readiness.State
}

type pendingCall struct {
	once     sync.Once
	response chan *amqplib.Delivery
}

func newPendingCall() *pendingCall {
	return &pendingCall{response: make(chan *amqplib.Delivery, 1)}
}

func (p *pendingCall) complete(response *amqplib.Delivery) {
	p.once.Do(func() { p.response <- response })
}

// Broker implements the core.MessageBroker interface for AMQP/RabbitMQ.
type Broker struct {
	work         lifecycle.Group
	ready        readiness.Signal
	metrics      observability.Metrics
	rpcSlots     rpclimit.Limiter
	config       *core.Config
	amqpConfig   *core.AMQPConfig
	conn         amqpConnection
	channel      amqpChannel
	dial         func(string, amqplib.Config) (amqpConnection, error)
	endpoints    []string
	nextEndpoint int // guarded by closeMu

	replyQueue        string
	replyConsumerStop chan struct{}
	replyConsumerTag  string
	replyGate         chan struct{}
	pending           map[string]*pendingCall
	pendingMu         sync.RWMutex
	subs              []subscriptionRegistration
	subsMu            sync.RWMutex

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

// NewBroker creates a new AMQP broker instance.
func NewBroker(config *core.Config) (core.MessageBroker, error) {
	var err error
	config, err = validation.Config(config)
	if err != nil {
		return nil, err
	}
	if config.AMQP == nil {
		config.AMQP = core.DefaultAMQPConfig()
	}
	if err := validation.AMQP(config.AMQP); err != nil {
		return nil, err
	}
	endpoints := config.AMQP.Endpoints
	if len(endpoints) == 0 {
		endpoints = []string{net.JoinHostPort(config.AMQP.Host, fmt.Sprint(config.AMQP.Port))}
	}

	return &Broker{
		config:     config,
		rpcSlots:   rpclimit.New(config.MaxPendingCalls),
		amqpConfig: config.AMQP,
		endpoints:  endpoints,
		dial: func(connString string, config amqplib.Config) (amqpConnection, error) {
			conn, err := amqplib.DialConfig(connString, config)
			if err != nil {
				return nil, err
			}
			return &amqpConnectionAdapter{conn: conn}, nil
		},
		pending:   make(map[string]*pendingCall),
		closeChan: make(chan struct{}),
	}, nil
}

// Backend returns the name of this backend.
func (b *Broker) Backend() string {
	return backendName
}

// Connect establishes a connection to the AMQP broker.
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

	client, err := b.open(ctx)
	b.closeMu.Lock()
	if b.closed {
		err = core.ErrClosed
	}
	if err == nil {
		b.conn, b.channel = client.conn, client.channel
		b.connected = true
		b.everConnected = true
		b.replyQueue = ""
	}
	close(b.connecting)
	b.connecting = nil
	b.closeMu.Unlock()
	b.ready.Notify()
	if err != nil && client != nil {
		_ = client.Close()
	}
	if err == nil {
		go b.monitorConnection(client.conn)
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
		event.Fields["address"] = strings.Join(b.endpoints, ", ")
	} else {
		event.Fields["address"] = client.address
	}
	b.emitEvent(ctx, event)
	return err
}

type openedConnection struct {
	conn    amqpConnection
	channel amqpChannel
	address string
}

func (c *openedConnection) Close() error { return c.conn.Close() }

func (b *Broker) open(ctx context.Context) (*openedConnection, error) {
	retries := b.config.ConnectionRetry
	if retries <= 0 {
		retries = 3
	}

	var err error
	for i := 0; i <= retries; i++ {
		for range b.endpoints {
			if waitErr := connection.Wait(ctx, b.closeChan, b.work.Stopping(), 0); waitErr != nil {
				return nil, waitErr
			}
			b.closeMu.Lock()
			address := b.endpoints[b.nextEndpoint]
			b.nextEndpoint = (b.nextEndpoint + 1) % len(b.endpoints)
			b.closeMu.Unlock()
			var client *openedConnection
			client, err = b.openEndpoint(ctx, address)
			if err == nil {
				return client, nil
			}
			if waitErr := connection.Wait(ctx, b.closeChan, b.work.Stopping(), 0); waitErr != nil {
				return nil, waitErr
			}
		}
		if i < retries {
			delay := connection.Backoff(b.config, i+1, 2*time.Second)
			if waitErr := connection.Wait(ctx, b.closeChan, b.work.Stopping(), delay); waitErr != nil {
				return nil, waitErr
			}
		}
	}

	return nil, &core.ErrConnectionFailed{Backend: backendName, Address: strings.Join(b.endpoints, ", "), Cause: err}
}

func (b *Broker) openEndpoint(ctx context.Context, address string) (*openedConnection, error) {
	return connection.Open(ctx, b.closeChan, net.Dialer{Timeout: 30 * time.Second}, func(dialer *connection.Dialer) (*openedConnection, error) {
		tlsConfig := b.amqpConfig.TLS
		if tlsConfig != nil {
			tlsConfig = tlsConfig.Clone()
		}
		conn, err := b.dial(b.connectionString(address), amqplib.Config{
			Heartbeat:       b.amqpConfig.Heartbeat,
			TLSClientConfig: tlsConfig,
			Properties:      amqplib.Table{"connection_name": b.config.ConnectionName},
			Dial: func(network, address string) (net.Conn, error) {
				conn, err := dialer.Dial(network, address)
				if err != nil {
					return nil, err
				}
				if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
					_ = conn.Close()
					return nil, err
				}
				return conn, nil
			},
		})
		if err != nil {
			return nil, err
		}
		channel, err := conn.Channel()
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to open channel: %w", err)
		}
		return &openedConnection{conn: conn, channel: channel, address: address}, nil
	})
}

func (b *Broker) connectionString(endpoint string) string {
	scheme := "amqp"
	if b.amqpConfig.TLS != nil {
		scheme = "amqps"
	}
	vhost := b.amqpConfig.VHost
	if vhost == "" {
		vhost = "/"
	}
	address := url.URL{
		Scheme:  scheme,
		User:    url.UserPassword(b.amqpConfig.Username, b.amqpConfig.Password),
		Host:    endpoint,
		Path:    "/" + vhost,
		RawPath: "/" + url.PathEscape(vhost),
	}
	return address.String()
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
	revoked := b.invalidateConsumers()
	b.closeMu.Unlock()
	b.ready.Notify()
	revoked()

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
		if b.closed || b.connected || b.work.IsStopping() {
			b.closeMu.RUnlock()
			return
		}
		b.closeMu.RUnlock()

		attempt++
		// Emit reconnect attempt event
		b.emitEvent(context.Background(), core.Event{
			Level:     core.EventLevelDebug,
			Name:      "reconnect_attempt",
			Operation: "connect",
			Fields: map[string]any{
				"attempt": attempt,
			},
		})

		err := b.Connect(context.Background())
		if err == nil {
			if restoreErr := b.restoreSubscriptions(); restoreErr != nil {
				if b.work.IsStopping() {
					return
				}
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
				delay := connection.Backoff(b.config, attempt, 2*time.Second)
				if connection.Wait(context.Background(), b.closeChan, b.work.Stopping(), delay) != nil {
					return
				}
				continue
			}
			b.closeMu.Lock()
			b.recovering = false
			b.closeMu.Unlock()
			b.ready.Notify()

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

		delay := connection.Backoff(b.config, attempt, 2*time.Second)
		if connection.Wait(context.Background(), b.closeChan, b.work.Stopping(), delay) != nil {
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
		if err := b.subscribeWithReadiness(sub.ctx, sub.destination, sub.handler, false, true, sub.readiness, sub.opts...); err != nil {
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
	return b.connected && !b.recovering && !b.closed && !b.work.IsStopping()
}

// IsRecovering returns true if the broker is actively recovering from a connection loss.
func (b *Broker) IsRecovering() bool {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	return b.recovering
}

// Shutdown drains admitted work and cancels business consumers at RabbitMQ.
// A deadline forces closure; handlers must observe their contexts to stop.
func (b *Broker) Shutdown(ctx context.Context) error {
	return b.work.Shutdown(ctx, func(ctx context.Context) error {
		b.invalidateConsumers()()
		b.closeMu.RLock()
		channel := b.channel
		b.closeMu.RUnlock()
		if publisher, ok := channel.(interface{ Flush(context.Context) error }); ok {
			return publisher.Flush(ctx)
		}
		return nil
	}, b.Close)
}

var _ core.Shutdowner = (*Broker)(nil)

// Close closes the connection and interrupts handlers without draining work.
func (b *Broker) Close() error {
	b.work.Close()
	var err error
	closed := false
	revoked := func() {}
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		b.stopReplyConsumerLocked()
		b.closed = true
		b.connected = false
		b.recovering = false
		channel, conn := b.channel, b.conn
		b.channel, b.conn = nil, nil
		revoked = b.invalidateConsumers()
		b.closeMu.Unlock()
		b.ready.Notify()

		close(b.closeChan)
		b.cancelAllPending()

		if conn != nil {
			if e := conn.Close(); e != nil && err == nil {
				err = e
			}
		}
		if channel != nil {
			if e := channel.Close(); e != nil && !errors.Is(e, amqplib.ErrClosed) && err == nil {
				err = e
			}
		}

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

// Publish sends a message to the specified queue.
func (b *Broker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	return b.publish(ctx, destination, msg, nil, opts...)
}

// RPC requests must stay on the channel that owns their exclusive reply queue.
func (b *Broker) publish(ctx context.Context, destination string, msg *core.Message, replyChannel amqpChannel, opts ...core.PublishOption) error {
	started := time.Now()
	options, err := validation.Publish(ctx, destination, msg, opts...)
	if err != nil {
		return validation.ContextError(err, "Publish", "context deadline")
	}
	if err := validatePublishing(destination, msg, options); err != nil {
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
	channel, err := b.currentChannel(false)
	if replyChannel != nil && (err != nil || channel != replyChannel) {
		return &core.ErrConnectionLost{Backend: backendName}
	}
	if err != nil {
		return err
	}

	msg = msg.Clone()

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
		err = validation.ContextError(err, "Publish", options.Timeout.String())
		b.emitEvent(ctx, core.Event{
			Level:       core.EventLevelError,
			Name:        core.EventPublishFailed,
			Operation:   "publish",
			Destination: destination,
			Err:         err,
		})
		return &core.ErrPublishFailed{Backend: backendName, Destination: destination, Cause: err}
	}

	b.emitEvent(ctx, core.Event{Level: core.EventLevelInfo, Name: core.EventPublishSucceeded, Operation: "publish", Destination: destination, CorrelationID: msg.CorrelationID, Duration: time.Since(started)})
	return nil
}

func validatePublishing(destination string, msg *core.Message, options *core.PublishOptions) error {
	if options.Immediate {
		return &core.ErrUnsupportedOperation{Backend: backendName, Operation: "immediate publishing"}
	}
	if err := validation.ShortStrings(destination, options.Exchange, msg.Subject, msg.ContentType, msg.CorrelationID, msg.ReplyTo, msg.MessageID); err != nil {
		return err
	}
	for key := range msg.Headers {
		if err := validation.ShortStrings(key); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe registers a handler for messages from the specified queue.
func (b *Broker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	started := time.Now()
	handler = b.metrics.WrapHandler(handler, destination, b.emitEvent)
	if err := b.subscribe(ctx, destination, handler, true, false, opts...); err != nil {
		return err
	}

	b.emitEvent(ctx, core.Event{Level: core.EventLevelInfo, Name: core.EventSubscribeSucceeded, Operation: "subscribe", Destination: destination, Duration: time.Since(started)})
	return nil
}

func (b *Broker) subscribe(ctx context.Context, destination string, handler core.Handler, track bool, allowRecovering bool, opts ...core.SubscribeOption) error {
	return b.subscribeWithReadiness(ctx, destination, handler, track, allowRecovering, &readiness.State{}, opts...)
}

func (b *Broker) subscribeWithReadiness(ctx context.Context, destination string, handler core.Handler, track bool, allowRecovering bool, state *readiness.State, opts ...core.SubscribeOption) error {
	options, err := validation.Subscribe(ctx, destination, handler, opts...)
	if err != nil {
		return err
	}
	if err := validation.ShortStrings(destination, options.Exchange, options.RoutingKey, options.ConsumerTag); err != nil {
		return err
	}
	if options.AutoAck && options.HandlerErrorPolicy == core.HandlerErrorRetry {
		return fmt.Errorf("%w: automatic acknowledgement cannot be combined with handler retries", core.ErrInvalidConfig)
	}
	setupCtx, finish, err := b.work.Begin(ctx)
	if err != nil {
		return err
	}
	defer finish()

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

	tag := options.ConsumerTag
	if tag == "" {
		tag = "weave-" + rand.Text()
	}
	msgs, err := channel.Consume(q.Name, tag, options.AutoAck, options.Exclusive, false, false, nil)
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
	_, finishConsumer, err := b.work.Continue(context.WithoutCancel(setupCtx))
	if err != nil {
		go func() { _ = channel.Cancel(tag, false) }()
		return err
	}

	if state == nil {
		state = &readiness.State{}
	}
	b.closeMu.RLock()
	var stopped error
	switch {
	case b.work.IsClosed():
		stopped = core.ErrClosed
	case b.work.IsStopping():
		stopped = core.ErrShuttingDown
	case b.channel != channel || !b.connected:
		stopped = &core.ErrNotConnected{Backend: backendName}
	}
	if stopped != nil {
		b.closeMu.RUnlock()
		finishConsumer()
		go func() { _ = channel.Cancel(tag, false) }()
		return stopped
	}
	if track {
		b.subsMu.Lock()
		b.subs = append(b.subs, subscriptionRegistration{ctx: ctx, destination: destination, handler: handler, opts: append([]core.SubscribeOption(nil), opts...), readiness: state})
		b.subsMu.Unlock()
	}
	assignment, previous := state.Assign(ctx, func(generation uint64) {
		b.ready.Notify()
		b.emitEvent(ctx, core.Event{Level: core.EventLevelInfo, Name: core.EventConsumerRevoked, Operation: "consume", Destination: destination, Fields: map[string]any{"consumer_tag": tag, "consumer_generation": generation}})
	})
	b.closeMu.RUnlock()
	previous()
	b.ready.Notify()

	var workers sync.WaitGroup
	cancelDone := make(chan struct{})
	consume := func(concurrent bool) {
		defer workers.Done()
		for {
			select {
			case <-b.closeChan:
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				handlerCtx, finishHandler, err := b.work.Handler(ctx)
				if err != nil {
					select {
					case <-cancelDone:
					case <-b.closeChan:
						return
					}
					if !options.AutoAck {
						if err := msg.Nack(false, true); err != nil {
							b.emitEvent(ctx, core.Event{Level: core.EventLevelError, Name: core.EventSubscribeFailed, Operation: "requeue", Destination: destination, Err: err})
						}
					}
					continue
				}
				handle := func() {
					defer finishHandler()
					b.handleMessage(handlerCtx, msg, handler, options.AutoAck, options.HandlerErrorPolicy)
				}
				if concurrent {
					go handle()
				} else {
					handle()
				}
			}
		}
	}

	if options.WorkerCount == 0 {
		workers.Add(1)
		go consume(true)
	} else {
		workers.Add(options.WorkerCount)
		for range options.WorkerCount {
			go consume(false)
		}
	}
	consumerDone := make(chan struct{})
	go func() { workers.Wait(); close(consumerDone) }()
	go func() {
		defer finishConsumer()
		defer assignment.Release()
		select {
		case <-ctx.Done():
		case <-b.work.Stopping():
		case <-consumerDone:
			return
		case <-b.closeChan:
			return
		}
		if err := channel.Cancel(tag, false); err != nil && !errors.Is(err, amqplib.ErrClosed) {
			b.emitEvent(ctx, core.Event{Level: core.EventLevelError, Name: core.EventSubscribeFailed, Operation: "cancel", Destination: destination, Err: err})
		}
		close(cancelDone)
		select {
		case <-consumerDone:
		case <-b.closeChan:
		}
	}()
	if assignment != nil {
		b.emitEvent(ctx, core.Event{Level: core.EventLevelInfo, Name: core.EventConsumerAssigned, Operation: "consume", Destination: destination, Fields: map[string]any{"consumer_tag": tag, "consumer_generation": assignment.Generation}})
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
				msg.Nack(false, b.work.IsClosed() || ctx.Err() != nil)
			}
		}
	}()

	err := handler(ctx, messageFromDelivery(msg))

	if !autoAck {
		if err != nil {
			// Interrupted work is unfinished even when ordinary handler errors are not retried.
			interrupted := b.work.IsClosed() || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
			msg.Nack(false, interrupted || policy == core.HandlerErrorRetry || core.IsDeadLetterFailed(err))
		} else {
			msg.Ack(false)
		}
	}
}

func messageFromDelivery(msg amqplib.Delivery) *core.Message {
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

	return coreMsg
}

// Call implements the request-response pattern.
func (b *Broker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (_ *core.Message, resultErr error) {
	started := time.Now()
	options, err := validation.Publish(ctx, destination, msg, opts...)
	if err != nil {
		return nil, validation.ContextError(err, "Call", "context deadline")
	}
	if err := validatePublishing(destination, msg, options); err != nil {
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
	channel, err := b.currentChannel(false)
	if err != nil {
		return nil, err
	}

	if err := b.ensureReplyQueue(ctx, channel); err != nil {
		return nil, validation.ContextError(err, "Call", options.Timeout.String())
	}

	corrID := msg.CorrelationID
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

	request := msg.Clone()
	b.closeMu.RLock()
	replyQueue := b.replyQueue
	validReply := b.connected && !b.recovering && !b.closed && b.channel == channel && replyQueue != ""
	b.closeMu.RUnlock()
	if !validReply {
		return nil, &core.ErrConnectionLost{Backend: backendName}
	}
	request.CorrelationID, request.ReplyTo = corrID, replyQueue
	if err := b.publish(ctx, destination, request, channel, opts...); err != nil {
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
		return messageFromDelivery(*response), nil
	}
}

func (b *Broker) ensureReplyQueue(ctx context.Context, channel amqpChannel) error {
	b.closeMu.Lock()
	if b.replyGate == nil {
		b.replyGate = make(chan struct{}, 1)
	}
	gate := b.replyGate
	b.closeMu.Unlock()
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closeChan:
		return core.ErrClosed
	}
	result := make(chan error, 1)
	go func() {
		defer func() { <-gate }()
		result <- b.initReplyQueue(channel)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closeChan:
		return core.ErrClosed
	}
}

func (b *Broker) initReplyQueue(channel amqpChannel) error {
	b.closeMu.RLock()
	if b.closed || b.channel != channel {
		b.closeMu.RUnlock()
		return &core.ErrConnectionLost{Backend: backendName}
	}
	queue, previousStop, previousTag := b.replyQueue, b.replyConsumerStop, b.replyConsumerTag
	b.closeMu.RUnlock()

	// A healed cluster can lose a temporary queue without notifying its consumer.
	name := queue
	if name == "" {
		name = "weave.rpc.reply-" + rand.Text()
	}
	q, err := channel.QueueDeclare(name, false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare reply queue: %w", err)
	}
	if queue != "" && q.Consumers > 0 {
		return nil
	}
	if queue != "" {
		b.resetReplyConsumer(previousStop)
		if previousTag != "" {
			if err := channel.Cancel(previousTag, false); err != nil {
				return fmt.Errorf("failed to cancel stale reply consumer: %w", err)
			}
		}
		if _, err := channel.QueueDelete(q.Name, false, false, false); err != nil {
			return fmt.Errorf("failed to delete stale reply queue: %w", err)
		}
		// A fresh address keeps late responses out of calls that reuse correlation IDs.
		q, err = channel.QueueDeclare("weave.rpc.reply-"+rand.Text(), false, true, true, false, nil)
		if err != nil {
			return fmt.Errorf("failed to declare replacement reply queue: %w", err)
		}
	}

	tag := "weave-rpc-" + rand.Text()
	msgs, err := channel.Consume(q.Name, tag, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume reply queue: %w", err)
	}

	stop := make(chan struct{})
	b.closeMu.Lock()
	if b.closed || b.channel != channel {
		b.closeMu.Unlock()
		return &core.ErrConnectionLost{Backend: backendName}
	}
	if b.replyQueue != "" {
		b.closeMu.Unlock()
		close(stop)
		return nil
	}
	b.replyQueue = q.Name
	b.replyConsumerStop = stop
	b.replyConsumerTag = tag
	b.closeMu.Unlock()

	go b.handleResponses(msgs, stop)

	return nil
}

func (b *Broker) handleResponses(msgs <-chan amqplib.Delivery, stop <-chan struct{}) {
	defer b.resetReplyConsumer(stop)
	for {
		select {
		case <-stop:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			corrID := msg.CorrelationId
			b.closeMu.RLock()
			if b.replyConsumerStop != stop {
				b.closeMu.RUnlock()
				return
			}
			b.pendingMu.RLock()
			pending, exists := b.pending[corrID]
			b.pendingMu.RUnlock()
			b.closeMu.RUnlock()

			if exists {
				pending.complete(&msg)
			}
		}
	}
}

func (b *Broker) resetReplyConsumer(stop <-chan struct{}) {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if b.replyConsumerStop != stop {
		return
	}
	b.stopReplyConsumerLocked()
	b.replyQueue = ""
	// Invalidate this generation before another call can register on a replacement.
	b.cancelAllPending()
}

func (b *Broker) cancelAllPending() {
	b.pendingMu.Lock()
	pending := b.pending
	b.pending = make(map[string]*pendingCall)
	b.pendingMu.Unlock()

	for _, call := range pending {
		call.complete(nil)
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
	b.replyConsumerTag = ""
}
