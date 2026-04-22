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

// Package runtime provides higher-level service abstractions for Weave.
//
// This package provides two main abstractions:
//
//   - Server: For building message-driven services that subscribe to queues/topics
//   - Client: For applications that only need to publish messages or make RPC calls
//
// # Server Example
//
//	server, err := runtime.NewServer(config)
//	server.Handle("orders", orderHandler)
//	server.Handle("payments", paymentHandler)
//	server.Start(ctx)
//
// # Client Example
//
//	client, err := runtime.NewClient(config)
//	client.Connect(ctx)
//	client.Publish(ctx, "orders", msg)
//	response, _ := client.Call(ctx, "users.get", request)
package runtime

import (
	"context"
	"fmt"
	"sync"

	payloadcodec "github.com/prabhatdotdev/weave/codec"
	"github.com/prabhatdotdev/weave/core"
)

// Server provides a high-level API for building message-driven services.
// It manages message subscriptions and handler registration.
//
// Use Server when building services that need to:
//   - Subscribe to one or more queues/topics
//   - Process incoming messages with registered handlers
//   - Optionally publish messages or make RPC calls
//
// Example:
//
//	server, err := runtime.NewServer(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	server.Handle("orders", func(ctx context.Context, msg *core.Message) error {
//	    // Process order
//	    return nil
//	})
//
//	if err := server.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer server.Stop()
type Server struct {
	broker   core.MessageBroker
	config   *core.Config
	handlers map[string]core.Handler

	mu        sync.RWMutex
	started   bool
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// NewServer creates a new Server with the given broker configuration.
func NewServer(config *core.Config) (*Server, error) {
	broker, err := core.New(config)
	if err != nil {
		return nil, err
	}

	return NewServerWithBroker(broker, config), nil
}

// NewServerWithBroker creates a new Server with an existing broker.
// This is useful for testing with mock brokers.
func NewServerWithBroker(broker core.MessageBroker, config *core.Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		broker:   broker,
		config:   config,
		handlers: make(map[string]core.Handler),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Handle registers a handler for a destination (queue/topic).
// Multiple handlers can be registered for different destinations.
// Returns the server for method chaining.
func (s *Server) Handle(destination string, handler core.Handler) *Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[destination] = handler
	return s
}

// Start connects to the broker and begins consuming messages.
// All registered handlers will start receiving messages.
func (s *Server) Start(ctx context.Context) (err error) {
	ctx, span := s.config.StartSpan(ctx, core.TraceSpanStart{
		Name:      "weave.runtime.server.start",
		Backend:   s.broker.Backend(),
		Component: "runtime",
		Operation: "server_start",
		Attributes: map[string]string{
			"role": "server",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":    "server",
				"outcome": traceOutcome(err),
			},
		})
	}()

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		err = fmt.Errorf("server already started")
		return err
	}
	handlers := make(map[string]core.Handler, len(s.handlers))
	for k, v := range s.handlers {
		handlers[k] = v
	}
	s.mu.Unlock()

	if !s.broker.IsConnected() {
		if err = s.broker.Connect(ctx); err != nil {
			if s.config != nil {
				s.config.EmitEvent(ctx, core.Event{
					Level:     core.EventLevelError,
					Name:      core.EventConnect,
					Backend:   s.broker.Backend(),
					Component: "runtime",
					Operation: "server_start",
					Err:       err,
					Fields:    map[string]any{"outcome": "failure"},
				})
				s.config.EmitCounter("weave.runtime.connect.failures", 1, map[string]string{"backend": s.broker.Backend(), "role": "server"})
				s.config.EmitHealth(ctx, s.HealthReportWithStatus(core.HealthStatusUnhealthy, map[string]any{
					"operation": "server_start",
					"error":     err.Error(),
				}))
			}
			return err
		}
		if s.config != nil {
			s.config.EmitEvent(ctx, core.Event{
				Level:     core.EventLevelInfo,
				Name:      core.EventConnect,
				Backend:   s.broker.Backend(),
				Component: "runtime",
				Operation: "server_start",
				Fields:    map[string]any{"outcome": "success"},
			})
			s.config.EmitCounter("weave.runtime.connect.success", 1, map[string]string{"backend": s.broker.Backend(), "role": "server"})
			s.config.EmitHealth(ctx, s.HealthReport())
		}
	}

	for dest, handler := range handlers {
		if err = s.broker.Subscribe(ctx, dest, handler); err != nil {
			if s.config != nil {
				s.config.EmitEvent(ctx, core.Event{
					Level:       core.EventLevelError,
					Name:        core.EventSubscribeFailed,
					Backend:     s.broker.Backend(),
					Component:   "runtime",
					Operation:   "server_subscribe",
					Destination: dest,
					Err:         err,
				})
				s.config.EmitCounter("weave.runtime.subscribe.failures", 1, map[string]string{"backend": s.broker.Backend(), "destination": dest, "role": "server"})
				s.config.EmitHealth(ctx, s.HealthReportWithStatus(core.HealthStatusDegraded, map[string]any{
					"operation":   "server_subscribe",
					"destination": dest,
					"error":       err.Error(),
				}))
			}
			return err
		}
	}

	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	if s.config != nil {
		s.config.EmitHealth(ctx, s.HealthReport())
	}

	return nil
}

// Publish sends a message to a destination.
// This allows servers to also act as clients when needed.
func (s *Server) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (err error) {
	ctx, span := s.config.StartSpan(ctx, core.TraceSpanStart{
		Name:          "weave.runtime.server.publish",
		Backend:       s.broker.Backend(),
		Component:     "runtime",
		Operation:     "server_publish",
		Destination:   destination,
		CorrelationID: correlationID(msg),
		Attributes: map[string]string{
			"role": "server",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":        "server",
				"destination": destination,
				"outcome":     traceOutcome(err),
			},
		})
	}()

	err = s.broker.Publish(ctx, destination, msg, opts...)
	return err
}

// PublishWithCodec encodes a payload into a message using the provided codec and publishes it.
func (s *Server) PublishWithCodec(ctx context.Context, destination string, payload any, codec payloadcodec.Codec, opts ...core.PublishOption) error {
	msg, err := payloadcodec.MarshalMessage(codec, payload)
	if err != nil {
		return err
	}
	return s.Publish(ctx, destination, msg, opts...)
}

// Call performs a request-reply operation.
// This allows servers to also act as clients when needed.
func (s *Server) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (response *core.Message, err error) {
	ctx, span := s.config.StartSpan(ctx, core.TraceSpanStart{
		Name:          "weave.runtime.server.call",
		Backend:       s.broker.Backend(),
		Component:     "runtime",
		Operation:     "server_call",
		Destination:   destination,
		CorrelationID: correlationID(msg),
		Attributes: map[string]string{
			"role": "server",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":        "server",
				"destination": destination,
				"outcome":     callOutcome(err),
			},
		})
	}()

	response, err = s.broker.Call(ctx, destination, msg, opts...)
	return response, err
}

// CallWithCodec encodes a request, performs the RPC call, and decodes the response with the same codec.
//
// Pass a nil response if you only want the encoded response message returned.
func (s *Server) CallWithCodec(ctx context.Context, destination string, request any, response any, codec payloadcodec.Codec, opts ...core.PublishOption) (*core.Message, error) {
	msg, err := payloadcodec.MarshalMessage(codec, request)
	if err != nil {
		return nil, err
	}

	reply, err := s.Call(ctx, destination, msg, opts...)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return reply, nil
	}

	if err := payloadcodec.UnmarshalMessage(codec, reply, response); err != nil {
		return nil, err
	}

	return reply, nil
}

// CallWithPolicy performs an RPC-style call with retry, backoff, and optional
// circuit-breaking behavior.
func (s *Server) CallWithPolicy(ctx context.Context, destination string, msg *core.Message, policy CallPolicy, opts ...core.PublishOption) (*core.Message, error) {
	return CallWithPolicy(ctx, s.broker, destination, msg, policy, opts...)
}

// Stop gracefully stops the server and closes the broker connection.
func (s *Server) Stop() error {
	var err error
	ctx, span := s.config.StartSpan(context.Background(), core.TraceSpanStart{
		Name:      "weave.runtime.server.stop",
		Backend:   s.broker.Backend(),
		Component: "runtime",
		Operation: "server_stop",
		Attributes: map[string]string{
			"role": "server",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":    "server",
				"outcome": traceOutcome(err),
			},
		})
	}()

	s.closeOnce.Do(func() {
		s.cancel()
		err = s.broker.Close()
		if s.config != nil {
			s.config.EmitEvent(ctx, core.Event{
				Level:     core.EventLevelInfo,
				Name:      core.EventDisconnect,
				Backend:   s.broker.Backend(),
				Component: "runtime",
				Operation: "server_stop",
				Fields:    map[string]any{"outcome": "success", "reason": "close"},
			})
			s.config.EmitCounter("weave.runtime.disconnect.events", 1, map[string]string{"backend": s.broker.Backend(), "role": "server"})
			s.config.EmitHealth(ctx, s.HealthReport())
		}
	})
	return err
}

// Broker returns the underlying MessageBroker.
func (s *Server) Broker() core.MessageBroker {
	return s.broker
}

// Config returns the server configuration.
func (s *Server) Config() *core.Config {
	return s.config
}

// HealthReport returns the current server health snapshot.
func (s *Server) HealthReport() core.HealthReport {
	recovering := s.broker.IsRecovering()
	status := core.HealthStatusDegraded
	if s.IsStarted() && s.broker.IsConnected() {
		status = core.HealthStatusHealthy
	} else if recovering {
		status = core.HealthStatusDegraded
	} else if !s.broker.IsConnected() {
		status = core.HealthStatusUnhealthy
	}
	return s.HealthReportWithStatus(status, map[string]any{
		"recovering": recovering,
	})
}

// HealthReportWithStatus returns the current server health snapshot with an
// explicit status and additional details.
func (s *Server) HealthReportWithStatus(status core.HealthStatus, details map[string]any) core.HealthReport {
	return core.HealthReport{
		Status:    status,
		Backend:   s.broker.Backend(),
		Component: "runtime.server",
		Connected: s.broker.IsConnected(),
		Started:   s.IsStarted(),
		Details:   cloneHealthDetails(details),
	}
}

// IsStarted returns true if the server has been started.
func (s *Server) IsStarted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

// Service is an alias for Server for backward compatibility.
// Deprecated: Use Server instead.
type Service = Server

// NewService creates a new Server with the given broker configuration.
// Deprecated: Use NewServer instead.
var NewService = NewServer

// NewServiceWithBroker creates a new Server with an existing broker.
// Deprecated: Use NewServerWithBroker instead.
var NewServiceWithBroker = NewServerWithBroker
