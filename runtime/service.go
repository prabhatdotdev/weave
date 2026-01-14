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
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.started = true
	handlers := make(map[string]core.Handler, len(s.handlers))
	for k, v := range s.handlers {
		handlers[k] = v
	}
	s.mu.Unlock()

	if !s.broker.IsConnected() {
		if err := s.broker.Connect(ctx); err != nil {
			return err
		}
	}

	for dest, handler := range handlers {
		if err := s.broker.Subscribe(ctx, dest, handler); err != nil {
			return err
		}
	}

	return nil
}

// Publish sends a message to a destination.
// This allows servers to also act as clients when needed.
func (s *Server) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	return s.broker.Publish(ctx, destination, msg, opts...)
}

// Call performs a request-reply operation.
// This allows servers to also act as clients when needed.
func (s *Server) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	return s.broker.Call(ctx, destination, msg, opts...)
}

// Stop gracefully stops the server and closes the broker connection.
func (s *Server) Stop() error {
	var err error
	s.closeOnce.Do(func() {
		s.cancel()
		err = s.broker.Close()
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
