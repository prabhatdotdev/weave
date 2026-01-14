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

// Package weave provides a unified message broker abstraction for Go applications.
//
// Weave supports multiple message queue backends through a pluggable transport
// architecture. Currently supported backends include:
//
//   - AMQP (RabbitMQ) - github.com/prabhatdotdev/weave/transport/amqp
//   - Apache Kafka - github.com/prabhatdotdev/weave/transport/kafka
//
// # Quick Start
//
// Import Weave and the desired transport:
//
//	import (
//	    "github.com/prabhatdotdev/weave"
//	    _ "github.com/prabhatdotdev/weave/transport/amqp"
//	)
//
// Create a broker and start using it:
//
//	config := weave.DefaultConfig()
//	broker, err := weave.New(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer broker.Close()
//
//	if err := broker.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Publish a message
//	msg := weave.NewMessage([]byte("Hello, World!"))
//	broker.Publish(ctx, "my-queue", msg)
//
//	// Subscribe to messages
//	broker.Subscribe(ctx, "my-queue", func(ctx context.Context, msg *weave.Message) error {
//	    fmt.Println("Received:", string(msg.Body))
//	    return nil
//	})
//
// # Package Structure
//
// Weave is organized into several packages:
//
//   - weave (this package) - Re-exports core types for convenience
//   - weave/core - Core interfaces, types, and errors
//   - weave/transport - Transport implementations (amqp, kafka)
//   - weave/runtime - High-level service abstractions
//   - weave/codec - Message encoding/decoding utilities
//   - weave/testkit - Testing utilities and mocks
//
// # Using Different Backends
//
// To use a specific backend, import its transport package:
//
//	// For RabbitMQ/AMQP
//	import _ "github.com/prabhatdotdev/weave/transport/amqp"
//
//	// For Apache Kafka
//	import _ "github.com/prabhatdotdev/weave/transport/kafka"
//
// Then configure the backend:
//
//	// AMQP
//	config := &weave.Config{
//	    Backend: "amqp",
//	    AMQP: &weave.AMQPConfig{
//	        Host: "localhost",
//	        Port: 5672,
//	    },
//	}
//
//	// Kafka
//	config := &weave.Config{
//	    Backend: "kafka",
//	    Kafka: &weave.KafkaConfig{
//	        Brokers: []string{"localhost:9092"},
//	        ConsumerGroup: "my-group",
//	    },
//	}
package weave

import (
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/runtime"
)

// Type aliases for convenience - these re-export core types at the root package level.
type (
	// MessageBroker is the main interface for message broker operations.
	// It combines all broker capabilities: connection, publishing, subscribing, and RPC.
	MessageBroker = core.MessageBroker

	// Connector defines connection lifecycle operations.
	Connector = core.Connector

	// Publisher defines message publishing operations (fire-and-forget).
	Publisher = core.Publisher

	// Caller defines request-reply operations (synchronous RPC).
	Caller = core.Caller

	// Subscriber defines message subscription operations (server-side).
	Subscriber = core.Subscriber

	// ClientBroker is a subset of MessageBroker for client-only operations.
	// Use when you only need to publish or make RPC calls.
	ClientBroker = core.Client

	// ServerBroker is a subset of MessageBroker for server-only operations.
	// Use when you only need to subscribe to messages.
	ServerBroker = core.Server

	// Message represents a message to be sent or received.
	Message = core.Message

	// Handler is a function that processes incoming messages.
	Handler = core.Handler

	// Config holds broker configuration.
	Config = core.Config

	// AMQPConfig holds AMQP-specific configuration.
	AMQPConfig = core.AMQPConfig

	// KafkaConfig holds Kafka-specific configuration.
	KafkaConfig = core.KafkaConfig

	// TLSConfig holds TLS configuration.
	TLSConfig = core.TLSConfig

	// SubscribeOption is a functional option for subscriptions.
	SubscribeOption = core.SubscribeOption

	// PublishOption is a functional option for publishing.
	PublishOption = core.PublishOption

	// Client provides a client-only interface for publishing and RPC calls.
	// Use Client when your application only needs to send messages.
	Client = runtime.Client

	// Server provides a server interface for handling incoming messages.
	// Use Server when building message-driven services.
	Server = runtime.Server

	// Service is an alias for Server for backward compatibility.
	// Deprecated: Use Server instead.
	Service = runtime.Service
)

// Re-export constructor functions.
var (
	// New creates a new MessageBroker from configuration.
	New = core.New

	// NewWithBackend creates a new MessageBroker with explicit backend name.
	NewWithBackend = core.NewWithBackend

	// MustNew creates a new MessageBroker or panics.
	MustNew = core.MustNew

	// Register registers a broker factory.
	Register = core.Register

	// AvailableBackends returns registered backend names.
	AvailableBackends = core.AvailableBackends

	// IsBackendAvailable checks if a backend is registered.
	IsBackendAvailable = core.IsBackendAvailable

	// NewClient creates a new Client for publishing and RPC calls.
	NewClient = runtime.NewClient

	// NewClientWithBroker creates a new Client with an existing broker.
	NewClientWithBroker = runtime.NewClientWithBroker

	// NewServer creates a new Server for handling incoming messages.
	NewServer = runtime.NewServer

	// NewServerWithBroker creates a new Server with an existing broker.
	NewServerWithBroker = runtime.NewServerWithBroker

	// NewService creates a new Server (deprecated, use NewServer).
	// Deprecated: Use NewServer instead.
	NewService = runtime.NewService

	// NewServiceWithBroker creates a new Server with an existing broker (deprecated).
	// Deprecated: Use NewServerWithBroker instead.
	NewServiceWithBroker = runtime.NewServiceWithBroker
)

// Re-export message constructors.
var (
	// NewMessage creates a new message with the given body.
	NewMessage = core.NewMessage

	// NewTextMessage creates a new message with a string body.
	NewTextMessage = core.NewTextMessage
)

// Re-export configuration defaults.
var (
	// DefaultConfig returns default configuration.
	DefaultConfig = core.DefaultConfig

	// DefaultAMQPConfig returns default AMQP configuration.
	DefaultAMQPConfig = core.DefaultAMQPConfig

	// DefaultKafkaConfig returns default Kafka configuration.
	DefaultKafkaConfig = core.DefaultKafkaConfig

	// DefaultNATSConfig returns default NATS configuration.
	DefaultNATSConfig = core.DefaultNATSConfig

	// DefaultRedisConfig returns default Redis configuration.
	DefaultRedisConfig = core.DefaultRedisConfig
)

// Re-export subscribe options.
var (
	// WithAutoAck enables automatic message acknowledgment.
	WithAutoAck = core.WithAutoAck

	// WithExclusive makes the subscription exclusive.
	WithExclusive = core.WithExclusive

	// WithConsumerTag sets the consumer identifier.
	WithConsumerTag = core.WithConsumerTag

	// WithPrefetchCount sets the prefetch limit.
	WithPrefetchCount = core.WithPrefetchCount

	// WithQueueBind configures exchange binding (AMQP).
	WithQueueBind = core.WithQueueBind

	// WithConsumerGroup sets the consumer group.
	WithConsumerGroup = core.WithConsumerGroup

	// WithStartFromBeginning starts from earliest offset.
	WithStartFromBeginning = core.WithStartFromBeginning
)

// Re-export publish options.
var (
	// WithTimeout sets the publish timeout.
	WithTimeout = core.WithTimeout

	// WithMandatory requires message routing (AMQP).
	WithMandatory = core.WithMandatory

	// WithExchange specifies target exchange (AMQP).
	WithExchange = core.WithExchange

	// WithPartition specifies target partition (Kafka).
	WithPartition = core.WithPartition

	// WithKey sets the message key (Kafka).
	WithKey = core.WithKey

	// WithPersistent makes the message persistent (AMQP).
	WithPersistent = core.WithPersistent

	// WithPriority sets message priority.
	WithPriority = core.WithPriority

	// WithExpiration sets message TTL.
	WithExpiration = core.WithExpiration
)

// Re-export common errors.
var (
	// ErrClosed is returned when operating on a closed broker.
	ErrClosed = core.ErrClosed

	// ErrNoReplyTo is returned when Call() has no reply mechanism.
	ErrNoReplyTo = core.ErrNoReplyTo

	// ErrAlreadyConnected is returned when Connect() is called twice.
	ErrAlreadyConnected = core.ErrAlreadyConnected

	// ErrInvalidConfig is returned for invalid configuration.
	ErrInvalidConfig = core.ErrInvalidConfig
)

// Re-export error checking functions.
var (
	// IsNotConnected returns true if the error indicates not connected.
	IsNotConnected = core.IsNotConnected

	// IsConnectionLost returns true if the error indicates connection lost.
	IsConnectionLost = core.IsConnectionLost

	// IsTimeout returns true if the error indicates a timeout.
	IsTimeout = core.IsTimeout

	// IsUnknownBackend returns true if the error indicates unknown backend.
	IsUnknownBackend = core.IsUnknownBackend

	// IsUnsupportedOperation returns true if operation is not supported.
	IsUnsupportedOperation = core.IsUnsupportedOperation
)
