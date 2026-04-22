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

// Package core provides the fundamental interfaces, types, and errors for Weave.
package core

import "context"

// Connector defines connection lifecycle operations.
type Connector interface {
	// Connect establishes a connection to the message broker.
	Connect(ctx context.Context) error

	// Close gracefully closes the connection to the broker.
	Close() error

	// IsConnected returns true if the broker connection is active.
	IsConnected() bool

	// IsRecovering returns true if the broker is actively recovering from a connection loss.
	// When recovering is true, publish/call/subscribe operations will fail.
	IsRecovering() bool

	// Backend returns the name of the backend (e.g., "amqp", "kafka").
	Backend() string
}

// Publisher defines message publishing operations (fire-and-forget).
type Publisher interface {
	// Publish sends a message to a destination (queue/topic/stream).
	Publish(ctx context.Context, destination string, message *Message, opts ...PublishOption) error
}

// Caller defines request-reply operations (synchronous RPC).
type Caller interface {
	// Call performs a synchronous request-reply operation.
	Call(ctx context.Context, destination string, message *Message, opts ...PublishOption) (*Message, error)
}

// Subscriber defines message subscription operations (server-side consumption).
type Subscriber interface {
	// Subscribe starts consuming messages from a destination.
	Subscribe(ctx context.Context, destination string, handler Handler, opts ...SubscribeOption) error
}

// MessageBroker defines the interface that all message broker backends must implement.
// It combines all broker capabilities: connection management, publishing, subscribing, and RPC.
type MessageBroker interface {
	Connector
	Publisher
	Caller
	Subscriber
}

// Client is a subset of MessageBroker for client-only operations.
// Use this interface when you only need to publish messages or make RPC calls,
// without subscribing to messages (server-side).
type Client interface {
	Connector
	Publisher
	Caller
}

// Server is a subset of MessageBroker for server-only operations.
// Use this interface when you only need to subscribe to messages,
// without publishing (client-side).
type Server interface {
	Connector
	Subscriber
}

// Handler is a function that processes incoming messages.
type Handler func(ctx context.Context, msg *Message) error

// BrokerFactory is a function that creates a new MessageBroker instance.
type BrokerFactory func(config *Config) (MessageBroker, error)
