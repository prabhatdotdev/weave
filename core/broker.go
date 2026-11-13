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

	// Close closes the broker without waiting for active work to drain.
	Close() error

	// IsConnected returns true if the broker connection is active.
	IsConnected() bool

	// IsRecovering returns true if the broker is actively recovering from a connection loss.
	// When recovering is true, publish/call/subscribe operations will fail.
	IsRecovering() bool

	// Backend returns the name of the backend (e.g., "amqp", "kafka").
	Backend() string
}

// Publisher defines publishing with broker acknowledgment.
type Publisher interface {
	// Publish waits for broker acknowledgment, not consumer processing.
	// Cancellation or connection loss after submission can leave delivery uncertain.
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

// Shutdowner is an optional capability implemented by brokers that can drain
// active work before closing. It does not change the MessageBroker interface.
type Shutdowner interface {
	// Shutdown rejects new work, drains admitted work, and closes the broker.
	// Expiry of ctx interrupts handlers and starts closing remaining resources.
	Shutdown(ctx context.Context) error
}

// Handler is a function that processes incoming messages.
type Handler func(ctx context.Context, msg *Message) error
