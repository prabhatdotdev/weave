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

// Package testkit provides testing utilities for Weave.
package testkit

import (
	"context"
	"sync"
	"testing"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/lifecycle"
	"github.com/prabhatdotdev/weave/internal/validation"
)

// MockBroker is a mock implementation of core.MessageBroker for testing.
// It can be used to test both client and server code.
type MockBroker struct {
	work lifecycle.Group
	mu   sync.Mutex

	connected     bool
	closed        bool
	published     []PublishedMessage
	subscriptions map[string]mockSubscription
	callResponses map[string]*core.Message
	callErrors    map[string]error
}

type mockSubscription struct {
	ctx     context.Context
	handler core.Handler
}

// PublishedMessage records a message that was published.
type PublishedMessage struct {
	Destination string
	Message     *core.Message
}

// NewMockBroker creates a new mock broker.
func NewMockBroker() *MockBroker {
	return &MockBroker{
		subscriptions: make(map[string]mockSubscription),
		callResponses: make(map[string]*core.Message),
		callErrors:    make(map[string]error),
	}
}

func (m *MockBroker) Backend() string { return "mock" }

func (m *MockBroker) Connect(ctx context.Context) error {
	if err := validation.Context(ctx); err != nil {
		return err
	}
	_, finish, err := m.work.Continue(ctx)
	if err != nil {
		return err
	}
	defer finish()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return core.ErrClosed
	}
	m.connected = true
	return nil
}

func (m *MockBroker) Close() error {
	m.work.Close()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.connected = false
	return nil
}

func (m *MockBroker) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected && !m.closed && !m.work.IsStopping()
}

func (m *MockBroker) IsRecovering() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return false // Mock broker never recovers
}

func (m *MockBroker) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	if _, err := validation.Publish(ctx, destination, msg, opts...); err != nil {
		return validation.ContextError(err, "Publish", "context deadline")
	}
	_, finish, err := m.work.Continue(ctx)
	if err != nil {
		return err
	}
	defer finish()
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.connected {
		return &core.ErrNotConnected{Backend: "mock"}
	}

	m.published = append(m.published, PublishedMessage{
		Destination: destination,
		Message:     msg.Clone(),
	})
	return nil
}

func (m *MockBroker) Subscribe(ctx context.Context, destination string, handler core.Handler, opts ...core.SubscribeOption) error {
	if _, err := validation.Subscribe(ctx, destination, handler, opts...); err != nil {
		return err
	}
	_, finish, err := m.work.Begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.connected {
		return &core.ErrNotConnected{Backend: "mock"}
	}

	m.subscriptions[destination] = mockSubscription{ctx: ctx, handler: handler}
	return nil
}

func (m *MockBroker) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	if _, err := validation.Publish(ctx, destination, msg, opts...); err != nil {
		return nil, validation.ContextError(err, "Call", "context deadline")
	}
	_, finish, err := m.work.Continue(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.connected {
		return nil, &core.ErrNotConnected{Backend: "mock"}
	}

	if err, ok := m.callErrors[destination]; ok {
		return nil, err
	}

	if resp, ok := m.callResponses[destination]; ok {
		return resp.Clone(), nil
	}

	return nil, &core.ErrTimeout{Operation: "Call", Duration: "mock"}
}

// SetCallResponse snapshots a mock response. A nil response clears it.
func (m *MockBroker) SetCallResponse(destination string, response *core.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if response == nil {
		delete(m.callResponses, destination)
		return
	}
	m.callResponses[destination] = response.Clone()
}

// SetCallError sets an error response for Call on a destination.
func (m *MockBroker) SetCallError(destination string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callErrors[destination] = err
}

// PublishedMessages returns all published messages.
func (m *MockBroker) PublishedMessages() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]PublishedMessage, len(m.published))
	for i, published := range m.published {
		result[i] = PublishedMessage{Destination: published.Destination, Message: published.Message.Clone()}
	}
	return result
}

// PublishedTo returns messages published to a specific destination.
func (m *MockBroker) PublishedTo(destination string) []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []PublishedMessage
	for _, pub := range m.published {
		if pub.Destination == destination {
			result = append(result, PublishedMessage{Destination: pub.Destination, Message: pub.Message.Clone()})
		}
	}
	return result
}

// AssertPublished verifies a message was published to a destination.
func (m *MockBroker) AssertPublished(t testing.TB, destination string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pub := range m.published {
		if pub.Destination == destination {
			return
		}
	}
	t.Errorf("expected publish to %s, but none found", destination)
}

// AssertPublishCount verifies the number of messages published to a destination.
func (m *MockBroker) AssertPublishCount(t testing.TB, destination string, expected int) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for _, pub := range m.published {
		if pub.Destination == destination {
			count++
		}
	}
	if count != expected {
		t.Errorf("expected %d publishes to %s, got %d", expected, destination, count)
	}
}

// AssertNotPublished verifies no message was published to a destination.
func (m *MockBroker) AssertNotPublished(t testing.TB, destination string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pub := range m.published {
		if pub.Destination == destination {
			t.Errorf("expected no publish to %s, but found %d messages", destination, 1)
			return
		}
	}
}

// SimulateMessage simulates receiving a message on a subscribed destination.
func (m *MockBroker) SimulateMessage(ctx context.Context, destination string, msg *core.Message) error {
	if _, err := validation.Publish(ctx, destination, msg); err != nil {
		return err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return core.ErrClosed
	}
	if !m.connected {
		m.mu.Unlock()
		return &core.ErrNotConnected{Backend: "mock"}
	}
	subscription, ok := m.subscriptions[destination]
	m.mu.Unlock()

	if !ok {
		return &core.ErrSubscribeFailed{Backend: "mock", Destination: destination}
	}

	handlerCtx, cancel := context.WithCancel(subscription.ctx)
	stop := context.AfterFunc(ctx, cancel)
	defer func() { stop(); cancel() }()
	if ctx.Err() != nil {
		cancel()
	}
	handlerCtx, finish, err := m.work.Handler(handlerCtx)
	if err != nil {
		return err
	}
	defer finish()
	return subscription.handler(handlerCtx, msg.Clone())
}

// Reset clears all recorded messages and call responses.
func (m *MockBroker) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = nil
	m.callResponses = make(map[string]*core.Message)
	m.callErrors = make(map[string]error)
}

// Subscriptions returns the destinations that have been subscribed to.
func (m *MockBroker) Subscriptions() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for dest := range m.subscriptions {
		result = append(result, dest)
	}
	return result
}

// HasSubscription returns true if the destination has been subscribed to.
func (m *MockBroker) HasSubscription(destination string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.subscriptions[destination]
	return ok
}

// Verify that MockBroker implements core.MessageBroker.
var _ core.MessageBroker = (*MockBroker)(nil)

// Shutdown rejects new operations and waits for simulated handlers to finish.
func (m *MockBroker) Shutdown(ctx context.Context) error {
	return m.work.Shutdown(ctx, nil, m.Close)
}

var _ core.Shutdowner = (*MockBroker)(nil)
