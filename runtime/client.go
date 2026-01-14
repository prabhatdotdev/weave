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

package runtime

import (
	"context"
	"sync"

	"github.com/prabhatdotdev/weave/core"
)

// Client provides a client-only interface for interacting with message brokers.
// Unlike Server, Client only supports publishing messages and making RPC calls.
// Use Client when your application only needs to send messages without subscribing.
//
// Example:
//
//	client, err := runtime.NewClient(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	if err := client.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Fire-and-forget publish
//	client.Publish(ctx, "orders", weave.NewMessage(orderData))
//
//	// Request-reply RPC call
//	response, err := client.Call(ctx, "users.get", weave.NewMessage(userID))
type Client struct {
	broker core.MessageBroker
	config *core.Config

	mu        sync.RWMutex
	connected bool
	closeOnce sync.Once
}

// NewClient creates a new Client with the given configuration.
func NewClient(config *core.Config) (*Client, error) {
	broker, err := core.New(config)
	if err != nil {
		return nil, err
	}
	return NewClientWithBroker(broker, config), nil
}

// NewClientWithBroker creates a new Client with an existing broker.
// This is useful for testing with mock brokers.
func NewClientWithBroker(broker core.MessageBroker, config *core.Config) *Client {
	return &Client{
		broker: broker,
		config: config,
	}
}

// Connect establishes a connection to the message broker.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return core.ErrAlreadyConnected
	}

	if err := c.broker.Connect(ctx); err != nil {
		return err
	}
	c.connected = true
	return nil
}

// Close gracefully closes the client connection.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
		err = c.broker.Close()
	})
	return err
}

// IsConnected returns true if the client is connected to the broker.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && c.broker.IsConnected()
}

// Backend returns the name of the backend (e.g., "amqp", "kafka").
func (c *Client) Backend() string {
	return c.broker.Backend()
}

// Publish sends a message to a destination without expecting a response.
// This is a fire-and-forget operation.
//
// Options can be used to customize the publish behavior:
//
//	client.Publish(ctx, "orders", msg,
//	    weave.WithTimeout(5*time.Second),
//	    weave.WithPersistent(),
//	)
func (c *Client) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()

	if !connected {
		return &core.ErrNotConnected{Backend: c.broker.Backend()}
	}
	return c.broker.Publish(ctx, destination, msg, opts...)
}

// Call performs a synchronous request-reply operation.
// It sends a message and waits for a response from the server.
//
// The timeout can be set using WithTimeout option:
//
//	response, err := client.Call(ctx, "users.get", request,
//	    weave.WithTimeout(10*time.Second),
//	)
func (c *Client) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (*core.Message, error) {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()

	if !connected {
		return nil, &core.ErrNotConnected{Backend: c.broker.Backend()}
	}
	return c.broker.Call(ctx, destination, msg, opts...)
}

// Broker returns the underlying MessageBroker.
// Use this for advanced operations not exposed by Client.
func (c *Client) Broker() core.MessageBroker {
	return c.broker
}

// Config returns the client configuration.
func (c *Client) Config() *core.Config {
	return c.config
}

// Ensure Client implements core.Client interface.
var _ core.Client = (*Client)(nil)
