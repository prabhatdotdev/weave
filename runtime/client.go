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
	"time"

	payloadcodec "github.com/prabhatdotdev/weave/codec"
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
func (c *Client) Connect(ctx context.Context) (err error) {
	ctx, span := c.config.StartSpan(ctx, core.TraceSpanStart{
		Name:      "weave.runtime.client.connect",
		Backend:   c.broker.Backend(),
		Component: "runtime",
		Operation: "client_connect",
		Attributes: map[string]string{
			"role": "client",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":    "client",
				"outcome": traceOutcome(err),
			},
		})
	}()

	c.mu.Lock()
	if c.connected {
		c.mu.Unlock()
		err = core.ErrAlreadyConnected
		return err
	}

	if err = c.broker.Connect(ctx); err != nil {
		c.mu.Unlock()
		if c.config != nil {
			c.config.EmitEvent(ctx, core.Event{
				Level:     core.EventLevelError,
				Name:      core.EventConnect,
				Backend:   c.broker.Backend(),
				Component: "runtime",
				Operation: "client_connect",
				Err:       err,
				Fields:    map[string]any{"outcome": "failure"},
			})
			c.config.EmitCounter("weave.runtime.connect.failures", 1, map[string]string{"backend": c.broker.Backend(), "role": "client"})
			c.config.EmitHealth(ctx, c.HealthReportWithStatus(core.HealthStatusUnhealthy, map[string]any{
				"operation": "client_connect",
				"error":     err.Error(),
			}))
		}
		return err
	}
	c.connected = true
	c.mu.Unlock()
	if c.config != nil {
		c.config.EmitEvent(ctx, core.Event{
			Level:     core.EventLevelInfo,
			Name:      core.EventConnect,
			Backend:   c.broker.Backend(),
			Component: "runtime",
			Operation: "client_connect",
			Fields:    map[string]any{"outcome": "success"},
		})
		c.config.EmitCounter("weave.runtime.connect.success", 1, map[string]string{"backend": c.broker.Backend(), "role": "client"})
		c.config.EmitHealth(ctx, c.HealthReport())
	}
	return nil
}

// Close gracefully closes the client connection.
func (c *Client) Close() error {
	var err error
	ctx, span := c.config.StartSpan(context.Background(), core.TraceSpanStart{
		Name:      "weave.runtime.client.close",
		Backend:   c.broker.Backend(),
		Component: "runtime",
		Operation: "client_close",
		Attributes: map[string]string{
			"role": "client",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":    "client",
				"outcome": traceOutcome(err),
			},
		})
	}()

	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
		err = c.broker.Close()
		if c.config != nil {
			c.config.EmitEvent(ctx, core.Event{
				Level:     core.EventLevelInfo,
				Name:      core.EventDisconnect,
				Backend:   c.broker.Backend(),
				Component: "runtime",
				Operation: "client_close",
				Fields:    map[string]any{"outcome": "success", "reason": "close"},
			})
			c.config.EmitCounter("weave.runtime.disconnect.events", 1, map[string]string{"backend": c.broker.Backend(), "role": "client"})
			c.config.EmitHealth(ctx, c.HealthReport())
		}
	})
	return err
}

// IsConnected returns true if the client is connected to the broker.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && c.broker.IsConnected()
}

// IsRecovering returns true if the broker is actively recovering from a connection loss.
func (c *Client) IsRecovering() bool {
	return c.broker.IsRecovering()
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
func (c *Client) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (err error) {
	ctx, span := c.config.StartSpan(ctx, core.TraceSpanStart{
		Name:          "weave.runtime.client.publish",
		Backend:       c.broker.Backend(),
		Component:     "runtime",
		Operation:     "client_publish",
		Destination:   destination,
		CorrelationID: correlationID(msg),
		Attributes: map[string]string{
			"role": "client",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":        "client",
				"destination": destination,
				"outcome":     traceOutcome(err),
			},
		})
	}()

	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()

	if !connected {
		err = &core.ErrNotConnected{Backend: c.broker.Backend()}
		return err
	}
	err = c.broker.Publish(ctx, destination, msg, opts...)
	return err
}

// PublishWithCodec encodes a payload into a message using the provided codec and publishes it.
func (c *Client) PublishWithCodec(ctx context.Context, destination string, payload any, codec payloadcodec.Codec, opts ...core.PublishOption) error {
	msg, err := payloadcodec.MarshalMessage(codec, payload)
	if err != nil {
		return err
	}
	return c.Publish(ctx, destination, msg, opts...)
}

// Call performs a synchronous request-reply operation.
// It sends a message and waits for a response from the server.
//
// The timeout can be set using WithTimeout option:
//
//	response, err := client.Call(ctx, "users.get", request,
//	    weave.WithTimeout(10*time.Second),
//	)
func (c *Client) Call(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) (response *core.Message, err error) {
	ctx, span := c.config.StartSpan(ctx, core.TraceSpanStart{
		Name:          "weave.runtime.client.call",
		Backend:       c.broker.Backend(),
		Component:     "runtime",
		Operation:     "client_call",
		Destination:   destination,
		CorrelationID: correlationID(msg),
		Attributes: map[string]string{
			"role": "client",
		},
	})
	defer func() {
		span.Finish(core.TraceSpanFinish{
			Err: err,
			Attributes: map[string]string{
				"role":        "client",
				"destination": destination,
				"outcome":     callOutcome(err),
			},
		})
	}()

	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()

	if !connected {
		err = &core.ErrNotConnected{Backend: c.broker.Backend()}
		return nil, err
	}
	start := time.Now()
	response, err = c.broker.Call(ctx, destination, msg, opts...)
	if err != nil {
		if c.config != nil && core.IsTimeout(err) {
			options := core.ApplyPublishOptions(opts...)
			c.config.EmitEvent(ctx, core.Event{
				Level:       core.EventLevelWarn,
				Name:        core.EventTimeout,
				Backend:     c.broker.Backend(),
				Component:   "runtime",
				Operation:   "client_call",
				Destination: destination,
				Err:         err,
				Fields: map[string]any{
					"timeout": options.Timeout.String(),
				},
			})
			c.config.EmitCounter("weave.runtime.call.timeouts", 1, map[string]string{"backend": c.broker.Backend(), "destination": destination, "role": "client"})
			c.config.EmitDuration("weave.runtime.call.duration", time.Since(start), map[string]string{"backend": c.broker.Backend(), "destination": destination, "role": "client", "outcome": "timeout"})
		}
		return nil, err
	}
	if c.config != nil {
		c.config.EmitDuration("weave.runtime.call.duration", time.Since(start), map[string]string{"backend": c.broker.Backend(), "destination": destination, "role": "client", "outcome": "success"})
	}
	return response, nil
}

// CallWithCodec encodes a request, performs the RPC call, and decodes the response with the same codec.
//
// Pass a nil response if you only want the encoded response message returned.
func (c *Client) CallWithCodec(ctx context.Context, destination string, request any, response any, codec payloadcodec.Codec, opts ...core.PublishOption) (*core.Message, error) {
	msg, err := payloadcodec.MarshalMessage(codec, request)
	if err != nil {
		return nil, err
	}

	reply, err := c.Call(ctx, destination, msg, opts...)
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
func (c *Client) CallWithPolicy(ctx context.Context, destination string, msg *core.Message, policy CallPolicy, opts ...core.PublishOption) (*core.Message, error) {
	return CallWithPolicy(ctx, c.broker, destination, msg, policy, opts...)
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

// HealthReport returns the current client health snapshot.
func (c *Client) HealthReport() core.HealthReport {
	status := core.HealthStatusUnhealthy
	recovering := c.broker.IsRecovering()
	if c.IsConnected() {
		status = core.HealthStatusHealthy
	} else if recovering {
		status = core.HealthStatusDegraded
	}
	return c.HealthReportWithStatus(status, map[string]any{
		"recovering": recovering,
	})
}

// HealthReportWithStatus returns the current client health snapshot with an
// explicit status and additional details.
func (c *Client) HealthReportWithStatus(status core.HealthStatus, details map[string]any) core.HealthReport {
	return core.HealthReport{
		Status:    status,
		Backend:   c.broker.Backend(),
		Component: "runtime.client",
		Connected: c.IsConnected(),
		Details:   cloneHealthDetails(details),
	}
}

func traceOutcome(err error) string {
	if err != nil {
		return "failure"
	}
	return "success"
}

func callOutcome(err error) string {
	if err == nil {
		return "success"
	}
	if core.IsTimeout(err) {
		return "timeout"
	}
	return "failure"
}

func correlationID(msg *core.Message) string {
	if msg == nil {
		return ""
	}
	return msg.CorrelationID
}

func cloneHealthDetails(details map[string]any) map[string]any {
	if len(details) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(details))
	for k, v := range details {
		cloned[k] = v
	}
	return cloned
}

// Ensure Client implements core.Client interface.
var _ core.Client = (*Client)(nil)
