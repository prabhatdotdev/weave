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

package amqp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

func TestRPCResponseDeliveryAndCloseWithLiveRabbitMQ(t *testing.T) {
	responder, deliveries, requestQueue := newLiveRabbitMQResponder(t)

	for i := 0; i < 25; i++ {
		brokerAny, err := NewBroker(core.DefaultConfig())
		if err != nil {
			t.Fatalf("iteration %d create broker: %v", i, err)
		}
		broker := brokerAny.(*Broker)
		if err := broker.Connect(context.Background()); err != nil {
			t.Fatalf("iteration %d connect broker: %v", i, err)
		}

		type callResult struct {
			response *core.Message
			err      error
		}
		result := make(chan callResult, 1)
		go func() {
			response, err := broker.Call(context.Background(), requestQueue, core.NewTextMessage("request"), core.WithTimeout(5*time.Second))
			result <- callResult{response: response, err: err}
		}()

		var request amqplib.Delivery
		select {
		case request = <-deliveries:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d timed out waiting for request", i)
		}

		if err := responder.PublishWithContext(context.Background(), "", request.ReplyTo, false, false, amqplib.Publishing{
			CorrelationId: request.CorrelationId,
			Body:          []byte("response"),
		}); err != nil {
			t.Fatalf("iteration %d publish response: %v", i, err)
		}
		if err := broker.Close(); err != nil {
			t.Fatalf("iteration %d close broker: %v", i, err)
		}

		select {
		case result := <-result:
			if result.err != nil && !core.IsConnectionLost(result.err) {
				t.Fatalf("iteration %d Call() error = %v, want success or ErrConnectionLost", i, result.err)
			}
			if result.err == nil && string(result.response.Body) != "response" {
				t.Fatalf("iteration %d Call() response = %q, want response", i, result.response.Body)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d Call() did not complete", i)
		}
	}
}

func TestDuplicateCorrelationIDWithLiveRabbitMQ(t *testing.T) {
	responder, deliveries, requestQueue := newLiveRabbitMQResponder(t)
	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("create broker: %v", err)
	}
	broker := brokerAny.(*Broker)
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := broker.Connect(ctx); err != nil {
		t.Fatalf("connect broker: %v", err)
	}

	type callResult struct {
		response *core.Message
		err      error
	}
	firstResult := make(chan callResult, 1)
	go func() {
		response, err := broker.Call(ctx, requestQueue, core.NewTextMessage("first").WithCorrelationID("shared-id"))
		firstResult <- callResult{response: response, err: err}
	}()

	var request amqplib.Delivery
	select {
	case request = <-deliveries:
	case <-ctx.Done():
		t.Fatal("responder did not receive the first call")
	}

	response, err := broker.Call(ctx, requestQueue, core.NewTextMessage("second").WithCorrelationID("shared-id"))
	if response != nil || !errors.Is(err, core.ErrDuplicateCorrelationID) {
		t.Fatalf("duplicate Call() = %#v, %v; want nil, ErrDuplicateCorrelationID", response, err)
	}
	if err := responder.PublishWithContext(ctx, "", request.ReplyTo, false, false, amqplib.Publishing{
		CorrelationId: request.CorrelationId,
		Body:          []byte("first-response"),
	}); err != nil {
		t.Fatalf("publish first response: %v", err)
	}

	select {
	case result := <-firstResult:
		if result.err != nil || result.response.BodyString() != "first-response" {
			t.Fatalf("first Call() = %#v, %v", result.response, result.err)
		}
	case <-ctx.Done():
		t.Fatal("first call was stranded by the duplicate")
	}
}

func TestCanceledHandlerRedeliversWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_AMQP_E2E") != "1" {
		t.Skip("set WEAVE_AMQP_E2E=1 to run the RabbitMQ integration test")
	}
	conn := dialLiveRabbitMQ(t)
	t.Cleanup(func() { _ = conn.Close() })
	admin, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	queue, err := admin.QueueDeclare(fmt.Sprintf("weave-cancel-%d", time.Now().UnixNano()), true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.QueueDelete(queue.Name, false, false, false) })
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.QueueDurable = true
	broker, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	workCtx, interrupt := context.WithCancel(ctx)
	defer interrupt()
	started := make(chan struct{}, 1)
	redelivered := make(chan *core.Message, 1)
	var attempts atomic.Int32
	if err := broker.Subscribe(ctx, queue.Name, func(handlerCtx context.Context, msg *core.Message) error {
		if attempts.Add(1) == 1 {
			started <- struct{}{}
			<-workCtx.Done()
			return fmt.Errorf("unfinished work: %w", workCtx.Err())
		}
		select {
		case redelivered <- msg:
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		}
		return nil
	}, core.WithWorkerCount(1)); err != nil {
		t.Fatal(err)
	}
	message := core.NewTextMessage("unfinished").WithHeader("trace-id", "cancel-regression")
	message.MessageID = "unfinished-message"
	if err := broker.Publish(ctx, queue.Name, message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	interrupt()
	select {
	case got := <-redelivered:
		if got.BodyString() != message.BodyString() || got.MessageID != message.MessageID || got.GetHeader("trace-id") != message.GetHeader("trace-id") {
			t.Fatalf("redelivery lost the original message: %+v", got)
		}
		if attempts.Load() != 2 {
			t.Fatalf("handler attempts = %d, want 2", attempts.Load())
		}
	case <-ctx.Done():
		t.Fatal("canceled work was not redelivered under the default handler error policy")
	}
}

func newLiveRabbitMQResponder(t *testing.T) (*amqplib.Channel, <-chan amqplib.Delivery, string) {
	t.Helper()
	if os.Getenv("WEAVE_AMQP_E2E") != "1" {
		t.Skip("set WEAVE_AMQP_E2E=1 to run the RabbitMQ integration test")
	}

	responderConn := dialLiveRabbitMQ(t)
	t.Cleanup(func() { _ = responderConn.Close() })

	responder, err := responderConn.Channel()
	if err != nil {
		t.Fatalf("open responder channel: %v", err)
	}
	t.Cleanup(func() { _ = responder.Close() })

	requests, err := responder.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare request queue: %v", err)
	}
	deliveries, err := responder.Consume(requests.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume requests: %v", err)
	}
	return responder, deliveries, requests.Name
}

func dialLiveRabbitMQ(t *testing.T) *amqplib.Connection {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := amqplib.Dial("amqp://guest:guest@localhost:5672/")
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("connect responder: RabbitMQ did not accept an AMQP handshake within 15s: %v", lastErr)
	return nil
}
