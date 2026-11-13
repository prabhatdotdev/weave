package amqp

import (
	"context"
	"sync"
	"testing"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokercontract"
	amqplib "github.com/rabbitmq/amqp091-go"
)

type contractChannel struct {
	*fakeAMQPChannel
	response *core.Message // protected by fakeAMQPChannel.mu
}

func incomingContractMessage(msg *core.Message) amqplib.Delivery {
	msg = msg.Clone()
	delivery := amqplib.Delivery{Body: msg.Body, CorrelationId: msg.CorrelationID, ReplyTo: msg.ReplyTo,
		ContentType: msg.ContentType, MessageId: msg.MessageID, Timestamp: msg.Timestamp, Headers: amqplib.Table{}}
	for key, value := range msg.Headers {
		delivery.Headers[key] = value
	}
	return delivery
}

func (c *contractChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqplib.Publishing) error {
	if err := c.fakeAMQPChannel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg); err != nil {
		return err
	}
	c.mu.Lock()
	response, stream := c.response, c.consumerStreams[msg.ReplyTo]
	if response != nil {
		response = response.Clone()
	}
	c.mu.Unlock()
	if response != nil && stream != nil {
		response.CorrelationID = msg.CorrelationId
		select {
		case stream <- incomingContractMessage(response):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type contractAck struct {
	once sync.Once
	done chan struct{}
}

func (a *contractAck) Ack(uint64, bool) error        { a.once.Do(func() { close(a.done) }); return nil }
func (a *contractAck) Nack(uint64, bool, bool) error { return a.Ack(0, false) }
func (a *contractAck) Reject(uint64, bool) error     { return a.Ack(0, false) }

func TestBrokerContract(t *testing.T) {
	brokercontract.Run(t, func(t *testing.T) brokercontract.Fixture {
		brokerAny, err := NewBroker(nil)
		if err != nil {
			t.Fatal(err)
		}
		broker := brokerAny.(*Broker)
		channel := &contractChannel{fakeAMQPChannel: newFakeAMQPChannel()}
		broker.dial = func(string, amqplib.Config) (amqpConnection, error) {
			return &fakeAMQPConnection{channel: channel}, nil
		}
		return brokercontract.Fixture{
			Broker: broker,
			Reply:  func(msg *core.Message) { channel.mu.Lock(); defer channel.mu.Unlock(); channel.response = msg.Clone() },
			Published: func() []*core.Message {
				channel.mu.Lock()
				defer channel.mu.Unlock()
				var messages []*core.Message
				for _, wire := range channel.published {
					msg := &core.Message{Body: wire.Body, CorrelationID: wire.CorrelationId, ReplyTo: wire.ReplyTo,
						ContentType: wire.ContentType, MessageID: wire.MessageId, Timestamp: wire.Timestamp, Headers: map[string]string{}}
					for key, value := range wire.Headers {
						if value, ok := value.(string); ok {
							msg.Headers[key] = value
						}
					}
					messages = append(messages, msg)
				}
				return messages
			},
			Deliver: func(ctx context.Context, destination string, msg *core.Message) error {
				channel.mu.Lock()
				stream := channel.consumerStreams[destination]
				channel.mu.Unlock()
				ack := &contractAck{done: make(chan struct{})}
				wire := incomingContractMessage(msg)
				wire.Acknowledger = ack
				select {
				case stream <- wire:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-ack.done:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
	})
}
