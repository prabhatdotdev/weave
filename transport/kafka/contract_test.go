package kafka

import (
	"context"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokercontract"
)

func incomingContractMessage(topic string, msg *core.Message) *sarama.ConsumerMessage {
	msg = msg.Clone()
	wire := &sarama.ConsumerMessage{Topic: topic, Value: msg.Body, Timestamp: msg.Timestamp}
	for key, value := range msg.Headers {
		wire.Headers = append(wire.Headers, &sarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
	}
	for key, value := range map[string]string{"correlation-id": msg.CorrelationID, "reply-to": msg.ReplyTo, "content-type": msg.ContentType, "message-id": msg.MessageID} {
		if value != "" {
			wire.Headers = append(wire.Headers, &sarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
		}
	}
	return wire
}

func TestBrokerContract(t *testing.T) {
	brokercontract.Run(t, func(t *testing.T) brokercontract.Fixture {
		config := core.DefaultConfig()
		config.Kafka = core.DefaultKafkaConfig()
		config.Kafka.ConsumerGroup, config.Kafka.ReplyTopic = "contract", "contract.internal.reply"
		brokerAny, err := NewBroker(config)
		if err != nil {
			t.Fatal(err)
		}
		broker := brokerAny.(*Broker)
		var mu sync.Mutex
		var response *core.Message
		var published []*sarama.ProducerMessage
		consume := func(ctx context.Context, wire *sarama.ConsumerMessage) error {
			_, routes := broker.activeSubscriptionRoutes()
			handler := &consumerGroupHandler{routes: routes, closeChan: broker.closeChan, pending: broker.pending,
				pendingMu: &broker.pendingMu, config: broker.config, work: &broker.work, metrics: &broker.metrics}
			messages := make(chan *sarama.ConsumerMessage, 1)
			messages <- wire
			close(messages)
			return handler.ConsumeClaim(&fakeSession{ctx: ctx}, &fakeClaim{messages: messages})
		}
		producer := &fakeSyncProducer{afterSend: func(wire *sarama.ProducerMessage) {
			mu.Lock()
			published = append(published, wire)
			reply := response
			if reply != nil {
				reply = reply.Clone()
			}
			mu.Unlock()
			if reply != nil && headerValue(wire, "reply-to") != "" {
				reply.CorrelationID = headerValue(wire, "correlation-id")
				if err := consume(context.Background(), incomingContractMessage(headerValue(wire, "reply-to"), reply)); err != nil {
					t.Error(err)
				}
			}
		}}
		broker.newProducer = func([]string, *sarama.Config) (messageProducer, error) { return producer, nil }
		broker.newConsumerGroup = func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) { return &fakeConsumerGroup{}, nil }
		return brokercontract.Fixture{
			Broker: broker,
			Reply:  func(msg *core.Message) { mu.Lock(); defer mu.Unlock(); response = msg.Clone() },
			Deliver: func(ctx context.Context, topic string, msg *core.Message) error {
				return consume(ctx, incomingContractMessage(topic, msg))
			},
			Published: func() []*core.Message {
				mu.Lock()
				defer mu.Unlock()
				var messages []*core.Message
				for _, wire := range published {
					body, err := wire.Value.Encode()
					if err != nil {
						t.Fatal(err)
					}
					msg := &core.Message{Body: body, Timestamp: wire.Timestamp, Headers: map[string]string{}}
					for _, header := range wire.Headers {
						value := string(header.Value)
						switch key := string(header.Key); key {
						case "correlation-id":
							msg.CorrelationID = value
						case "reply-to":
							msg.ReplyTo = value
						case "content-type":
							msg.ContentType = value
						case "message-id":
							msg.MessageID = value
						default:
							msg.Headers[key] = value
						}
					}
					messages = append(messages, msg)
				}
				return messages
			},
		}
	})
}
