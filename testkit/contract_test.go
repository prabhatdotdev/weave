package testkit

import (
	"testing"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/internal/brokercontract"
)

func TestBrokerContract(t *testing.T) {
	brokercontract.Run(t, func(t *testing.T) brokercontract.Fixture {
		broker := NewMockBroker()
		return brokercontract.Fixture{
			Broker: broker,
			Published: func() []*core.Message {
				var messages []*core.Message
				for _, published := range broker.PublishedMessages() {
					messages = append(messages, published.Message)
				}
				return messages
			},
			Reply:   func(msg *core.Message) { broker.SetCallResponse("contract", msg) },
			Deliver: broker.SimulateMessage,
		}
	})
}
