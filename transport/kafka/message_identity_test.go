package kafka

import (
	"context"
	"reflect"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

func TestMessageIdentityRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name, id, header, want string
	}{
		{name: "no identity"},
		{name: "explicit identity", id: "order-created-42", want: "order-created-42"},
		{name: "unicode identity", id: "订单-42", want: "订单-42"},
		{name: "field wins over custom header", id: "canonical", header: "stale", want: "canonical"},
		{name: "raw header interoperability", header: "external-producer", want: "external-producer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			producer := &fakeSyncProducer{}
			broker := &Broker{
				config: &core.Config{}, kafkaConfig: core.DefaultKafkaConfig(),
				producer: producer, connected: true,
			}
			msg := core.NewTextMessage("body").WithHeader("trace-id", "trace")
			msg.MessageID = test.id
			if test.header != "" {
				msg.Headers["message-id"] = test.header
			}
			original := msg.Clone()
			if err := broker.Publish(context.Background(), "orders", msg); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(msg, original) {
				t.Fatal("Publish mutated caller-owned message")
			}
			headers := make([]*sarama.RecordHeader, 0, len(producer.lastMsg.Headers))
			identityHeaders := 0
			for _, header := range producer.lastMsg.Headers {
				if string(header.Key) == "message-id" {
					identityHeaders++
				}
				headers = append(headers, &header)
			}
			wantHeaders := 1
			if test.want == "" {
				wantHeaders = 0
			}
			if identityHeaders != wantHeaders {
				t.Fatalf("identity headers=%d, want %d", identityHeaders, wantHeaders)
			}
			var received *core.Message
			consumer := &consumerGroupHandler{
				handler:   func(_ context.Context, msg *core.Message) error { received = msg; return nil },
				closeChan: make(chan struct{}),
			}
			claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
			claim.messages <- &sarama.ConsumerMessage{Topic: "orders", Value: []byte("body"), Headers: headers}
			close(claim.messages)
			if err := consumer.ConsumeClaim(&fakeSession{}, claim); err != nil {
				t.Fatal(err)
			}
			if received == nil || received.MessageID != test.want || received.GetHeader("trace-id") != "trace" {
				t.Fatalf("received=%+v, want MessageID=%q", received, test.want)
			}
			if _, exists := received.Headers["message-id"]; exists {
				t.Fatal("reserved message-id header leaked into application headers")
			}
		})
	}
}
