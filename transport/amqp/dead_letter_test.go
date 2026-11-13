package amqp

import (
	"context"
	"errors"
	"testing"

	"github.com/prabhatdotdev/weave/core"
	amqplib "github.com/rabbitmq/amqp091-go"
)

type deadLetterPublisherFunc func(context.Context, string, *core.Message, ...core.PublishOption) error

func (f deadLetterPublisherFunc) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	return f(ctx, destination, msg, opts...)
}

func TestDeadLetterHandoffControlsAMQPAcknowledgment(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "confirmed destination"
		if failed {
			name = "failed destination"
		}
		t.Run(name, func(t *testing.T) {
			ack := &ackRecorder{}
			calls := 0
			handler, err := core.DeadLetterHandler(func(context.Context, *core.Message) error {
				calls++
				return errors.New("processing failed")
			}, core.RetryPolicy{MaxAttempts: 3}, deadLetterPublisherFunc(func(_ context.Context, destination string, msg *core.Message, _ ...core.PublishOption) error {
				if ack.ackCalls != 0 || ack.nackCalls != 0 {
					t.Fatal("source settled before dead-letter publish completed")
				}
				envelope, err := core.DecodeDeadLetterMessage(msg)
				if err != nil || destination != "dead" || envelope.Original.MessageID != "original" || envelope.Attempt != 3 {
					t.Fatalf("unexpected dead-letter envelope: %+v err=%v", envelope, err)
				}
				if failed {
					return errors.New("destination unavailable")
				}
				return nil
			}), "dead", core.DeadLetterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			broker := &Broker{}
			broker.handleMessage(context.Background(), amqplib.Delivery{
				Acknowledger: ack, DeliveryTag: 1, MessageId: "original", Body: []byte("body"),
			}, handler, false, core.HandlerErrorNoRetry)
			if calls != 3 {
				t.Errorf("handler attempts=%d, want 3", calls)
			}
			if failed {
				if ack.ackCalls != 0 || ack.nackCalls != 1 || !ack.lastRequeue {
					t.Fatalf("failed destination discarded source: %+v", ack)
				}
			} else if ack.ackCalls != 1 || ack.nackCalls != 0 {
				t.Fatalf("confirmed destination did not acknowledge source: %+v", ack)
			}
		})
	}
}
