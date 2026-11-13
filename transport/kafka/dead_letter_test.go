package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prabhatdotdev/weave/core"
)

type deadLetterPublisherFunc func(context.Context, string, *core.Message, ...core.PublishOption) error

func (f deadLetterPublisherFunc) Publish(ctx context.Context, destination string, msg *core.Message, opts ...core.PublishOption) error {
	return f(ctx, destination, msg, opts...)
}

func TestDeadLetterHandoffControlsKafkaOffsets(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "confirmed destination"
		if failed {
			name = "failed destination"
		}
		t.Run(name, func(t *testing.T) {
			session := &fakeSession{}
			calls := 0
			handler, err := core.DeadLetterHandler(func(_ context.Context, msg *core.Message) error {
				calls++
				if msg.Offset == 11 {
					return nil
				}
				return errors.New("processing failed")
			}, core.RetryPolicy{MaxAttempts: 3}, deadLetterPublisherFunc(func(_ context.Context, destination string, msg *core.Message, _ ...core.PublishOption) error {
				if len(session.marked) != 0 {
					t.Fatal("source marked before dead-letter publish completed")
				}
				envelope, err := core.DecodeDeadLetterMessage(msg)
				if err != nil || destination != "dead" || envelope.Original.Offset != 10 || envelope.Original.Subject != "user-key" || envelope.Attempt != 3 {
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
			consumer := &consumerGroupHandler{
				routes:    map[string]subscriptionRoute{"users": {handler: handler, policy: core.HandlerErrorNoRetry}},
				closeChan: make(chan struct{}),
			}
			claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
			claim.messages <- &sarama.ConsumerMessage{Topic: "users", Key: []byte("user-key"), Offset: 10, Value: []byte("failed")}
			claim.messages <- &sarama.ConsumerMessage{Topic: "users", Offset: 11, Value: []byte("later")}
			close(claim.messages)
			err = consumer.ConsumeClaim(session, claim)
			if failed {
				if !core.IsDeadLetterFailed(err) || len(session.marked) != 0 || calls != 3 {
					t.Fatalf("failed destination advanced source: err=%v marked=%d calls=%d", err, len(session.marked), calls)
				}
			} else if err != nil || len(session.marked) != 2 || calls != 4 {
				t.Fatalf("successful handoff: err=%v marked=%d calls=%d", err, len(session.marked), calls)
			}
		})
	}
}
