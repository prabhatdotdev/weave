package kafka

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestMessageIdentityWithLiveKafka(t *testing.T) {
	broker, topics, ctx := newLiveKafkaBroker(t, "message-identity", 1)
	received := make(chan *core.Message, 3)
	if err := broker.Subscribe(ctx, topics[0], func(_ context.Context, msg *core.Message) error {
		received <- msg.Clone()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	message := core.NewTextMessage("order created").WithHeader("trace-id", "trace-1")
	message.MessageID = "order-created-42"
	message.Timestamp = time.Now().UTC().Truncate(time.Millisecond)
	original := message.Clone()
	for range 2 {
		if err := broker.Publish(ctx, topics[0], message); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(message, original) {
		t.Fatal("publishing changed the original message")
	}
	if err := broker.Publish(ctx, topics[0], core.NewTextMessage("unidentified")); err != nil {
		t.Fatal(err)
	}
	var previousOffset int64 = -1
	for _, want := range []string{"order-created-42", "order-created-42", ""} {
		select {
		case msg := <-received:
			if msg.MessageID != want || msg.Offset <= previousOffset {
				t.Fatalf("received ID=%q offset=%d, want ID=%q after offset=%d", msg.MessageID, msg.Offset, want, previousOffset)
			}
			if want != "" && (msg.BodyString() != "order created" || msg.GetHeader("trace-id") != "trace-1" || !msg.Timestamp.Equal(original.Timestamp)) {
				t.Fatalf("message metadata changed: %+v", msg)
			}
			previousOffset = msg.Offset
		case <-ctx.Done():
			t.Fatal("message was not delivered")
		}
	}
	if err := broker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
