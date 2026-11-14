package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prabhatdotdev/weave/core"
)

func TestDeduplicateRetriesFailuresAndRemembersSuccess(t *testing.T) {
	failure := errors.New("temporary failure")
	calls := 0
	handler := deduplicate(func(context.Context, *core.Message) error {
		calls++
		if calls == 1 {
			return failure
		}
		return nil
	})
	msg := core.NewTextMessage("order created")
	msg.MessageID = "order-1"
	if err := handler(context.Background(), msg); !errors.Is(err, failure) {
		t.Fatalf("first delivery error=%v", err)
	}
	for range 2 {
		if err := handler(context.Background(), msg.Clone()); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want one failure and one success", calls)
	}
	msg.MessageID = "order-2"
	if err := handler(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("distinct ID was skipped: calls=%d", calls)
	}
}

func TestDeduplicateConcurrentCopies(t *testing.T) {
	calls := 0
	handler := deduplicate(func(context.Context, *core.Message) error { calls++; return nil })
	msg := core.NewTextMessage("order created")
	msg.MessageID = "order-1"
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := handler(context.Background(), msg.Clone()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("processed concurrent copies %d times", calls)
	}
}

func TestDeduplicateWithoutIdentity(t *testing.T) {
	calls := 0
	handler := deduplicate(func(context.Context, *core.Message) error { calls++; return nil })
	for range 2 {
		if err := handler(context.Background(), core.NewTextMessage("same body")); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("unidentified deliveries were coalesced: calls=%d", calls)
	}
}

func TestDeduplicateUsesOriginalIdentity(t *testing.T) {
	calls := 0
	handler := deduplicate(func(_ context.Context, msg *core.Message) error {
		calls++
		msg.MessageID = "changed-by-handler"
		return nil
	})
	msg := core.NewTextMessage("order created")
	msg.MessageID = "order-1"
	for range 2 {
		if err := handler(context.Background(), msg.Clone()); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler mutation lost the original identity: calls=%d", calls)
	}
}

func TestDeduplicateCancellationDoesNotRememberUnfinishedWork(t *testing.T) {
	calls := 0
	handler := deduplicate(func(context.Context, *core.Message) error { calls++; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := core.NewTextMessage("order created")
	msg.MessageID = "order-1"
	if err := handler(ctx, msg); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delivery error=%v", err)
	}
	if calls != 0 {
		t.Fatal("canceled delivery was processed")
	}
	if err := handler(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("canceled delivery prevented a later attempt")
	}
}

func TestDeduplicateCanceledMessageWithoutIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	handler := deduplicate(func(context.Context, *core.Message) error {
		called = true
		return nil
	})
	if err := handler(ctx, core.NewTextMessage("no identity")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delivery error=%v", err)
	}
	if called {
		t.Fatal("canceled message without an ID repeated a side effect")
	}
}

func Example() {
	main()
	// Output: processed order-created-42: order created
}
