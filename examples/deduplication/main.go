package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/prabhatdotdev/weave/core"
)

func main() {
	handler := deduplicate(func(_ context.Context, msg *core.Message) error {
		fmt.Printf("processed %s: %s\n", msg.MessageID, msg.BodyString())
		return nil
	})
	msg := core.NewTextMessage("order created")
	msg.MessageID = "order-created-42"
	for range 2 {
		if err := handler(context.Background(), msg.Clone()); err != nil {
			log.Fatal(err)
		}
	}
}

// deduplicate remembers successful deliveries for one logical consumer.
func deduplicate(next core.Handler) core.Handler {
	var mu sync.Mutex
	// shortcut: this process-local map grows without eviction; use a durable inbox in production.
	completed := make(map[string]struct{})
	return func(ctx context.Context, msg *core.Message) error {
		id := msg.MessageID
		if id == "" {
			if err := ctx.Err(); err != nil {
				return err
			}
			return next(ctx, msg)
		}
		// Hold the lock through processing so concurrent copies cannot both run.
		mu.Lock()
		defer mu.Unlock()
		if _, exists := completed[id]; exists {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := next(ctx, msg); err != nil {
			return err
		}
		completed[id] = struct{}{}
		return nil
	}
}
