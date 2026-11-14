package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/testkit"
)

func TestLoggingHookExportsOperationalMetrics(t *testing.T) {
	for _, failed := range []bool{false, true} {
		var output bytes.Buffer
		hook := loggingHook(slog.New(slog.NewJSONHandler(&output, nil)))
		event := core.Event{Name: core.EventCallFinished, Backend: "kafka", Operation: "call", Destination: "orders", Duration: 1500 * time.Microsecond, ActiveHandlers: 2, PendingRPCs: 3}
		if failed {
			event.Err = errors.New("destination failed")
		}
		hook(context.Background(), event)
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["event"] != core.EventCallFinished || record["duration_ms"] != 1.5 || record["active_handlers"] != float64(2) || record["pending_rpcs"] != float64(3) {
			t.Fatalf("record=%v", record)
		}
		if failed && (record["level"] != "ERROR" || record["error"] != "destination failed") {
			t.Fatalf("lost error: %v", record)
		}
		if !failed && (record["level"] != "INFO" || record["error"] != nil) {
			t.Fatalf("success marked failed: %v", record)
		}
	}
}

func TestRunWithLiveBrokers(t *testing.T) {
	if os.Getenv("WEAVE_OBSERVABILITY_E2E") != "1" && os.Getenv("WEAVE_EXAMPLES_E2E") != "1" {
		t.Skip("set WEAVE_EXAMPLES_E2E=1 and provision the example brokers and topics")
	}
	for _, backend := range []string{"amqp", "kafka"} {
		t.Run(backend, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := run(ctx, backend, "weave.example.observability.replies", logger); err != nil {
				t.Fatalf("example failed: %v\n%s", err, output.String())
			}
			decoder := json.NewDecoder(&output)
			calls, handlers := 0, 0
			last := make(map[string]map[string]any)
			for decoder.More() {
				var record map[string]any
				if err := decoder.Decode(&record); err != nil {
					t.Fatal(err)
				}
				if record["msg"] != "weave event" {
					continue
				}
				owner, _ := record["broker"].(string)
				last[owner] = record
				if record["event"] == core.EventCallFinished {
					calls++
					if owner != "client" || record["error"] != nil || record["duration_ms"].(float64) <= 0 || record["pending_rpcs"] != float64(0) {
						t.Errorf("call metrics=%v", record)
					}
				}
				if record["event"] == core.EventHandlerFinished {
					handlers++
					if owner != "responder" || record["error"] != nil || record["duration_ms"].(float64) <= 0 {
						t.Errorf("handler metrics=%v", record)
					}
				}
			}
			if calls != 1 || handlers < 1 {
				t.Fatalf("calls=%d handlers=%d; RPC bypassed the observed handler", calls, handlers)
			}
			for _, owner := range []string{"client", "responder"} {
				if last[owner]["active_handlers"] != float64(0) || last[owner]["pending_rpcs"] != float64(0) {
					t.Errorf("%s leaked work: %v", owner, last[owner])
				}
			}
		})
	}
}

func TestLoggingHookPreservesSeverityAndLifecycleFields(t *testing.T) {
	for _, level := range []core.EventLevel{core.EventLevelDebug, core.EventLevelWarn, core.EventLevelError} {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
		loggingHook(logger)(context.Background(), core.Event{
			Name: core.EventConsumerAssigned, Level: level,
			Fields: map[string]any{"consumer_group": "orders", "partitions": []int32{1, 2}},
		})
		var record struct {
			Level  string `json:"level"`
			Fields struct {
				Group      string  `json:"consumer_group"`
				Partitions []int32 `json:"partitions"`
			} `json:"fields"`
		}
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record.Level != strings.ToUpper(string(level)) || record.Fields.Group != "orders" || len(record.Fields.Partitions) != 2 {
			t.Fatalf("lost lifecycle context: %s", output.String())
		}
	}
}

func TestObservedHandlerIgnoresOldRequestsAndPreservesRouting(t *testing.T) {
	broker := testkit.NewMockBroker()
	if err := broker.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	handler := observedHandler("current", broker)
	request := core.NewTextMessage("observe me").WithHeader("example-run", "old")
	if err := handler(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.WithHeader("example-run", "current")
	if err := handler(context.Background(), request); !errors.Is(err, core.ErrNoReplyTo) {
		t.Fatalf("missing reply destination error=%v", err)
	}
	request.WithReplyTo("replies").WithCorrelationID("observed-call")
	if err := handler(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	responses := broker.PublishedTo("replies")
	if len(responses) != 1 || responses[0].Message.BodyString() != "observed" || responses[0].Message.CorrelationID != "observed-call" {
		t.Fatalf("responses=%+v", responses)
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handler(context.Background(), request); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("lost reply-publish failure: %v", err)
	}
}
