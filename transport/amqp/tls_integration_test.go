package amqp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

func TestTLSWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_TLS_E2E") == "" {
		t.Skip("set WEAVE_TLS_E2E=1 and start testdata/tls/compose.yaml")
	}
	certificate, err := os.ReadFile("../../testdata/tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	trusted := x509.NewCertPool()
	if !trusted.AppendCertsFromPEM(certificate) {
		t.Fatal("invalid test CA")
	}
	for _, scenario := range []string{"verified", "untrusted CA", "hostname mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			config := core.DefaultConfig()
			config.ConnectionRetry, config.RetryDelay = 1, time.Millisecond
			config.AMQP = core.DefaultAMQPConfig()
			config.AMQP.Port = 5671
			config.AMQP.QueueExclusive, config.AMQP.QueueAutoDelete = true, true
			config.AMQP.TLS = &tls.Config{RootCAs: trusted}
			if scenario == "untrusted CA" {
				config.AMQP.TLS.RootCAs = x509.NewCertPool()
			}
			if scenario == "hostname mismatch" {
				config.AMQP.TLS.ServerName = "wrong.example"
			}
			broker, err := NewBroker(config)
			if err != nil {
				t.Fatal(err)
			}
			defer broker.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = broker.Connect(ctx)
			if scenario != "verified" {
				if err == nil || broker.IsConnected() {
					t.Fatal("connection succeeded without verifying the server certificate")
				}
				return
			}
			if err != nil {
				t.Fatalf("verified TLS connection: %v", err)
			}
			queue := fmt.Sprintf("weave-tls-%d", time.Now().UnixNano())
			received := make(chan string, 1)
			if err := broker.Subscribe(ctx, queue, func(_ context.Context, msg *core.Message) error { received <- msg.BodyString(); return nil }); err != nil {
				t.Fatal(err)
			}
			if err := broker.Publish(ctx, queue, core.NewTextMessage("encrypted"), core.WithMandatory(), core.WithPersistent(), core.WithTimeout(3*time.Second)); err != nil {
				t.Fatal(err)
			}
			select {
			case body := <-received:
				if body != "encrypted" {
					t.Fatalf("received %q", body)
				}
			case <-ctx.Done():
				t.Fatal("TLS consumer did not receive a confirmed publish")
			}
		})
	}
}

func TestConfirmedMandatoryPublishingWithLiveRabbitMQ(t *testing.T) {
	if os.Getenv("WEAVE_AMQP_E2E") == "" {
		t.Skip("set WEAVE_AMQP_E2E=1 to test publisher confirmations")
	}
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.QueueAutoDelete, config.AMQP.QueueExclusive = true, true
	broker, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := broker.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	queue := fmt.Sprintf("weave-confirm-%d", time.Now().UnixNano())
	err = broker.Publish(ctx, queue, core.NewTextMessage("unroutable"), core.WithMandatory())
	if !errors.Is(err, core.ErrUnroutable) {
		t.Fatalf("mandatory publish to a missing queue returned %v", err)
	}
	received := make(chan string, 10)
	if err := broker.Subscribe(ctx, queue, func(_ context.Context, msg *core.Message) error { received <- msg.BodyString(); return nil }); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 10)
	for i := range 10 {
		go func() {
			results <- broker.Publish(ctx, queue, core.NewTextMessage(fmt.Sprint(i)), core.WithMandatory(), core.WithPersistent())
		}()
	}
	for range 10 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("confirmed publish after basic.return: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent publishers did not complete")
		}
	}
	seen := make(map[string]bool)
	for len(seen) != 10 {
		select {
		case body := <-received:
			seen[body] = true
		case <-ctx.Done():
			t.Fatalf("confirmed messages were missing: %v", seen)
		}
	}
}
