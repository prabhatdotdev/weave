package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

func TestTLSWithLiveKafka(t *testing.T) {
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
	for _, scenario := range []string{"TLS", "SCRAM-SHA-256", "SCRAM-SHA-512", "untrusted CA", "hostname mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			config := core.DefaultConfig()
			config.ConnectionRetry, config.RetryDelay = 1, time.Millisecond
			config.Kafka = core.DefaultKafkaConfig()
			config.Kafka.Brokers = []string{"localhost:19095"}
			config.Kafka.TLS = &tls.Config{RootCAs: trusted}
			config.Kafka.ConsumerGroup = fmt.Sprintf("weave-tls-%d", time.Now().UnixNano())
			config.Kafka.RequiredAcks = -1
			if scenario == "SCRAM-SHA-256" || scenario == "SCRAM-SHA-512" {
				config.Kafka.Brokers = []string{"localhost:19096"}
				config.Kafka.SASL = &core.SASLConfig{Enable: true, Mechanism: scenario, Username: "weave", Password: "weave-test-password"}
			}
			if scenario == "untrusted CA" {
				config.Kafka.TLS.RootCAs = x509.NewCertPool()
			}
			if scenario == "hostname mismatch" {
				config.Kafka.TLS.ServerName = "wrong.example"
			}
			brokerAny, err := NewBroker(config)
			if err != nil {
				t.Fatal(err)
			}
			broker := brokerAny.(*Broker)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = broker.Connect(ctx)
			if scenario == "untrusted CA" || scenario == "hostname mismatch" {
				defer broker.Close()
				if err == nil || broker.IsConnected() {
					t.Fatal("connection succeeded without verifying the server certificate")
				}
				return
			}
			if err != nil {
				t.Fatalf("verified TLS connection: %v", err)
			}
			admin, err := sarama.NewClusterAdmin(config.Kafka.Brokers, broker.buildSaramaConfig())
			if err != nil {
				_ = broker.Close()
				t.Fatal(err)
			}
			topic := config.Kafka.ConsumerGroup
			t.Cleanup(func() { _ = admin.DeleteTopic(topic); _ = admin.Close() })
			t.Cleanup(func() { _ = broker.Close() })
			if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
				t.Fatal(err)
			}
			if err := broker.Publish(ctx, topic, core.NewTextMessage("encrypted"), core.WithTimeout(3*time.Second)); err != nil {
				t.Fatal(err)
			}
			received := make(chan string, 1)
			if err := broker.Subscribe(ctx, topic, func(handlerCtx context.Context, msg *core.Message) error {
				select {
				case received <- msg.BodyString():
				case <-handlerCtx.Done():
				}
				return nil
			}, core.WithStartFromBeginning()); err != nil {
				t.Fatal(err)
			}
			select {
			case body := <-received:
				if body != "encrypted" {
					t.Fatalf("received %q", body)
				}
			case <-ctx.Done():
				t.Fatal("TLS consumer did not receive an acknowledged publish")
			}
		})
	}
}
