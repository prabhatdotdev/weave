package kafka

import (
	"crypto/tls"
	"testing"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

func TestTLSAndSASLConfigurationAreIndependentSnapshots(t *testing.T) {
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.TLS = &tls.Config{ServerName: "localhost"}
	config.Kafka.ClientID = "tls-client"
	config.Kafka.SASL = &core.SASLConfig{Enable: true, Mechanism: "SCRAM-SHA-512", Username: "user", Password: "password"}
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()
	saramaConfig := broker.buildSaramaConfig()
	if !saramaConfig.Net.TLS.Enable || saramaConfig.Net.TLS.Config.InsecureSkipVerify || saramaConfig.Net.TLS.Config.MinVersion != tls.VersionTLS12 || saramaConfig.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA512 || saramaConfig.ClientID != "tls-client" {
		t.Fatal("TLS, SASL, or client identity configuration was not applied")
	}
	config.Kafka.Brokers[0] = "changed:9092"
	config.Kafka.TLS.ServerName = "changed"
	config.Kafka.SASL.Username = "changed"
	if broker.kafkaConfig.Brokers[0] != "localhost:9092" || broker.kafkaConfig.TLS.ServerName != "localhost" || broker.kafkaConfig.SASL.Username != "user" || config.Kafka.TLS.MinVersion != 0 {
		t.Fatal("constructor modified or retained caller-owned configuration")
	}
	saramaConfig.Net.TLS.Config.ServerName = "changed-again"
	if broker.kafkaConfig.TLS.ServerName != "localhost" {
		t.Fatal("Sarama configuration retained the broker's TLS settings by reference")
	}
}
