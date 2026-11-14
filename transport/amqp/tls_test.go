package amqp

import (
	"crypto/tls"
	"testing"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

func TestTLSConfigurationAndEscapedConnectionAddress(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = core.DefaultAMQPConfig()
	config.AMQP.TLS = &tls.Config{ServerName: "localhost"}
	config.AMQP.Host, config.AMQP.Port = "::1", 5671
	config.AMQP.Username, config.AMQP.Password = "user:@/", "password:@/#"
	config.AMQP.VHost = "tenant/one"
	brokerAny, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()
	address, err := amqplib.ParseURI(broker.connectionString(broker.endpoints[0]))
	if err != nil {
		t.Fatal(err)
	}
	if address.Scheme != "amqps" || address.Host != "::1" || address.Port != 5671 || address.Username != config.AMQP.Username || address.Password != config.AMQP.Password || address.Vhost != config.AMQP.VHost {
		t.Fatalf("connection address did not preserve TLS, IPv6, or escaped credentials: %#v", address)
	}
	config.AMQP.TLS.ServerName = "changed"
	config.AMQP.Host = "changed"
	if broker.amqpConfig.TLS.ServerName != "localhost" || broker.amqpConfig.Host != "::1" || config.AMQP.TLS.MinVersion != 0 || broker.amqpConfig.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatal("constructor modified or retained caller-owned configuration")
	}
}
