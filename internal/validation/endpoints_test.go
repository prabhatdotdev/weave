package validation_test

import (
	"errors"
	"testing"

	"github.com/prabhatdotdev/weave/core"
	"github.com/prabhatdotdev/weave/transport/amqp"
)

func TestAMQPEndpointsRejectMalformedAddresses(t *testing.T) {
	for _, endpoint := range []string{
		"", "localhost", ":5672", "localhost:", "localhost:0", "localhost:65536", "localhost:-1", "localhost:+5672", "localhost:amqp",
		"amqp://localhost:5672", "amqps://localhost:5671", "user:pass@localhost:5672", "localhost:5672/vhost", "localhost:5672?tls=false",
		"bad?host:5672", "bad#host:5672", "bad%host:5672", "bad\\host:5672", "bad host:5672", "bad\u00a0host:5672", "bad\x00host:5672", string([]byte{0xff}) + ":5672",
		"::1:5672", "[not::ipv6]:5672",
	} {
		t.Run(endpoint, func(t *testing.T) {
			config := core.DefaultConfig()
			config.AMQP = &core.AMQPConfig{Endpoints: []string{"valid:5672", endpoint}}
			broker, err := amqp.NewBroker(config)
			if broker != nil {
				_ = broker.Close()
			}
			if !errors.Is(err, core.ErrInvalidConfig) {
				t.Fatalf("accepted endpoint %q: %v", endpoint, err)
			}
		})
	}
}

func TestAMQPEndpointsOverrideLegacyHostAndPort(t *testing.T) {
	config := core.DefaultConfig()
	config.AMQP = &core.AMQPConfig{Endpoints: []string{"rabbit-1.example:5672", "127.0.0.1:5672", "[::1]:5671", "[fe80::1%lo0]:5672"}}
	broker, err := amqp.NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	_ = broker.Close()
	config.AMQP.Endpoints = []string{}
	if _, err := amqp.NewBroker(config); !errors.Is(err, core.ErrInvalidConfig) {
		t.Fatalf("empty endpoints did not validate legacy host/port: %v", err)
	}
}
