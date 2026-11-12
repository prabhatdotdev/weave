package kafka

import (
	"errors"
	"testing"

	"github.com/xdg-go/scram"

	"github.com/prabhatdotdev/weave/core"
)

func TestSCRAMAuthentication(t *testing.T) {
	for _, mechanism := range []struct {
		name string
		hash scram.HashGeneratorFcn
	}{
		{"SCRAM-SHA-256", scram.SHA256},
		{"SCRAM-SHA-512", scram.SHA512},
	} {
		t.Run(mechanism.name, func(t *testing.T) {
			config := core.DefaultConfig()
			config.Kafka = core.DefaultKafkaConfig()
			config.Kafka.SASL = &core.SASLConfig{Enable: true, Mechanism: mechanism.name, Username: "alice", Password: "correct-password"}
			brokerAny, err := NewBroker(config)
			if err != nil {
				t.Fatal(err)
			}
			saramaConfig := brokerAny.(*Broker).buildSaramaConfig()
			if err := saramaConfig.Validate(); err != nil {
				t.Fatalf("SCRAM configuration is invalid: %v", err)
			}
			generator := saramaConfig.Net.SASL.SCRAMClientGeneratorFunc
			if generator == nil {
				t.Fatal("SCRAM client generator is missing")
			}
			credentials, err := mechanism.hash.NewClient("alice", "correct-password", "")
			if err != nil {
				t.Fatal(err)
			}
			stored := credentials.GetStoredCredentials(scram.KeyFactors{Salt: "test-salt", Iters: 4096})
			server, err := mechanism.hash.NewServer(func(username string) (scram.StoredCredentials, error) {
				if username != "alice" {
					return scram.StoredCredentials{}, errors.New("unknown user")
				}
				return stored, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, password := range []string{"correct-password", "wrong-password"} {
				t.Run(password, func(t *testing.T) {
					client := generator()
					if err := client.Begin("alice", password, ""); err != nil {
						t.Fatal(err)
					}
					conversation := server.NewConversation()
					response, err := client.Step("")
					if err != nil {
						t.Fatal(err)
					}
					challenge, err := conversation.Step(response)
					if err != nil {
						t.Fatal(err)
					}
					response, err = client.Step(challenge)
					if err != nil {
						t.Fatal(err)
					}
					challenge, err = conversation.Step(response)
					if password == "wrong-password" {
						if err == nil || conversation.Valid() {
							t.Fatal("incorrect password was accepted")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if _, err := client.Step(challenge); err != nil {
						t.Fatal(err)
					}
					if !client.Done() || !conversation.Valid() {
						t.Fatal("SCRAM exchange did not authenticate both peers")
					}
				})
			}
		})
	}
}

func TestNewBrokerRejectsUnsupportedSASLMechanisms(t *testing.T) {
	config := core.DefaultConfig()
	config.Kafka = core.DefaultKafkaConfig()
	config.Kafka.SASL = &core.SASLConfig{Enable: true, Mechanism: "SCRAM-SHA-999"}
	if _, err := NewBroker(config); !errors.Is(err, core.ErrInvalidConfig) {
		t.Fatalf("NewBroker() error = %v, want ErrInvalidConfig", err)
	}
}
