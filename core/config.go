// Copyright 2025 PrabhatDotDev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import "time"

// Config holds the configuration for a message broker.
type Config struct {
	Backend         string
	ConnectionName  string
	ConnectionRetry int
	RetryDelay      time.Duration

	// Logger receives structured runtime and transport events.
	Logger EventLogger
	// EventHook receives structured runtime and transport events.
	EventHook EventHook
	// Metrics receives basic counters and duration measurements.
	Metrics MetricsHook
	// Tracing receives span lifecycle callbacks for high-level runtime operations.
	Tracing TracingHook
	// HealthReporter receives runtime health snapshots.
	HealthReporter HealthReporter
	// HealthHook receives runtime health snapshots as a callback.
	HealthHook HealthHook

	AMQP  *AMQPConfig
	Kafka *KafkaConfig

	// Shorthand for AMQP
	Host     string
	Port     int
	Username string
	Password string
	VHost    string
}

// AMQPConfig holds AMQP/RabbitMQ-specific configuration.
type AMQPConfig struct {
	Host            string
	Port            int
	Username        string
	Password        string
	VHost           string
	Heartbeat       time.Duration
	TLS             *TLSConfig
	Exchange        string
	ExchangeType    string
	QueueDurable    bool
	QueueAutoDelete bool
	QueueExclusive  bool
}

// KafkaConfig holds Apache Kafka-specific configuration.
type KafkaConfig struct {
	Brokers           []string
	ClientID          string
	ConsumerGroup     string
	TLS               *TLSConfig
	RequiredAcks      int
	MaxRetries        int
	RetryBackoff      time.Duration
	CompressionType   string
	AutoOffsetReset   string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	SASL              *SASLConfig
}

// TLSConfig holds TLS/SSL configuration.
type TLSConfig struct {
	Enable             bool
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
}

// SASLConfig holds SASL authentication configuration (for Kafka).
type SASLConfig struct {
	Enable    bool
	Mechanism string
	Username  string
	Password  string
}

// DefaultConfig returns a Config with sensible defaults for AMQP.
func DefaultConfig() *Config {
	return &Config{
		Backend:         "amqp",
		ConnectionRetry: 3,
		RetryDelay:      2 * time.Second,
		AMQP:            DefaultAMQPConfig(),
	}
}

// DefaultAMQPConfig returns default AMQP configuration.
func DefaultAMQPConfig() *AMQPConfig {
	return &AMQPConfig{
		Host:      "localhost",
		Port:      5672,
		Username:  "guest",
		Password:  "guest",
		VHost:     "/",
		Heartbeat: 10 * time.Second,
	}
}

// DefaultKafkaConfig returns default Kafka configuration.
func DefaultKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Brokers:           []string{"localhost:9092"},
		RequiredAcks:      1,
		MaxRetries:        3,
		RetryBackoff:      100 * time.Millisecond,
		AutoOffsetReset:   "latest",
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	}
}

// WithBackend sets the backend and returns the config for chaining.
func (c *Config) WithBackend(backend string) *Config {
	c.Backend = backend
	return c
}

// WithAMQP sets AMQP configuration and returns the config for chaining.
func (c *Config) WithAMQP(amqp *AMQPConfig) *Config {
	c.AMQP = amqp
	c.Backend = "amqp"
	return c
}

// WithKafka sets Kafka configuration and returns the config for chaining.
func (c *Config) WithKafka(kafka *KafkaConfig) *Config {
	c.Kafka = kafka
	c.Backend = "kafka"
	return c
}
