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
	ConnectionName  string
	ConnectionRetry int
	RetryDelay      time.Duration
	EventHook       EventHook
	AMQP            *AMQPConfig
	Kafka           *KafkaConfig
}

// AMQPConfig holds AMQP/RabbitMQ-specific configuration.
type AMQPConfig struct {
	Host            string
	Port            int
	Username        string
	Password        string
	VHost           string
	Heartbeat       time.Duration
	Exchange        string
	QueueDurable    bool
	QueueAutoDelete bool
	QueueExclusive  bool
}

// KafkaConfig holds Apache Kafka-specific configuration.
type KafkaConfig struct {
	Brokers           []string
	ClientID          string
	ConsumerGroup     string
	RequiredAcks      int
	MaxRetries        int
	RetryBackoff      time.Duration
	AutoOffsetReset   string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	SASL              *SASLConfig
}

// SASLConfig holds SASL authentication configuration (for Kafka).
type SASLConfig struct {
	Enable    bool
	Mechanism string
	Username  string
	Password  string
}

// DefaultConfig returns common connection defaults.
func DefaultConfig() *Config {
	return &Config{
		ConnectionRetry: 3,
		RetryDelay:      2 * time.Second,
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
