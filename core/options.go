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

// HandlerErrorPolicy controls what happens when a handler returns an error.
type HandlerErrorPolicy string

const (
	// HandlerErrorNoRetry does not request transport-level retries.
	HandlerErrorNoRetry HandlerErrorPolicy = "no_retry"
	// HandlerErrorRetry requests transport-level retry behavior where supported.
	HandlerErrorRetry HandlerErrorPolicy = "retry"
)

// SubscribeOptions holds configuration for subscription operations.
type SubscribeOptions struct {
	AutoAck            bool
	Exclusive          bool
	ConsumerTag        string
	PrefetchCount      int
	QueueBind          bool
	Exchange           string
	RoutingKey         string
	ConsumerGroup      string
	StartFromBeginning bool
	HandlerErrorPolicy HandlerErrorPolicy
}

// SubscribeOption is a functional option for configuring subscriptions.
type SubscribeOption func(*SubscribeOptions)

// WithAutoAck enables automatic message acknowledgment.
func WithAutoAck() SubscribeOption {
	return func(o *SubscribeOptions) { o.AutoAck = true }
}

// WithExclusive makes the subscription exclusive to this consumer.
func WithExclusive() SubscribeOption {
	return func(o *SubscribeOptions) { o.Exclusive = true }
}

// WithConsumerTag sets the consumer identifier.
func WithConsumerTag(tag string) SubscribeOption {
	return func(o *SubscribeOptions) { o.ConsumerTag = tag }
}

// WithPrefetchCount sets the prefetch limit.
func WithPrefetchCount(count int) SubscribeOption {
	return func(o *SubscribeOptions) { o.PrefetchCount = count }
}

// WithQueueBind configures exchange binding (AMQP-specific).
func WithQueueBind(exchange, routingKey string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.QueueBind = true
		o.Exchange = exchange
		o.RoutingKey = routingKey
	}
}

// WithConsumerGroup sets the consumer group (Kafka/Redis/NATS).
func WithConsumerGroup(group string) SubscribeOption {
	return func(o *SubscribeOptions) { o.ConsumerGroup = group }
}

// WithStartFromBeginning starts consuming from the earliest offset.
func WithStartFromBeginning() SubscribeOption {
	return func(o *SubscribeOptions) { o.StartFromBeginning = true }
}

// WithHandlerErrorRetry requests transport-level retry when a handler returns an error.
func WithHandlerErrorRetry() SubscribeOption {
	return func(o *SubscribeOptions) { o.HandlerErrorPolicy = HandlerErrorRetry }
}

// WithHandlerErrorNoRetry disables transport-level retry when a handler returns an error.
func WithHandlerErrorNoRetry() SubscribeOption {
	return func(o *SubscribeOptions) { o.HandlerErrorPolicy = HandlerErrorNoRetry }
}

// PublishOptions holds configuration for publish operations.
type PublishOptions struct {
	Timeout      time.Duration
	Mandatory    bool
	Immediate    bool
	Exchange     string
	Partition    int
	Key          string
	DeliveryMode uint8
	Priority     uint8
	Expiration   string
}

// PublishOption is a functional option for configuring publish operations.
type PublishOption func(*PublishOptions)

// WithTimeout sets the publish timeout.
func WithTimeout(timeout time.Duration) PublishOption {
	return func(o *PublishOptions) { o.Timeout = timeout }
}

// WithMandatory requires message routing to a queue.
func WithMandatory() PublishOption {
	return func(o *PublishOptions) { o.Mandatory = true }
}

// WithExchange specifies the target exchange (AMQP).
func WithExchange(exchange string) PublishOption {
	return func(o *PublishOptions) { o.Exchange = exchange }
}

// WithPartition specifies the target partition (Kafka).
func WithPartition(partition int) PublishOption {
	return func(o *PublishOptions) { o.Partition = partition }
}

// WithKey sets the message key for partitioning (Kafka).
func WithKey(key string) PublishOption {
	return func(o *PublishOptions) { o.Key = key }
}

// WithPersistent makes the message persistent (AMQP delivery mode 2).
func WithPersistent() PublishOption {
	return func(o *PublishOptions) { o.DeliveryMode = 2 }
}

// WithPriority sets the message priority (0-9).
func WithPriority(priority uint8) PublishOption {
	return func(o *PublishOptions) { o.Priority = priority }
}

// WithExpiration sets the message TTL.
func WithExpiration(ttl string) PublishOption {
	return func(o *PublishOptions) { o.Expiration = ttl }
}

// ApplySubscribeOptions applies all functional options and returns the result.
func ApplySubscribeOptions(opts ...SubscribeOption) *SubscribeOptions {
	options := &SubscribeOptions{HandlerErrorPolicy: HandlerErrorNoRetry}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// ApplyPublishOptions applies all functional options and returns the result.
func ApplyPublishOptions(opts ...PublishOption) *PublishOptions {
	options := &PublishOptions{
		Partition:    -1,
		DeliveryMode: 1,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
