# Transport Configuration Guide

This guide covers configuration for all supported message queue transports. The `Backend` field in `Config` selects which transport implementation to use.

## AMQP (RabbitMQ)

### Basic Configuration

```go
config := &mqservice.Config{
    Backend: "amqp",
    AMQP: &mqservice.AMQPConfig{
        Host:     "localhost",
        Port:     5672,
        Username: "guest",
        Password: "guest",
        VHost:    "/",
    },
}

broker, err := mqservice.New("amqp", config)
if err != nil {
    log.Fatal(err)
}
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend:         "amqp",
    ConnectionName:  "my-service",
    ConnectionRetry: 3,
    RetryDelay:      2 * time.Second,
    
    AMQP: &mqservice.AMQPConfig{
        Host:      "rabbitmq.example.com",
        Port:      5672,
        Username:  "myapp",
        Password:  "secret",
        VHost:     "/production",
        Heartbeat: 10 * time.Second,
        
        // Exchange settings
        Exchange:     "my-exchange",
        ExchangeType: "topic", // direct, fanout, topic, headers
        
        // Queue settings
        QueueDurable:    true,  // Survive broker restart
        QueueAutoDelete: false, // Don't delete when last consumer disconnects
        QueueExclusive:  false, // Allow multiple consumers
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:   true,
            CertFile: "/path/to/cert.pem",
            KeyFile:  "/path/to/key.pem",
            CAFile:   "/path/to/ca.pem",
        },
    },
}
```

### Connection URL

Alternatively, use a connection URL:

```go
// Set via environment or config
os.Setenv("AMQP_URL", "amqp://user:pass@rabbitmq.example.com:5672/vhost")
```

### Default Values

```go
config := mqservice.DefaultConfig() // Returns AMQP defaults
// Host: "localhost"
// Port: 5672
// Username: "guest"
// Password: "guest"
// VHost: "/"
// Heartbeat: 10s
```

---

## Apache Kafka

### Basic Configuration

```go
import _ "github.com/prabhatdotdev/weave/transport/kafka"

config := &mqservice.Config{
    Backend: "kafka",
    Kafka: &mqservice.KafkaConfig{
        Brokers:       []string{"localhost:9092"},
        ConsumerGroup: "my-service-group",
    },
}

broker, err := mqservice.New("kafka", config)
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend: "kafka",
    
    Kafka: &mqservice.KafkaConfig{
        Brokers:  []string{
            "kafka1.example.com:9092",
            "kafka2.example.com:9092",
            "kafka3.example.com:9092",
        },
        ClientID:      "my-service-client",
        ConsumerGroup: "my-service-group",
        
        // Producer settings
        RequiredAcks:    -1, // Wait for all replicas (0=none, 1=leader, -1=all)
        MaxRetries:      3,
        RetryBackoff:    100 * time.Millisecond,
        CompressionType: "snappy", // none, gzip, snappy, lz4, zstd
        
        // Consumer settings
        AutoOffsetReset:   "latest", // or "earliest"
        SessionTimeout:    10 * time.Second,
        HeartbeatInterval: 3 * time.Second,
        
        // SASL Authentication
        SASL: &mqservice.SASLConfig{
            Enable:    true,
            Mechanism: "SCRAM-SHA-512", // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
            Username:  "kafka-user",
            Password:  "kafka-pass",
        },
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:   true,
            CAFile:   "/path/to/ca.pem",
            CertFile: "/path/to/cert.pem",
            KeyFile:  "/path/to/key.pem",
        },
    },
}
```

### Kafka-Specific Features

#### Topics vs Partitions

```go
// Subscribe to topic
broker.Subscribe(ctx, "user-events", handler)

// Publish to specific partition
msg := mqservice.NewMessage(data)
msg.Subject = "user-123" // Used as partition key
broker.Publish(ctx, "user-events", msg)
```

#### Consumer Groups

Multiple instances with the same `ConsumerGroup` share message consumption:

```go
config.Kafka.ConsumerGroup = "my-service-group"
```

### Default Values

```go
config := mqservice.DefaultKafkaConfig()
// Brokers: ["localhost:9092"]
// RequiredAcks: 1
// MaxRetries: 3
// AutoOffsetReset: "latest"
```

---

## AWS Kinesis

### Basic Configuration

```go
import _ "github.com/prabhatdotdev/weave/transport/kinesis"

config := &mqservice.Config{
    Backend: "kinesis",
    Kinesis: &mqservice.KinesisConfig{
        Region:     "us-east-1",
        StreamName: "my-stream",
    },
}
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend: "kinesis",
    
    Kinesis: &mqservice.KinesisConfig{
        Region:          "us-west-2",
        AccessKeyID:     "AKIAIOSFODNN7EXAMPLE", // Or use IAM role
        SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        SessionToken:    "", // For temporary credentials
        Endpoint:        "", // For LocalStack: "http://localhost:4566"
        
        // Stream settings
        StreamName:        "production-events",
        ShardIteratorType: "LATEST", // TRIM_HORIZON, LATEST, AT_TIMESTAMP
        
        // Consumer settings
        ConsumerName:       "my-service",
        CheckpointInterval: 60 * time.Second,
    },
}
```

### IAM Role (Recommended)

Instead of access keys, use IAM roles:

```go
config.Kinesis.AccessKeyID = ""     // Empty = use IAM role
config.Kinesis.SecretAccessKey = "" // Empty = use IAM role
```

Required IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:PutRecord",
        "kinesis:PutRecords",
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:DescribeStream"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/my-stream"
    }
  ]
}
```

---

## NATS

### Basic Configuration

```go
import _ "github.com/prabhatdotdev/weave/transport/nats"

config := &mqservice.Config{
    Backend: "nats",
    NATS: &mqservice.NATSConfig{
        Servers: []string{"nats://localhost:4222"},
    },
}
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend: "nats",
    
    NATS: &mqservice.NATSConfig{
        Servers: []string{
            "nats://nats1.example.com:4222",
            "nats://nats2.example.com:4222",
        },
        Username:      "nats-user",
        Password:      "nats-pass",
        Token:         "", // Alternative to username/password
        MaxReconnects: -1, // -1 = infinite
        ReconnectWait: 2 * time.Second,
        
        // JetStream (for persistence)
        JetStream:       true,
        StreamName:      "my-stream",
        ConsumerDurable: "my-service",
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:   true,
            CertFile: "/path/to/cert.pem",
            KeyFile:  "/path/to/key.pem",
            CAFile:   "/path/to/ca.pem",
        },
    },
}
```

---

## ActiveMQ

### Basic Configuration

```go
import _ "github.com/prabhatdotdev/weave/transport/activemq"

config := &mqservice.Config{
    Backend: "activemq",
    ActiveMQ: &mqservice.ActiveMQConfig{
        BrokerURL: "tcp://localhost:61616",
        Username:  "admin",
        Password:  "admin",
    },
}
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend: "activemq",
    
    ActiveMQ: &mqservice.ActiveMQConfig{
        BrokerURL: "ssl://activemq.example.com:61617",
        Username:  "app-user",
        Password:  "app-pass",
        UseTopics: false, // false = queues, true = topics
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:   true,
            CertFile: "/path/to/cert.pem",
            KeyFile:  "/path/to/key.pem",
            CAFile:   "/path/to/ca.pem",
        },
    },
}
```

---

## Redis Streams

### Basic Configuration

```go
import _ "github.com/prabhatdotdev/weave/transport/redis"

config := &mqservice.Config{
    Backend: "redis",
    Redis: &mqservice.RedisConfig{
        Addr:          "localhost:6379",
        ConsumerGroup: "my-service-group",
    },
}
```

### Full Configuration

```go
config := &mqservice.Config{
    Backend: "redis",
    
    Redis: &mqservice.RedisConfig{
        Addr:     "redis.example.com:6379",
        Password: "redis-pass",
        DB:       0, // Database number
        
        // Stream settings
        ConsumerGroup: "my-service-group",
        ConsumerName:  "instance-1",
        MaxLen:        10000, // Trim stream to max length
        
        // TLS
        TLS: &mqservice.TLSConfig{
            Enable:             true,
            InsecureSkipVerify: false,
        },
    },
}
```

---

## Environment Variables

All transports support environment variable overrides:

```bash
# AMQP
export MQ_BACKEND=amqp
export AMQP_HOST=rabbitmq.example.com
export AMQP_PORT=5672
export AMQP_USERNAME=myapp
export AMQP_PASSWORD=secret

# Kafka
export MQ_BACKEND=kafka
export KAFKA_BROKERS=kafka1:9092,kafka2:9092
export KAFKA_CONSUMER_GROUP=my-service

# Kinesis
export MQ_BACKEND=kinesis
export AWS_REGION=us-east-1
export KINESIS_STREAM_NAME=my-stream
```

Load from environment:

```go
config := mqservice.LoadConfigFromEnv()
broker, err := mqservice.New(config.Backend, config)
```

---

## Connection Retry

Configure retry behavior:

```go
config := &mqservice.Config{
    Backend:         "amqp",
    ConnectionRetry: 5,                // Retry 5 times
    RetryDelay:      3 * time.Second, // Wait 3s between attempts
    AMQP:            mqservice.DefaultAMQPConfig(),
}
```

---

## Best Practices

### 1. Use Default Configs

Start with defaults and override only what's needed:

```go
config := mqservice.DefaultConfig()
config.AMQP.Host = "production-rabbitmq"
```

### 2. Externalize Credentials

Never hardcode credentials:

```go
config.AMQP.Username = os.Getenv("RABBITMQ_USERNAME")
config.AMQP.Password = os.Getenv("RABBITMQ_PASSWORD")
```

### 3. Enable TLS in Production

Always use TLS for production:

```go
config.AMQP.TLS = &mqservice.TLSConfig{
    Enable: true,
    CAFile: "/etc/ssl/ca.pem",
}
```

### 4. Set Connection Names

For debugging and monitoring:

```go
config.ConnectionName = "user-service-prod-1"
```

### 5. Test with LocalStack

Use LocalStack for local AWS testing:

```go
if os.Getenv("ENV") == "local" {
    config.Kinesis.Endpoint = "http://localhost:4566"
}
```

---

## Switching Transports

To switch transports, only change the config:

```go
// Was using AMQP
config := &mqservice.Config{
    Backend: "amqp",
    AMQP: &mqservice.AMQPConfig{...},
}

// Now using Kafka - no code changes!
config := &mqservice.Config{
    Backend: "kafka",
    Kafka: &mqservice.KafkaConfig{...},
}
```

Your application code remains the same!
