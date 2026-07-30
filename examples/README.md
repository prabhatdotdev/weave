# Weave Examples

Runnable examples for RabbitMQ/AMQP and Apache Kafka.

| Example | Broker | Payload | Processes | Guide |
|---|---|---|---:|---|
| AMQP + Kafka | RabbitMQ or Kafka | JSON | 2 | [amqp-kafka](amqp-kafka/README.md) |
| JSON services | RabbitMQ | JSON | 3 | [json](json/README.md) |
| Protobuf services | RabbitMQ | Protocol Buffers | 3 | [protobuf](protobuf/README.md) |

Start with `amqp-kafka`. Its server and client are separate processes and the
same workflow runs against either backend.

## Requirements

- Go 1.24+
- Docker with Docker Compose

Run commands from the repository root unless a section says otherwise.

## AMQP + Kafka

This example is an independent Go module. It covers connection health,
publish/subscribe, JSON, headers, content type, request-reply, correlation IDs,
bounded retry, timeout detection, and backend-specific metadata.

Start both brokers:

```bash
cd examples/amqp-kafka
docker compose up -d --wait
```

Run AMQP in two terminals:

```bash
# Terminal 1
go run . -role server -backend amqp
```

```bash
# Terminal 2
go run . -role client -backend amqp
```

Stop the server with `Ctrl+C`, then run Kafka:

```bash
# Terminal 1
go run . -role server -backend kafka
```

```bash
# Terminal 2
go run . -role client -backend kafka
```

Application output is prefixed with `[SERVER]` or `[CLIENT]`. The server logs
each request; the client prints the JSON result fetched from the server.

Clean up:

```bash
docker compose down -v
cd ../..
```

CLI flags, environment variables, expected output, architecture, and
troubleshooting are documented in the
[AMQP + Kafka guide](amqp-kafka/README.md).

## JSON Services

Start RabbitMQ:

```bash
docker compose up -d rabbitmq
```

Run each process in a separate terminal:

```bash
go run ./examples/json/profile-service
go run ./examples/json/user-service
go run ./examples/json/client
```

See the [JSON guide](json/README.md) for the request flow and expected output.

## Protobuf Services

Use the same RabbitMQ instance and run each process in a separate terminal:

```bash
go run ./examples/protobuf/profile-service
go run ./examples/protobuf/user-service
go run ./examples/protobuf/client
```

Generated Go types are committed. `protoc` is needed only when changing
[`services.proto`](protobuf/proto/services.proto). See the
[Protobuf guide](protobuf/README.md) for regeneration instructions.

## Validation

The root module and independent example module must be checked separately:

```bash
go test ./...
go vet ./...

cd examples/amqp-kafka
go test ./...
go vet ./...
go build ./...
```

Stop the root RabbitMQ environment when finished:

```bash
cd ../..
docker compose down -v
```
