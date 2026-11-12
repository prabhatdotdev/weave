These certificates and the private key are public, test-only fixtures for
localhost. They expire in 2040 and must never be used outside local regression
tests. The CA private key is not retained.

```sh
docker compose -f testdata/tls/compose.yaml up -d --build --wait
WEAVE_TLS_E2E=1 go test -race ./transport/amqp ./transport/kafka -run TLSWithLive
docker compose -f testdata/tls/compose.yaml down --volumes
```

The fixtures use certificate verification and include Kafka SCRAM over TLS.
RabbitMQ listens on 5671, Kafka TLS on 19095, and Kafka SASL/TLS on 19096.
