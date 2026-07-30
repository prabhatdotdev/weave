# AMQP and Kafka

Start the brokers:

```sh
docker compose up -d
```

Run the same publish/subscribe example against either transport:

```sh
go run . -backend amqp
go run . -backend kafka
```
