# JSON

Start RabbitMQ, then run:

```sh
go run ./examples/json
```

The example connects directly through the AMQP transport, subscribes, encodes
an order with `codec.JSON`, and publishes it.
