# Examples

- [`json`](json): direct AMQP publish/subscribe with `codec.JSON`
- [`protobuf`](protobuf): protobuf codec round trip without generated files
- [`amqp-kafka`](amqp-kafka): the same broker interface with either transport
- [`deduplication`](deduplication): skip completed message IDs and retry failures
- [`observability`](observability): operation durations and work counts through `EventHook`
- [`load-test`](load-test): AMQP request/reply load measurement

Each example is independent and intentionally small.
