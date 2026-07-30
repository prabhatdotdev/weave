# API

Canonical package documentation:

- <https://pkg.go.dev/github.com/prabhatdotdev/weave/core>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/codec>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/transport/amqp>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/transport/kafka>
- <https://pkg.go.dev/github.com/prabhatdotdev/weave/testkit>

## Core

`core.MessageBroker` composes connection, publish, call, and subscribe
operations. `core.Message` carries the body plus broker-neutral metadata.

```go
msg := core.NewMessage(body).
	WithCorrelationID("request-1").
	WithHeader("source", "api")
```

Publish options:

- `WithTimeout`
- `WithMandatory`
- `WithExchange`
- `WithPartition`
- `WithKey`
- `WithPersistent`
- `WithPriority`
- `WithExpiration`

Subscribe options:

- `WithAutoAck`
- `WithExclusive`
- `WithConsumerTag`
- `WithPrefetchCount`
- `WithWorkerCount`
- `WithQueueBind`
- `WithConsumerGroup`
- `WithStartFromBeginning`
- `WithHandlerErrorRetry`

`core.RetryHandler` adds bounded, context-aware retries around a handler.

## Codecs

`codec.JSON` and `codec.Protobuf` implement the same `Codec` interface.

```go
msg, err := codec.MarshalMessage(codec.JSON, value)
err = codec.UnmarshalMessage(codec.JSON, msg, &decoded)
```

`codec.UnmarshalAndValidate` performs typed decoding followed by an
application-owned validator.

## Events

Set `Config.EventHook` to receive structured connection, publish, subscribe,
timeout, and recovery events. Metrics and traces belong in the callback or in
the application's existing observability stack.

## Errors

Transport errors use concrete types such as `ErrNotConnected`, `ErrTimeout`,
and `ErrConnectionLost`. The `IsNotConnected`, `IsTimeout`, and
`IsConnectionLost` helpers use `errors.As`.
