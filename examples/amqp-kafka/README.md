# AMQP and Kafka

Start the brokers:

```sh
docker compose up -d
```

Run the same two-destination publish/subscribe example against either transport:

```sh
go run . -backend amqp
go run . -backend kafka
```

Each command prints one independently consumed message for
`weave.example.users` and `weave.example.orders` (the order may vary). The
Kafka's users handler intentionally panics once, while the orders handler
returns an error once for both transports. Both use `WithHandlerErrorRetry` to
demonstrate contained failures and safe redelivery before later messages are
processed. Handlers also select on their context so subscription cancellation,
deadlines, and tracing values propagate into active work.

AMQP and Kafka calls that are in flight while the broker disconnects or closes
complete once: either with the response that won the race or with
`ErrConnectionLost`. Callers can handle shutdown without risking a stranded
call. Caller-supplied correlation IDs must also be unique among in-flight
calls; duplicates are rejected before publishing:

```go
response, err := broker.Call(ctx, "weave.example.rpc", request)
switch {
case errors.Is(err, core.ErrDuplicateCorrelationID):
	// Wait for the existing call or choose a different correlation ID.
case core.IsConnectionLost(err):
	// Retry after reconnecting, if the operation is safe to repeat.
}
```
