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

Each run also completes the same request/reply round trip through `Call`:

```text
weave.example.rpc: pong: request-1
weave.example.rpc: pong: request-2
```

Kafka initializes its reply consumer on the first call. If that initialization
fails, no reply topic is retained and the next call retries it. Later calls
reuse the same reply consumer; after a reconnect, Kafka restores that one
registration instead of accumulating obsolete reply-topic loops.

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
