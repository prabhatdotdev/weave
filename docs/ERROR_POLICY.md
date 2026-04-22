# Error Handling And Retry Policy

This document defines the runtime contract for handler failures, retries, and timeout behavior.

## Design Goals

- predictable behavior across transports
- explicit retry semantics
- clear guidance for dead-letter and poison-message handling

## Standard Handler Failure Semantics

Weave uses `SubscribeOption` to control handler-error behavior:

- default: `WithHandlerErrorNoRetry()` (implicit)
- opt-in retry: `WithHandlerErrorRetry()`
- bounded in-process retry wrapper: `RetryHandler(handler, RetryPolicy{...})`

Default behavior is intentionally conservative to reduce poison-message retry loops.

### AMQP

- Success: `Ack`
- Handler error with no-retry policy: `Nack(requeue=false)`
- Handler error with retry policy: `Nack(requeue=true)`
- Handler panic: `Nack(requeue=false)`

### Kafka

- Success: message is marked (committed by the group session)
- Handler error with no-retry policy: message is marked
- Handler error with retry policy: message is not marked (offset remains uncommitted)

Note: with Kafka retry policy enabled, retry timing depends on consumer-group commit/rebalance behavior, not immediate in-process redelivery.

## Publish Retry Behavior

### AMQP

- `Publish` and `Call` publish operations do not perform automatic message-level retries.
- If publish fails, the call returns `ErrPublishFailed`.

### Kafka

- Producer-level retries can occur inside Sarama according to Kafka config (`MaxRetries`, `RetryBackoff`).
- Weave itself does not add a second message-level retry loop on top of producer behavior.
- On publish failure, Weave returns `ErrPublishFailed`.

## Subscribe Retry Behavior

### Connection and consumer recovery

- AMQP: background reconnect loop restores subscriptions after connection loss.
- Kafka: operations and consume loops trigger reconnect on demand.
- Pending `Call()` waits are cancelled on detected connection loss and surface as `ErrConnectionLost`.
- Weave does not automatically retry `ErrConnectionLost`; use `CallWithPolicy(...)` only when repeating the RPC is safe.
- Handler-local state is not rolled back. If a disconnect happens around the ack/commit boundary, message redelivery is possible after recovery, so side effects should be idempotent.

### Handler-level retries

- Controlled only by `WithHandlerErrorRetry` / `WithHandlerErrorNoRetry`.
- `RetryHandler(...)` adds bounded in-process retries before the transport sees the final handler outcome.
- Recommended pattern for portable behavior: `RetryHandler(...)` plus `WithHandlerErrorNoRetry()`.
- Recommended pattern for transport-assisted redelivery after bounded local retries: `RetryHandler(...)` plus `WithHandlerErrorRetry()`.

## RPC Timeout Behavior

- `Call` timeout is controlled by `WithTimeout`.
- On timeout, Weave returns `ErrTimeout`.
- Weave does not automatically retry timed-out RPC calls.
- Retry after timeout should be an application decision because call side effects may be non-idempotent.

## Dead-Letter And Poison-Message Guidance

1. Prefer idempotent handlers so retries are safe.
2. Use the default no-retry policy unless you explicitly need transport-level retry.
3. For AMQP, configure DLX/DLQ on queues and use no-retry policy for terminal failures.
4. For Kafka, send terminal failures to a dedicated dead-letter topic from application code.
5. Use `NewDeadLetterEnvelope(...)` to standardize the payload sent to DLQ/DLT destinations.
6. Include structured failure metadata (`error`, `attempt`, `correlation-id`, handler name, timestamp).
6. Track repeated failures and alert on poison-message patterns.

## Recommended Strategy

- Validation and schema errors: no-retry + dead-letter
- External dependency/transient errors: retry policy + bounded app-level attempts
- Unknown errors: no-retry + dead-letter + alert
- RPC handler failures: return `NewErrorMessage(...)` payloads for machine-readable responses

## API Reference

- `WithHandlerErrorNoRetry()`
- `WithHandlerErrorRetry()`
- `RetryHandler(...)`
- `DefaultRetryPolicy()`
- `WithTimeout(...)`
- `DefaultCallPolicy()`
- `NewCircuitBreaker(...)`
- `NewErrorMessage(...)`
- `NewDeadLetterEnvelope(...)`
- `IsTimeout(err)`
- `IsCircuitOpen(err)`
- `ErrPublishFailed` and `ErrSubscribeFailed` error types
