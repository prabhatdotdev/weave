# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [0.1.0] - 2026-04-22

Initial public release of Weave.

### Added

- Unified `MessageBroker` abstraction with transport registration and backend selection by configuration.
- High-level `Client` and `Server` runtime APIs for publish, subscribe, and request-reply flows.
- Implemented AMQP and Kafka transports with documented transport-specific semantics.
- Built-in JSON and Protocol Buffers codecs with codec-aware runtime helpers.
- Shared retry helpers, RPC call policy, and circuit breaker support.
- Structured error payload, dead-letter envelope, observability hooks, and health reporting helpers.
- Automated coverage across `core`, `runtime`, `codec`, `testkit`, `transport/amqp`, and `transport/kafka`.
- CI workflow, release gate workflow, and a documented pre-release checklist.

### Changed

- Documentation, examples, and API guidance were aligned with the currently supported feature set.
- Recovery and reconnect behavior is now explicitly documented per transport.
- README stability language now reflects validated support instead of blanket production-readiness claims.

### Removed

- Placeholder backend configuration for unimplemented transports was removed from the public config surface.

### Notes

- AMQP and Kafka are the only implemented transports in `v0.1.0`.
- Recovery support exists for both transports, but reconnect semantics differ by backend.
- In-flight RPC calls are not retried automatically after disconnects; callers should handle `ErrConnectionLost` and `ErrTimeout` explicitly.
