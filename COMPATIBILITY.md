# Compatibility And Support

This document defines the compatibility expectations for the `v0.1.x` line of Weave.

## Versioning Policy

Weave follows Semantic Versioning:

- `MAJOR`: incompatible public API changes
- `MINOR`: backward-compatible features and new capabilities
- `PATCH`: backward-compatible fixes and documentation corrections

Because Weave is entering the `0.x` series, APIs may still evolve more quickly than a mature `1.x` library. Within `v0.1.x`, changes should remain backward compatible unless a documented breaking change is called out in release notes.

## Supported Weave Line

- Current release line: `v0.1.x`
- Current release target: `v0.1.0`

## Toolchain Compatibility

- Go: `1.24+`

The minimum supported Go version is derived from `go.mod`. Future releases may raise the minimum Go version when required by dependencies or language features; such changes should be documented in release notes.

## Transport Compatibility

The following broker versions are the validated baseline for the `v0.1.x` line:

| Transport | Baseline Version | Notes |
|-----------|------------------|-------|
| RabbitMQ / AMQP | RabbitMQ `3.8+` | See `docs/TRANSPORT_MATRIX.md` for feature limitations and reconnect semantics |
| Apache Kafka | Kafka `2.8+` | See `docs/TRANSPORT_MATRIX.md` for partitioning, retry, and reconnect behavior |

## Support Expectations

For `v0.1.x`, the repository supports:

- The public API exported from the root package plus documented runtime helpers.
- AMQP and Kafka as the only implemented transport backends.
- JSON and Protocol Buffers as built-in codec integrations.
- Documented reconnect, retry, observability, and health-reporting behaviors.

The repository does not currently promise:

- Support for placeholder or planned transports such as NATS, Redis, Kinesis, or ActiveMQ.
- Exact behavioral equivalence between AMQP and Kafka where broker models differ.
- Automatic replay of in-flight RPC calls after disconnects.

## Upgrade Guidance

When upgrading within `v0.1.x`:

- Read the changelog before updating.
- Review release notes for behavior changes in reconnect, retry, and observability paths.
- Treat transport-specific semantics in `docs/TRANSPORT_MATRIX.md` and `docs/TRANSPORTS.md` as the source of truth for operational behavior.

## Release Discipline

Every release in the `v0.1.x` line should:

- Pass CI, race-enabled tests, coverage gate, `go vet`, and `go build`
- Keep `README.md`, examples, and transport docs aligned with implementation
- Update `CHANGELOG.md` when user-visible behavior changes
