# Contributing

## Setup

```sh
git clone https://github.com/prabhatdotdev/weave.git
cd weave
go mod download
```

The library and example module require Go 1.25 or newer.

## Validate

```sh
go fmt ./...
go vet ./...
go test -race ./...
make test-coverage-check
```

Transport changes should include the smallest test that proves the behavior.
Recovery changes belong in the existing recovery test files.

The shared contract suite runs the same ownership, metadata, cancellation, and
closed-state assertions against AMQP, Kafka, and `MockBroker`. Transport test
fixtures supply only wire I/O; add common expectations in
[`internal/brokercontract`](internal/brokercontract/contract.go):

```sh
go test -race ./transport/amqp ./transport/kafka ./testkit -run TestBrokerContract
```

CI also checks both module roots with the pinned `govulncheck` release on the
current stable Go toolchain. The normal validation job continues testing the
minimum Go version in `go.mod`. To reproduce the vulnerability check, use
current stable Go (the scanner requires Go 1.26 or newer):

```sh
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
govulncheck ./...
```

Run the same `govulncheck ./...` command from `examples/amqp-kafka` as well.
Reachable findings fail CI; they are not suppressed or converted to warnings.
See the
[Go vulnerability-management documentation](https://go.dev/doc/security/vuln/).

## Design

- Keep `core.MessageBroker` transport-neutral.
- Put backend behavior in its transport package.
- Prefer direct constructors over registries or factories.
- Use standard-library packages before adding dependencies.
- Add configuration only when a transport reads it.
- Keep examples short and runnable.

## Pull requests

Describe the problem, the chosen behavior, and how it was verified. Avoid
unrelated formatting or refactors in the same change.
