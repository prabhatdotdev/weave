# Contributing

## Setup

```sh
git clone https://github.com/prabhatdotdev/weave.git
cd weave
go mod download
```

## Validate

```sh
go fmt ./...
go vet ./...
go test -race ./...
make test-coverage-check
```

Transport changes should include the smallest test that proves the behavior.
Recovery changes belong in the existing recovery test files.

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
