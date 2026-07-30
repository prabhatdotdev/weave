# Release checklist

- [ ] `go mod tidy` leaves no diff.
- [ ] `go vet ./...` passes.
- [ ] `go test -race ./...` passes.
- [ ] `make test-coverage-check` passes.
- [ ] The nested AMQP/Kafka example builds.
- [ ] Public API changes are reflected in README and docs.
- [ ] The version tag and release notes are ready.
- [ ] The tag is pushed only after CI passes.
