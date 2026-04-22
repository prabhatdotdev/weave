# Contributing to Weave

Thank you for considering contributing to this project! This document outlines the process for contributing and some guidelines to follow.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/weave.git`
3. Create a new branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Run tests: `make test`
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin feature/your-feature-name`
8. Create a Pull Request

## Development Setup

### Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose (for running RabbitMQ)
- Make (optional, but recommended)

### Setup

```bash
# Clone the repository
git clone https://github.com/prabhatdotdev/weave.git
cd weave

# Install dependencies
make install-deps

# Start RabbitMQ
make rabbitmq-start

# Run tests
make test
```

## Code Style

### Go Code

- Follow standard Go conventions and style guidelines
- Run `go fmt` before committing: `make fmt`
- Run `go vet` to catch common mistakes: `make vet`
- Use meaningful variable and function names
- Add comments for exported types and functions

### Example

```go
// Good
func (s *Service) Call(ctx context.Context, queueName string, body []byte) ([]byte, error) {
    // Implementation
}

// Not so good
func (s *Service) c(x context.Context, q string, b []byte) ([]byte, error) {
    // Implementation
}
```

## Testing

### Writing Tests

- Write tests for all new functionality
- Ensure tests are deterministic and don't depend on external state
- Use table-driven tests when appropriate
- Mock external dependencies

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Run benchmarks
make bench
```

### Test Structure

```go
func TestFeatureName(t *testing.T) {
    // Arrange
    service := setupTestService(t)
    defer service.Close()
    
    // Act
    result, err := service.SomeMethod()
    
    // Assert
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if result != expected {
        t.Errorf("expected %v, got %v", expected, result)
    }
}
```

## Documentation

- Update README.md and docs/ when adding or changing public behavior
- Keep examples aligned with the current exported API
- Prefer pkg.go.dev comments as the canonical API reference for signatures
- Add inline comments for complex logic when needed

## Pull Request Process

1. **Keep PRs focused**: One feature or fix per PR
2. **Write clear commit messages**: Use present tense ("Add feature" not "Added feature")
3. **Update documentation**: Include relevant documentation updates
4. **Add tests**: Ensure new code is covered by tests
5. **Check CI**: Ensure all CI checks pass (`go test ./...`, `go vet ./...`, `go build ./...`)
6. **Link issues**: Reference any related issues in the PR description

### PR Template

```markdown
## Description
Brief description of the changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
Describe the tests you ran and their results

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex logic
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] All tests pass
```

## Commit Message Guidelines

Follow the conventional commits specification:

```
type(scope): subject

body

footer
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Examples

```
feat(client): add retry logic for failed requests

Add exponential backoff retry mechanism for handling
transient failures in client calls.

Closes #123
```

```
fix(service): handle connection loss gracefully

Properly cleanup resources and notify pending requests
when connection is lost unexpectedly.
```

## Code Review Process

1. Maintainers will review your PR within a few days
2. Address any feedback or requested changes
3. Once approved, a maintainer will merge your PR
4. Your contribution will be credited in the release notes

## CI And Release Gate

The repository uses GitHub Actions for required validation:

- `CI` workflow runs on pushes to `master`.
- Required checks in CI:
    - `go test ./...`
    - `go vet ./...`
    - `go build ./...`

Release tags (`v*`) are guarded by a `Release Gate` workflow that verifies CI has already passed on the tagged commit.

Recommended repository settings:

1. Protect `master` with required status check `CI / Test Vet Build`.
2. Disable direct pushes to protected branches.
3. Restrict who can create release tags if your process requires it.

Optional next step:

- Add `golangci-lint` as a separate required workflow once the codebase is stable enough for strict linting.

## Test-First Policy

To maintain code reliability and prevent regressions, new contributions must include appropriate test coverage:

### For Bug Fixes

1. **Write a test that reproduces the bug** before fixing it
2. Verify the test fails with the current code
3. Implement the fix
4. Verify the test now passes

**Example:**

```go
// Test that reproduces the bug
func TestConnectionRecoveryAfterTimeout(t *testing.T) {
    broker := testkit.NewMockBroker()
    broker.Connect(context.Background())
    
    client := runtime.NewClientWithBroker(broker, nil)
    
    // Simulate timeout
    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
    _, err := client.Call(ctx, "test", weave.NewMessage([]byte("test")))
    cancel()
    
    if err == nil || !weave.IsTimeout(err) {
        t.Fatal("expected timeout error")
    }
    
    // Should recover automatically or with explicit reconnect
    ctx = context.Background()
    _, err = client.Call(ctx, "test", weave.NewMessage([]byte("test")))
    if err != nil {
        t.Fatalf("recovery failed: %v", err)
    }
}
```

### For New Features

1. **Write tests first** - define the expected behavior through tests
2. Implement the feature to make tests pass
3. Add documentation alongside the code

**Test coverage requirements:**

- Unit tests for all public APIs
- Integration tests for transport-specific behavior
- Example code or tests demonstrating typical usage
- Error cases and edge cases covered

### Running Tests Locally

```bash
# Run all tests
make test

# Run with race detector to catch concurrency issues
make test-verbose

# Run a specific test
go test -run TestConnectionRecoveryAfterTimeout ./runtime

# Generate the critical-package coverage report
make test-coverage

# Enforce the critical-package coverage threshold
make test-coverage-check
```

### Test Guidelines

- **Deterministic:** Tests must not depend on timing, network order, or random state
- **Isolated:** Each test should be independent and not affect others
- **Clear:** Use table-driven tests for multiple scenarios
- **Mock external dependencies:** Use `testkit.MockBroker` for unit tests
- **Document:** Add comments explaining non-obvious test logic

### What Gets Tested in CI

CI runs on every push to `master` and every pull request with:

```bash
go test ./...
make test-coverage-check
```

The coverage gate currently enforces a minimum of 75% total coverage across the critical packages:

- `./core`
- `./runtime`
- `./testkit`
- `./transport/amqp`
- `./transport/kafka`

If either the test suite or the coverage gate fails in CI, the PR cannot be merged. Tests and the coverage check must also pass locally before pushing.

## Doc-to-Code Ownership Rule

Documentation must be kept in sync with implementation. This is a joint responsibility:

### Rule: Any public API claim in documentation must exist in code

**What this means:**

- If you document a function, the function must be exported and callable
- If you document a feature, it must be fully implemented
- If you document an option, it must work as documented
- If you document a transport capability, it must be tested

**Examples:**

❌ **Not allowed:**
```go
// Document in README: "Automatic dead-letter handling for all transports"
// But implement: Only in AMQP, not in Kafka
```

✅ **Correct:**
```go
// Document in README: "Dead-letter handling (AMQP only)"
// And implement: Feature in AMQP with docs/examples
```

### Enforcing Alignment

1. **Code Review:** PRs will be rejected if:
   - Docs promise features not yet implemented
   - Code adds features not documented
   - Documented behavior doesn't match actual behavior

2. **In the release notes:** Breaking changes in documentation require an explicit migration note

3. **In the PR Checklist:** "Documentation matches implementation" is required

### Documentation Sources (in priority order)

1. **Code comments (godoc)** - The canonical source for APIs
2. **README.md** - Overview and quick-start
3. **docs/** - Detailed guides and specifications
4. **Examples** - Executable code showing usage
5. **Issue discussions** - Context and rationale

When these conflict, resolve as follows:

- If godoc is wrong, fix the code or the comment
- If README contradicts godoc, update both to match reality, not the other way around
- If docs/ contradicts a working example, update docs/ to match the example

### When Adding New Public APIs

1. **Write the godoc comment first** - this is the contract
2. **Implement the feature** - make the contract real
3. **Add an example** - show how to use it
4. **Update README or docs/** - explain when/why to use it
5. **Write tests** - verify it works as documented

### Common Issues and Fixes

| Issue | Fix |
|-------|-----|
| Documented feature doesn't work | Implement it or remove documentation |
| Code has a feature, docs are missing | Add documentation |
| Example code fails to compile | Update example to match current API |
| README lists unsupported backends | Remove or mark as "planned only" |
| Godoc comment is vague | Make it specific and include an example |

## Feature Requests and Bug Reports

### Bug Reports

Include:
- Clear description of the issue
- Steps to reproduce
- Expected behavior
- Actual behavior
- Go version and OS
- Code samples or error messages

### Feature Requests

Include:
- Clear description of the feature
- Use cases and benefits
- Proposed API or interface (if applicable)
- Alternatives considered

## Questions?

Feel free to open an issue for questions or join discussions.

## Code of Conduct

- Be respectful and considerate
- Welcome newcomers and help them get started
- Focus on what is best for the community
- Show empathy towards other community members

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
