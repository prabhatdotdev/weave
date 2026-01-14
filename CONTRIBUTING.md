# Contributing to AMQP Service Starter

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

- Update README.md if adding new features
- Add inline comments for complex logic
- Include examples for new functionality
- Update CHANGELOG.md (if it exists)

## Pull Request Process

1. **Keep PRs focused**: One feature or fix per PR
2. **Write clear commit messages**: Use present tense ("Add feature" not "Added feature")
3. **Update documentation**: Include relevant documentation updates
4. **Add tests**: Ensure new code is covered by tests
5. **Check CI**: Ensure all CI checks pass
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
