# Release Checklist

This document defines the release gate criteria and provides a checklist for cutting a new release.

## Pre-Release Requirements

All items below must be verified before creating a release tag.

### 1. Documentation Alignment ✓

**Why:** False expectations are the largest current risk to users.

- [ ] All public APIs documented in godoc comments
- [ ] README features match actual implementation
- [ ] No undocumented features in code
- [ ] No promised features lacking implementation
- [ ] Transport limitations clearly stated (e.g., recovery contract differences between AMQP and Kafka)
- [ ] Example code is current and runnable

**Verification:**
```bash
# Inspect exported package docs that changed
go doc github.com/prabhatdotdev/weave
go doc github.com/prabhatdotdev/weave/runtime

# Verify examples compile
cd examples/json && go build ./...
cd examples/protobuf && go build ./...
```

### 2. Automated Tests Pass ✓

**Why:** Tests are the base requirement for trusting runtime behavior.

- [ ] `go test ./...` passes with meaningful coverage
- [ ] `make test-coverage-check` passes (minimum 75% across critical packages)
- [ ] All core RPC and subscription behavior covered
- [ ] Transport connection lifecycle tested
- [ ] Publish/subscribe/call timeout behavior tested
- [ ] Error cases tested (disconnect, timeout, handler errors)
- [ ] No race conditions detected

**Verification:**
```bash
# Run tests with race detector
make test-verbose

# Generate the critical-package coverage report
make test-coverage

# Enforce the release coverage gate
make test-coverage-check
```

### 3. CI Validation Passes ✓

**Why:** Automated validation is mandatory for release discipline.

- [ ] All GitHub Actions workflows pass on the release commit
- [ ] Tests pass
- [ ] Critical-package coverage is at or above the enforced 75% threshold
- [ ] `go vet` passes
- [ ] `go build` succeeds for all packages
- [ ] No lint errors (if linting is enabled)

**Verification:**
- Navigate to the commit in GitHub and verify all checks pass
- Or run locally: `make test && make vet && make build`

### 4. Code Quality Standards Met ✓

**Why:** Code quality directly affects maintainability.

- [ ] Code formatted with `go fmt`
- [ ] No unused imports
- [ ] Consistent error handling patterns
- [ ] Function signatures well-documented
- [ ] No TODOs or FIXMEs without corresponding issues

**Verification:**
```bash
make fmt
make vet
```

### 5. Runtime Behavior Hardened ✓

**Why:** Production expectations must be clear and consistent.

- [ ] Connection recovery behavior documented
- [ ] Handler failure semantics defined
- [ ] Timeout behavior tested and documented
- [ ] Retry semantics clear and consistent
- [ ] Dead-letter/poison-message strategy explained in docs

**Verification:**
- Check docs/ERROR_POLICY.md for complete error handling spec
- Verify transport-specific docs in docs/TRANSPORTS.md

### 6. Observability Complete ✓

**Why:** Users need visibility into production behavior.

- [ ] All critical events can be logged/monitored
- [ ] Logger interface or hook fully implemented
- [ ] Tracing hook is implemented for high-level runtime operations
- [ ] Transport errors generate actionable signals
- [ ] Documentation shows how to enable logging, metrics, and tracing hooks

**Verification:**
- Check core/observability.go for event types
- Review documentation in docs/OBSERVABILITY.md

### 7. Transport Backend Consistency ✓

**Why:** Users need clear transport guarantees.

- [ ] Transport capability matrix is current
- [ ] Supported backends clearly listed
- [ ] Placeholder backends removed or explicitly marked "reserved"
- [ ] Each transport has documented limitations
- [ ] Config does not promise unsupported backends

**Verification:**
- Check docs/TRANSPORTS.md or README for capability matrix
- Review core/config.go for available backend options

### 8. Breaking Changes Documented ✓

**Why:** Users need clear migration paths.

- [ ] Migration examples provided if applicable
- [ ] Deprecation warnings added for replaced APIs
- [ ] Version compatibility expectations stated

**Verification:**
- Search codebase for deprecated APIs and ensure docs exist
- Review `CHANGELOG.md`, `COMPATIBILITY.md`, and release notes draft before tagging

## Release Process

### Step 1: Pre-Release Validation

1. Create a release branch from master: `git checkout -b release/v1.x.y`
2. Review all checklist items above
3. Draft release notes for the GitHub release
4. Update version in documentation if applicable

### Step 2: Tag Commit

Once all checklist items are verified:

```bash
# Create annotated tag with release notes
git tag -a vX.Y.Z -m "Release X.Y.Z: [brief description]"

# Push tag to trigger release gate
git push origin vX.Y.Z
```

### Step 3: Verify Release Gate

1. GitHub will automatically run the `Release Gate` workflow on the tagged commit
2. The workflow verifies that CI passed on that commit
3. If CI did not pass, the release is blocked
4. If release gate passes, proceed to Step 4

**Release blocked?** See [Release Gate Failures](#release-gate-failures) below.

### Step 4: Create Release Notes

On GitHub, create a release from the tag with:

- Summary of changes
- New features (with usage examples if major)
- Bug fixes
- Known limitations or breaking changes
- Upgrade instructions

## Release Gate Failures

**Problem:** Release gate workflow failed

**Cause:** CI did not pass on the commit you tagged

**Fix:**
1. Delete the tag: `git tag -d vX.Y.Z && git push origin :refs/tags/vX.Y.Z`
2. Run tests locally: `make test-verbose`
3. Fix any failures
4. Commit fixes to master/release branch
5. Create a fresh commit (do not re-use the old commit)
6. Tag the new commit and push

## Versioning

This project uses [Semantic Versioning](https://semver.org/):

- **MAJOR.MINOR.PATCH**
  - MAJOR: Incompatible API changes
  - MINOR: New functionality (backward compatible)
  - PATCH: Bug fixes only

### Examples

- API removal or signature change: MAJOR
- New transport backend: MINOR
- Bug fix in existing behavior: PATCH
- New optional config field: MINOR

## What Blocks a Release

- Any failing automated test
- Any CI validation failure (`go vet`, `go build`)
- Documentation describing APIs that don't exist
- README claiming features not yet implemented
- No test coverage for critical runtime paths

## What Does Not Block a Release

- Code style warnings (if linting is optional)
- Minor documentation improvements already in progress
- Performance optimization work not affecting public API

## Questions?

See the [Next Release Plan](Next_release.md) for context on why these requirements exist.
