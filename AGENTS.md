# AGENTS.md

These instructions apply to the entire repository. Use
[CONTRIBUTING.md](CONTRIBUTING.md) for contributor guidance and
[.github/workflows/ci.yml](.github/workflows/ci.yml) for the complete CI workflow.

## Architecture

Weave is a Go messaging library with module path `github.com/prabhatdotdev/weave`.
The library and its separate example module require Go 1.25 or newer.

| Path | Responsibility |
| --- | --- |
| `core/` | Transport-neutral interfaces, messages, configuration, options, errors, retries, dead-letter helpers, and observability contracts |
| `transport/amqp/` | RabbitMQ implementation using `github.com/rabbitmq/amqp091-go` |
| `transport/kafka/` | Kafka implementation using `github.com/IBM/sarama` |
| `codec/` | JSON and Protocol Buffers encoding, decoding, and payload validation |
| `testkit/` | In-memory `MockBroker` for application tests |
| `internal/` | Shared validation, connection/backoff, lifecycle, readiness, RPC limits, metrics, and broker test helpers |
| `examples/` | Small runnable examples; `examples/amqp-kafka/` has its own `go.mod` and `go.sum` |
| `scripts/`, `testdata/` | Coverage and live-test runners, Docker Compose fixtures, and test TLS material |
| `docs/` | Quick start, API and transport guides, performance documentation, benchmark records, and assets |

- Keep `core.MessageBroker` transport-neutral. Backend-specific behavior belongs
  in its transport package; shared helpers must not depend on a concrete transport.
- Use `amqp.NewBroker` and `kafka.NewBroker` directly. Both return
  `(core.MessageBroker, error)` and accept nil configuration for defaults.
- Keep optional capabilities such as `core.Shutdowner` and
  `core.ConsumerReadiness` separate from the main broker interface.
- The nested example module uses
  `replace github.com/prabhatdotdev/weave => ../..` to exercise this checkout.
  Root-level Go commands do not traverse this module boundary.

## Planning and Approval

- Require a formal implementation plan for new features; public API or
  compatibility changes; shared AMQP/Kafka behavior; concurrency, recovery, or
  delivery semantics; multi-package refactoring; and dependency changes with
  meaningful compatibility, security, or operational risk.
- Each plan must identify the goal and scope, affected components, architecture
  and compatibility considerations, risks and trade-offs, testing strategy, and
  acceptance criteria. Present the plan for review before implementation.
- Use [implementation-plan template](docs/IMPLEMENTATION_PLAN_TEMPLATE.md)
  for formal plans, including plans kept in Codex Plan mode. Persist a plan as
  `docs/PLAN-{topic}.md` when it needs cross-chat continuation, team review or
  handoff, or a durable design or approval record; otherwise keep it in the chat.
- Documentation-only changes and simple isolated fixes that meet none of these
  triggers need only a brief scope and validation summary. Existing engineering
  and testing requirements still apply.
- Read-only investigation may proceed before approval. Obtain explicit user
  approval of the plan before significant implementation and explicit approval
  before any architecture or public API change; the plan approval may cover
  these changes when clearly identified.
- Within the approved scope, proceed with relevant local tests and fixes
  without repeated approval. If scope or design materially changes, revise the
  plan and obtain approval before implementing the change.
- Obtain explicit user approval before commits and PR creation; implementation
  approval does not authorize either.
- Obtain separate explicit approval for each push, merge, deployment,
  destructive operation, external write, or other consequential external side
  effect. Approval of a plan, commit, or PR does not authorize these actions.
  An explicit user request counts as approval for the named action only.

## Development Workflow

1. Read the affected package, its tests, and relevant documentation before editing.
   Trace callers, configuration, and examples that depend on the behavior.
2. For behavior changes, prefer Red-Green-Refactor: add or update a
   focused test and verify it fails for the intended reason before
   implementation when feasible. Defect regressions should reproduce
   the reported failure. If this is impractical, explain why and add
   appropriate tests alongside implementation.
3. Make the smallest complete change to pass the tests. Refactor only
   as needed, keeping tests passing. For shared behavior, account
   for AMQP, Kafka, and `testkit.MockBroker`; keep unrelated refactors
   out of the diff. Follow the Testing section for validation.
4. Format changed Go files with `gofmt`. When dependencies change, run
   `go mod tidy` in the affected module roots and include the matching `go.mod`
   and `go.sum` updates.
5. Update `README.md`, `docs/API.md`, `docs/TRANSPORTS.md`, and affected example
   code/READMEs when public behavior changes. Keep examples short and runnable.
6. Review the final diff for unintended changes. Summarize the problem, resulting
   behavior, checks run, any skipped checks or environmental limits, and
   unresolved risks.

Run commands from the repository root unless stated otherwise. Use
`go mod download` to install existing dependencies. For release work, follow
[RELEASE_CHECKLIST.md](RELEASE_CHECKLIST.md).

## Git and Pull Requests

- Use `feature/{topic}` branches with short, descriptive kebab-case topics.
  Confirm the branch, worktree, base commit, and working-tree state before edits;
  use separate worktrees when concurrent work needs isolation.
- Stage only intended paths or hunks. Review `git diff --cached` and run
  `git diff --cached --check` before committing; preserve unrelated work.
- Use concise imperative commit subjects matching recent history. Add a body
  when rationale, compatibility impact, or risks need explanation.
- Prepare PRs using [.github/pull_request_template.md](.github/pull_request_template.md)
  and contributor guidance. Verify the target branch, commit list, and full diff
  from the merge base; disclose inherited unpushed commits that expand PR scope.
- Treat remote-tracking refs as cached observations, not current remote state;
  refresh them only within the task's authorized remote access.
- Merge only after required CI passes for the latest PR revision. Existing
  review and approval gates still apply.
- Retain the existing `release/v{version}` branch and `v{version}` tag naming,
  release checklist, and tagged-commit CI gate.

## Code Review and Quality Gates

- Before requesting commit or PR approval, review the final diff and affected
  code against the approved scope.
- Independent Codex review is recommended for significant runtime, concurrency,
  public API, or security changes when available. If unavailable for such a
  change, perform self-review and disclose the limitation. Self-review is
  sufficient for smaller changes.
- Apply Architecture, Coding Standards, Testing, and Restriction as review
  criteria: correctness and regressions, concurrency and resource lifecycles,
  AMQP/Kafka delivery and recovery, public API compatibility, input validation,
  and dependency security.
- List findings in descending severity with file/line, concrete impact, and a
  recommended fix. Substantiated defects, contract violations, security issues,
  and missing required regressions are blocking; style and optional improvements
  are non-blocking suggestions.
- Reuse the Testing section's required local checks and select opt-in suites by
  affected behavior; preserve all CI requirements.
- Fix blocking findings within the approved scope, rerun affected tests and
  applicable required gates, and review the updated diff.
- Unresolved blocking findings and failed required checks block readiness.
  Report unavailable or pending required checks as incomplete, with reasons and
  remaining risks; never record them as passed or Not Applicable.

## Coding Standards

- Follow neighboring Go code, including import grouping and exported API comments.
  Prefer the standard library and existing helpers over new dependencies or layers.
- Reuse shared packages under `internal/`: `validation`, `connection`, `lifecycle`,
  `readiness`, `rpclimit`, and `observability`. Add configuration only when a
  transport reads it.
- Validate public inputs before broker I/O. Preserve typed and sentinel errors and
  their `errors.Is`/`errors.As` behavior; use `%w` when wrapping an error whose
  identity callers need to inspect.
- Keep cancellation and timeout classification consistent: explicit cancellation
  matches `context.Canceled`; deadline expiry matches `context.DeadlineExceeded`
  and `core.IsTimeout`.
- `Publish` and `Call` leave caller messages unchanged. Retained bodies and headers
  need owned copies; use `Message.Clone` where appropriate. Preserve portable
  metadata, including application-owned `MessageID` values.
- Protect concurrent state using existing synchronization patterns. Clean up
  pending RPCs, capacity slots, workers, and clients on failure or cancellation.
- Preserve the separate lifetimes of operation contexts, subscriptions, and broker
  connections. Keep readiness tied to active consumer setup during recovery and
  rebalances. `Close` closes without draining; `Shutdown(ctx)` rejects new work
  and drains admitted work within its context.

## Testing

### Local checks

Normal tests use fakes and mocks without live brokers. For Go behavior changes,
run focused tests first, then the standard checks from the repository root:

```sh
go test -race ./...
go vet ./...
go build ./...
make test-coverage-check
```

The coverage gate requires **75% aggregate coverage** across the critical packages
listed in `scripts/check_coverage.sh`. Coverage artifacts and `.gocache/` are ignored.

Put shared broker expectations in `internal/brokercontract/contract.go`; transport
fixtures supply wire I/O only. Keep recovery regressions in existing recovery test
files. Use standard `testing`, table-driven cases where useful, bounded contexts,
and resource cleanup.

```sh
go test -race ./transport/amqp ./transport/kafka ./testkit -run TestBrokerContract
make test-recovery
```

When shared code, dependencies, or the nested example change, also validate from
`examples/amqp-kafka/`:

```sh
cd examples/amqp-kafka
go test -race ./...
go vet ./...
go build ./...
```

Documentation-only changes may skip local runtime tests and coverage checks only
when no executable examples or commands change. Verify paths, commands,
documentation links, and the diff. Mark each skipped check **Not Applicable** in
the PR and explain why. All testing and coverage requirements for source-code
changes and all CI requirements remain unchanged.

### Live broker checks

Keep live tests opt-in. With Docker available, start the shared fixture and run
the relevant transport suite from the repository root:

```sh
docker compose -f examples/amqp-kafka/compose.yaml up -d --wait
WEAVE_AMQP_E2E=1 go test -race ./transport/amqp -run 'WithLiveRabbitMQ$' -count=1
WEAVE_KAFKA_E2E=1 go test -race ./transport/kafka -run 'WithLiveKafka$' -count=1
```

Provision Kafka topics and custom RabbitMQ exchanges before application use.
Follow [examples/README.md](examples/README.md) for topics and `WEAVE_EXAMPLES_E2E`
checks, and [docs/TRANSPORTS.md](docs/TRANSPORTS.md) for separate TLS and SCRAM
fixtures. After using the shared fixture, stop it with
`docker compose -f examples/amqp-kafka/compose.yaml down`.

For load, recovery, failover, or partition changes, select the appropriate runner;
each manages and cleans up its own fixtures:

```sh
./scripts/test_stress.sh
./scripts/test_stress.sh bench
./scripts/test_clusters.sh amqp
./scripts/test_clusters.sh kafka
```

See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for scope and resource requirements.
Cluster tests require Docker Compose 2.33.1 or newer; use their runner to supply
container and network IDs. CI also cross-compiles tests for Linux/386 and runs
`govulncheck` in both modules using current stable Go; see `CONTRIBUTING.md` for
scanner installation and toolchain requirements.

## Restriction

- Do not add a root facade, transport registry, factory layer, unused configuration,
  or speculative abstraction. Preserve the existing direct-constructor design.
- Do not report publish success before the configured broker acknowledgment.
  Publish success does not imply handler completion, and timeouts or connection
  loss after submission can leave delivery uncertain. Do not promise exactly-once
  processing or hide the need for application-level idempotency.
- Do not acknowledge or advance offsets past a failed dead-letter handoff as
  completed work. Preserve the original delivery for another attempt and retain
  documented handler-error policies.
- Do not weaken input validation, error classification, cancellation, ownership,
  or synchronization guarantees to simplify an implementation.
- Do not lower the coverage gate, skip applicable regressions, or suppress reachable
  vulnerability findings to make checks pass.
- Do not remove the nested example module's local replace directive or assume
  root-level Go checks validate that module.
- Do not include generated binaries, coverage files, caches, or logs in changes.
  Push release tags only after CI passes for the tagged commit.
