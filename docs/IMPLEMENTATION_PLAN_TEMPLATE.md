# Implementation Plan: {title}

Status: Draft
Approval: {pending, or reference to explicit user approval and approved scope}

## Goal and Scope

- Problem and intended outcome: {description}
- Included: {scope}
- Excluded: {boundaries}

## Affected Components

- Packages, files, and callers: {paths and responsibilities}
- Transport impact: {AMQP, Kafka, MockBroker; explain relevant differences}
- Documentation and examples: {affected paths, or none}

## Architecture and Compatibility

- Proposed approach: {current behavior → intended behavior}
- Package boundaries and shared helpers: {placement and rationale}
- Public API, configuration, and compatibility: {changes, migration, or none}
- Dependencies: {changes and rationale, or none}

## Reliability and Risks

- Concurrency and lifecycle: {ownership, synchronization, cancellation, cleanup}
- Recovery and delivery: {failure outcomes, readiness, acknowledgments/offsets,
  retries, and dead-letter handling relevant to this change}
- Trade-offs and alternatives: {chosen approach and material downside}
- Main risks and mitigations: {include rollback or recovery where relevant}
- Open decisions: {questions requiring resolution, or none}

## Implementation Steps

1. {Concrete step and affected components}
2. {Next step, including documentation or examples where needed}

## Validation

Follow [AGENTS.md testing requirements](../AGENTS.md#testing).

- Focused regressions: {scenario → expected result, including failure paths}
- Applicable suites and module roots: {selection and rationale}
- Broker fixtures or environmental limits: {requirements, or none}
- Completion evidence: {results; explain any Not Applicable checks}

## Acceptance Criteria

- [ ] {Observable outcome and how it will be verified}
- [ ] {Relevant compatibility or reliability outcome and verification}
- [ ] {Any remaining change-specific completion condition}
