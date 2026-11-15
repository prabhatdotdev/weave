## Summary

- Describe the user-visible or runtime behavior changed in this PR.

## Validation

Documentation-only changes may skip local runtime tests and coverage checks only
when no executable examples or commands change. Verify paths, commands, and
documentation links. Label each skipped check **Not Applicable — explanation**
instead of marking it complete. All testing and coverage requirements for
source-code changes and all CI requirements remain unchanged.

- [ ] I added or updated automated tests for every behavior change in this PR.
- [ ] If this PR changes runtime, transport, recovery, or error-handling behavior, I wrote the regression test before or alongside the code change.
- [ ] I ran `go test ./...`.
- [ ] I ran `make test-coverage-check` and the critical package coverage gate still passes.
- [ ] I updated docs or examples for any changed public behavior.
