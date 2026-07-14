# Sluice repository rules

These rules apply to the entire repository.

## Correctness first

- Treat Raft state and task lifecycle correctness as release blockers. A
  performance change must preserve durable submission, single-owner claim,
  exactly-once final-state commit, tenant isolation, cluster capacity bounds,
  and recovery after node or leader loss.
- Define the requirement boundary before changing a distributed protocol:
  covered behavior, invariants, failure model, timeout/lease semantics, and
  explicit non-goals. Do not silently expand the protocol's responsibilities.
- Current-state mirrors and historical series are different data classes.
  Document which one is stored before adding a field or metric.

## Mandatory regression coverage

- Every confirmed defect must add both a focused unit regression test and a
  complete integration regression test in the same change.
- Unit tests must reproduce the component-level failure deterministically.
- Integration tests must exercise the real production boundary involved. For
  distributed behavior, start a real multi-node Raft cluster and use the real
  HTTP/gRPC, worker, allocation, persistence, and recovery paths; mocks alone
  are not sufficient.
- A pure UI defect still needs a component test and an end-to-end/browser test.
- Prefer condition-based waits with explicit deadlines. Do not hide liveness
  bugs behind unbounded waits or long unconditional sleeps.
- Run `make test` with the race detector before merging a correctness or
  scheduling change.

## Design and case history

- Update `docs/DESIGN.md` whenever scheduling, consensus, storage, task state,
  timeout, borrowing, or work-stealing behavior changes.
- Update `docs/TESTING.md` with a stable case ID, the reproduced failure,
  required invariant, unit test, and integration test. Keep historical cases
  after the bug is fixed so later iterations cannot regress them.
- If an integration path is temporarily impossible to automate, the change is
  incomplete: record the blocker and do not describe the behavior as covered.
