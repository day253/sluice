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
- Preserve the control/data-plane boundary: within one Raft shard, only the
  leader selects concrete task-to-node assignments and commits them; the
  leader runs no business workers, and followers never self-claim from a
  replicated global pending snapshot. Scale assignment with additional Raft
  shards, not decentralized claim races.
- Aggregate assignment and completion requests across all node streams before
  Raft Apply. Per-node-stream batches multiply consensus round trips by cluster
  size and can strand already claimed tasks behind client timeouts and leases.

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
- A scheduling or protocol design change is itself a regression risk even
  without a newly observed defect. Add focused unit coverage, a real
  multi-node integration case, and the requirement/non-goal boundary in the
  design and case matrix in the same change.

## Design and case history

- Update `docs/DESIGN.md` whenever scheduling, consensus, storage, task state,
  timeout, borrowing, or work-stealing behavior changes.
- Update `docs/TESTING.md` with a stable case ID, the reproduced failure,
  required invariant, unit test, and integration test. Keep historical cases
  after the bug is fixed so later iterations cannot regress them.
- If an integration path is temporarily impossible to automate, the change is
  incomplete: record the blocker and do not describe the behavior as covered.

## Performance evidence and observability

- Re-run the relevant fixed-shape performance baseline after every change that
  can affect submission, scheduling, consensus, storage, worker execution, or
  recovery. Never carry an earlier throughput conclusion across such a change.
- Record every baseline and comparison in `docs/PERF.md`, including date,
  hardware/failure domain, replica/voter/non-voter topology, tenant and task
  counts, processor and payload shape, request batch/concurrency, submission
  time, drain time, end-to-end throughput, errors, and unfinished final state.
  Keep earlier results so a bottleneck moving from one subsystem to another is
  visible instead of rewriting history.
- Compare like-for-like runs. If the environment, workload, protocol, or
  benchmark tooling differs, label the result separately and do not claim a
  percentage improvement from the mismatch. Record blocked or incomplete
  scale experiments and their infrastructure cause.
- A performance change is incomplete without enough read-only observability to
  identify its new limiting stage. At minimum preserve Raft Apply latency and
  batch fill, pending-selection work, dispatcher queue depth, task backlog, and
  error/lease-recovery signals relevant to the changed path.
- Performance diagnostics are process-local current state or bounded historical
  series. They must not be written into Raft, included in FSM snapshots, or fed
  back into scheduling decisions unless a separate protocol change explicitly
  defines and tests that behavior.
