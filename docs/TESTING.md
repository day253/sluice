# Testing policy

Every confirmed defect must add a regression test in the same change. The
test must reproduce the failure and assert the behavior that must remain
correct after future refactors.

Tests are split into two layers:

- **Unit tests** (`make unit-test`) cover one package or component with
  in-memory fakes. They should be deterministic and fast. Validation, FSM
  command semantics, HTTP serialization, and leader-forwarding edge cases
  belong here.
- **Integration tests** (`make integration-test`) start a real multi-node
  cluster and exercise Raft, worker streams, HTTP/gRPC forwarding, and
  recovery over loopback TCP. They verify that the components work together;
  they must not be replaced by mocks when the defect involves networking or
  consensus.

The default `make test` runs both layers with the race detector. A bug-fix
commit should name the regression test in its description or review notes.

Recent submission regressions are covered by:

- `pkg/grpc.TestSubmitForwardsBeforeFollowerTenantValidation` (unit): a
  follower with a stale tenant snapshot forwards to the leader instead of
  returning a transient 404.
- `test/integration.TestHTTPSubmitThroughFollower` (integration): a real
  HTTP request sent to a follower is accepted and completed by the cluster.
- `pkg/grpc.TestSubmitBatchUsesOneRaftApply` and
  `test/integration.TestHTTPBatchSubmitThroughFollower` (unit/integration):
  batch submission persists multiple tasks in one Raft log entry while
  preserving the follower forwarding path.
