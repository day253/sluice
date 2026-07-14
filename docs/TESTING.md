# Testing and regression policy

分布式系统首先保证正确性，再优化吞吐。每个已确认缺陷必须在同一变更中留下可复现
Case、单元回归测试和真实集成回归测试；修复后仍保留这些 Case，作为后续迭代的边界。
仓库级强制规则见 `AGENTS.md`。

## 两层测试职责

- **Unit**（`make unit-test`）：使用内存 FSM/Queue 或最小 fake，确定性覆盖输入校验、
  状态迁移、调度决策、批处理语义和边界条件。
- **Integration**（`make integration-test`）：启动真实多节点 Raft、TCP、HTTP/gRPC、
  Worker、Allocator、持久化目录和恢复流程。涉及网络、共识、Leader 转发或故障恢复的
  Case 不得用 mock 代替。
- **Release gate**（`make test`）：两层测试都以 `-race -count=1` 执行。等待必须有明确
  deadline，并断言最终状态和 exactly-once final-state，而不是只观察中间 pending。

## 新问题处理模板

1. 记录现象、触发规模和可观测证据。
2. 写出必须维持的正确性不变量和明确的非目标。
3. 增加能在旧实现失败的最小 Unit Case。
4. 增加走真实生产边界、能在旧实现失败的 Integration Case。
5. 修复实现并运行 `make test`；调度/存储/共识改动同步更新 `docs/DESIGN.md`。
6. 远程部署后复核健康、容量上限、积压下降、错误日志和故障恢复路径。

## 历史 Case 矩阵

| Case | 故障与不变量 | Unit | Integration |
|---|---|---|---|
| SUBMIT-001 | Follower 的租户镜像可能落后；请求必须先转发 Leader，不能瞬时 404 | `pkg/grpc.TestSubmitForwardsBeforeFollowerTenantValidation` | `test/integration.TestHTTPSubmitThroughFollower` |
| SUBMIT-002 | 批量提交必须用单条 CreateTaskBatch Raft 日志，同时保留 Follower 转发和全部任务完成 | `pkg/grpc.TestSubmitBatchUsesOneRaftApply` | `test/integration.TestHTTPBatchSubmitThroughFollower` |
| SCHED-001 | Worker 只能执行 Leader 已提交的具体 assignment，不能从复制的 fresh pending 自发 Claim；任务最终只处理一次 | `pkg/worker.TestPoolWorker_ExecutesLeaderAssignmentWithoutWorkerClaim`、`pkg/grpc.TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` | `test/integration.TestFreshRecoveryDoesNotCauseCrossNodeClaimStorm` |
| SCHED-002 | aged pending 也必须由 Leader 唯一选择并批量 Claim；多节点不能因同时扫描产生 rejected-claim 风暴 | `pkg/grpc.TestSelectPendingForSlot_PreservesLocalityAndAgeBoundary`、`TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` | `test/integration.TestLeaderAssignmentDrainsAgedBacklogWithoutClaimCompetition` |
| STEAL-001 | work steal 是 Leader 对既有空闲槽位的调度：同 tenant/同节点优先，本节点其他 tenant 可立即分配，跨节点 fresh 必须等待 5 秒 | `pkg/grpc.TestSelectPendingForSlot_PreservesLocalityAndAgeBoundary`、旧协议边界 `TestCanStealRequiresAgedPendingTask` | `test/integration.TestWorkStealUsesAgedPendingWork`（跨节点 5 秒边界） |
| LEADER-001 | Leader 只调度与提交，不接收 allocation、不运行或获取业务任务；选主后立即清空 Worker，Follower 继续完成任务 | `pkg/allocator.TestReconcile_LeaderHasNoAllocation`、`TestReconcile_OnlyLeaderClearsStaleAllocation`、`pkg/worker.TestPoolWorker_GuardPreventsLeaderExecution`、`pkg/grpc.TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` | `test/integration.TestLeaderIsControlPlaneOnly`、`TestFailover` |
| ALLOC-001 | 多个 aged backlog 必须公平共享闲置容量，且有效 Worker 总数不能超过集群容量；无 pending 立即释放借用 | `pkg/allocator.TestApplyBorrowing_ProbesSpareCapacityExponentially`、`TestApplyBorrowing_SharesCapacityAcrossBackloggedTenants`、`TestApplyBorrowing_DoesNotProbeWithoutPendingBacklog` | `test/integration.TestAdaptiveIdleBorrowing` |
| RECOVERY-001 | 节点宕机后 inflight→pending→claim→complete；每个任务最终只处理一次 | `pkg/allocator.TestRequeueStaleTasks_ExpiredClaimReturnsToPending` | `test/integration.TestRecovery` |
| RECOVERY-002 | 全集群重启后 pending 立即恢复，遗留 inflight 在 Claim lease 后恢复，不能永久卡住或重复最终状态 | FSM/Allocator lease 与 requeue 单测 | `test/integration.TestFullClusterRestartRecoversExpiredClaims` |
| FAIRNESS-001 | 超配时使用 Max-Min Fairness，任何分配不得越过租户上限或集群容量 | `pkg/allocator.TestMaxMinFairness_Oversubscribed` | `test/integration.TestOversubscription` |

## 正确性断言清单

调度、批处理、work-steal、超时或恢复变更至少检查：

- accepted 的任务已经存在于 Raft FSM；
- pending → inflight → done/failed 的迁移合法且由 Leader 提交；
- 同一任务没有并发 Claim 所有者，最终状态只提交一次；
- Follower 转发、Leader 切换和 stream 重连不会丢任务；
- 节点宕机和全量重启后，任务在有界时间内继续处理；
- Leader 不出现在 allocation，normal + borrowed 不超过存活 Follower 容量；
- Worker 不选择 task ID；work-steal 不绕过 tenant/status/queue locality/age 校验；
- AssignmentStream 超时或响应未知时不能回退自发 Claim；只有明确的旧 Leader
  `Unimplemented` 才允许滚动升级兼容路径；
- 测试在 race detector 下没有数据竞争，且没有无界等待。
