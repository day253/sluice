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
6. 性能相关变化按 `AGENTS.md` 的固定字段重跑同形状基线，在 `docs/PERF.md` 追加前后
   结果和瓶颈变化；环境不同时分开记录。
7. 远程部署后复核健康、容量上限、积压下降、错误日志和故障恢复路径。

## 历史 Case 矩阵

| Case | 故障与不变量 | Unit | Integration |
|---|---|---|---|
| CTRL-001 | 扩容 Worker 不能扩大 Raft membership/日志副本；control 不执行 Processor，worker 无 Raft/PVC/FSM，重复 session 注册不写日志；Worker 断流先撤容量但不立即重派 inflight，正常退出必须排空已开始任务；迁移删除旧 Node 镜像但保留 inflight lease | `pkg/allocator.TestExecutionNodesExcludeEveryExplicitControlReplica`；`pkg/api.TestStatelessWorkerRegistrationHasDedicatedNonRaftBoundary`；`pkg/node.TestWorkerRegistrationSameSessionIsNoOp`；`pkg/raft.TestWorkerOfflineRemovesCapacityWithoutRequeueingInflight`、`TestRetireNodeDeletesLegacyIdentityWithoutRequeueingInflight`；`pkg/worker.TestStatelessPoolDrainCommitsInflightBeforeShutdown` | `test/integration.TestStatelessWorkerRoleSplit`（真实 3 control + stateless Worker 注册/allocation/Assignment/Result/重启/Leader 切换）；`TestNewMembersBeyondVoterLimitAreNonvoters`（真实 7 replica 先 3/4、再 prune 为 3/0） |
| CTRL-002 | 同 ID 新 Worker 已订阅后，旧 session 延迟退出不能删除新 AllocationPush 或把新 session 标 down；后续 allocation 必须到达 replacement | `pkg/grpc.TestOldAllocationStreamCannotUnregisterReplacementSession`、`TestWorkerSessionReconnectCancelsOfflineAndDisconnectPreservesInflightLease` | `test/integration.TestStatelessWorkerRoleSplit`（新旧 session 重叠，旧流关闭后新增 tenant/撤另一 Worker，replacement 收新 allocation 并排空，再切 Leader） |
| CTRL-003 | Pod/5-0 membership Ready 后，旧 Snapshot 的 retained control 不得继续保留 100 Worker/allocation；目标集群 DNS 返回 fake IP 时 Helm Worker 必须经 K8s API 获取 Service ClusterIP，CRD operator 必须把实际 ClusterIP 注入 Worker；部署必须验 FSM 角色/容量而非只验 health | `pkg/raft.TestSetControlNodesRemovesLegacyCapacityWithoutChangingLivenessOrInflight`；`pkg/node.TestControlNodesNeedingMigrationUsesOnlyRaftMembersInStableOrder`；`charts/sluice/charttest.TestWorkerEntrypointUsesStableServiceIPInsteadOfClusterDNS`；`internal/controller.TestClusterReconcilerAutoscalingTargetsOnlyStatelessWorkers` | `test/integration.TestStatelessWorkerRoleSplit`（注入 legacy control 容量/allocation→Leader 切换→5/0 control 镜像收敛→Worker 排空）；`scripts/deploy-remote.sh` 真实 MicroK8s 自动断言 5 control/50 Worker/5000 容量/allocation owner；远程 CRD/operator Worker 注册与 HPA 扩缩容 smoke |
| CTRL-004 | 新 Leader 上 AllocationPush 可能先于 leadership observer 打开；接管恢复不能把已 connected session 覆盖成 disconnected 并在 5 秒后误标 down，真实断流仍须在 grace 后撤容量 | `pkg/grpc.TestLeaderRecoveryPreservesWorkerSessionThatAlreadyConnected` | `test/integration.TestStatelessWorkerRoleSplit`（真实 3-voter Leader 切换后跨过生产 5 秒 grace，持续断言 Worker up、有 allocation，再提交并 exactly-once 排空） |
| HPA-001 | Helm/CRD HPA 只能修改无状态 Worker StatefulSet；control/Raft 数量固定且为奇数，operator 不能与 HPA 争抢 replicas；扩容增加执行容量但不增加 membership/PVC，缩容保留 inflight drain/lease 语义，关闭 HPA 恢复静态值 | `api/v1.TestSluiceClusterDeepCopyOwnsAutoscalingMetricsAndBehavior`；`internal/controller.TestClusterReconcilerAutoscalingTargetsOnlyStatelessWorkers`、`TestClusterReconcilerDoesNotFightHPAAndRestoresStaticReplicasWhenDisabled`、`TestNormalizeClusterSpecRejectsAutoscalingControlPlaneOrInvalidBounds`；`charts/sluice/charttest.TestWorkerAutoscalingTargetsOnlyStatelessStatefulSet`、`TestAutoscalingDefaultsProtectWorkerDrain`、`TestChartAndStandaloneCRDsExposeWorkerAutoscaling`、`TestOptionalOperatorCanManageWorkerHPA` | `test/integration.TestStatelessWorkerRoleSplit`（真实 3 voter + stateless Worker 扩/缩容、durable backlog、allocation/Assignment/Result、membership 不变、缩容后继续排空、replacement 与 Leader 切换、exactly-once final state）；远程 Helm autoscaling/v2 server dry-run 与真实 StatefulSet 50→51→50 验收 |
| HPA-002 | HPA 51→50 缩容后，FSM 可保留 `worker-50` 的 down 身份镜像；部署门禁不能把它当成额外运行副本或容量，也不能允许 allocation 指向它 | `charts/sluice/charttest.TestRemoteTopologyValidationAllowsHPAReplicaHistory`（保留 down 身份通过、指向 down 的 allocation 和用 down 冒充 up 容量均失败） | `scripts/deploy-remote.sh` 对真实 MicroK8s revision 46 验证 5 个 up control、50 个 up Worker/5000 容量、allocation 仅属于 up Worker，同时接受 HPA 实测留下的第 51 个 down 身份；Raft 保持 5/0 |
| SUBMIT-001 | Follower 的租户镜像可能落后；请求必须先转发 Leader，不能瞬时 404 | `pkg/grpc.TestSubmitForwardsBeforeFollowerTenantValidation` | `test/integration.TestHTTPSubmitThroughFollower` |
| SUBMIT-002 | 批量提交必须用单条 CreateTaskBatch Raft 日志，同时保留 Follower 转发和全部任务完成 | `pkg/grpc.TestSubmitBatchUsesOneRaftApply` | `test/integration.TestHTTPBatchSubmitThroughFollower` |
| SUBMIT-003 | 1000 条批次经 Follower 不得被固定 10 秒窗口误判；未知结果使用幂等键重试必须返回相同 ID、只处理一次且不创建本地 Queue 副本 | `pkg/grpc.TestSubmitBatchFollowerUsesConfiguredForwardTimeout`、`TestSubmitBatchIdempotencyKeysReuseTaskIDs` | `test/integration.TestHTTPBatchSubmitThroughFollower`（1000 条、真实 Follower HTTP、重复提交） |
| SCHED-001 | Worker 只能执行 Leader 已提交的具体 assignment，不能从复制的 fresh pending 自发 Claim；任务最终只处理一次 | `pkg/worker.TestPoolWorker_ExecutesLeaderAssignmentWithoutWorkerClaim`、`pkg/grpc.TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` | `test/integration.TestFreshRecoveryDoesNotCauseCrossNodeClaimStorm` |
| SCHED-002 | aged pending 也必须由 Leader 唯一选择并批量 Claim；多节点不能因同时扫描产生 rejected-claim 风暴 | `pkg/grpc.TestSelectPendingForSlot_PreservesLocalityAndAgeBoundary`、`TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` | `test/integration.TestLeaderAssignmentDrainsAgedBacklogWithoutClaimCompetition` |
| SCHED-003 | Assignment 必须跨所有节点流全局聚批；节点数增加不能把健康请求拖过流超时并让已 claim 任务等待 30 秒 lease | `pkg/grpc.TestAssignmentStreamBatchesDistinctLeaderCommittedTasks`（两个独立节点流仅一条 Apply） | `test/integration.TestGlobalLeaderBatchingDrainsWithoutLeaseRecovery`（7 节点、lease 前排空、零 assignment/completion timeout 与 lease 回收） |
| SCHED-004 | 4900 Worker 同时拉取/完成不能形成无界请求尖峰；单节点每类未决请求受固定 credit 约束、Raft Claim/Complete 批次≤128，单请求等待不能关闭共享流；Leader 仍唯一分配且业务 Worker 并发不受 credit 限制 | `pkg/grpc.TestClaimClientBoundsPerNodeRaftRequests`、`TestInternalServiceBoundsGlobalRaftBatchSize` | `test/integration.TestNodeCreditsDrainProductionWorkerFanoutWithoutLeaseRecovery`（真实 8 节点/4900 执行 Worker/4096 任务、批次边界、lease 前排空、exactly-once final state） |
| SCHED-005 | allocation/借用缩容只能停止 retiring Worker 获取下一条任务，不能 cancel 已 claim Processor 并制造 30 秒 lease 尾部；已开始任务必须直接提交一次最终状态 | `pkg/worker.TestPoolReconcileScaleDownRetiresAfterInflightCompletion` | `test/integration.TestAllocationScaleDownLetsInflightProcessorsFinish`（真实 3 节点 Raft/Assignment/Result/Worker，执行中缩容、零取消、无需 lease、exactly-once final state） |
| PERF-001 | 50 个执行实例不能等于 50 voter；新成员超过奇数上限后必须是 non-voter，旧超大 voter 集合必须先转移 Leader 再安全 demote；2 万 pending 每批只能建一次索引；durable Raft pending 不能再逐条复制到 Leader 本地 Queue | `pkg/raft.TestDesiredVoterIDsUseStableOrdinalOrder`、`TestValidateMaxVotersRejectsUnsafeEvenQuorum`；`pkg/grpc.TestPendingSelectorIndexesLargeBacklogOncePerBatch`、`TestSubmitBatchDoesNotDuplicateRaftPendingIntoLocalQueue`；`pkg/api.TestRaftStatusEndpointReportsBoundedMembership` | `test/integration.TestNewMembersBeyondVoterLimitAreNonvoters`、`TestOversizedVoterSetTransfersLeaderAndMigrates`、`TestBoundedVotersDrainTwentyThousandHTTPTasks`（真实 7 节点 Raft + Follower HTTP/gRPC + Worker + Bolt 持久化，4 tenant/20000 条、全部排空且每条只执行一次） |
| PERF-002 | 每个 ClaimBatch 不能复制/排序全部剩余 backlog；派生索引只随已提交 FSM 迁移更新，Apply/Leader 失败不能因只读选择丢 pending，四级 FIFO/locality/age 策略不变 | `pkg/raft.TestPendingIndexSelectsWithoutRescanningBacklog`；`pkg/grpc.TestDispatchAssignmentsReadsOnlyIndexedCandidatesBeforeRaftApply` | `test/integration.TestBoundedVotersDrainTwentyThousandHTTPTasks`（真实 7 节点、20k HTTP→Raft→Worker→Result，排空且每条一次）；远程 5 control/50 stateless Worker 同形状性能与扫描放大复测 |
| PERF-003 | 每节点 8-credit 要 16 条活跃流才能填满 128 条，allocation 集中时把 Claim/Complete 碎成额外共识往返；当前每类 32、合计 64N 仍有界，四流可填满一批，不能改变 Leader 唯一分配、128 上限、ACK-after-commit 或 lease | `pkg/grpc.TestFourWorkerStreamsCanFillOneGlobalRaftBatch`、`TestClaimClientBoundsPerNodeRaftRequests` | `test/integration.TestNodeCreditsDrainProductionWorkerFanoutWithoutLeaseRecovery`（同一真实 8 节点/4900 槽/4096 任务，旧 151/151 批→要求每类≤64 批、全部 item、零 timeout/lease、每任务一次）；远程固定 20k 基线 |
| PERF-004 | idle 租户 durable Create 后不能继续等待最长 3 秒 tick；只有 allocation≤1 且 Limit>1 的 tenant 才非阻塞唤醒，burst 必须合并，active tenant 不增加 allocation 日志；通知丢失仍由 periodic tick 恢复 | `pkg/grpc.TestSubmitBatchNotifiesWorkOnlyAfterDurableApply`；`pkg/allocator.TestWorkNotificationsCoalesceBeforeAllocatorRunLoop`；`pkg/raft.TestAllocatedWorkersForTenantsReturnsRequestedCurrentTotals` | `test/integration.TestDurableSubmissionWakesIdleAllocator`（真实 2 节点，把 tick 延长到 1h，经 Follower HTTP→Leader Raft→Allocator→allocation push→Worker，2s 内恢复并 exactly-once 排空）；远程 idle 4 tenant/20k 同形基线 |
| CI-001 | Worker benchmark 不能给并发任务重复 TaskID 后无界等待；fixture ID 必须唯一，进度等待必须有 deadline，实际 CI benchmark 命令必须有界结束 | `pkg/worker.TestBenchmarkTaskIDsAreUniqueAcrossConcurrentWorkers`、`TestBenchmarkWaitHasExplicitDeadline` | `test/integration.TestWorkerBenchmarkCommandTerminates`（真实启动 `go test -bench` 子进程，10/100 Worker 并发 Case 必须在 20 秒内结束） |
| OBS-001 | 性能修改后必须重跑并保留同形状基线；Leader 必须以不写 Raft、不影响调度的方式暴露 Apply 延迟/批次、pending selection 和 dispatcher 队列；Follower 查询必须代理当前 Leader，不能随机返回本地全零 | `pkg/metrics.TestPerformanceRecordsRaftBatchAndSchedulerWindows`、`TestCommandShapeCountsReplicatedItems`、`TestCollectorStoresBoundedPerformanceHistory`；`pkg/grpc.TestDispatchAssignmentsObservesPendingScanBeforeRaftApply`；`pkg/api.TestPerformanceEndpointReturnsConfiguredLeaderDiagnostics`；`pkg/node.TestResolveLeaderAPIAddressUsesRegisteredOrRaftHost` | `test/integration.TestPerformanceDiagnosticsProxyFromFollower`（真实 3 节点、Follower HTTP batch、Leader Create/Claim/Complete、Worker/Result、Follower→Leader 诊断代理、最终 unfinished=0） |
| UI-001 | 性能诊断不能只剩难读的 JSON；WebUI 必须显示 Create/Claim/Complete Apply、扫描放大、队列和两张 174 点时序图，同时保留可直接打开的原始 JSON；诊断暂不可用不能拖垮集群主视图 | `pkg/webui.TestDashboardIncludesPerformanceVisualizationAndJSONLink`、`TestPerformanceJSONRouteStillDelegatesToAPI` | `test/integration.TestPerformanceDiagnosticsProxyFromFollower`（真实 3 节点生产 HTTP 同时验证 dashboard 和 Leader JSON）；真实浏览器 1280px/390px 渲染、每秒刷新、Canvas、JSON 新页和零 console error |
| UI-002 | WebUI 每秒轮询不能从 performance 和 `/metrics` 重复拉取、序列化两份 174 点性能历史；workload metrics 请求必须在复制 ring buffer 前排除 `performance:`，默认 performance JSON 继续完整可用 | `pkg/metrics.TestCollectorStoresBoundedPerformanceHistory`（完整/当前/排除前缀三种读取）；`pkg/api.TestPerformanceEndpointReturnsConfiguredLeaderDiagnostics`、`TestPerformanceEndpointCanReturnCurrentSnapshotWithoutHistory`、`TestMetricsEndpointCanExcludePerformanceHistories`；`pkg/webui.TestDashboardIncludesPerformanceVisualizationAndJSONLink` | `test/integration.TestPerformanceDiagnosticsProxyFromFollower`（真实 3 节点、完整 Leader JSON、`history=0`、排除本地 performance 的 metrics、Dashboard 生产脚本）；远程 50 实例同形状 2 万任务开页面基线 |
| UI-003 | performance 当前值与 174 点曲线必须来自同一个 Leader；负载均衡连到 Follower 时，不能把该 Follower 的本地零历史与 Leader 当前累计值拼在一张图上 | `pkg/webui.TestDashboardIncludesPerformanceVisualizationAndJSONLink`（性能历史来自完整 Leader 端点，通用 metrics 显式排除 performance）；`pkg/api.TestMetricsEndpointCanExcludePerformanceHistories` | `test/integration.TestPerformanceDiagnosticsProxyFromFollower`（真实 Follower 页面、Follower→Leader 完整诊断历史、Follower 本地 metrics 排除 performance）；远程页面连接 `sluice-36`、Leader `sluice-3` 的浏览器回归 |
| UI-004 | 四张 174 点图必须可用鼠标读取某个时间点；提示显示时间桶、最近系列和原始值，Worker 图同时显示 Limit；50 实例不能把全部系列塞入提示框；快速跨图移动时只能保留当前图一个 tooltip；交互只读且不得增加请求或历史存储 | `pkg/webui.TestDashboardChartsExposeNearestPointTooltip` | `test/integration.TestPerformanceDiagnosticsProxyFromFollower`（真实 3 节点 Follower HTTP 交付 tooltip 脚本及 Leader 历史）；远程 50 实例浏览器跨图 pointer hover、唯一可见 tooltip、内容和零 console error 回归 |
| UI-005 | 四张时序图必须能在新页查看原始 JSON；Worker/unfinished 使用复制 ring 前的 `prefix` 筛选，性能图保持同一 Leader JSON；链接未点击时不得增加轮询、历史或 Raft 写入 | `pkg/metrics.TestCollectorFiltersMetricHistoriesByPrefix`；`pkg/api.TestMetricsEndpointCanFilterHistoriesByPrefix`；`pkg/webui.TestDashboardChartsExposeRawJSONLinks` | `test/integration.TestPerformanceDiagnosticsProxyFromFollower`（真实 3 节点 Follower HTTP、prefix 纯目标系列、每条 174 点、四图链接）；远程 50 实例浏览器验证 href/`_blank`/`noopener` 和零 console error，同一 Mac 隧道实际 GET 解析三类 JSON；Browser Use 裸 JSON 顶层导航被 `ERR_BLOCKED_BY_CLIENT` 拦截，故不宣称该项自动浏览器渲染已覆盖 |
| RESULT-001 | Completion 必须跨所有节点流全局聚批；只有 Raft 已提交结果可 ACK，取消流不能确认未提交结果 | `pkg/grpc.TestResultStreamBatchesCompletionsAcrossNodeStreams`、`TestDispatchCompletionsDoesNotAcknowledgeCanceledJob` | `test/integration.TestGlobalLeaderBatchingDrainsWithoutLeaseRecovery` |
| STEAL-001 | work steal 是 Leader 对既有空闲槽位的调度：同 tenant/同节点优先，本节点其他 tenant 可立即分配，跨节点 fresh 必须等待 5 秒 | `pkg/grpc.TestSelectPendingForSlot_PreservesLocalityAndAgeBoundary`、旧协议边界 `TestCanStealRequiresAgedPendingTask` | `test/integration.TestWorkStealUsesAgedPendingWork`（跨节点 5 秒边界） |
| LEADER-001 | Leader 只调度与提交，不接收 allocation、不运行或获取业务任务；选主后立即清空 Worker，Follower 继续完成任务 | `pkg/allocator.TestReconcile_LeaderHasNoAllocation`、`TestReconcile_OnlyLeaderClearsStaleAllocation`、`pkg/worker.TestPoolWorker_GuardPreventsLeaderExecution`、`pkg/grpc.TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` | `test/integration.TestLeaderIsControlPlaneOnly`、`TestFailover` |
| ALLOC-001 | 多个 aged backlog 必须公平共享闲置容量，且有效 Worker 总数不能超过集群容量；无 pending 立即释放借用 | `pkg/allocator.TestApplyBorrowing_ProbesSpareCapacityExponentially`、`TestApplyBorrowing_SharesCapacityAcrossBackloggedTenants`、`TestApplyBorrowing_DoesNotProbeWithoutPendingBacklog` | `test/integration.TestAdaptiveIdleBorrowing` |
| ALLOC-002 | 并发 Worker 注册、定时 tick 和 Leader 切换不能重叠执行 allocator 的读-算-提交周期；leader-local idle/borrow map 不得竞争，allocation snapshot 必须按顺序提交并保持 Leader-only/容量边界 | `pkg/allocator.TestConcurrentReconcileRequestsAreSerialized`（阻塞第一次 Apply，确定性断言第二次不能进入） | `test/integration.TestStatelessWorkerRoleSplit`（真实 3 voter，同时释放两个 stateless Worker 经 HTTP 注册触发 reconcile，allocation/任务/Leader 切换全链路在 `-race` 下收敛） |
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
- AssignmentStream 响应未知时不能回退自发 Claim；当前 Assignment/Result 不使用会关闭
  节点共享流的单请求固定超时，只有明确的旧 Leader
  `Unimplemented` 才允许滚动升级兼容路径；
- 单节点 Assignment/Result 未决请求各不超过 32、合计不超过 64，Leader 的
  Claim/Complete Raft 批次不超过 128；credit 不能降低已分配的业务 Worker 并发；
- Worker 扩容不改变 Raft voter/总成员数；control 不执行，Worker 无 Raft/FSM/PVC；
  迁移旧成员先转移 Leader，再移除 replica/Node 镜像而不提前重派 inflight；
- Kubernetes HPA 只能拥有 Worker scale subresource；Helm/operator 不得覆盖其当前 replicas，
  control 副本不自动扩缩，Worker 缩容不得绕过 drain、offline grace 和 claim lease；
- 新 Leader 的 session recovery 只给未连接 Worker 启动 offline grace；先于 leadership
  observer 建立的 AllocationPush 必须保持 connected，不能被恢复逻辑误标 down；
- API 批量提交只创建 Raft pending，不写重复本地 Queue；大 backlog 使用随已提交 FSM
  状态增量维护、可重建的派生索引，同时保持原四级 FIFO/tenant/node/age 优先级；
- allocation 缩容不 cancel 已 claim Processor；retiring Worker 先提交最终状态再退出，
  且退出前不能请求下一条 assignment；
- 测试在 race detector 下没有数据竞争，且没有无界等待。
