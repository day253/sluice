# Sluice — 分布式多租户限流系统

## 核心原则

**Leader 管分配，Follower 管执行。**

- Leader 运行 Allocator、AssignmentStream 和 Raft 写入，不运行任何业务 Worker。
- Follower 的 Worker 只上报空闲槽位，不读取全局 pending 快照来决定具体任务。
- Leader 是单一任务调度权威：选择具体任务和执行节点后，先用一条
  `OpClaimBatch` Raft 日志提交，再把 payload 返回给 Worker。
- FSM 中的 pending/inflight 是事实来源；节点本地 Queue 只是可丢失的局部性提示。
- 单 shard 最多保留 5 个稳定 voter；额外执行实例以 non-voter 复制 FSM，不进入提交多数派。

## 任务生命周期

```
1. 提交: Client → 任意 Node → raft.Apply(OpCreateTaskBatch) → FSM (pending)
2. 请求: Follower Worker → AssignmentStream(node, preferred_tenant)，只报告一个空闲槽位
3. 选择: Leader 校验 allocation[node][preferred_tenant] > 0，并按以下优先级选一个任务：
         a. 本节点 + preferred tenant
         b. preferred tenant 的任意节点队列
         c. 本节点其他 tenant
         d. 已等待超过 5s 的任意 tenant（work steal）
4. 批量: Leader 跨所有节点流全局聚批(5ms/最多128条) → raft.Apply(OpClaimBatch)
         原子提交 task: pending→inflight、NodeID=执行节点
5. 返回: Leader → AssignmentStream → 已提交的 task_id/tenant/payload
6. 执行: Follower Worker 只处理 Leader 返回的已提交任务
7. 完成: Worker → ResultStream → Leader → raft.Apply(OpCompleteBatch)
```

提交请求不携带处理耗时预估。任务进入 FSM 后按 `CreatedAt` FIFO 排队，由实际
处理结果和待处理时长驱动调度，避免客户端估时不准导致饥饿。

work steal 也是 Leader 的调度决策，不是各节点自发抢占。空闲 Worker 的请求仍携带其
配额所属 tenant 作为偏好；当该 tenant 无任务时，Leader 可立即分配本节点其他 tenant
的任务，或在任务跨节点等待超过 5 秒后分配全局积压。它不增加 Worker 数，只复用既有
空闲槽位，因此不会绕过集群容量上限。

## 限流模型

- **维度**: 并发数（同时 inflight 的任务数）
- **全局**: Allocator 只在活跃 Follower 上计算每租户每节点的有效 worker 配额
- **执行**: 每个 Worker 同步持有至多一个 Assignment 请求/任务；Leader 校验请求节点
  确实拥有其 preferred tenant 配额，实际并发由 Worker 数硬限制
- **缩容**: allocation 减少只让多余 Worker 停止请求下一条 assignment；已 claim 并开始
  的 Processor 必须先完成并提交最终状态。进程关闭仍使用硬取消和 lease 恢复，普通借用
  回收或 Leader 角色切换不能制造 30 秒尾部重试
- **空闲**: 连续 3 周期 0 inflight → idle → 降为 1 worker
- **超售**: sum(limits) > total_workers → Max-Min Fairness 按比例分配
- **借用**: `max_workers` 是正常保底配额；所有等待超过 5 秒的 tenant backlog 都可以
  共享集群剩余容量。借用目标按 tenant 独立试探为 `1, 3, 7, ...`（大集群首轮为 64），
  每轮受 pending 数、剩余容量和公平份额限制；backlog 消失后立即回收。

## 分配算法

```
Allocator (Leader, 每 3s):
  1. 读 FSM → 活跃 Follower（排除 Leader）+ 租户配置 + inflight 计数
  2. Max-Min Fairness → 每租户应得 worker 总数
  3. 均匀分布到各 Follower；Leader allocation 必须不存在
  4. 空闲检测: inflight=0 连续 3 周期 → idle → 1 worker
  5. 空闲租户释放的 worker 二次分配给活跃租户
  6. 对所有 backlog 已等待 5s 的租户，按公平份额自适应试探增加借用 worker
  7. raft.Apply(OpUpdateAllocation)
```

### 借用额度与写入规则

- `FSMState.Allocations` 是当前时刻的镜像，不保存借用变化历史。
- `NodeAllocation.Tenants[tenant]` 是节点实际启动的有效 worker 数，包含借用。
- `NodeAllocation.Borrowed[tenant]` 是其中超过 `TenantConfig.MaxWorkers` 的当前借用数，
  仅用于 API/UI 展示；AssignmentStream 使用 `Tenants` 校验执行槽位。
- 每次调度只写一条 `OpUpdateAllocation`，借用和回收与普通分配一起原子替换，
  不追加单独的借用日志，也不把 Leader 内存中的试探目标写成历史数据。
- Leader 切换后试探目标清零，从租户正常配额重新开始；这保证旧 Leader 的借用
  不会在新任期无限保留。
- 节点按带数字后缀的 ID 进行稳定排序（`node-2` 在 `node-10` 之前），避免相同
  分配在节点间来回抖动。

## AssignmentStream — Leader 单一调度权威

```text
Follower workers                         Leader
     │                                     │
     │──Assignment(node, preferred)───────►│  每个请求代表一个空闲槽位
     │──Assignment(node, preferred)───────►│  跨全部节点流全局聚批 5ms / 128
     │                                     │  从全局 FIFO pending 中选不同 task
     │                                     │  raft.Apply(OpClaimBatch)
     │                                     │  pending→inflight + execution NodeID
     │◄──AssignedTask(task, payload)────────│  只返回已提交成功的任务
     │──Process─────────────────────────────│
     │──ResultStream───────────────────────►│  raft.Apply(OpCompleteBatch)
```

Leader 只有一个 Assignment dispatcher：来自所有节点流的空闲槽位先进入同一 5ms
窗口，最多 128 条请求只读一次 pending/allocation 并提交一条 `OpClaimBatch`。提交结果
再路由回原节点流，每条流按 5ms/128 条合并响应。这样 Raft 往返次数随总吞吐增长，而不
随节点流数量线性增长；也保证“读 pending、选不同 task、提交 ClaimBatch”不会被另一个
节点流交叉重复选择。ResultStream 同样使用跨所有节点流的全局 dispatcher，把完成状态
合并为 `OpCompleteBatch`，避免大量节点分别提交完成日志。

每个 Follower 的 `ClaimClient` 对 Assignment 和 Result 各维护 8 个独立 credit。业务
Worker 可以按 allocation 全量并发执行，但一个节点同时等待 Raft 确认的拉取和完成请求
分别最多 8 个；确认后立即释放 credit 给下一个空闲 Worker，因此这个窗口只限制控制面
排队，不限制 Processor 并发。N 个执行节点的 Leader 未决请求上界分别为 `8N`，并继续
按全局 128 条切分 Raft 日志，避免 4900 Worker 同时启动形成请求尖峰。

Raft FSM 仍保留最终防线：若状态已变化，未成功 claim 的任务不会返回给 Worker。响应
丢失时任务保持 inflight，30 秒 lease 到期后由 Leader 重新放回 pending。Assignment 和
Result 请求一旦写入共享流，就等待 Raft 确认或流/Leader 明确失效，不设置会把整个节点
共享流关闭的单请求固定超时；失去 quorum 时，credit 窗口会提供背压，流错误或 Leader
切换负责解除等待。服务端 Raft Apply 的未知提交结果仍由 30 秒 lease 兜底。

Leader 每个 dispatcher 批次只构建一次 pending FIFO 索引：`node+tenant`、`tenant`、
`node`、`aged global` 四个索引与四级选择优先级一一对应。每个索引游标只向前移动，
已选 task ID 惰性跳过；正常复杂度由原来的 `O(slots × pending)` 降为
`O(pending + slots)`，不改变 FIFO、租户偏好、本机偏好或 5 秒 steal 边界。

`ClaimStream` 保留为滚动升级兼容路径：新 Worker 连接不支持 AssignmentStream 的旧
Leader 时才退回旧 Claim 协议；连接支持新协议的 Leader 后，不允许因超时回退到自发
Claim，避免“Leader 已提交但响应未知”时产生重复执行。旧 Claim 协议仍保留 15 秒等待
上限，仅作为滚动兼容，不是当前 Leader 调度路径。


## API

```
外部 (gRPC, 全部 unary):
  Submit(SubmitRequest) → SubmitResponse
  SubmitBatch(SubmitBatchRequest) → SubmitBatchResponse
  GetTask(GetTaskRequest) → TaskStatus
  WaitTask(WaitTaskRequest) → TaskStatus
  UpsertTenant / DeleteTenant / ListTenants / ClusterStatus / Health

内部 (gRPC streaming, 节点间):
  AssignmentStream(bidi) — Follower 上报空闲槽位，Leader 批量选择并提交具体任务
  ClaimStream(bidi)      — 仅滚动升级期间兼容旧节点的批量认领
  ResultStream(bidi)    — 批量完成
  AllocationPush(srv-stream) — 推送分配方案

HTTP REST:
  /api/v1/tasks / /admin/tenants / /admin/nodes / /admin/allocations / /health

`GET /api/v1/admin/allocations` 返回当前每节点有效 worker 数和 `borrowed` 镜像；
历史趋势仍由 metrics 接口提供，不把借用控制器的每次试探额外写入历史存储。
```

## 节点角色

| 角色 | 职责 |
|------|------|
| Leader | Allocator、Assignment/Claim/Result 服务端、Raft 写入、API；业务 Worker=0 |
| Follower | Workers（通过 AssignmentStream 获取已提交任务）、API |

所有节点可接任务（OpCreateTask）、可查询（FSM 本地读）。
只有 Leader 执行 Raft Apply（claim/complete/allocation）。

### Raft 成员与执行实例边界

- `replicas` 表示复制 FSM、提供 API 并可运行 Worker 的进程数，不等于共识多数派大小。
- 单 shard 默认 `raftVoters=5`。按带数字后缀的节点 ID 稳定选择最低序号 voter；后续
  新成员以 non-voter 加入，仍接收完整日志并可作为 Follower 运行 Worker。
- 旧的全 voter 集群升级时，Leader 先确保目标 voter 已提升；若自身不在目标集合，先把
  leadership 转移到目标 voter，再由新 Leader 逐个 demote 多余 voter。迁移不删除成员、
  PVC、FSM 或任务。
- 5 voter 可容忍任意 2 个 voter 同时失效。3 个及以上 voter 同时不可用时失去 quorum，
  需要运维修复；本版本不自动扩大 voter 集合来掩盖该故障，也不实现跨 shard 成员调度。
  non-voter 丢失只降低执行/API 容量，不改变已提交任务。
- `GET /api/v1/admin/raft` 返回当前 Leader、voter 和 non-voter 的稳定排序快照，供部署
  验收确认 quorum 没有随 50 个执行实例扩大。

### 批量提交、转发与幂等边界

- `SubmitBatch` 最多接收 1000 条任务，只写一条 `OpCreateTaskBatch` Raft 日志。
- Follower 把完整请求转发给 Leader，转发窗口为 60 秒，覆盖大 voter 集群的正常提交
  延迟；客户端自己的更短 deadline 仍优先。
- 携带非空 `idempotency_key` 时，任务 ID 由 `tenant_id + idempotency_key` 稳定生成。
  请求结果未知后用相同键重试，会返回相同 task ID；FSM 已存在 unfinished task 或仍保留
  completed result 时不会重复插入任务；当前实现也不创建本地 Queue 副本。
- 幂等去重窗口与查询结果窗口一致：当前保留最近 10000 个完成结果。结果淘汰后再次使用
  同一键不保证去重；需要更长业务幂等周期时由上游保存 task ID/去重记录。空键维持每次
  创建新任务的语义。
- 新提交只写 Raft FSM 的 pending，不再复制到 Leader 本地 Bolt Queue。Leader 不执行
  业务任务，Follower 转发后所谓“本地”实际也是 Leader，既没有局部性，又会为 1000 条
  批次增加 1000 次本地事务，并在分配时重复扫描删除。本地 Queue 仅保留为旧版本滚动
  兼容数据；当前 Assignment 路径不写、不删，也不把它当作所有权或恢复事实源。

## 故障处理

- Follower 宕机: Leader 心跳超时 → OpNodeDown → inflight→pending
- Leader 宕机: 新 Leader 选举 → 立即停止本机 Worker → 发布排除自己的新 allocation
  → ClaimClient 重连。被取消的旧 inflight 由完成提交或 30 秒 lease 恢复，不能无 lease
  立即重派，否则不可取消的 Processor 可能与新执行者重复产生业务副作用。
- 任务不丢失: OpCreateTask 即写 Raft → 节点恢复后 FSM 仍有 pending 记录

## 调度正确性不变量与需求边界

必须同时满足：

1. **持久化先于执行**：API 返回 accepted 前，任务已通过 Raft 写入 pending。
2. **单一调度权威**：只有 Leader 选择 task→node；同一时刻一个任务最多属于一个节点；
   Worker 不从复制的 pending 快照自发 Claim，所有状态迁移由 Leader 批量提交 Raft。
3. **控制/执行隔离**：Leader allocation 必须为空，AssignmentStream 必须拒绝给本机
   分配业务任务；选主后先停止本机 Worker，再发布 follower-only allocation。
4. **容量有界**：所有 Follower 有效 worker 之和不得超过存活 Follower 总容量；借用只改变
   当前分配镜像，不改变租户配置的保底 Limit。
5. **租户隔离**：Assignment 请求必须来自拥有 preferred tenant Worker 的节点；跨 tenant
   任务只能消耗这个已存在的空闲槽位，不能凭空增加并发。
6. **可恢复**：节点宕机使其 inflight 回到 pending；进程整体重启后，遗留 inflight
   在 30 秒 Claim lease 到期后回到 pending，最终状态只提交一次。
7. **有界活性**：Leader 优先同 tenant/同节点队列，再使用本节点其他 tenant 的任务；
   跨节点 steal 只兜底等待超过 5 秒的任务，且选择过程不产生 Worker Claim 风暴。
8. **控制面背压**：单节点 Assignment/Result 未决请求各不超过 8；每条 Claim/Complete
   Raft 日志不超过 128 个任务。单请求等待不能关闭同节点其他健康请求共用的流。
9. **共识规模有界**：单 shard voter 数不超过配置的正奇数上限；执行副本扩容只能增加
   non-voter。已有超大 voter 集合必须先转移 leadership 再 demote，不能删除 FSM 副本。
10. **缩容不抢占执行**：allocation 缩容只禁止 retiring Worker 获取下一条任务；已经由
    Leader claim 的 Processor 允许完成并提交。它可在缩容窗口内暂时高于新 allocation，
    但不得追加新 claim；本版本不提供业务任务抢占或安全取消协议。

当前需求范围：系统负责 durable queue、Leader 单一调度、并发配额、空闲容量借用、
节点内优先和跨节点兜底的 work-steal，以及失败后的至少一次执行尝试/单次最终状态提交。系统当前不提供
业务 Processor 的事务性 exactly-once 副作用；Processor 在结果提交失败或 Claim lease
过期时可能被重试，因此业务处理器必须幂等。`QueueNodeID` 只用于 pending 阶段的调度
局部性，不是任务所有权，也不进入历史时序存储；当前 API 新提交不生成本地 Queue hint，
因此该字段为空，历史/滚动兼容记录仍按原四级策略处理。

### Multi-Raft 扩展边界

当前版本只有一个 Raft Group，因此全局只有一个 Assignment Leader。协议边界已经把
“执行槽位请求”和“具体任务选择”分开：未来可按 tenant/task shard 建多个 Raft Group，
每个 shard 仍只有自己的 Leader 负责选择与 ClaimBatch，共享 Worker 节点按 shard 建流。
Multi-Raft 用于横向扩展调度与日志提交吞吐，不改变单 shard 的单一调度权威，也不让
Worker 恢复为自发抢任务。当前版本不实现跨 shard 事务、公平性或迁移协议。

## 历史故障 Case

### SCHED-001：全节点重复扫描 fresh pending

- **现象**：50 个节点各自用大量 Worker 扫描同一份 Raft pending 集合，每个任务被
  多节点同时请求 Claim；FSM 拒绝重复 Claim，但线上每分钟产生数千条
  `failed to claim task: claim rejected`，大部分资源消耗在无效竞争，积压下降很慢。
- **根因**：把全局恢复扫描当成本地取队列；节点间没有 fresh pending 的扫描所有权。
- **第一阶段修复**：把 fresh pending 恢复扫描限制到 Leader，解决 fresh 数据竞争，
  但 aged steal 仍由所有 Follower 扫描，不能彻底解决竞争。
- **最终修复**：Worker 不再扫描全局 pending；统一通过 AssignmentStream 报告空闲槽位，
  由 Leader 选择不同任务并先提交 ClaimBatch。
- **回归覆盖**：见 `docs/TESTING.md` 的 SCHED-001、SCHED-002 和 RECOVERY-001。

### SCHED-002：aged work-steal 仍产生跨节点竞争

- **现象**：fresh 扫描收口后，50 节点线上仍有约 2926 次/分钟 rejected claim，积压仅
  约 3.7 task/s 下降；所有节点仍会扫描同一批超过 5 秒的 pending。
- **根因**：用客户端 hash 或本地 reservation 只能降低碰撞，无法建立唯一调度权威；
  membership/视图变化还会让不同节点计算出不同 owner。
- **修复**：Leader 对所有节点流串行完成“选择 + ClaimBatch 提交”，Follower 只执行返回
  的任务；同节点、同 tenant 和 aged fallback 都是 Leader 策略，不再是 Worker 抢占。
- **边界**：单 Raft Group 的 Leader 调度吞吐是当前上限；未来通过 Multi-Raft 分 shard，
  不是让 Worker 自发竞争。
- **回归覆盖**：`TestAssignmentStreamBatchesDistinctLeaderCommittedTasks`、
  `TestLeaderAssignmentDrainsAgedBacklogWithoutClaimCompetition`。

### SCHED-003：按节点流批处理导致健康任务等待 lease

- **现象**：50 个 Pod 都有空闲 Worker 时，每条 AssignmentStream 各自聚批并持有全局
  `claimMu` 完成一次 Raft Apply；后续节点排队超过 Worker 的 5 秒等待上限。客户端已经
  不再收到 Leader 已提交的 assignment，任务停在 inflight，积压以 30 秒 lease 为台阶
  缓慢下降；线上一次出现 44、49 条 claim 到期回收。
- **根因**：调度权虽然集中到 Leader，但日志批次仍按连接划分，Raft 往返次数与节点数
  线性增长；锁只消除了重复选择，没有合并共识成本。
- **当时修复**：所有节点的空闲槽位进入 Leader 全局 dispatcher，一次选择、一次
  `OpClaimBatch`；Worker 等待上限当时调整为 15 秒，但不能依靠放大超时替代全局聚批。
- **边界**：单 shard dispatcher 仍是一个 Leader 内存组件；Leader 切换后旧流取消，未
  确认 assignment 仍按 30 秒 lease 恢复。Multi-Raft 按 shard 各自拥有 dispatcher。
- **回归覆盖**：`TestAssignmentStreamBatchesDistinctLeaderCommittedTasks` 使用两个独立
  节点流并断言仅一条 Raft Apply；真实 7 节点 Case
  `TestGlobalLeaderBatchingDrainsWithoutLeaseRecovery` 要求在 lease 前完成且零流超时/回收。

### SCHED-004：4900 Worker 请求尖峰反复关闭共享流

- **现象**：50 Pod 集群中 Leader 排除后共有 49 个执行节点、4900 个 Worker。四个租户
  约 18818 个 unfinished 连续多个历史点完全不下降；30 分钟内出现 674 次 assignment
  timeout。Follower 每约 16 秒重连一次，Leader 每 30 秒按 1676～2048 条回收过期 claim。
- **根因**：每个 Worker 同时发送一个 Assignment，Leader 可把 2048 个请求放进一条
  Raft 日志；后续请求超过固定 15 秒。任意一个请求超时会调用全局 `invalidate`，同时关闭
  该节点共享的 Claim/Assignment/Result 三条流。Leader 已提交但响应接收者消失的任务只
  能等待 lease，重连后的 4900 个请求又形成下一轮尖峰。
- **修复**：每节点 Assignment/Result 各使用 8-credit 未决窗口；已发送请求不再用固定
  客户端 deadline 破坏共享流；Leader 将 Claim/Complete Raft 日志统一限制为 128 条。
- **不变量**：Leader 仍唯一选择并提交 task→node，credit 只限制等待共识的控制请求，
  不降低 allocation 或 Processor 并发；连接/Leader 失败仍取消整条流，未知提交仍走
  30 秒 lease。没有新增任务转移、取消执行或 Processor exactly-once 语义。
- **回归覆盖**：`TestClaimClientBoundsPerNodeRaftRequests` 在服务端故意不确认时验证每类
  只有 8 个请求且流不重建；`TestInternalServiceBoundsGlobalRaftBatchSize` 固定 128 上限；
  真实 8 节点 `TestNodeCreditsDrainProductionWorkerFanoutWithoutLeaseRecovery` 启动 4900 个
  执行 Worker、处理 4096 个任务，断言所有 Raft 批次不超过 128、lease 前排空、每任务
  只执行一次且没有超时重连或 lease 回收。

### PERF-001：2 万任务被共识规模、重复存储和重复扫描共同拖慢

- **现象证据**：远程单机 MicroK8s 的 50 Pod 全部是 voter，多数派为 26。WebUI 按
  4 tenant、500 条/批、4 并发提交 20000 条任务，accepted 用时 164.369 秒；开始后
  464.508 秒仍有 7795 unfinished。完成数以 128 条为台阶，每个台阶约等待 5～7 秒；
  allocation 已达到 4900，DemoProcessor 每条仅随机等待 50～200ms，业务 Worker 不是瓶颈。
- **根因一（共识）**：50 个 voter 让每条 Create/Claim/Complete 日志等待 26 份 Bolt
  落盘确认；在同一物理盘上，增加 Worker 实例同时扩大了最慢的 quorum 临界路径。
- **根因二（存储）**：`SubmitBatch` 已写一条 durable Raft pending 后，又在 Leader 本地
  Bolt Queue 对每条任务各写一次事务；分配后还按 task ID 逐条扫描、删除这份副本。
  Leader 不运行 Worker，Follower 请求转发后这份“本地”副本没有调度局部性价值。
- **根因三（选择）**：每个最多 128 slot 的 dispatcher 批次，对每个 slot 都重新扫描
  全部 pending；20000 积压时一次批次最多检查 256 万条记录。
- **修复**：voter 默认限制为 5，额外实例以 non-voter 加入并安全迁移已有 voter；当前
  提交只存 Raft pending；dispatcher 一次构建四类 FIFO 索引并复用游标。
- **正确性边界**：没有减少 Create/Claim/Complete 的 Raft 持久化，也没有让 Worker
  自发 claim。Leader 仍唯一分配并先提交 claim；non-voter 仍复制完整 FSM；30 秒 lease、
  tenant 隔离、容量上限和单次最终状态提交不变。单 shard 的两次状态共识仍是吞吐上限，
  本 Case 不实现 Multi-Raft，也不承诺 Processor 副作用 exactly-once。
- **验证**：本机真实 7 实例/3 voter、4 tenant、Follower HTTP 路径的 20000 条提交由旧
  实现首批 1000 条超过 15 秒，改善为全部 20000 条 1.338 秒提交、43.800 秒排空；每条
  只执行一次。单测和完整集成映射见 `docs/TESTING.md` 的 PERF-001。

### SCHED-005：借用回收取消正在执行的任务

- **现象**：PERF-001 首轮优化部署后，20000 条提交只需 3.290 秒，33.9 秒已降到 388
  unfinished，但 44.0 秒仍是 388，最终到 61.0 秒才归零。多个 Pod 在 allocation 更新
  周期同时记录 `task interrupted; waiting for lease recovery`，尾部正好等待 30 秒 lease。
- **根因**：`Pool.Reconcile` 缩小 tenant Worker 数时直接 cancel 每个 Worker 的 context；
  同一个 context 也传给正在运行的 Processor。借用试探/回收本是普通容量调整，却被当成
  节点关闭，已经 claim 的任务被中断且没有最终状态，只能等 lease 重派。
- **修复**：Worker 分离 `retire` 信号和 Pool 硬停止 context。缩容关闭 retire 信号；空闲
  Worker 立即退出，已获得 assignment 的 Worker 完成 Processor、提交 Complete 后退出，
  不再请求新任务。只有进程 Shutdown 会 cancel Processor context。
- **边界**：系统没有通用业务取消/抢占协议；已开始 Processor 不因 Limit/借用回收中断。
  缩容期间旧 inflight 可短暂超过新的 allocation，但 retiring Worker 不得产生新 claim。
  节点/进程真实丢失仍由 30 秒 lease 恢复，Processor 副作用仍要求幂等。
- **回归覆盖**：`TestPoolReconcileScaleDownRetiresAfterInflightCompletion` 确定性阻塞
  Processor 后缩容；真实 3 节点 `TestAllocationScaleDownLetsInflightProcessorsFinish` 走
  Leader assignment、Raft claim/complete、Pool Reconcile，断言零取消、零 lease 等待、
  每任务只处理一次。
- **远程复测**：50 个执行实例、5 voter/45 non-voter、4 tenant 按条目轮转提交同一组
  20000 条任务，批量写入 2.880 秒，端到端 29.688 秒（写入后排空 26.808 秒）；原有
  30 秒 lease 尾部消失。测试窗口内所有 Pod 的执行中断、lease recovery、提交失败与
  error 日志均为 0，最终四个租户 unfinished 均为 0。

### OBS-001：性能改动后缺少阶段化证据

- **风险**：PERF-001 消除了每个 slot 重扫 pending，但 50000 条复测证明每个 128 条
  dispatcher 批次仍复制排序全部剩余 pending；如果继续引用 20000 条结论，会把已经
  转移到 selection 的瓶颈误认为仍是 Worker 或 quorum。只看端到端时间也无法区分
  Apply 变慢、批次填充不足和 Leader dispatcher 排队。
- **规则**：任何影响提交、调度、共识、存储、执行或恢复的变化，都必须重跑固定形状
  基线并在 `docs/PERF.md` 追加环境、拓扑、负载、提交/排空/端到端结果和新限制；旧结果
  保留，环境不同不得直接计算提升百分比。
- **实现**：Leader 进程内记录每类 Raft Apply 的次数、任务数、错误、批次和延迟；记录
  pending selection 的扫描/选中数与耗时，以及两个全局 dispatcher 队列深度。Collector
  每秒把区间值采样到 174 点历史；`/api/v1/admin/performance` 同时返回累计当前快照和
  历史，Follower 在服务端代理当前 Leader。
- **数据边界**：累计值和队列深度是当前 Leader 进程镜像，174 点数据是有界历史；两者
  都不进入 Raft/FSM/Snapshot，不参与分配、借用或 lease 决策。Leader 切换后读取新
  Leader 的本地观测，不承诺跨 Leader 拼接无缝时间线；当前也不提供逐 non-voter
  replication lag。
- **读取边界**：端点默认返回完整当前快照与 174 点历史，`history=0` 可只返回 Leader
  当前快照。WebUI 必须从该 Leader 端点读取性能当前值和历史；普通 `/metrics` 是连接
  节点的本地历史，不能冒充 Leader 数据。页面用 `/metrics?performance=0` 读取 workload/
  allocation 历史，让 Collector 在复制 ring buffer 前排除 `performance:`，因此每秒只
  序列化和传输一份性能历史。参数都只改变只读响应形状，不改变采样、保存或任务协议。
- **正确性边界**：观测只包裹既有 ApplyFuture 和选择流程，不增加或重排任何 Raft log，
  不改变 Leader 唯一分配、Worker 执行、批次上限、超时和最终状态语义。指标失败不能
  阻断任务路径。
- **回归覆盖**：`pkg/metrics` 验证累计/区间窗口、批次解析和历史；`pkg/grpc` 验证真实
  pending 选择在 Claim Apply 前记录；`pkg/api` 验证只读端点。真实 3 节点
  `TestPerformanceDiagnosticsProxyFromFollower` 走 Follower HTTP、Leader Raft、Worker、
  Result 和 Follower→Leader 诊断代理，要求 Create/Claim/Complete 与 selection 可见且
  最终 unfinished=0；同一路径还验证 `history=0` 经 Follower 传到 Leader 后保留当前值、
  不复制历史，默认请求仍返回完整 JSON。

### UI-004：174 点曲线的点值读取

- **需求**：Worker 分配、租户未完成任务、Raft Apply 和调度器四张时序图都必须能用鼠标
  读取某个采样点。提示包含该点所属的时间桶、离鼠标纵坐标最近的系列和原始值；Worker
  系列同时显示实例 Limit，避免只看纵轴估算。
- **时间语义**：174 点保持 `30 days + 24 hours + 60 minutes + 60 seconds` 的现有顺序。
  日/时/分提示的是最近已经完成的采样桶，最右秒点标为 `Latest`。悬浮层只是已有历史
  快照的只读投影，不新增服务端字段、历史副本或网络请求，也不改变每秒刷新周期。
- **密集曲线边界**：50 实例场景不在提示框中枚举全部系列，而是根据鼠标纵坐标选择最近
  的实际点；竖向参考线保留当前时间位置。相同坐标的系列按图例的稳定顺序选择。当前只
  覆盖鼠标/Pointer 悬浮，不把键盘逐点浏览或缩放拖拽扩进本次范围。
- **回归覆盖**：`pkg/webui.TestDashboardChartsExposeNearestPointTooltip` 固定组件结构、四类
  单位和 Worker Limit；真实 3 节点 `TestPerformanceDiagnosticsProxyFromFollower` 从 Follower
  的生产 HTTP 页面验证脚本随 Leader 诊断一起交付；远程浏览器实际移动鼠标后断言 tooltip
  可见、包含时间/系列/数值且页面没有 console error。

### RESULT-001：每节点完成流放大 Raft 日志

- **风险**：Assignment 修复后，大量节点可能同时完成任务；若 ResultStream 各自提交
  `OpCompleteBatch`，同样会造成节点数级别的 Raft 往返和 completion timeout。
- **修复**：完成请求跨全部节点流全局聚批；只有已提交的 task ID 才向原流确认，流在
  提交前取消不得误回 ACK。
- **回归覆盖**：`TestResultStreamBatchesCompletionsAcrossNodeStreams`、
  `TestGlobalLeaderBatchingDrainsWithoutLeaseRecovery`。

### SUBMIT-003：Follower 超时返回但批次已经提交

- **现象**：1000 条批次经 Follower 转发时，硬编码 10 秒 deadline 返回 HTTP 408，但
  Leader 随后完成了 Raft commit；调用方无法判断是否应该重试。
- **修复**：Follower 转发窗口扩为 60 秒；非空幂等键生成稳定 task ID，重试不会重复
  pending/result；当前提交不创建本地 Queue 副本。
- **边界**：网络调用无法消除未知结果；60 秒不是 exactly-once 保证。幂等去重受最近
  10000 个完成结果窗口约束，业务 Processor 副作用仍必须幂等。
- **回归覆盖**：`TestSubmitBatchFollowerUsesConfiguredForwardTimeout`、
  `TestSubmitBatchIdempotencyKeysReuseTaskIDs`、`TestHTTPBatchSubmitThroughFollower`。

### LEADER-001：Leader 同时调度和执行

- **风险**：Leader 的 CPU/Worker 被业务处理占用会拖慢选举、Assignment 和 Raft commit；
  角色切换后残留 allocation 还会让新 Leader 继续拉取业务任务。
- **修复**：Allocator 排除当前 Leader；无 Follower 时提交空 allocation；节点成为 Leader
  时立即 Reconcile 空 Worker，并由 Worker guard 与 Assignment 服务端双重拒绝执行。
- **边界**：角色切换前已经开始的不可取消 Processor 允许完成；不会给新 Leader 分配新
  任务，未完成 claim 走 lease 恢复，避免立即重派造成副作用并发。
- **回归覆盖**：见 `docs/TESTING.md` 的 LEADER-001。

### ALLOC-001：多租户积压时闲置容量未被使用

- **现象**：集群有 5000 Worker，但租户 Limit 合计只有 690；多个租户同时积压时，
  旧策略仅允许“唯一活跃租户”借用，导致四千多个 Worker 长期空闲。
- **修复**：所有等待超过 5 秒且仍有 pending 的租户按稳定 tenant ID 顺序共享剩余
  容量；每租户独立试探并受 pending 数、公平份额和集群总容量约束。
- **边界**：借用不保证固定吞吐，不把控制器试探值存成历史；pending 消失立即释放，
  Leader 切换后从正常配额重新试探。
- **回归覆盖**：见 `docs/TESTING.md` 的 ALLOC-001。
