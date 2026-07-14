# Sluice — 分布式多租户限流系统

## 核心原则

**Leader 管分配，子节点管执行。**

- Allocator（Leader）决定全局 worker 分配方案
- Workers（所有节点）执行任务处理
- ClaimStream 是 claim 协调器：Leader 根据分配方案批准/拒绝认领
- 限流在 Leader 端强制：节点配额满则拒绝认领

## 任务生命周期

```
1. 提交: Client → 任意 Node → raft.Apply(OpCreateTaskBatch) → FSM (pending)
2. 发现: Worker → fsm.FindPendingTasks(tenantID) → 发现 pending 任务
3. 认领: Worker → ClaimStream → Leader
         Leader 检查 allocation[node][tenant] > 0 ?
         是 → 批准；否则仅当 steal=true 且该任务已等待超过 5s 才批准
4. 批量: Leader 聚批(5ms/128条) → raft.Apply(OpClaimBatch) → pending→inflight
5. 返回: Leader → ClaimStream → {claimed:[], failed:[]}
6. 执行: Worker 处理 claimed 任务
7. 完成: Worker → ResultStream → Leader → raft.Apply(OpCompleteBatch)
```

提交请求不携带处理耗时预估。任务进入 FSM 后按 `CreatedAt` FIFO 排队，由实际
处理结果和待处理时长驱动调度，避免客户端估时不准导致饥饿。

空闲 worker 先尝试本节点其他租户的本地队列，再在本地队列为空时向 leader 请求“偷取”
其他租户中等待超过 5 秒的 pending 任务。leader 会再次校验任务状态、租户和等待时长，
同节点队列任务可立即放行。成功后仍通过原有批量 Claim Raft 日志提交。work-steal 不
增加配额，只复用已经存在的空闲并发；ClaimBatch 和 ResultBatch 分别用一条 Raft 日志提交。

## 限流模型

- **维度**: 并发数（同时 inflight 的任务数）
- **全局**: Allocator 计算每租户每节点的有效 worker 配额
- **执行**: Leader 在 ClaimStream 中检查 `inflight[node][tenant] < alloc[node][tenant]`
- **空闲**: 连续 3 周期 0 inflight → idle → 降为 1 worker
- **超售**: sum(limits) > total_workers → Max-Min Fairness 按比例分配
- **借用**: `max_workers` 是正常保底配额；所有等待超过 5 秒的 tenant backlog 都可以
  共享集群剩余容量。借用目标按 tenant 独立试探为 `1, 3, 7, ...`（大集群首轮为 64），
  每轮受 pending 数、剩余容量和公平份额限制；backlog 消失后立即回收。

## 分配算法

```
Allocator (Leader, 每 3s):
  1. 读 FSM → 活跃节点 + 租户配置 + inflight 计数
  2. Max-Min Fairness → 每租户应得 worker 总数
  3. 均匀分布到各节点
  4. 空闲检测: inflight=0 连续 3 周期 → idle → 1 worker
  5. 空闲租户释放的 worker 二次分配给活跃租户
  6. 对所有 backlog 已等待 5s 的租户，按公平份额自适应试探增加借用 worker
  7. raft.Apply(OpUpdateAllocation)

### 借用额度与写入规则

- `FSMState.Allocations` 是当前时刻的镜像，不保存借用变化历史。
- `NodeAllocation.Tenants[tenant]` 是节点实际启动的有效 worker 数，包含借用。
- `NodeAllocation.Borrowed[tenant]` 是其中超过 `TenantConfig.MaxWorkers` 的当前借用数，
  仅用于 API/UI 展示；ClaimStream 只执行 `Tenants` 的有效额度检查。
- 每次调度只写一条 `OpUpdateAllocation`，借用和回收与普通分配一起原子替换，
  不追加单独的借用日志，也不把 Leader 内存中的试探目标写成历史数据。
- Leader 切换后试探目标清零，从租户正常配额重新开始；这保证旧 Leader 的借用
  不会在新任期无限保留。
- 节点按带数字后缀的 ID 进行稳定排序（`node-2` 在 `node-10` 之前），避免相同
  分配在节点间来回抖动。
```

## ClaimStream — Claim 协调器

```
Worker (任意节点)                    Leader
     │                                  │
     │──ClaimStream────────────────────►│  bidi gRPC
     │──ClaimReq(t1, tenant, node)─────►│
     │──ClaimReq(t2, tenant, node)─────►│  聚批 5ms
     │──ClaimReq(t3, tenant, node)─────►│
     │                                  │  检查: alloc[node][tenant] > 0?
     │                                  │  inflight[node][tenant] < alloc[node][tenant]?
     │                                  │  raft.Apply(OpClaimBatch)
     │◄──ClaimBatch([t1,t2], [t3])─────│  t3 被拒: 配额满或未分配
```

## API

```
外部 (gRPC, 全部 unary):
  Submit(SubmitRequest) → SubmitResponse
  SubmitBatch(SubmitBatchRequest) → SubmitBatchResponse
  GetTask(GetTaskRequest) → TaskStatus
  WaitTask(WaitTaskRequest) → TaskStatus
  UpsertTenant / DeleteTenant / ListTenants / ClusterStatus / Health

内部 (gRPC streaming, 节点间):
  ClaimStream(bidi)     — 批量认领
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
| Leader | Allocator、ClaimStream/ResultStream 服务端、Workers、API |
| Follower | Workers（通过 ClaimStream 认领）、API |

所有节点可接任务（OpCreateTask）、可查询（FSM 本地读）。
只有 Leader 执行 Raft Apply（claim/complete/allocation）。

## 故障处理

- Follower 宕机: Leader 心跳超时 → OpNodeDown → inflight→pending
- Leader 宕机: 新 Leader 选举 → Allocator 重启 → ClaimClient 重连
- 任务不丢失: OpCreateTask 即写 Raft → 节点恢复后 FSM 仍有 pending 记录

## 调度正确性不变量与需求边界

必须同时满足：

1. **持久化先于执行**：API 返回 accepted 前，任务已通过 Raft 写入 pending。
2. **单一 Claim 所有权**：同一时刻一个任务最多属于一个节点；所有 Claim/Complete
   状态迁移由 Leader 批量提交 Raft，Worker 的本地队列不是事实来源。
3. **容量有界**：所有节点有效 worker 之和不得超过存活节点总容量；借用只改变
   当前分配镜像，不改变租户配置的保底 Limit。
4. **租户隔离**：普通 Claim 必须有该节点/租户额度；work-steal 必须显式标记并由
   Leader 校验任务状态、tenant、队列来源或等待时间。
5. **可恢复**：节点宕机使其 inflight 回到 pending；进程整体重启后，遗留 inflight
   在 30 秒 Claim lease 到期后回到 pending，最终状态只提交一次。
6. **有界活性**：本地队列立即消费；Leader 扫描没有本地队列的 pending；跨节点
   steal 只兜底等待超过 5 秒的任务，避免用全局抢占制造 Claim 风暴。

当前需求范围：系统负责 durable queue、并发配额、空闲容量借用、节点内优先和跨节点
兜底的 work-steal，以及失败后的至少一次执行尝试/单次最终状态提交。系统当前不提供
业务 Processor 的事务性 exactly-once 副作用；Processor 在结果提交失败或 Claim lease
过期时可能被重试，因此业务处理器必须幂等。`QueueNodeID` 只用于 pending 阶段的调度
局部性，不是任务所有权，也不进入历史时序存储。

## 历史故障 Case

### SCHED-001：全节点重复扫描 fresh pending

- **现象**：50 个节点各自用大量 Worker 扫描同一份 Raft pending 集合，每个任务被
  多节点同时请求 Claim；FSM 拒绝重复 Claim，但线上每分钟产生数千条
  `failed to claim task: claim rejected`，大部分资源消耗在无效竞争，积压下降很慢。
- **根因**：把全局恢复扫描当成本地取队列；节点间没有 fresh pending 的扫描所有权。
- **修复**：普通全局恢复扫描仅由 Leader 执行；Worker 顺序固定为本租户本地队列、
  本节点其他租户队列、Leader 恢复扫描、超过 5 秒的跨节点 steal。同一 Pool 继续用
  task reservation 防止本节点多个 Worker 重复 Claim。
- **边界**：Follower 不扫描 fresh 全局 pending，但仍处理自己的本地队列；旧数据没有
  `QueueNodeID` 时由 Leader 恢复，超过 5 秒后也允许其他节点兜底偷取。
- **回归覆盖**：见 `docs/TESTING.md` 的 SCHED-001、STEAL-001 和 RECOVERY-001。

### ALLOC-001：多租户积压时闲置容量未被使用

- **现象**：集群有 5000 Worker，但租户 Limit 合计只有 690；多个租户同时积压时，
  旧策略仅允许“唯一活跃租户”借用，导致四千多个 Worker 长期空闲。
- **修复**：所有等待超过 5 秒且仍有 pending 的租户按稳定 tenant ID 顺序共享剩余
  容量；每租户独立试探并受 pending 数、公平份额和集群总容量约束。
- **边界**：借用不保证固定吞吐，不把控制器试探值存成历史；pending 消失立即释放，
  Leader 切换后从正常配额重新试探。
- **回归覆盖**：见 `docs/TESTING.md` 的 ALLOC-001。
