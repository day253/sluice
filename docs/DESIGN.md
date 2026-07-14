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
