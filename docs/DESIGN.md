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
         是 → 批准  否 → 拒绝
4. 批量: Leader 聚批(5ms/128条) → raft.Apply(OpClaimBatch) → pending→inflight
5. 返回: Leader → ClaimStream → {claimed:[], failed:[]}
6. 执行: Worker 处理 claimed 任务
7. 完成: Worker → ResultStream → Leader → raft.Apply(OpCompleteBatch)
```

## 限流模型

- **维度**: 并发数（同时 inflight 的任务数）
- **全局**: Allocator 计算每租户每节点的 worker 配额
- **执行**: Leader 在 ClaimStream 中检查 `inflight[node][tenant] < alloc[node][tenant]`
- **空闲**: 连续 3 周期 0 inflight → idle → 降为 1 worker
- **超售**: sum(limits) > total_workers → Max-Min Fairness 按比例分配

## 分配算法

```
Allocator (Leader, 每 3s):
  1. 读 FSM → 活跃节点 + 租户配置 + inflight 计数
  2. Max-Min Fairness → 每租户应得 worker 总数
  3. 均匀分布到各节点
  4. 空闲检测: inflight=0 连续 3 周期 → idle → 1 worker
  5. 空闲租户释放的 worker 二次分配给活跃租户
  6. raft.Apply(OpUpdateAllocation)
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
  /api/v1/tasks / /admin/tenants / /admin/nodes / /health
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
