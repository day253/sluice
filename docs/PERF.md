# Cluster Performance Model

评估单 Raft shard 的性能瓶颈。区分执行/复制实例数 `R`、共识 voter 数 `V`、
每条日志任务数 `B`；扩容 Worker 时不能默认同时扩大 `V`。

---

## 1. 瓶颈模型

```
一个已提交任务批次的关键路径（Create 后，Claim/Complete 各一次 Raft commit）：

  pending index    O(pending) ░░░░░░░░░░░░░░░░  每个 dispatcher 批次一次
  FSM.ClaimBatch   O(B)       ░░░░░░░░░░░░░░░░
  ★ Raft log sync  ~1 ms      ████████████████  取决于磁盘
  ★ Raft replicate  RTT×2     ████████░░░░░░░░  取决于 V/R 和网络
  Business.Process  ? ms      ████████████████  取决于用户
  FSM.CompleteBatch O(B)      ░░░░░░░░░░░░░░░░
  ★ Raft log sync  ~1 ms      ████████████████
  ─────────────────────────────────────────────
```

正常情况下主要瓶颈是 Raft 落盘/复制；但大 backlog 若对每个 slot 重扫 pending，或把
Raft pending 再逐条写入本地 Bolt Queue，调度/重复存储也会成为可观测瓶颈。当前实现
分别用每批索引和单一 Raft pending 消除这两项放大。

---

## 2. 各维度缩放分析

### 2.1 voter 数 V 与执行实例数 R

| V | 多数派 | Leader voter 出带宽 | 选举超时 | voter 心跳负载 |
|---|--------|-------------|---------|---------|
| 1 | 1 | 0 | 即时 | 0 |
| 3 | 2 | 2× 副本 | ~1.5s | 2 条/100ms |
| 5 | 3 | 4× 副本 | ~1.5s | 4 条/100ms |
| 7 | 4 | 6× 副本 | ~1.5s | 6 条/100ms |
| 11 | 6 | 10× 副本 | ~2s | 10 条/100ms |

Raft commit 延迟取决于 voter 多数派中的落盘/网络尾延迟，而不是可用 Worker 数。

Leader 总复制带宽仍约为 `(R-1) × entry_size × commit_rate`，non-voter 也接收日志；
但 commit 不等待 non-voter，因此执行实例扩容不会把它们放进 quorum 临界路径。

**实际限制**：
- V=3：可容忍 1 个 voter 失效；V=5：可容忍 2 个 voter 失效，是默认值。
- V=11：每次提交需要 6 份持久确认，通常已经没有可用性收益。
- **V > 11 不推荐**；`R` 可以更大，但单 shard 的复制出带宽和所有副本的落盘总量仍
  会线性增长，继续扩展应使用独立 Worker 数据面或 Multi-Raft。

### 2.2 租户数 T

| T | Allocator 延迟 | FSM 内存 |
|---|---------------|---------|
| 100 | 6.6 µs | ~6 KB |
| 1,000 | 30.5 µs | ~55 KB |
| 10,000 | ~300 µs | ~550 KB |
| 100,000 | ~3 ms | ~5.5 MB |

Allocator 每 3s 运行一次。即使 10 万租户，3ms 仅占周期的 0.1%。

### 2.3 Worker 数 W

```
单节点 worker pool 吞吐：

  W=1     467K task/s    ████░░░░░░░░░░░░
  W=10    845K task/s    ████████░░░░░░░░
  W=100   838K task/s    ████████░░░░░░░░  ← mock raft mutex 上限
  W=GOMAX 819K task/s    ████████░░░░░░░░
```

mock raft 下约 800K-960K task/s 是单节点 Processor/Pool CPU 上限，不能代表生产吞吐；
生产吞吐还受实际 Raft commit 延迟和每批填充率限制。

### 2.4 并发任务数 (inflight)

FSM `Tasks` map 存储所有 processing 状态的任务：

```
inflight 任务数      map 操作延迟    内存
100                  150 ns         ~20 KB
1,000                150 ns         ~200 KB
10,000               200 ns         ~2 MB
100,000              500 ns         ~20 MB
```

`OpNodeDown` 需遍历所有 inflight 任务：O(inflight)。

---

## 3. 吞吐预测公式

```text
consensus_capacity ≈ B / (claim_commit_latency + complete_commit_latency)
processor_capacity ≈ active_workers / average_process_time
task/s ≈ min(consensus_capacity, processor_capacity)

B ≤ 128，并且还受所有执行节点未决 credit 总数限制。
commit_latency = voter 多数派持久化 entry 的尾延迟。
```

Process 与下一批控制面提交可并发，不能把每条任务错误建模为串行执行两次 commit。
相反，批次填充率决定了共识成本如何摊薄：例如 128 条 Claim 和 128 条 Complete 各用
一次 20ms commit，控制面理论上约为 `128 / 40ms = 3200 task/s`；如果只有 8 个 slot
进入批次，同样延迟下只有约 200 task/s。

PERF-001 的故障环境是 50 voter/多数派 26 且共用一台物理机磁盘，单个 128 条完成台阶
实测约 5～7 秒。把执行实例与 voter 解耦后，真实 7 实例/3 voter 集成 Case 的 20000 条
Follower HTTP 提交为 1.338 秒、消费为 43.800 秒。环境、磁盘与 Processor 不同，数值
不能直接当生产 SLA，但 Case 会阻止复杂度、重复存储和 quorum 规模回退。

---

## 4. 极限值

| 维度 | 极限 | 限制因素 |
|------|------|---------|
| voter 数 | 默认 5，建议≤11 | 选举收敛、quorum 落盘尾延迟 |
| 执行/复制实例数 | 当前验证 50 | Leader 复制带宽、全副本总落盘量 |
| 租户数 | ~100K | Allocator 内存分配 |
| 单 shard task/s | 无固定常数 | batch 填充率、两次 Raft commit、磁盘/网络 |
| inflight 任务 | ~10K | FSM map 操作 + 故障恢复遍历 |
| 队列深度 | 无硬限制 | BoltDB 页分裂，百万级也 OK |

---

## 5. 优化方向

| 瓶颈 | 方案 | 预期提升 |
|------|------|---------|
| voter 多数派过大 | `raftVoters=5`，额外实例用 non-voter | commit 不再等待所有执行实例 |
| Raft 磁盘 sync | Create/Claim/Complete batch Apply | 按实际批次摊薄至多 128 条 |
| 网络 RTT | gRPC streaming + pipelining | 1.5× |
| pending 重复扫描 | 每 dispatcher 批次构建四类 FIFO 索引 | `O(slots×pending)` → `O(pending+slots)` |
| 重复本地 Queue | 只存 Raft pending | 去掉每任务本地写和扫描删除 |
| 空闲租户 worker | 已实现 idle detection | 已生效 |
| leader 单点写 | multi-raft (每租户一个 raft group) | N× 写入能力 |
| FSM snapshot | 增量快照 | 减少 I/O 抖动 |

### 5.1 任务批量调度与 work-steal

Assignment dispatcher 保持 worker 到达顺序并以最多 128 条为一批提交 `OpClaimBatch`；
节点上的多个 worker 并发执行，Result dispatcher 以同样的全局窗口提交
`OpCompleteBatch`。空闲 worker
优先偷取本机其他租户队列，跨节点 work-steal 只放行等待超过 5 秒的 pending 任务；
排序依据是实际 `CreatedAt`，不依赖客户端估时。

选择器在一次 dispatcher 批次内复用 `node+tenant`、`tenant`、`node`、`aged` 四个 FIFO
索引。新 API 提交不写 Leader 本地 Bolt Queue；`QueueNodeID` 只兼容历史记录，当前任务
直接从 Raft pending 进入 Leader assignment。

### 5.2 空闲容量借用

`max_workers` 是租户的正常保底配额，不是集群空闲时的硬上限。Allocator
先完成 Max-Min Fairness 和 idle redistribution，再查看 FSM 的 pending 数：

```text
每个 backlog 等待超过 5 秒的 tenant:
  borrowed target = 1 → 3 → 7 → ...（大集群首轮 64）≤ spare / pending

某个 tenant 的 pending backlog 消失:
  该 tenant borrowed target = 0（当前 reconciliation 周期内立即回收）
```

这样单租户低配额、其他租户空闲时可以逐步吃满剩余并发；新租户有任务时，
借用 worker 不会继续挤占其保底配额。`NodeAllocation.Tenants` 和
`NodeAllocation.Borrowed` 只保存当前镜像，借用的变化不产生额外历史写入，
因此每轮仍只有一条 `OpUpdateAllocation` Raft 日志。Leader 切换会丢弃试探
控制器的内存目标并从保底配额重新探测，换取更安全的故障恢复边界。
