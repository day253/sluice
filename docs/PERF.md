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

正常情况下主要瓶颈是 Raft 落盘/复制；但大 backlog 若反复扫描 pending，或把 Raft
pending 再逐条写入本地 Bolt Queue，调度/重复存储也会成为可观测瓶颈。当前实现已用
单批索引消除 `O(slots×pending)` 并删除本地 Queue 副本，但每个最多 128 条的 dispatcher
批次仍重新复制、排序全部 pending；跨批次仍有 `O(batches×pending)` 放大。

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

远程 50 实例改为 5 voter/45 non-voter，并修复 allocation 缩容取消正在执行任务后，
相同 4 tenant/20000 条按 tenant 轮转的批量测试提交为 2.880 秒，端到端为 29.688 秒；
测试窗口没有任务中断、lease recovery、提交失败或 error，最终 unfinished 为 0。

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
| pending 跨批扫描 | FSM 维护可重建的派生 FIFO 索引，每批只取候选 | `O(batches×pending)` → 近似 `O(tasks)` |
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

---

## 6. 性能基线历史

性能相关代码每次变化后都追加同形状结果，不覆盖旧数据。提交时间从第一条 HTTP 请求
开始到全部 accepted；排空时间从提交结束到连续三次 unfinished=0；端到端包含两者。

### 2026-07-19：50 副本固定任务量阶梯

- 环境：单台 16 核、60 GiB、NVMe 的 MicroK8s，单一物理故障域。
- 拓扑：50 Pod，5 voter/45 non-voter；每 Pod 100 Worker、CPU request/limit
  100m/500m、内存 request/limit 128/512 MiB；Leader 不执行任务。
- 负载：4 tenant 按任务轮转；DemoProcessor 随机 50～200ms；小 JSON payload；
  HTTP 500 条/批、4 个并发请求；每轮开始前 unfinished=0，结束要求连续三次为 0。

| tasks | accepted | 端到端 | 写入后排空 | task/s | t50 | t90 |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.134s | 6.656s | 6.522s | 150.2 | 3.648s | 5.654s |
| 5,000 | 0.885s | 9.487s | 8.602s | 527.1 | 5.975s | 7.982s |
| 20,000 | 3.971s | 28.746s | 24.775s | 695.7 | 16.480s | 25.672s |
| 50,000 | 13.419s | 105.935s | 92.517s | 472.0 | 69.746s | 99.330s |

结论变化：20,000 以内主要表现为批次填充/共识上限；50,000 只增加 2.5 倍，端到端
却增加 3.7 倍。Leader 每个 ClaimBatch 都调用 `FindAllPendingTasks` 并对剩余 backlog
复制排序，成为新的可见限制。以 50,000/128 估算约 391 个 Claim 批次，平均 25,000
pending 时接近 978 万条记录被重复检查；下一轮优化不能继续沿用“pending 扫描已经
完全解决”的旧结论。

同环境把请求改为 1,000 条/批、4 并发后，20,000 条 accepted 为 2.551s、端到端
30.642s、652.7 task/s。相对本轮 500 条请求，写入日志数减半使 accepted 更快，但消费
时间没有改善；因此调大提交批次只优化 Create，不是 Claim/Complete/选择瓶颈的修复。

计划中的 3/5/10/20 节点隔离集群对比未产生数据：MicroK8s 重启后 Calico 为新 Pod
创建网络时返回 `Unauthorized`，临时 Pod/PVC 未进入运行，临时 namespace 已删除；现有
50 Pod 保持 50/50 Ready。该节点规模实验属于基础设施阻塞，不得用当前任务量曲线代替。

---

## 7. 控制面性能观测

`GET /api/v1/admin/performance` 返回当前 Leader 的进程内累计快照和 174 点历史；请求打到
Follower 时由服务端代理 Leader，外部调用者不需要访问集群内部 Pod IP。指标是只读的
进程状态：不写 Raft、不进入 FSM Snapshot，也不参与调度。

当前包含：

- `create_task_batch`、`claim_batch`、`complete_batch` 等操作的 Apply 次数、任务数、
  error、平均/最大/最近延迟和平均批次大小；
- `performance:raft:<op>:{apply-rate,item-rate,batch-size,apply-us,apply-max-us,errors}`
  的 174 点历史；
- pending selection 次数、累计扫描/选中数、平均/最大/最近耗时；
- assignment/completion Leader 全局 dispatcher 队列深度及其历史。

这些数据可以直接区分：Raft commit 变慢、批次填不满、pending 选择放大和 dispatcher
排队。当前非目标是每个 non-voter 的 match index/replication lag；Hashicorp Raft 没有
通过现有接口暴露完整逐副本进度，后续需要独立的 Raft observer/metrics sink。
