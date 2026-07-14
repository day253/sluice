# Cluster Performance Model

评估大规模 Raft 集群的性能瓶颈，按节点数 N 分析每条关键路径。

---

## 1. 瓶颈模型

```
每个 task 的关键路径（2 次 Raft commit）：

  Queue.Pop        14 ns      ░░░░░░░░░░░░░░░░
  FSM.ClaimTask    5.7 µs     ░░░░░░░░░░░░░░░░
  ★ Raft log sync  ~1 ms      ████████████████  取决于磁盘
  ★ Raft replicate  RTT×2     ████████░░░░░░░░  取决于 N 和网络
  Business.Process  ? ms      ████████████████  取决于用户
  FSM.Complete     5.7 µs     ░░░░░░░░░░░░░░░░
  ★ Raft log sync  ~1 ms      ████████████████
  ─────────────────────────────────────────────
```

**结论：调度层不是瓶颈。瓶颈始终在 Raft 落盘和网络复制。**

---

## 2. 各维度缩放分析

### 2.1 节点数 N

| N | 多数派 | Leader 出带宽 | 选举超时 | 心跳负载 |
|---|--------|-------------|---------|---------|
| 1 | 1 | 0 | 即时 | 0 |
| 3 | 2 | 2× 副本 | ~1.5s | 2 条/100ms |
| 5 | 3 | 4× 副本 | ~1.5s | 4 条/100ms |
| 7 | 4 | 6× 副本 | ~1.5s | 6 条/100ms |
| 11 | 6 | 10× 副本 | ~2s | 10 条/100ms |

Raft 写延迟 = max(leader disk sync, majority network RTT)。

Leader 出带宽 = (N-1) × entry_size × commit_rate。  
典型值：entry_size ≈ 1 KB，commit_rate = 1000/s → 1 MB/s × (N-1)。

**实际限制**：
- N=1：吞吐受限于磁盘（~1000 task/s × 2 次 commit = 2000 writes/s）
- N=3：多数派需要 1 个 follower 确认，延迟 ≈ max(1ms disk, 0.5ms RTT) = 1ms
- N=11：多数派需要 5 个 follower，gRPC 并发复制，延迟仍 ≈ 1ms（pipeline）
- **N > 11 不推荐**：选举时间波动大，心跳负载线性增长，收益递减

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

mock raft 下约 800K-960K task/s 是单节点 CPU 上限。  
**生产环境受 Raft 限制远低于此**（~1000 commit/s = ~500 task/s）。

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

```
task/s = 1 / (2 × RaftCommitLatency + ProcessTime)

其中 RaftCommitLatency = max(DiskSync, RTT × ceil((N+1)/2 - 1))

典型值（SSD，1 Gbps 网络，N=3）：
  RaftCommitLatency = max(1ms, 0.5ms) = 1ms
  ProcessTime = 1ms (假设)
  task/s = 1 / (2×1ms + 1ms) = 333 task/s

典型值（SSD，1 Gbps 网络，N=5）：
  RaftCommitLatency = max(1ms, 1ms) = 1ms
  task/s = 1 / (2×1ms + 1ms) = 333 task/s  （多数派从 2→3 不增加延迟）

典型值（SSD，1 Gbps 网络，N=11）：
  RaftCommitLatency = max(1ms, 5×0.5ms) = 2.5ms
  task/s = 1 / (2×2.5ms + 1ms) = 167 task/s  （多数派从 5→6 开始明显增加）
```

**生产环境预估**：
- N=1：~250 task/s（无复制，纯磁盘限制）
- N=3-7：~333 task/s（多数派稳定在 2-3 个确认，RTT 不增加）
- N=11+：~167 task/s（多数派需 6 个确认，RTT 开始主导）

---

## 4. 极限值

| 维度 | 极限 | 限制因素 |
|------|------|---------|
| 节点数 | ~11 | 选举收敛、心跳负载 |
| 租户数 | ~100K | Allocator 内存分配 |
| task/s (单节点) | ~500 | Raft 磁盘 I/O |
| task/s (N=3 集群) | ~333 | 任务需 2 次 Raft commit |
| inflight 任务 | ~10K | FSM map 操作 + 故障恢复遍历 |
| 队列深度 | 无硬限制 | BoltDB 页分裂，百万级也 OK |

---

## 5. 优化方向

| 瓶颈 | 方案 | 预期提升 |
|------|------|---------|
| Raft 磁盘 sync | 批量提交 (batch Apply) | 2-5× |
| 网络 RTT | gRPC streaming + pipelining | 1.5× |
| FSM copy | 版本号 + diff 更新 | 减少 90% 分配 |
| 空闲租户 worker | 已实现 idle detection | 已生效 |
| leader 单点写 | multi-raft (每租户一个 raft group) | N× 写入能力 |
| FSM snapshot | 增量快照 | 减少 I/O 抖动 |

### 5.1 任务批量调度

提交方可提供 `estimated_duration_ms`。同一节点的 ClaimStream 会把任务按
短作业优先排序，再以最多 128 条为一批提交 `OpClaimBatch`；节点上的多个
worker 并发执行，ResultStream 以同样的批量窗口提交 `OpCompleteBatch`。
未提供估时的任务在未知任务之间保持 FIFO，避免旧客户端行为改变。
