# Cluster Performance Model

评估单 Raft shard 的性能瓶颈。区分 control/Raft 成员数 `C`、stateless Worker 实例数
`R`、共识 voter 数 `V`、每条日志任务数 `B`；扩容 Worker 不改变 `C/V`。

---

## 1. 瓶颈模型

```
一个已提交任务批次的关键路径（Create 后，Claim/Complete 各一次 Raft commit）：

  pending lookup   O(B log P) ░░░░░░░░░░░░░░░░  可重建增量派生索引
  FSM.ClaimBatch   O(B)       ░░░░░░░░░░░░░░░░
  ★ Raft log sync  ~1 ms      ████████████████  取决于磁盘
  ★ Raft replicate  RTT×2     ████████░░░░░░░░  取决于 V/R 和网络
  Business.Process  ? ms      ████████████████  取决于用户
  FSM.CompleteBatch O(B)      ░░░░░░░░░░░░░░░░
  ★ Raft log sync  ~1 ms      ████████████████
  ─────────────────────────────────────────────
```

正常情况下主要瓶颈是 Raft 落盘/复制与批次填充率。历史实现曾反复扫描 pending、把
Raft pending 再逐条写本地 Bolt Queue；当前已删除 Queue 副本，并用随已提交 FSM 状态
增量更新、Restore 时重建的四类有序索引消除跨 ClaimBatch 的全量复制/排序。选择读路径
不修改索引，实际状态和索引只在 Raft Apply 内同步迁移。

---

## 2. 各维度缩放分析

### 2.1 control 成员 C、voter 数 V 与执行实例数 R

| V | 多数派 | Leader voter 出带宽 | 选举超时 | voter 心跳负载 |
|---|--------|-------------|---------|---------|
| 1 | 1 | 0 | 即时 | 0 |
| 3 | 2 | 2× 副本 | ~1.5s | 2 条/100ms |
| 5 | 3 | 4× 副本 | ~1.5s | 4 条/100ms |
| 7 | 4 | 6× 副本 | ~1.5s | 6 条/100ms |
| 11 | 6 | 10× 副本 | ~2s | 10 条/100ms |

Raft commit 延迟取决于 voter 多数派中的落盘/网络尾延迟，而不是可用 Worker 数。

角色拆分后 Leader 总复制带宽约为 `(C-1) × entry_size × commit_rate`。Worker 不接收
Raft 日志，只维持 allocation/assignment/result 流，因此 `R` 扩大不会增加日志副本或
quorum 临界路径；它仍会增加 controller 流、调度请求和结果网络负载。

**实际限制**：
- V=3：可容忍 1 个 voter 失效；V=5：可容忍 2 个 voter 失效，是默认值。
- V=11：每次提交需要 6 份持久确认，通常已经没有可用性收益。
- **V > 11 不推荐**；当前生产 `C=V=5`。`R` 可以独立扩大，但单 shard 的
  Claim/Complete commit 率和 Controller CPU 仍有上限，继续扩展调度写吞吐需 Multi-Raft。

#### 2.1.1 controller / follower / Worker 选型依据

`follower` 不是独立的容量池：一个 Raft shard 在稳定状态始终是 `1 Leader + (C-1)
Follower`。增加 Follower 只增加故障容忍和读/转发入口，不增加写吞吐；每条写仍由唯一
Leader 排序并等待多数派。因此 control 数首先由故障模型选择，而不是由 task/s 选择：

| 场景 | control 构成 | 容忍 | 选择依据 |
|---|---|---:|---|
| 本地开发/一次性实验 | 1 Leader | 0 | 不作为生产或性能结论 |
| 一般生产 shard | 1 Leader + 2 Follower | 1 control/故障域 | 最小可靠多数派，写放大较低 |
| 关键生产 shard | 1 Leader + 4 Follower | 2 control/故障域 | 需要滚动维护同时再容忍 1 个故障 |
| 7 个及以上 | 不作为提吞吐手段 | `floor((C-1)/2)` | 只在明确更高故障域要求下使用；通常应增加 shard |

这里的“容忍”要求副本位于独立磁盘/主机/可用区。当前远程 MicroK8s 的 5 control 都在
同一台物理机上，只能验证 Pod/进程/Raft 行为，不能宣称容忍整机故障。

Worker 数由业务处理时间决定。先用处理耗时的 p95 和 30% 余量计算需要的有效槽位，
再按每实例的配置容量换算，并额外保留一个实例故障余量：

```text
required_slots = ceil(target_task_s × processor_p95_seconds × 1.30)
worker_instances = ceil(required_slots / slots_per_instance) + 1
```

使用当前 Demo Processor p95≈200ms、每实例 100 槽的示例（不是其他业务的 SLA）：

| 目标处理量 | 有效槽位（含 30%） | Worker 实例（含 N+1） | control/shard 判断 |
|---:|---:|---:|---|
| 200 task/s | 52 | 2 | 1 仅开发；生产仍用 3 control |
| 500 task/s | 130 | 3 | 3 control 单 shard |
| 1,000 task/s | 260 | 4 | 当前单 shard 约 1,069 task/s 安全水位，余量很小 |
| 1,500 task/s | 390 | 5 | 至少 2 shard；不能把单轮峰值当持续 SLA |
| 3,000 task/s | 780 | 9 | 至少 3 shard |
| 5,000 task/s | 1,300 | 14 | 至少 5 shard，明确租户到 shard 归属 |
| 10,000 task/s | 2,600 | 27 | 至少 10 shard；不能靠增加 Follower 或单 shard Worker 达成 |

租户 `max_workers` 总和也必须覆盖 SLA 所需槽位；空闲借用是提高利用率的 best-effort，
不能替代租户保底容量。最终 shard 数按同硬件最新固定形状基线计算：

```text
safe_shard_task_s = measured_sustained_task_s × 0.70
shards = ceil(target_task_s / safe_shard_task_s)
```

如果 `processor_capacity` 已高于 `safe_shard_task_s`，再增加 Worker 只会增加流和结果压力；
应先看 Claim/Complete 平均批次、Apply 延迟和 dispatcher queue，再决定优化单 shard 或分片。

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

角色拆分前，远程 50 实例改为 5 voter/45 non-voter，并修复 allocation 缩容取消正在执行任务后，
相同 4 tenant/20000 条按 tenant 轮转的批量测试提交为 2.880 秒，端到端为 29.688 秒；
测试窗口没有任务中断、lease recovery、提交失败或 error，最终 unfinished 为 0。

---

## 4. 极限值

| 维度 | 极限 | 限制因素 |
|------|------|---------|
| voter 数 | 默认 5，建议≤11 | 选举收敛、quorum 落盘尾延迟 |
| control/Raft 成员数 | 默认 5 | quorum、复制落盘与故障容忍 |
| stateless Worker 实例数 | 当前目标验证 50 | Controller 流/CPU、Processor 与网络 |
| 租户数 | ~100K | Allocator 内存分配 |
| 单 shard task/s | 无固定常数 | batch 填充率、两次 Raft commit、磁盘/网络 |
| inflight 任务 | ~10K | FSM map 操作 + 故障恢复遍历 |
| 队列深度 | 无硬限制 | BoltDB 页分裂，百万级也 OK |

---

## 5. 优化方向

| 瓶颈 | 方案 | 预期提升 |
|------|------|---------|
| 执行规模扩大 Raft | 5 control + 独立 stateless Worker | 日志副本恒定为 control 数 |
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

### 2026-07-19：OBS-001 上线后基线

- 代码/镜像：`b038b4c` / `b038b4c-20260719-observability-offline`；Helm revision 33。
- 环境、拓扑、tenant、Processor、payload 和请求并发均与上一节一致。部署前通过
  滚动重建 `calico-node` 刷新宿主机 CNI 凭据，一次性 canary Pod 1 秒内 Ready；
  50 Pod OrderedReady 升级约 10.5 分钟，全程最低 49/50 Ready。
- accepted 仍是全部 HTTP 202 返回时间。进度和排空判定改为每次从 health 解析当前
  Leader 的稳定 ClusterIP，直接读 Leader `ListTenants`；最终要求 250ms 间隔连续
  3 次 unfinished=0。使用普通 ClusterIP 读取时会随机命中异步 non-voter，在新写入
  尚未追上时读到过期的 0，不能用作性能计时依据。
- 在修正读取口径前有一轮预热任务，虽然已全部排空，但完成结果仍在 FSM 中；
  因此本节是新的运行基线，与上一节不是严格 A/B，不把差异全部归因于观测代码。

| tasks | HTTP batch | accepted | 端到端 | 写入后排空 | task/s | t50 | t90 | Apply error | 最终 unfinished |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 500 | 0.126s | 6.596s | 6.470s | 151.6 | 4.542s | 5.714s | 0 | 0 |
| 5,000 | 500 | 0.785s | 9.324s | 8.539s | 536.3 | 5.883s | 8.383s | 0 | 0 |
| 20,000 | 500 | 4.571s | 32.068s | 27.498s | 623.7 | 19.372s | 29.397s | 0 | 0 |
| 50,000 | 500 | 16.322s | 124.381s | 108.059s | 402.0 | 86.200s | 118.094s | 0 | 0 |
| 20,000 | 1,000 | 3.756s | 31.033s | 27.277s | 644.5 | 19.132s | 28.643s | 0 | 0 |

上一节到本节的观察方向是：1k/5k 基本持平，20k/500 从 695.7 降到
623.7 task/s，50k/500 从 472.0 降到 402.0 task/s，20k/1000 从 652.7 降到
644.5 task/s。但因 Leader 读取口径和 FSM 热状态不同，这只是回归预警，不是“观测开销
造成固定百分比下降”的因果证据。后续优化前应从相同干净 Snapshot 各跑多次
on/off A/B，并报告中位数和波动区间。

本次新观测把当前主要限制从估算变成了实测：

| shape | Claim Apply/平均批次 | Claim 平均 Apply | Complete Apply/平均批次 | pending scanned | selection 平均耗时 |
|---|---:|---:|---:|---:|---:|
| 20k / HTTP 500 | 162 / 123.5 | 138.582ms | 194 / 103.1 | 1,497,900 | 27.384ms |
| 50k / HTTP 500 | 399 / 125.3 | 161.476ms | 542 / 92.3 | 9,198,148 | 105.233ms |
| 20k / HTTP 1,000 | 160 / 125.0 | 134.997ms | 192 / 104.2 | 1,520,430 | 26.196ms |

结论变化：

1. Claim 批次在大负载下已接近 128 上限，Create 也精确填满 500/1,000，全程
   Apply error=0。当前不存在“只允许 5 个任务提交”的上限；5 是 voter 数，
   commit 等待 voter 多数派，不等待 45 个 non-voter 全部确认。
2. 50k 仅为选出 50k 任务就重复检查了 9.20M 条 pending，与先前 9.78M 的理论
   估算同量级；选择平均耗时从 20k 的 27ms 增到 105ms。下一个优化目标应是
   持久的有序 pending 索引/增量 cursor，避免每个 ClaimBatch 重新复制和排序全量 backlog。
3. 把 HTTP 批次从 500 增到 1,000 使 20k Create Apply 从 40 次降为 20 次，但
   Claim/Complete 次数和 pending 扫描几乎不变；因此继续增大 HTTP 批次只能优化
   accepted，无法根治消费曲线。
4. 增加 non-voter/执行节点不会扩大 voter 多数派，但 Leader 仍需为更多复制流、
   worker stream 和完成结果付出 CPU/网络成本。因此“节点更多也绝不降速”不是当前
   保证；需要在干净隔离集群上继续 3/5/10/20/50 阶梯才能定量。

主机在 50k 运行中的一次采样为 load average 19.45/11.65/6.71、可用内存
53 GiB，内存不是当时首要限制。MicroK8s Metrics API 尚未可用，因此这一轮不宣称
有 Pod CPU throttling 的定量证据；下一步观测空白是补 metrics-server/cAdvisor 的 CPU throttle、
network/disk，以及 Leader Go CPU/heap profile。

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

### 7.1 WebUI 性能诊断轮询 A/B（2026-07-19）

环境固定为远程单机 MicroK8s、50 Pod、5 voter/45 non-voter、每 Pod 100 Worker；负载为
4 个 tenant 按条目轮转的 20000 条任务，HTTP 每批 500、4 个并发请求。浏览器保持可见，
WebUI 每秒刷新。以下均为单轮观测，能定位数量级回归，但不是隔离压测或统计显著性结论。

| 版本/页面状态 | accepted | 排空 | 端到端 | 吞吐 |
|---|---:|---:|---:|---:|
| `3362ab7`，完整 performance 与含 performance 的 `/metrics` 同时轮询 | 3.668s | 29.542s | 33.210s | 602.2 task/s |
| `3362ab7`，页面关闭 | 3.143s | 26.776s | 29.918s | 668.5 task/s |
| `b70959e`，Leader 完整 performance + `/metrics?performance=0` | 3.617s | 26.000s | 29.617s | 675.3 task/s |

初版页面把同一类 174 点性能历史从两个端点重复序列化和传输，开页面相对关页面端到端
慢约 11%。不能简单把通用 `/metrics` 的历史复用到性能图：该端点属于当前连接节点，
负载均衡连到 Follower 时会把 Follower 零历史与 Leader 当前值错误拼接。最终实现从
Follower 代理的 `/api/v1/admin/performance` 读取同一 Leader 的当前值和 31 条历史，
通用 metrics 在复制 ring buffer 前排除 `performance:`。最终开页面结果比初版快 10.8%，
与初版关页面的 29.918 秒处于同一波动范围。

最终轮次中 Create 为 40 Apply/20000 items/平均批次 500，Claim 为 168/20000/119，
Complete 为 188/20000/106；三者 error 均为 0，最终 unfinished=0，assignment/completion
队列深度均为 0。选择 20000 条任务仍扫描 1629061 条 pending（81.5x），因此性能可视化
没有改变此前瓶颈结论：下一项调度优化仍应是持久有序 pending 索引或增量 cursor，而不是
继续增加 HTTP 批次或 voter 数。

线上响应验证：完整 performance JSON 保留且含 31 条、每条 174 点历史；本轮约 14.7KB。
`/metrics?performance=0` 返回 60 条 workload/allocation 历史且全部为 174 点，
`performance:` 条目为 0。浏览器连接 `sluice-36`、诊断来源 Leader `sluice-3` 时，
Create/Claim/Complete 当前值与历史峰值均非零，证明 Follower 页面没有再混用本地性能历史。

### 7.2 174 点悬浮交互复核（2026-07-20）

版本 `9bdde75` 在同一台远程 MicroK8s、50 Pod、5 voter/45 non-voter、每 Pod 100 Worker
上复核。负载仍是 4 tenant 按条目轮转的 20000 条任务、HTTP 每批 500、4 并发请求；
浏览器保持可见并每秒刷新，鼠标停在图上但压测期间不移动。测试前经历两次 50 Pod
OrderedReady 滚动和一轮 500 条交互预热，Leader 为 `sluice-sluice-1`，因此不是上一节
`b70959e`/Leader `sluice-sluice-3` 的干净隔离 A/B。

| 版本/页面状态 | accepted | 排空 | 端到端 | 吞吐 | t50 | t90 |
|---|---:|---:|---:|---:|---:|---:|
| `9bdde75`，四图支持最近点悬浮且跨图只保留一个 tooltip | 3.415s | 30.551s | 33.966s | 588.8 task/s | 21.142s | 30.826s |

本轮 Create 为 40 Apply/20000 items/平均批次 500/平均 Apply 240.378ms，Claim 为
169/20000/118.3/129.823ms，Complete 为 198/20000/101.0/149.188ms；三者 error 均为
0，最终 unfinished=0，assignment/completion 队列均为 0。选择 20000 条任务扫描
1688091 条 pending（84.4x），与上一节 81.5x 同量级，主要瓶颈仍是每批重复扫描 pending。

相对上一节最终单轮结果，accepted 从 3.617s 改善到 3.415s，但端到端从 29.617s 增到
33.966s、吞吐低 12.8%。由于 Leader、滚动历史和 FSM 热状态不同，这只是回归预警，
不能归因于悬浮交互。实现没有增加接口或轮询；每秒绘图复用原本已经构造的 174 点数组，
跨图清理只在 pointer move 时遍历 4 个图，压测静止鼠标时不执行。若后续要把该波动定性为
前端开销或服务端退化，必须在相同干净 Snapshot 上做多轮 tooltip on/off A/B 并报告中位数。

### 7.3 图表原始 JSON 链接复核（2026-07-20）

版本 `50b58d0`、Helm revision 39 在相同的远程 50 Pod、5 voter/45 non-voter、每 Pod
100 Worker 环境复核。负载继续使用 4 tenant 按条目轮转的 20000 条任务、HTTP 每批 500、
4 并发请求；浏览器保持可见且 Dashboard 每秒刷新。该轮发生在完整 OrderedReady 滚动后，
Leader 为 `sluice-sluice-4`，因此仍是回归基线而不是与 7.2 的隔离 A/B。

| 版本/页面状态 | accepted | 排空 | 端到端 | 吞吐 | t50 | t90 |
|---|---:|---:|---:|---:|---:|---:|
| `50b58d0`，四张图均提供新页原始 JSON | 3.769s | 27.826s | 31.595s | 633.0 task/s | 18.602s | 28.605s |

本轮 Create 为 40 Apply/20000 items/平均批次 500，Claim 为 164/20000/122.0，Complete
为 186/20000/107.5；三者 error 均为 0，最终 unfinished=0，assignment/completion 队列
均为 0。选择 20000 条任务扫描 1541166 条 pending（77.1x），所以性能结论没有改变：
当前主要服务端放大仍是 Claim 批次间重复扫描 pending，不是图表 JSON 序列化。

相对 7.2 的单轮结果，accepted 慢 0.354s，但端到端快 2.371s、吞吐高 7.5%，t50/t90
也更早；这些变化与 Leader、滚动后的 FSM 热状态和单轮波动混在一起，不能归因于本次 UI/API
变更。实现没有增加周期请求：四个链接仅在用户点击时请求；Worker 和 unfinished 使用
`prefix` 在复制 ring buffer 前筛选，两张性能图复用已有的同一 Leader 诊断端点。线上单次响应
分别为 Worker 23523 bytes/13.1ms、unfinished 2512 bytes/13.2ms、performance
16067 bytes/15.8ms；未点击时成本为 0。下一步性能优化仍应针对持久有序 pending 索引或增量
cursor，并用相同 Snapshot 多轮对照验证，而不是删掉只读 JSON 可追溯能力。

### 7.4 control/Worker 拆分与增量 pending 索引复核（2026-07-20）

- 镜像 `localhost:32000/sluice:role-split-indexed-v2-20260720`，Helm revision 41；
  拓扑从 50 个同时承担复制和执行的进程改为 5 个 control/Raft voter 加 50 个
  stateless Worker，每个 Worker 100 并发，总执行容量 5000。Raft 配置已验证为
  5 voter/0 non-voter，control 的执行容量均为 0，全部 allocation owner 都是 Worker。
- 测试形状保持为 4 tenant 按条目轮转的 20000 条任务、HTTP 每批 500、4 并发请求；
  直接请求当前 Leader。测试前 unfinished=0，结束条件为 250ms 间隔连续 3 次
  unfinished=0。Demo Processor 仍为每任务 50～200ms，没有用缩短业务耗时制造提升。
- 本轮同时启用随 Raft Apply 增量维护、Snapshot Restore 时重建的 pending 派生索引；
  Worker 不保存 Queue、FSM 或 Raft 日志，也不自行从全局 pending 镜像竞争任务。

| 版本/拓扑 | accepted | 排空 | 端到端 | 吞吐 | t50 | t90 | 最终 unfinished |
|---|---:|---:|---:|---:|---:|---:|---:|
| revision 39，5 voter + 45 non-voter/执行节点 | 3.769s | 27.826s | 31.595s | 633.0 task/s | 18.602s | 28.605s | 0 |
| revision 41，5 control + 50 stateless Worker | 1.056s | 13.534s | 14.589s | 1370.9 task/s | 9.491s | 13.089s | 0 |

同一远程主机上的单轮回归结果为吞吐提高 116.6%，端到端缩短 53.8%，accepted
缩短 72.0%。两轮之间包含架构和索引两个改动，因此这个数据证明组合版本的结果，
不能把全部提升单独归因给其中一个改动，也不替代后续相同 Snapshot 的多轮统计基准。

| 指标 | revision 39 | revision 41 |
|---|---:|---:|
| Create Apply/平均批次 | 40 / 500.0 | 40 / 500.0 |
| Claim Apply/平均批次 | 164 / 122.0 | 223 / 89.7 |
| Complete Apply/平均批次 | 186 / 107.5 | 231 / 86.6 |
| pending scanned / selected | 1,541,166 / 20,000 | 31,738 / 20,000 |
| pending 扫描放大 | 77.1x | 1.59x |
| Raft Apply error | 0 | 0 |

增量索引把 pending 检查量降低 97.9%，但没有改变状态权威边界：索引只是由 FSM
已提交状态派生，Claim 成功后才移除，Restore 会完整重建。Claim/Complete 批次变小、
Apply 次数反而增加，说明当前仍有跨 50 条 Worker stream 的 credit/result 到达碎片；
吞吐仍能翻倍，是因为 Leader 不再向 45 个执行副本复制和落盘每条日志，同时重复选择
热点基本消失。本轮 Create/Claim/Complete 平均 Apply 分别为 48.798ms、41.177ms、
40.898ms，三者 error 都为 0；压测后 55 个当前 Pod 的最近 5 分钟日志中没有 panic、
fatal、error、timeout、interrupted 或 lease recovery。

下一阶段的观测重点从“全量 pending 重扫”转为：全局 dispatcher 如何跨 Worker stream
提高 Claim/Complete 批次填充率、Leader 的 Raft fsync/多数派尾延迟，以及单 control shard
的结果汇聚 CPU/网络上限。继续增加 Worker 不扩大 Raft quorum，但达到 Processor 容量后
仍会受这些 control-plane 上限约束；横向扩展控制写吞吐仍需要按明确租户归属做 Multi-Raft。

### 7.5 credit 填充、idle 唤醒与 session 恢复复核（2026-07-23）

本轮保持远程物理环境、Demo Processor 和提交形状不变：同一台 ThinkPad 上的 MicroK8s，
5 control voter + 50 stateless Worker、每 Worker 100 槽；四租户 Limit 分别为
100/60/30/500，每轮从 unfinished=0、每租户 allocation=1 开始，按条目轮转提交 20,000
条任务，HTTP batch=500、concurrency=4，直接访问 Leader。所有结束条件均为 250ms
采样连续三次 unfinished=0。该环境所有 Pod 位于同一物理故障域，不是跨主机 HA 测试。

先把每节点 Assignment/Result 未决 credit 从 8 提到 32。真实 8 节点/4900 槽/4096
任务集成 Case 中，Claim/Complete Apply 从 151/151 降到 42/42～43，测试耗时从约
14.83 秒降到约 8.20 秒；每类每节点 32、合计 64 仍是固定上限，Raft batch 上限仍为
128。远程 50 条活跃 stream 本来就足以用 8-credit 填批，因此 revision 42 单轮相对
revision 41 没有收益：1338.9 对 1370.9 task/s，属于 -2.3% 单轮波动，不能宣称回退。

revision 42 暴露了另一瓶颈：四个 idle tenant 在新任务 durable Create 后仍等待最长
3 秒 allocator tick。revision 44 在提交成功后只对 allocation≤1、Limit>1 的租户发送
合并、非阻塞的本地唤醒；周期 tick 仍是丢通知/切 Leader 的 fallback。滚动中同时确认并
修复 CTRL-004：新 Leader 的恢复回调不能覆盖已经先连接的 Worker session。最终镜像为
`localhost:32000/sluice:perf004-session-recovery-20260723`、Helm revision 44，部署门禁
验证 5 voter/0 non-voter、5 个零执行容量 control、50/50 up Worker、总容量 5000。

| 版本/轮次 | accepted | accepted 后排空 | 端到端 | 吞吐 | t50 | t90 | 最终 unfinished |
|---|---:|---:|---:|---:|---:|---:|---:|
| revision 41，credit=8，单轮 | 1.056s | 13.534s | 14.589s | 1370.9 task/s | 9.491s | 13.089s | 0 |
| revision 42，credit=32、仍等 tick，单轮 | 1.321s | 13.616s | 14.938s | 1338.9 task/s | 9.644s | 13.541s | 0 |
| revision 44，idle durable wake，轮次 A | 3.051s | 10.227s | 13.278s | 1506.3 task/s | 7.453s | 11.450s | 0 |
| revision 44，idle durable wake，轮次 B | 2.748s | 10.180s | 12.927s | 1547.1 task/s | 7.095s | 11.083s | 0 |

revision 44 两轮平均为 1526.7 task/s、范围 1506.3～1547.1。相对 revision 42 的一个
同形单轮高 14.0%，但前者两轮、后者一轮，仍应把它视为回归证据而非统计显著的 SLA。
accepted 变慢是因为 Worker 已在提交阶段同步发起 Claim/Complete，与 40 条 Create Apply
共享 Leader/Raft，而非前端提交上限：轮次 A 提交完成时已处理 3156 条，轮次 B 已处理
2449 条；旧版本提交后前三秒只处理约 67 条。端到端和 t50/t90 都因此提前。

| 指标 | revision 42 | revision 44 A | revision 44 B |
|---|---:|---:|---:|
| Create Apply/items | 40 / 20,000 | 40 / 20,000 | 40 / 20,000 |
| Claim Apply/items/平均批次 | 224 / 20,000 / 89.3 | 173 / 20,000 / 115.6 | 172 / 20,000 / 116.3 |
| Complete Apply/items/平均批次 | 234 / 20,000 / 85.5 | 179 / 20,000 / 111.7 | 179 / 20,000 / 111.7 |
| allocation Apply/items | 未单列 | 7 / 350 | 7 / 350 |
| pending scanned/selected | 31,297 / 20,000 | 31,863 / 20,000 | 未单列 |
| Raft error / final unfinished | 0 / 0 | 0 / 0 | 0 / 0 |

revision 44 当前 Leader 汇总两轮的 Create/Claim/Complete 平均 Apply 为
109.650/53.469/55.605ms，Claim/Complete 平均批次为 115/111；dispatcher 最终深度为 0，
40,000 个选择只检查 63,718 个 pending（1.59x）。因此下一限制已经不是 Worker 数、前端
并发或重复扫描，而是单 shard 两次 durable consensus 的延迟和剩余批次空洞。以两轮平均
乘 70% 余量，当前同硬件/同负载的规划水位为约 1,069 task/s：1,000 task/s 可由一个
shard 承担但余量很小，1,500/3,000/5,000/10,000 task/s 至少规划 2/3/5/10 个 shard。

继续提升的优先级是：先实现按租户稳定归属的 Multi-Raft，让每个 shard 维持 3 或 5 个
control；其次才评估在不改变 single-owner claim 和 ACK-after-commit 的前提下，把同时到达
的 Claim 与 Complete 合成一种混合 Raft command，共享一次 fsync/复制。增加 Follower 只会
提高故障容忍并增加复制，增加 Worker 在 690 个正常租户槽已高于 Processor 所需且控制面
饱和后也不会继续提高吞吐。

本机 release gate 的相同 7-replica/3-voter/20k Case 本轮提交 2.673 秒、提交后排空
11.000 秒；全量 `make test`（unit + integration，全部 `-race -count=1`）通过，集成耗时
210.144 秒。Worker 纯内存基准约为 0.80M（1 Worker）、0.93M（10 Worker）、0.94M
（100 Worker）、1.00M task/s（10×10），远高于远程 1.5K task/s，再次说明本轮不是
Pool/Processor 调度 CPU 上限。

### 7.6 Worker-only HPA / CRD operator 复核（2026-07-23）

本轮实现只改变 Kubernetes 部署控制面，未改变任务提交、Raft command、allocator 或
Processor 数据路径。固定基线仍按规则重跑。远程环境保持同一台 ThinkPad 单物理故障域、
MicroK8s、5 control voter + 50 stateless Worker、每 Worker 100 槽；Demo Processor 每任务
50～200ms。四租户 Limit 为 100/60/30/500，按条目轮转提交 20,000 条，payload 是小型 JSON，
HTTP batch=500、concurrency=4，直接访问同一个 Leader。Helm revision 45 的运行镜像为
`localhost:32000/sluice:bddafeb-20260722173728`；标签中的 commit 是部署前基准，镜像包含
本节待提交工作树，故不把标签误当源码证明。提交后正式镜像
`localhost:32000/sluice:636ff40-20260722181142` 已滚动到 revision 46 的 5 control/50
Worker；该替换只改变镜像标签，下面基线来自同一份二进制和 Chart 内容，不冒充另一轮性能
样本。

| 版本/拓扑 | accepted | accepted 后排空 | 端到端 | 吞吐 | t50 | t90 | 最终 unfinished |
|---|---:|---:|---:|---:|---:|---:|---:|
| revision 44，HPA 前，两轮范围 | 2.748～3.051s | 10.180～10.227s | 12.927～13.278s | 1506.3～1547.1 task/s | 7.095～7.453s | 11.083～11.450s | 0 |
| revision 45，HPA 默认关闭，单轮 | 1.314s | 11.812s | 13.125s | 1523.8 task/s | 7.507s | 11.616s | 0 |

revision 45 的 Create 为 40 Apply/20,000 items/平均批次 500/平均 Apply 63.996ms，Claim
为 179/20,000/111/50.344ms，Complete 为 187/20,000/106/49.005ms；三类 error 均为 0。
选择 20,000 条任务检查 31,854 个 pending（1.59x），最终 assignment/completion queue 均
为 0，Pod 日志没有 panic、assignment/completion timeout、lease recovery 或 interrupted。
滚动阶段有一条 `register node (non-fatal): node is not the leader`，属于 control 切 Leader 时
可重试的 join，不发生在负载窗口。单轮结果仍落在 revision 44 的吞吐范围内；accepted 更快、
t50/t90 略慢是单轮抖动，不能归因于默认关闭、且不在任务路径执行的 HPA 模板。

实际 Kubernetes HPA 验收分两层：

- 直接 Helm 入口应用 `autoscaling/v2` HPA，把 `sluice-sluice-worker` 的 min=max 从 51
  改回 50。Controller event 分别记录 `Current number of replicas below Spec.MinReplicas` 和
  `above Spec.MaxReplicas`，Worker StatefulSet 确认 50→51→50；全程 control=5，最终 Raft
  仍为原 5 voter/0 non-voter。临时 HPA 删除后主 release 回到静态 50/50 Ready。
- CRD 入口临时部署带 Lease 选主的 operator，创建 1 control + 1 Worker 的独立真实
  `SluiceCluster`。operator 为 control 创建 Pod-specific ClusterIP Service，并把 API Service
  ClusterIP 注入 Worker；FSM 验证 control 容量 0、Worker 容量 2。修改
  `spec.autoscaling.minReplicas/maxReplicas` 后，HPA 与 FSM 均收敛 1→2→1，Raft 始终只有
  `smoke-0` 一个 voter、无 non-voter。smoke namespace、operator、RBAC 和 Lease 随后全部
  删除，主 release 保持 5/50。

HPA 51→50 后 FSM 按设计保留 `worker-50` 的 down 身份镜像，但没有 allocation 指向它，
也不计入 5000 个可用执行槽。revision 46 首次部署后的旧门禁用 FSM Node 总数等同 Pod
副本数，因而误报 56≠55；HPA-002 改为检查 5 个 up control、50 个 up Worker、up Worker
容量和 allocation owner。它只修复验收判断，不改变上述性能路径，因此不另算性能样本。

目标 MicroK8s 的 metrics-server 仍未启用，所以本轮只验证 HPA API、scale ownership、
min/max 控制器路径和 Sluice 注册/缩容正确性，不宣称 CPU 或 backlog 指标驱动的自动决策
已经在该主机验收。默认 CPU 70% 需要 Metrics API；按 unfinished backlog 扩容需要另装
autoscaling/v2 external/custom metrics adapter。指标缺失时 HPA 会显示 `cpu: <unknown>/70%`，
但不影响 min/max 安全边界。生产容量结论仍是：HPA 只能补足 Processor 执行容量，当前单
shard 约 1.5K task/s 的 Raft Apply 上限不会因继续增加 Worker 自动提高；超过规划水位仍应
优先拆 Multi-Raft shard。最终 `make test` 的 unit + integration 全部以 `-race -count=1`
通过，真实集成阶段耗时 220.520 秒。
