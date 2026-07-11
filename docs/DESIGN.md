# 分布式多租户限流系统 - 设计文档

## 1. 概述

基于 Raft 共识协议的分布式多租户并发限流系统。每个租户（公司）拥有独立的请求队列和并发 worker 配额，
多台机器组成集群协同工作，支持超售（所有租户限额之和 > 集群总 worker 数）、运行时配置变更、
节点故障自动迁移和任务级恢复。

### 1.1 核心指标

| 指标 | 描述 |
|------|------|
| 限流维度 | 并发数（同时处于 "处理中" 状态的请求数） |
| 最小保证 | 每个租户至少 1 个 worker |
| 超售策略 | Max-Min Fairness 按比例分配 |
| 故障恢复时间 | < 5s（心跳超时 + 重分配） |
| 任务状态一致性 | claim 即持久化，节点宕机任务不丢失 |

---

## 2. 架构设计

### 2.1 分层架构

```
┌─────────────────────────────────────────────────┐
│                   API 层 (HTTP)                   │
│    POST /tasks  GET /tasks/:id  Admin APIs        │
├─────────────────────────────────────────────────┤
│                 编排层 (Node)                      │
│   组件创建、生命周期、依赖注入                       │
├──────────┬──────────┬─────────────┬──────────────┤
│  Queue   │  Worker  │  Allocator  │  Raft        │
│  持久队列 │  并发池  │  分配引擎    │  共识 & FSM  │
├──────────┴──────────┴─────────────┴──────────────┤
│               存储层                               │
│   PebbleDB (WAL + FSM 持久化)                     │
└─────────────────────────────────────────────────┘
```

### 2.2 控制面 vs 数据面

```
控制面 (Raft FSM) — 强一致，数据量小
├── 集群节点注册表
├── 租户配置
├── Worker 分配方案
├── In-flight 任务注册表
└── 已完成任务结果 (带 TTL)

数据面 (本地)
├── PebbleDB WAL 持久化队列
├── Worker goroutine 池
└── 本地缓存 (任务状态等)
```

---

## 3. 核心组件设计

### 3.1 Raft 状态机 (FSM)

```go
type FSMState struct {
    // 集群
    Nodes      map[string]*NodeInfo      // nodeID -> 节点信息
    LeaderID   string

    // 租户
    Tenants    map[string]*TenantConfig  // tenantID -> 配置

    // 分配方案 (leader 计算，写 Raft)
    Allocations map[string]*NodeAllocation // nodeID -> 分配

    // 任务追踪
    InFlight   map[string]*InFlightTask  // taskID -> 任务
    Results    map[string]*TaskResult    // taskID -> 结果 (LRU淘汰)

    // 版本号 (乐观锁/幂等)
    Version    uint64
}
```

**Snapshot**: 定期快照到磁盘，排除 Results (单独管理)。

### 3.2 任务状态机

```
                  ┌──────────┐
                  │  PENDING  │  ← 任务创建，在队列中等待
                  └────┬─────┘
                       │ worker 认领
                  ┌────▼─────┐
                  │ INFLIGHT  │  ← 正在处理中 (payload 已写入 FSM)
                  └────┬─────┘
                    ┌──┴──┐
               ┌────▼─┐ ┌─▼─────┐
               │ DONE  │ │FAILED │  ← 最终状态
               └──────┘ └───────┘
```

状态转换都是通过 Raft Apply 完成的，保证集群一致。

### 3.3 Worker 分配算法 (Max-Min Fairness)

```text
输入:
  totalWorkers     = 所有节点 worker 总和
  tenants          = [{id, maxWorkers}, ...]
  minPerTenant     = 1

算法:
  1. 初始化: remaining = totalWorkers
  2. 第一轮: 每个租户分配 min(maxWorkers, minPerTenant)
  3. 迭代:
     fair = remaining / 未满足租户数
     for each 未满足租户:
       allocate = min(maxWorkers - already, fair)
     remaining = 剩余的 - 本轮分配
     直到 remaining == 0 或所有租户已满足
  4. 按节点权重分配到各节点 (默认平均分配)

示例:
  totalWorkers=100, A:100, B:50, C:30
  第一轮: A=1, B=1, C=1, remaining=97
  第二轮: fair=97/3=32.3
    A: min(100-1, 32)=32, B: min(50-1, 32)=32, C: min(30-1, 32)=29
    总分配=93, remaining=4
  第三轮: fair=4/2=2
    A: min(67, 2)=2, B: min(17, 2)=2
  最终: A=1+32+2=35, B=1+32+2=35, C=1+29=30
  总计 = 35+35+30 = 100 ✓
```

### 3.4 队列设计

```
每租户 × 每节点 = 独立的持久化队列

Node-1:
  tenant-A-queue: [task1, task2, task3]  ← PebbleDB WAL
  tenant-B-queue: [task4]
  ...

Node-2:
  tenant-A-queue: [task5]
  tenant-B-queue: [task6, task7]
  ...
```

- 入队: 写入本地 PebbleDB WAL (sync=true)，返回 taskID
- 出队: worker 认领时从队头取
- 故障恢复: 节点重启后重放 WAL，恢复未认领的任务
- **未被认领的任务存在本地 WAL 中，不在 Raft 里**

### 3.5 Worker 生命周期

```go
type WorkerPool struct {
    mu        sync.Mutex
    nodeID    string
    tenants   map[string]*tenantWorkerGroup
}

type tenantWorkerGroup struct {
    tenantID   string
    maxWorkers int           // 本节点该租户配额
    workers    []*Worker     // 当前运行的 worker
    queue      Queue         // 该租户的本地队列
}
```

**Worker 主循环**:
```
for {
    task := claimTaskFromQueue()    // 从队列取任务
    if task == nil { time.Sleep(); continue }
    
    writeClaimToRaft(taskID, nodeID)  // 写入 FSM: PENDING → INFLIGHT
    
    result := process(task)
    
    writeResultToRaft(taskID, result) // 写入 FSM: INFLIGHT → DONE/FAILED
}
```

**Worker 数量调整**:
- Allocator 通过 Raft 下发新的分配方案
- 每个节点对比新旧方案，spawn/kill worker goroutine
- Kill 时发送 context.Cancel，worker 完成当前任务后退出

### 3.6 故障检测与迁移

```
Leader 定期 (1s) 检查所有节点心跳
  ↓ 心跳超时 (3s)
标记节点为 Down (Raft log)
  ↓
扫描该节点所有 InFlight 任务
  ↓
重新标记为 PENDING (Raft log)
  ↓
Leader 重新计算分配方案 (排除 Down 节点)
  ↓
其他节点 worker 认领重新变为 PENDING 的任务
```

**幂等性保证**: 
- 原始节点可能还活着（网络分区），任务被两个节点同时处理
- 客户端重试提交相同任务（幂等 key）
- 通过 taskID 去重：先完成的写 Results，后完成的发现 taskID 已有结果则丢弃

---

## 4. API 设计

### 4.1 任务 API

```
POST /api/v1/tasks
Content-Type: application/json

{
  "tenant_id": "company-a",
  "payload": {"url": "https://...", "method": "POST", "body": "..."},
  "idempotency_key": "optional-uuid"   // 可选，幂等提交
}

Response 202:
{
  "task_id": "uuid",
  "status": "pending"
}

---

GET /api/v1/tasks/{task_id}

Response 200:
{
  "task_id": "uuid",
  "status": "pending|inflight|done|failed",
  "result": {"status": 200, "body": "..."},  // done 时存在
  "error": "..."                               // failed 时存在
}

---

GET /api/v1/tasks/{task_id}/wait?timeout=30s

长轮询，直到任务完成或超时。
```

### 4.2 管理 API

```
PUT /api/v1/admin/tenants/{tenant_id}
{
  "max_workers": 100,
  "name": "公司A"
}

GET /api/v1/admin/tenants

GET /api/v1/admin/nodes

GET /api/v1/admin/allocations
```

---

## 5. 数据流

### 5.1 正常流程

```
1. Client → Node A: POST /tasks {tenant:"A", payload:...}
2. Node A:
   a. 生成 taskID (UUID)
   b. 写入本地 PebbleDB WAL (tenant-A queue)
   c. 返回 {task_id, status:"pending"}
   
3. Node A Worker (分配给 tenant-A):
   a. 从本地队列取出 taskID
   b. 通过 Raft Apply: {op: "claim", task_id, node_id, payload}
   c. 任务状态变为 INFLIGHT，所有节点可见
   d. 处理任务
   e. 通过 Raft Apply: {op: "complete", task_id, result}
   f. 任务状态变为 DONE

4. Client → 任意 Node: GET /tasks/{task_id}
   → 从 FSM Results 或 InFlight 中读取并返回
```

### 5.2 故障恢复流程

```
0. 初始状态: Node-A 有 3 个 tenant-A 的 INFLIGHT 任务

1. Node-A 宕机 (心跳超时 3s)

2. Leader 检测到，写入 Raft log:
   {op: "node-down", node_id: "node-a"}

3. FSM Apply "node-down":
   a. 标记 Node-A 为 Down
   b. 遍历 InFlight，找到 node_id="node-a" 的所有任务
   c. 将它们状态改回 PENDING，清除 node_id
   d. 重新计算分配方案 (Node-A 的 worker 配额分给其他节点)

4. 新分配方案 Apply:
   a. Node-B 多获得 3 个 tenant-A worker
   b. Node-B 的 worker 从本地队列认领任务
   
5. 3 个任务被重新认领:
   a. taskID 在 InFlight 中已存在 → PENDING → INFLIGHT (新 node_id)
   b. 重新处理

6. Node-A 可能恢复 (网络修复):
   a. 重新注册，标记为 Up
   b. 重新计算分配，Node-B 的额外 worker 退还
   c. Node-A 本地队列中未被 claim 的任务继续排队
   d. 之前 claim 但 Node-A 实际未完成的任务：幂等性保证不会重复处理
      (Node-A 可能还持有这些任务，尝试写 result 时发现已由 Node-B 完成)
```

---

## 6. 存储设计

### 6.1 PebbleDB 用途

| Key Prefix | 内容 | 位置 |
|------------|------|------|
| `/raft/fsm/` | Raft FSM 状态 | Raft 数据目录 |
| `/raft/logs/` | Raft 日志 | Raft 数据目录  |
| `/local/queue/{tenant}/` | 本地队列任务 | 各节点数据目录 |
| `/local/snapshots/` | Raft 快照 | Raft 数据目录 |

### 6.2 本地队列格式

```
Key: /local/queue/{tenant_id}/{task_id}
Value: JSON(TaskEnvelope{
    TaskID:    string
    TenantID:  string
    Payload:   json.RawMessage
    CreatedAt: time.Time
    IdempotencyKey: string
})
```

出队时删除 key。

---

## 7. 并发安全

| 组件 | 并发策略 |
|------|----------|
| Raft FSM Apply | 单线程串行，无需加锁 |
| FSM 读取 | sync.RWMutex |
| 本地队列 (PebbleDB) | PebbleDB 本身线程安全 |
| Worker 管理 | sync.Mutex |
| 分配重计算 | 仅在 Leader 上运行，通过 Raft 分发 |

---

## 8. 依赖

| 库 | 用途 |
|---|------|
| `github.com/hashicorp/raft` | Raft 共识协议 |
| `github.com/hashicorp/raft-boltdb` | Raft 日志持久化 |
| `github.com/cockroachdb/pebble` | 本地 WAL 队列 & 状态存储 |
| `github.com/google/uuid` | UUID 生成 |
| `github.com/gorilla/mux` | HTTP 路由 |
| `github.com/hashicorp/go-hclog` | Raft 日志 |
| `go.uber.org/zap` | 应用日志 |

---

## 9. 后续扩展

- [ ] gRPC API
- [ ] Prometheus metrics
- [ ] Webhook 任务完成通知
- [ ] 队列优先级
- [ ] 跨节点任务窃取 (work stealing)
- [ ] 租户级别的 SLA 保证
- [ ] 请求去重 (基于 idempotency_key)
- [ ] Dashboard 管理界面
