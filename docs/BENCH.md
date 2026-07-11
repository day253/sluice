# Benchmark Report

**Date**: 2026-07-12  
**CPU**: AMD Ryzen 7 PRO 5850U (16 threads)  
**OS**: Linux, Go 1.26, `-benchtime=1s`

---

## 1. Allocator — Scheduling Efficiency

### Max-Min Fairness

| Benchmark | Ops/s | Latency | Mem/op | Allocs/op |
|-----------|-------|---------|--------|-----------|
| `MaxMin_10` | 1,611,940 | **757 ns** | 696 B | 6 |
| `MaxMin_100` | 166,927 | **6.6 µs** | 5,656 B | 9 |
| `MaxMin_1000` | 40,084 | **30.5 µs** | 54,656 B | 6 |

Scaling is sub-linear: 100× tenants → only 40× latency increase.  
Even with 1000 tenants the scheduler runs in **30 µs** — negligible vs the 3-second reconcile interval.

### Idle Detection

| Benchmark | Ops/s | Latency | Mem/op |
|-----------|-------|---------|--------|
| `IdleDetection_100` | 69,541 | **17 µs** | 3.5 KB |
| `IdleDetection_1000` | 830 | **1.4 ms** | 54.9 KB |

The 1000-tenant case slows due to per-tenant map access patterns.  
At the 3-second cycle, 1.4 ms = 0.05% CPU overhead.

### Full Reconcile (read FSM → compute → apply)

| Benchmark | Ops/s | Latency | Mem/op | Allocs/op |
|-----------|-------|---------|--------|-----------|
| `Reconcile_10` | 50,366 | **23.6 µs** | 13.7 KB | 192 |
| `Reconcile_50` | 16,999 | **70.5 µs** | 41.7 KB | 398 |
| `Reconcile_100` | 8,116 | **136 µs** | 76.3 KB | 651 |

The FSM `copyState()` deep-copy dominates allocations.  
This is a candidate for optimization: copy-on-write or version tracking.

### Distribution Across Nodes

| Benchmark | Ops/s | Latency |
|-----------|-------|---------|
| `Distribute_100×5` | 92,905 | **12.8 µs** |
| `Distribute_1000×10` | 3,624 | **320 µs** |

---

## 2. Worker Pool — Task Scheduling Throughput

All benchmarks use mock Raft (instant Apply) to isolate scheduling overhead.

| Benchmark | Throughput | Latency | Mem/op |
|-----------|------------|---------|--------|
| 1 tenant × 1 worker | **467,283 tasks/s** | 2.1 µs | 1,089 B |
| 1 tenant × 10 workers | **844,977 tasks/s** | 1.2 µs | 1,067 B |
| 1 tenant × 100 workers | **837,805 tasks/s** | 1.2 µs | 1,067 B |
| 10 tenants × 10 workers | **959,916 tasks/s** | 1.0 µs | 1,035 B |
| Claim→Complete seq | **818,847 tasks/s** | — | — |

**Key observations**:
- Scaling from 1→10 workers yields 1.8× throughput (mock raft mutex limits further scaling)
- Multi-tenant scheduling (10×10) achieves highest throughput — contention spreads across per-tenant groups
- Plateau at ~800K-960K tasks/s is bounded by mock raft mutex + queue mutex contention

### Micro-operations

| Benchmark | Latency |
|-----------|---------|
| `ReconcileNoop` (100 tenants, no change) | **210 ns** |
| `StartStopWorker` (spawn + kill goroutine) | **951 ns** |

---

## 3. Raft FSM — State Machine Performance

| Benchmark | Latency | Mem/op | Allocs/op |
|-----------|---------|--------|-----------|
| `ClaimTask` | **5.7 µs** | 1,978 B | 41 |
| `CompleteTask` | **5.7 µs** | 1,979 B | 41 |
| `NodeDownRequeue` (100 tasks) | **6.1 µs** | 1,963 B | 37 |
| `ReadConcurrent` | **150 ns** | 248 B | 4 |
| `Snapshot` (1000 tasks) | **2.7 µs** | 1,033 B | 13 |

JSON marshal/unmarshal dominates Apply operations.  
Concurrent reads benefit from `sync.RWMutex` — 150 ns per read.

---

## 4. Queue — Local Task Buffer

| Benchmark | Latency | Allocs/op |
|-----------|---------|-----------|
| `Enqueue` | **67 ns** | 1 |
| `Dequeue` | **14 ns** | 0 |
| `EnqueueDequeue` pair | **54 ns** | 1 |
| `MultiTenant` (10 tenants) | **65 ns** | 1 |
| `Concurrent` (parallel) | **127 ns** | 1 |

Mutex contention adds ~2× overhead under concurrency.  
At 54 ns/pair, the queue supports **18 million enqueue+dequeue ops/s**.

---

## 5. Bottleneck Analysis

```
Anatomy of a single task in production (estimated):

  Queue dequeue       14 ns     ░░░░░░░░░░░░░░░░  negligible
  Raft claim (FSM)   5.7 µs     ░░░░░░░░░░░░░░░░  negligible  
  ★ Raft log append  ~1 ms      ████████████████  disk sync
  ★ Raft replication ~0.5 ms    ████████░░░░░░░░  network RTT
  Business logic     ~? ms      ████████████████  user-defined
  Raft complete      5.7 µs     ░░░░░░░░░░░░░░░░  negligible
  ─────────────────────────────────────────────────
  Scheduling overhead per task: ~2 µs (allocator + worker + queue + FSM)
  Raft overhead per task:       ~1.5 ms (disk + network)
```

**Conclusion**: The scheduler itself is not the bottleneck.  
At 1000 tenants, the allocator runs in 30 µs — the reconcile cycle (3 s) uses 0.001% CPU.  
Worker scheduling overhead is ~2 µs per task.  
Production throughput is bounded by **Raft disk sync** and **business logic**, not by scheduling.
