# Load Lab：从页面组合复杂业务负载

Load Lab 是 WebUI 内的一组有界原子操作，不是另一个测试协议。它调用和业务客户端相同的
tenant PUT 与 task batch POST，所以可以快速观察真实 Raft、分配、Worker、积压和扩容行为。

## 原子操作

1. **Create tenants only**：在稳定池 `load-lab-001` 到 `load-lab-100` 中创建或更新指定
   数量的 tenant。重复执行不会无限增加 tenant。
2. **Create + submit load**：先完成 tenant upsert，再按 tenant round-robin 构造任务，
   使用 500 条 batch、4 个并发请求提交。
3. **Quota profile**：
   - `Equal`：全部使用 Base quota。
   - `Tiered`：依次使用 5、20、100。
   - `Ramp`：从约四分之一 Base quota 平滑增加到 Base quota。
4. **Task shape**：
   - `Even`：每个 tenant 相同任务数。
   - `Hotspot`：第一个 tenant 是 base 的 100 倍，其余为 base。
   - `Pyramid`：tenant 依次为 base 的 1、3、8 倍。
5. **Delivery**：
   - `Burst`：立即提交。
   - `Waves`：分成 1～20 波，每波间隔约 1 秒。

每条任务使用 `run-id:tenant-id:index` 作为幂等键。未知提交结果最多重试一次，同一个 run
不会因为重试创建第二份任务。单次最多 100 个 tenant、每 tenant base 5000 条、计算后的
总任务数 100000 条。

## 组合示例

### 验证 workload autoscaling

- Tenants：100
- Tasks / tenant：200
- Base quota：50
- Quota profile：Equal
- Task shape：Even
- Delivery：Burst

这是 20000 条任务的固定形状。观察 `Unfinished tasks by tenant` 是否先上升后归零、
`Worker allocation by instance` 是否接近 Limit，以及 Kubernetes Worker StatefulSet 是否
按 backlog/utilization 扩容。若 Worker 已经扩出而 Raft Apply/dispatcher 图饱和，瓶颈已经
移动到 control shard，继续增加 Worker 不会线性提高吞吐。

### 验证公平性与空闲借用

- Tenants：60
- Tasks / tenant：200
- Quota profile：Tiered
- Task shape：Even
- Delivery：Burst

5/20/100 三档 tenant 同时到达，适合观察低 Limit tenant 的基础保障、高 Limit tenant 的
吞吐，以及空闲容量是否被有积压的 tenant 临时借用。

### 验证突发热点和回收

- Tenants：100
- Tasks / tenant：50
- Task shape：Hotspot
- Delivery：Burst

第一个 tenant 得到 5000 条，其余各 50 条。冷 tenant 应快速完成；热点 tenant 可逐步占用
空闲 Worker，但新出现的其他 tenant 负载必须能让借用迅速回退。

### 验证波峰

- Tenants：50
- Tasks / tenant：200
- Quota profile：Ramp
- Delivery：Waves（5）

总计 10000 条分五次到达，可观察 autoscaler 的 5 秒采样、扩容速率上限和任务到达速度之间
的关系。

## 状态与停止语义

页面显示 `preparing → submitting → draining → completed`，或 `failed/stopped`。Stop 只阻止
尚未发出的 batch；已经返回 accepted 的任务是 durable work，仍继续监控到 unfinished
归零。最近 10 次运行保存在当前浏览器 `localStorage`，每条可打开原始 JSON；它不是服务端
审计记录，不能替代集群 API、日志或性能基线。

开始新 run 前，若稳定 Load Lab tenant 池还有 unfinished，页面会拒绝叠加。需要并发叠加
多个独立客户端或超过安全上限的性能实验时，使用版本化 benchmark，并按
[`PERF.md`](PERF.md) 记录固定形状和环境。
