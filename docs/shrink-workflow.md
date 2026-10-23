# 磁盘缩容工作流

正式实现位于 [ShrinkDiskWorkflow](../crates/storage-pool/src/workflows/shrink_disk.rs)，
由 `PoolHandle::shrink_disk(DiskId)` 提交。均衡是独立领域能力，工作流不会分批迁移 BG。

## 完整业务链路

```text
检查当前 Pool、磁盘身份及缩容条件；等待已有 topo 完成
  → 读取已提交磁盘状态
  → 提交 ShrinkIntent(job_id)
  → 提交 DisableAllocation
  → 一次提交/重连整盘排空 job，等待同一个 job，持续转发进度
  → 读取均衡已提交的 VD placement 和磁盘 allocation
  → RequireDiskUnreferenced
  → RequireEmpty
  → FinalizeShrink：确认 IO Down → 关闭磁盘 → 提交 Removed
  → 释放活跃目录与物理归属，保留 Removed 记录
```

`ShrinkIntent` 是 MemberDisk 的持久管理意图，不是 Task 检查点；它携带稳定的 EvacuationId。
从意图提交起 `allocatable()` 即返回 false；随后另一步提交 Inactive。
暂停期间仍保留 IO 服务，不重新启用分配。`online_disk` 也不会清除缩容意图或恢复分配。

排空 Activity 使用 job/control 资源，不长期占用 Disk/Node 写资源。均衡模块自行拥有复制、放置和 remap 的互斥。
完成后会刷新并验证真实引用、分配，不能用均衡返回 Ok 代替它们。

Down、Removed 和 Pool 目录更新置于一个短、不可中途暂停的 Activity（`pool.finalize-shrink`）中。
这样 topo 只能在这个步骤之前或之后执行，无法在 Down 后、Removed 前重新打开磁盘。
元数据提交必须再次原子核验 revision、缩容身份、Inactive、无分配和无引用。
IO 已发生但元数据结果不明确时返回 Uncertain，保留隔离，不伪装成没有发生过。

`pool.disks()` 返回活跃成员，`pool.disk(id)` 可查询保留的 Removed 实体。
Removed 不是遗忘身份；同一 Pool 不重新接纳该成员身份，旧观察和任务历史仍有效。

## 使用与接入

```rust,ignore
let manager = PoolManager::new(config, metadata, data_plane)
    .with_balancer(balancer)?;
// 可选：.with_task_store(your_store, persistence_config)?
let pool = manager.create_pool(pool_id)?;
// 配置 TaskStore 时，完成恢复初始化后才开放普通任务准入：
// pool.recover_tasks().await?;

let mut run = pool.shrink_disk(disk_id).await?;
let task = pool.observe_task(run.id())?;
let removed = run.result().await?;
```

`with_balancer` 必须在创建 Pool 前配置；默认未配置时明确拒绝缩容，不以占位成功替代排空。
`ports::Balancer` 是你现有均衡服务的适配入口：

- `check_shrink(pool, disk)`：只读检查，返回 Ready 或 Wait(reason)。真实健康度、容量及放置策略由该模块判定。
- `evacuate(pool, disk, job_id)`：按稳定 ID 创建或重连整盘 job；重试不能创建第二个 job。请求的接受状态不明必须返回 Uncertain。
- `find(pool, job_id)`：按身份查询，None 必须代表确认不存在，不能把连接失败当成不存在。

`EvacuationJob` 提供 progress/watch、wait、pause、resume、cancel。
pause/cancel 成功必须表示在途工作已稳定；resume 应核对当前拓扑；wait 返回真实最终结果。
均衡适配层串行化同一 job 的控制指令，取消等待不会丢弃已经发送的领域指令。

元数据使用 `metadata::KvMetadata`，接入自己的数据库只需实现 `KvStore`。
仓储统一提供完整一致视图、revision 校验、意图/分配/成员状态及原子 remap；
`MetadataChange::Remap` 表达复制确认后的引用/分配切换，不执行物理复制。

磁盘、VD 命令及 Pool 查询始终读取同一个权威仓储；不再切换缓存模式，也不由 Workflow 显式刷新领域缓存。
数据库契约、记录布局和可共享数据库的接入示例见 [元数据模块](metadata.md)。

## topo、条件与取消

同 Pool 只有一个前序缩容，暂停、等待条件期间仍保留名额。后续缩容排队。
Node/Disk/Tier 变更及 Pool Load/Unload/Delete 均参与池级协调；其他 Pool 的无关变更不影响本池。

```text
topo 受理 → 关闭缩容步骤门禁 → 等实际暂停确认 → topo 执行
所有 topo 结束 → 清除系统原因 → 重新检查业务条件 → 恢复原 job
```

首次启动和每次恢复都检查条件；不满足显示 WaitingConditions(reason)，不会当作失败。
用户暂停与系统暂停独立。暂停确认绑定控制代次及 Activity 作用域，不能复用过期确认。
真实源盘 Down 后即使 topo 已结束，缩容仍等待；Up 后也先重新检查，再恢复。
Pool 卸载期间均衡保持暂停；加载并完成新拓扑核验后才继续。

`pool.cancel(run.id())` 表达用户取消。均衡适配器通过 ControlToken 感知，发送 Cancel 并等待停稳，
任务结束前不释放执行占用。已提交 remap 保留，不把 BG 搬回原盘。
意图及 Inactive 保留，避免取消意外重新启用分配；用户确认放弃时执行：

```rust,ignore
let mut abort = pool.abort_shrink_disk(disk_id).await?;
let inactive_member = abort.result().await?;
```

此流程等待该磁盘仍在运行的缩容结束，确认 job 不存在或已停稳后才清除意图，仍保持 Inactive。
重新缩容会使用新的 job ID；恢复分配需要另一项独立的业务验证，不由取消暗中执行。
若 Removed 已真实提交，完成结果优先于晚到的取消请求。

## Task 历史与切换后续办

配置 TaskStore 时，`shrink_disk` 自动提交可续办的 Shrink Task 并登记内置恢复函数；未配置时仅在内存运行。
输入保存磁盘不可变身份和 job ID。意图、分配、placement 和 Removed 等业务事实由元数据仓储持久化。
恢复时核验这些事实并调用领域幂等命令，不额外保存一份未参与恢复决策的阶段检查点，也不反序列化 Future。

`ControlToken::cancellation_reason()` 区分 Requested 与 Interrupted：用户取消终止 job；
宿主/服务停止则等待 job 暂停并结束本次执行，保留外部 job 给下一任继续。
该区别不改变 token 的协作语义，领域适配器仍须主动感知它。

新宿主获得既有 Pool 所有权、旧执行停稳后：

```rust,ignore
let pool = manager.create(same_pool_id_and_uuid)?;
pool.restore_metadata().await?;      // 重建 Tier/Disk/VD 与路由/物理归属
// 从实际拓扑源重新获取连接与加载状态；restore_metadata 不捏造这些事实。
let tasks = pool.recover_tasks().await?;
let workflow_id = pool.continue_task(task_id).await?;
// 用户原本暂停的任务，显式调用 resume_task(task_id)。
```

同一个 TaskId 关联新的执行，重连同一个外部 job。未解 Uncertain 仍需业务对账；没有自动解除隔离或分布式租约。

## 运行与验证

```sh
cargo run --offline --locked -p storage-pool --example shrink_disk
cargo test --offline --locked -p storage-pool --all-features --test shrink_disk
```

示例调用正式 Pool/Workflow，创建镜像卷、缩容 d1、提交 topo、展示真实提交后的引用和完整 Activity 历史。
注入的是 MemoryMetadata、SimulatedDataPlane、SimulatedBalancer；最后一个只模拟已确认的数据复制/remap，
不执行真实磁盘 IO。生产环境替换这些端口即可复用同一工作流。

场景测试覆盖完整迁移与移除、容量等待、取消/放弃意图、虚假排空成功、真实 topo 暂停、源盘 Down/Up、
不可中断终结、提交失败隔离、Pool 卸载/加载，以及同 Task/同 job 的宿主接管。
通用控制边界另见 [Activity 控制](activity-control.md) 和 `tests/topology_shrink.rs`。
