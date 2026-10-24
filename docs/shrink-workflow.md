# 缩容工作流与 C 均衡接入

这里的缩容指 **从存储池移除指定成员盘**。请求仅指定目标盘集合，完成条件是目标盘已排空并退出 Pool。
流程不创建、删除或调整上层用户卷，也不改变池内 VD 的身份、BG 数量和副本数；均衡只改变 BG 的物理落盘位置。
现有 VD 领域保存 BG 引用，因此移盘前通过它核验目标盘已无引用，这属于池内数据布局检查。
示例中的 `CreateVolume` 用于准备带 BG 数据的测试盘；测试中的删卷用于验证迁移后的普通清理能力，都不是缩容步骤。

主流程：[ShrinkWorkflow](../crates/storage-pool/src/workflows/shrink.rs)。
它保持普通 `Workflow::run` 和 `ctx.activity(...).await`，没有第二套事件循环、
特殊执行句柄或自动重放引擎。所有步骤接收完整目标盘集合，迁移不会按盘分批提交。

## 执行顺序

主流程用 `loop + match progress.stage` 穷举全部阶段，每轮处理一个阶段。
从检查点恢复时直接进入保存的阶段；新增阶段需要补齐对应的 `match` 分支。

| 当前阶段 | 动作或结果 | 下一阶段 |
| --- | --- | --- |
| `RecordIntent` | 记录目标盘缩容意图 | `DisableAllocation` |
| `DisableAllocation` | 禁止目标盘分配 | `Migrate` |
| `Migrate` | 整批迁移返回 `Completed` | `VerifyEvacuated` |
| `Migrate` | 停稳并返回 `Suspended`：保存续作数据，等待允许继续 | 保持 `Migrate`，唤醒后重新调用 Activity |
| `VerifyEvacuated` | 确认权威 BG 引用与分配均已清空 | `DisableIo` |
| `DisableIo` | 关闭目标盘 IO | `RemoveMembers` |
| `RemoveMembers` | 再次校验引用与 IO，移除成员并清理 Pool 索引 | `Complete` |
| `Complete` | 返回成功 | 流程结束 |

动作成功后才推进并保存下一阶段；错误通过 `?` 返回，不跳过失败步骤。
`ShrinkStage` 表示业务进度，`RunState` 表示执行状态；迁移被挂起时，业务阶段仍为 `Migrate`。

这对应旧业务文档的 RequestShrink → DisableAllocation → Evacuate → NoRefs → Down → Remove。
现行 TaskCenter 的 topo 优先暂停协议代替旧文档的“取消旧执行并重新调和”。

步骤直接调用已有领域服务，不再设立 Shrink 领域服务或按 WF 阶段分发的领域命令：

| 责任 | 实现 |
| --- | --- |
| 记录意图、停止分配 | DiskService：`RequestShrink`、`DisableAllocation`，整批提交元数据 |
| 迁移、暂停、取消、进度 | Balance，接入 C 的独立控制接口 |
| 检查 BG 引用 | VdService：`RequireDisksUnreferenced` |
| 检查分配、关闭 IO、移除成员 | DiskService：`RequireEmptyMany`、`SetIoMany`、`RemoveEmptyMany` |
| 清理路由与全局磁盘归属 | Pool 的 `DiskRemoval`，确认成员删除后更新 |

所有工作流共用 `Domains` 的领域客户端。`ShrinkStage` 只用于 WF 检查点及观测。
`MemoryMetadata` 是示例的唯一存储数据源；模拟均衡直接更新其中的真实 BG 引用、分配位图和归属。

## 元数据后端接入

继续使用原来的 `MetadataStore`，不引入第二个缩容存储接口。除 `commit` 外，缩容要求实现：

- `disks(pool)`：返回该池已提交的成员盘、分配归属、已删除盘的回执。
- `volumes(pool)`：返回已提交 VD 及用于幂等清理的已删除 VD 回执。
- 整批 `RequestShrink`、`DisableAllocation`、`RemoveDisks`：先验证全部 revision，再原子提交。
  移除还必须检查所有目标 IO 已关闭、分配与引用均清空；不能部分成功却返回普通失败。

读取必须反映已确认的提交，不可用滞后副本。Disk/VD 在 Activity 内刷新自己负责的对象后执行
版本检查；公共查询直接读取同一权威后端。Disk 只刷新本次持有执行锁的对象，保留不同磁盘的并发。
现有仅写适配器可以继续执行普通流程，但缩容会明确拒绝不支持权威读取的后端。

C 均衡需要把已完成的 BG 移动提交到**同一元数据源**：引用、源/目标分配、归属、revision 必须一致。
正常完成、暂停、取消的回调都必须在这些效果稳定后返回。因此 topo 接手时能读到暂停前已迁移的部分，
取消也不会抹掉已提交的移动。只有“完成”回调而引用未清空，工作流会失败并保留成员与 IO。
真实 KV 的原子提交、选主 fencing 及 C 绑定仍由部署适配器实现。

## 公共调用

```rust,ignore
let manager = PoolManager::new(config, metadata, data_plane)
    .with_balance(balance_backend);
let pool = manager.get(pool_uuid)?;
let run = pool.shrink(ShrinkRequest::new(target_disks)?).await?;
let task = pool.current_shrink(); // 不要求 REST 客户端保存 TaskId
```

每池最多一个未结束的缩容任务，包括排队、条件等待和挂起。效果不确定的任务继续占用隔离范围。
普通 topo 先于排队的缩容；在途缩容先实际停稳、保存检查点、确认挂起，topo 才能开始。
用户挂起与每个 topo 原因分别维护。topo 完成仅移除自己的原因。

默认策略的排队优先级为 topo > 其他操作 > shrink，同级 FIFO。运行中的资源占用仍须等待
实际结束或允许的 YieldTo 停稳交接。这个排序修正了旧版本的 shrink → topo → volume → shrink 等待环。
自定义 SchedulingPolicy 的 YieldTo 与 priority 必须保持一致，不自动消解任意自定义策略的环。

## 控制语义

`RunCondition` / `Readiness` 是继续执行条件。`wait_until_runnable()` 检查取消、挂起原因和
WF 定义的就绪条件；需要等待时保存 WF Future，并在通知后重新检查。首次执行、每次 Activity
及恢复边界都检查。业务条件变化后调用 `recheck_task(task_id)`；topo 释放会自动触发重新检查。
条件包括目标盘在线、BG 健康、同 Tier 容量及故障域；按 `ShrinkStage` 判断，
避免已经完成迁移后仍用初始容量条件阻塞收尾。领域命令提交时还需校验实际状态/版本，
Ready 不构成永久的资源锁。

迁移的 Reply 是公共类型：

```rust,ignore
ActivityExit::Completed(MigrationProgress)
ActivityExit::Suspended(MigrationCheckpoint)
```

- Suspended 表示本次 Activity 已停稳并结束，只留下可序列化的数据，没有遗留的运行实例需要 WF 再清理。
- WF 保存续作信息后才调用 `wait_until_runnable()` 确认任务挂起。恢复后再次调用普通 Activity，TaskId 不变。
- Err(Failure::Cancelled) 表示取消后已停稳。取消后的 Task 是终态，不能 resume，也不进入自动恢复。
- 取消不撤销已经迁移的数据，也不自动恢复目标盘分配。重新开放分配需要显式管理决策。
- 当前 Activity 历史中的 Completed 表示调用已经结束。整个迁移步骤是否完成由 ActivityExit 及 WF 阶段表达。
- 基础框架仍支持 ActivityControl 的“保留调用”模式；新 ShrinkWorkflow 采用“停稳返回”模式。

## C 包装层

实现 [BalanceBackend / BalanceOperation](../crates/storage-pool/src/domains/balance/mod.rs)：

- `submit(Migrate)`：向 C 提交整批请求，返回独立持有回调生命周期的操作对象。
- `suspend()` / `cancel()`：调用 C 的独立控制接口，只代表指令交付，不代表已经停稳。
- `completion()`：等待本次调用的真实终态。正常完成返回 Completed；暂停且释放执行实例返回 Suspended；
  取消并完成清理返回 Cancelled。重复读取或取消等待不应丢失终态。
- `progress()`：保留最新进度的 watch 通道，BalanceHost 将其上报到同一个 TaskSnapshot。

[BalanceHost](../crates/storage-pool/src/domains/balance/runtime.rs) 同时等待原 C 完成回调和
`ctx.control()`。发送暂停/取消后继续等待 completion，不会丢弃在途操作或提前报告停稳。
控制命令必须可独立到达 C，不能排在长时间的完成等待或同一把资源锁后面。
控制命令与完成竞态时需幂等；已完成的事实必须保留。无法确认效果返回 Uncertain，保留隔离。

只有 completion 确认 C 不再使用回调上下文时才可回收内存。checkpoint 内禁止保存 C 指针、
Future 或进程内句柄。恢复信息可以引用 C 侧持久化的业务状态。

## 可选持久化

PoolManager 的快捷创建入口仍使用内存任务。持久化组合根使用已有的 Driver 接口：

```rust,ignore
let mut driver = WorkflowDriver::new(pool_id.as_str(), config.clone())?
    .with_task_ids(pool_uuid, TaskIds::for_leader(leader_epoch)?)?;
let domains = Domains::register(&mut driver, pool_id, metadata, data_plane, Some(balance))?;
driver.define::<ShrinkWorkflow>(Dependencies::default().with(domains.clone()))?;
driver.enable_persistence(task_store).await?;
let restored = driver.restore().await?;
let center = TaskCenter::from_restored(driver, &config, Arc::new(PoolSchedulingPolicy), restored)?;
let run = center.submit(ShrinkWorkflow::new(request, domains)?.durable()).await?;
```

TaskStore 可替换为自己的 KV 实现，须提供已有接口要求的持久提交、owner epoch fencing 和 revision CAS。
切主装配先恢复真实领域状态并完成数据面 fencing，再恢复 WF。阶段检查点记录下一业务步骤及迁移续作数据。
存储步骤必须能在“效果已提交、检查点未写入”的窗口后幂等重入；均衡用稳定 TaskId 关联业务操作，并核对真实进度。
检查点写失败按 Uncertain 隔离，不当作普通失败释放占用。用户暂停状态恢复后继续保持暂停。

此变更没有实现全量 Activity 历史归档。TaskStore 中的任务续作记录与长期审计查询是不同能力。

## 演示与验证

```sh
cargo run --locked -p storage-pool --example shrink_workflow
cargo test --locked -p storage-pool --test shrink_workflow
```

示例运行真实 ShrinkWorkflow，使用内存元数据和模拟 C 端迁移，演示 topo 停稳交接及自动恢复。
用户暂停、条件等待、取消与持久化恢复由下述回归测试覆盖。
示例先真实创建四块成员盘和双副本 VD，再把两块目标盘的数据迁到剩余成员盘并移除目标。
测试通过手动 C 回调控制停稳时机，验证命令接受不等于停稳、整批顺序、唯一缩容、
运行中/挂起后取消、取消与暂停竞态、恢复检查点、取消不恢复，以及迁移完成后独立核验引用。

回归测试还验证：暂停后的部分迁移对现有查询可见；拓扑操作不会重新开启分配；成员移除清理事件路由及
PoolManager 磁盘归属；迁移后的 VD 仍可通过原删除流程释放分配；批量 revision 冲突不会留下半批意图。
