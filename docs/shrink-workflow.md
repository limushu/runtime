# 缩容工作流与 C 均衡接入

主流程：[ShrinkWorkflow::run](../crates/storage-pool/src/workflows/shrink.rs)。
它保持普通 `Workflow::run` 和 `ctx.activity(...).await`，没有第二套事件循环、
特殊执行句柄或自动重放引擎。所有步骤接收完整目标盘集合，迁移不会按盘分批提交。

## 执行顺序

1. 记录缩容意图。
2. 禁止目标盘分配。
3. 调用均衡领域迁移全部目标盘；停稳返回时保存续作数据，等待允许继续，然后再次调用。
4. 查询权威 BG 引用及分配归属，确认目标盘已排空。
5. 关闭目标盘 IO。
6. 再次校验引用与 IO 状态，提交成员移除。

这对应旧业务文档的 RequestShrink → DisableAllocation → Evacuate → NoRefs → Down → Remove。
现行 TaskCenter 的 topo 优先暂停协议代替旧文档的“取消旧执行并重新调和”。

`ShrinkStorage` 的各个方法负责真实领域效果，必须接入应用已有的权威元数据、VD 引用和
数据面。它是能力边界，不在这里复制一份磁盘目录。适配器还应更新现有领域查询视图及
PoolManager 的成员索引，不能只删除外部记录而让其余管理入口继续看到旧成员。
`SimulatedShrinkStorage` 专门用于演示和测试，不是 MetadataStore/C 的生产实现。

## 公共调用

```rust,ignore
let manager = PoolManager::new(config, metadata, data_plane)
    .with_shrink(shrink_storage, balance_backend);
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
条件按 `ShrinkStage` 判断，避免已经完成迁移后仍用初始容量条件阻塞收尾。领域命令提交时还需
校验实际状态/版本，Ready 不构成永久的资源锁。

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
let services = ShrinkServices::register(&mut driver, pool_id, storage, balance)?;
driver.define::<ShrinkWorkflow>(Dependencies::default().with(services.clone()))?;
driver.enable_persistence(task_store).await?;
let restored = driver.restore().await?;
let center = TaskCenter::from_restored(driver, &config, Arc::new(PoolSchedulingPolicy), restored)?;
let run = center.submit(ShrinkWorkflow::new(request, services)?.durable()).await?;
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

示例运行真实 ShrinkWorkflow，模拟 C 端迁移和领域存储，演示 topo 交接、用户暂停、条件等待及恢复。
测试通过手动 C 回调控制停稳时机，验证命令接受不等于停稳、整批顺序、唯一缩容、
运行中/挂起后取消、取消与暂停竞态、恢复检查点、取消不恢复，以及迁移完成后独立核验引用。
