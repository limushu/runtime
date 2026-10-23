# TaskCenter 持久历史与可选续办

第一版统一在 TaskCenter 内管理 Task。普通任务记录历史；显式可续办任务额外保存参数与检查点。
不增加独立 Operation 管理层，也不把全局事件中心作为接入前提。

## 配置自己的数据库

实现 [TaskStore](../crates/storage-pool/src/task_center/persistence/store.rs)，通过依赖注入传入：

```rust,ignore
let manager = PoolManager::new(config, metadata, data_plane)
    .with_task_store(Arc::new(MyTaskStore::new(my_database)), PersistenceConfig {
        queue_capacity: 1024,
        max_resume_bytes: 1024 * 1024,
    })?;
let pool = manager.create(request)?;
// 已完成系统原有的 Pool 加载/所有权接管，旧执行者已停止。
pool.recover_tasks().await?;
```

启用存储后，新 TaskCenter 的普通准入保持关闭，必须调用一次 recover_tasks；新建的空池也一样。
恢复过程先识别旧实例记录、安装未结束任务的对象占用，再开放准入；仅发现任务，不自动续跑。
未配置 TaskStore 时保持原有纯内存行为。数据库地址、连接池、表名和编码由适配器自行配置。

TaskStore 协议只有三个存储操作：

- get(pool, task_id)：读取本池任务。
- compare_and_set(expected_revision, task)：原子创建或按版本替换，成功必须表示耐久提交。
- list(pool, query)：按创建时间和 TaskId 降序分页，支持只查未结束可续办任务。

记录和历史/可续办索引必须在同一事务中更新；版本冲突返回 false。
提交结果无法判断时返回 Failure::Uncertain，不能假装已提交或明确未提交。
记录格式由 format_version 标识，未知版本明确拒绝；业务续办版本另由 ResumeSpec.version 管理。
before 是服务返回的游标，不能跨 Pool 使用；页大小 1..200。

接口为同步协议，在每个 TaskCenter 独立的阻塞工作线程调用，不在 Tokio 调度线程执行数据库 IO。
使用异步 SDK 的适配器可以在该工作线程桥接其运行时；数据库实现不进入 Workflow 或领域模型。
适配器不得自行无限重试；应用通过 PersistenceHealth 观察失败与历史缺口。

内置适配器：

- MemoryTaskStore：模拟或测试，不耐久。
- RedbTaskStore：事务 KV 参考实现，需显式启用 kv-redb feature；默认构建不依赖 redb。

RedbTaskStore::open 是阻塞方法，在运行时外或 spawn_blocking 中调用。
TaskStore 可以由多个池共享，任务读写按 Pool UUID 隔离；切换前后必须使用同一份数据与相同 Pool UUID。
Pool 的领域元数据加载和主备所有权仍沿用系统原有机制，这里不持久恢复 PoolCatalog 或真实磁盘状态。

## 普通历史

已配置的 Pool 业务入口自动记录任务，无须逐个修改 Workflow。
独立嵌入 TaskCenter 的应用可用 with_task_store 构造，并通过 submit_task 提交自定义 Workflow。
生命周期记录来自任务/Activity 实际边界，不依赖 observer、gather 或界面刷新。

```rust,ignore
let mut run = pool.attach_node(node, sequence, connected).await?;
let task_id = pool.persistent_task_id(run.id())?;
run.result().await?;
pool.flush_task_history().await?;
let detail = pool.task_detail(task_id).await?;
let page = pool.task_history(TaskQuery::default()).await?;
```

TaskId 是稳定 UUID；原 WorkflowId / TaskRef 属于某次运行实例，记录为 execution。
旧 WorkflowId 到 TaskId 的内存映射遵循运行时历史淘汰，长期查询使用 TaskId 或分页查询。
每次执行的 Activity 序号独立，任务详情保留各次执行及其全部已录制步骤。
KV 历史不受 Config::task_history / activity_history 的热缓存上限影响。
拓扑通知创建的任务附带 source_event UUID；无关事件不生成 Task，本版本不单独归档全局事件。

普通历史有界排队、异步写入；队列满和写入失败计入 persistence_health，不静默声明完整。
flush_history / flush_task_history 等待已入队更新，历史曾有缺失时返回错误。
突然退出可能缺失尚未提交的历史尾部；本版不提供完整状态事件日志或零丢失承诺。
归档的数据为 Task/Activity 当前及最终状态，不保存每次瞬时状态转换或任意成功返回值。
旧实例普通任务未确认结束时，recover_tasks 将其标为 Interrupted，历史查询不会重发它。

## 显式可续办任务

```rust,ignore
let task = center.submit_resumable(workflow, ResumeSpec {
    kind: "shrink".into(), version: 1,
    input: encode(&shrink_spec)?, checkpoint: encode(&initial_checkpoint)?,
}).await?;
// PoolHandle 对应入口为 submit_resumable_task。
```

kind/version 必须注册匹配的恢复函数；input/checkpoint 是业务自行编码的字节数组，数据库不解释它们。
任务须声明非空对象集合，所有执行继续遵循同一个 TaskCenter 冲突规则。
参数和初始检查点耐久提交后才允许执行；准入拒绝保留 Ready 及拒绝原因，便于解释和续办。

任务内部在确认一批业务工作后执行：

```rust,ignore
let confirmed = ctx.activity(&service, request).await?;
ctx.save_checkpoint(encode(&confirmed)?).await?;
```

save_checkpoint 等待耐久确认，失败停止后续依赖步骤并保留 NeedsCheck 和对象隔离。
它与原有同步 checkpoint() 不同：后者仍仅检查取消与当前执行边界。
不要将完成百分比当作可恢复检查点，也不能仅凭 Activity Completed 推导续办位置。

## 切换与控制

1. 计划切换时暂停任务并 wait_suspended，确认当前 Activity 停稳且检查点保存，再 shutdown。
2. 新宿主绑定同一 Pool UUID，注册新宿主的恢复函数和服务客户端。
3. recover_tasks 发现旧任务并恢复占用。暂停任务保持 Paused，其他可续办任务显示 Ready/NeedsCheck。
4. 用户或业务策略调用 continue_task；暂停任务通过 resume_task 明确恢复。
5. 恢复函数查询真实业务状态及下游任务，核对后构造 Workflow；在原 TaskId 下新增执行。

```rust,ignore
center.register_recovery("shrink", 1, move |stored_task| async move {
    // 解码、查询实际进度，确认旧工作停稳，再构造新宿主的 Workflow。
    restore_shrink(stored_task).await
})?;
center.recover_tasks().await?;
let execution = center.continue_task(task_id).await?;
```

PoolHandle 对应提供 register_task_recovery、recover_tasks、continue_task、pause_task、resume_task、cancel_task。
缩容使用 `submit_resumable_shrink`（PoolHandle 为 `submit_resumable_shrink_task`），并实现 Workflow 的
continuation_guard；`StoredTask.class` 保存 Shrink 分类，接管后仍保留池级单缩容名额，不按名字猜测任务类型。
正式 `PoolHandle::shrink_disk` 已封装上述提交，配置 TaskStore 即自动登记 `storage.shrink-disk` v1 恢复函数。
接管时先 `restore_metadata` 并获取真实拓扑，再 `recover_tasks`、`continue_task`；详见 [完整缩容](shrink-workflow.md)。
同 Task 的并发续办由任务 revision CAS 排斥；恢复函数不能改变 Workflow 名称和声明对象。
恢复函数应只查询/验证及构造流程，副作用仍放到受准入控制的 Workflow Activity 中。
未知 kind/version、错误对象或终态任务不会被重新执行。

用户取消是整个 Task 的取消意图；宿主 shutdown 导致一次执行 Cancelled 不等于整个 Task 取消。
用户暂停标记和系统 topo 暂停标记分开保存；接管后的系统阻塞由新宿主当前任务重新建立。
运行中条件不满足显示 WaitingConditions，具体原因保留在最近执行的 RunState 中，不当作失败。
NeedsCheck 不能通过简单 cancel 释放隔离；先由业务确认实际效果。
旧执行的检查点和审计更新不能覆盖新一次执行。
若当前宿主的 Activity 本身返回 Uncertain，其底层资源隔离仍保留，本版不提供原地解除接口；
需完成业务对账及宿主接管后再续办。单纯检查点保存失败可在核对后于同宿主续办。

本版复用已有的单 Pool 所有者约束，不提供跨主租约/fencing；调用 recover_tasks 前必须确保旧执行者已停止。
不尝试恢复 Future、任意等待信号或计时器；其业务语义由显式恢复函数负责。

## 关闭与示例

shutdown 会停稳执行并 flush，但允许继续查询历史。
完全退出且不再查询时调用 close_task_store，等待写线程释放适配器引用；共享适配器的外部引用仍由调用方管理。

```sh
cargo run --locked -p storage-pool --features kv-redb --example task_persistence -- /tmp/tasks.redb
cargo test --locked -p storage-pool --all-features --test task_persistence
```

示例是模拟缩容：暂停于确认第一块迁移后，切换控制宿主，再完成两块；同 TaskId 下关联两次执行。
KV 真实持久化，迁移领域状态用进程内模拟器表示；不宣称已经实现真实缩容或断电自动恢复。
该分块示例仅验证持久化协议；真实硬盘缩容一次提交完整排空给均衡模块，按 [阶段流程](shrink-workflow.md) 推进。
`examples/shrink_disk.rs` 调用正式业务工作流，`tests/shrink_disk.rs` 验证同 TaskId、同外部 job 的接管。
KV 参考适配器每次更新事务替换完整任务记录，适用于首版验证；超长任务需进一步拆分步骤存储及设计保留/清理策略。
