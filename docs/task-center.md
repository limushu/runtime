# TaskCenter：任务、规则和执行

**TaskCenter 是 Task 的管理主体，WorkflowDriver 是一次执行的推进器。**
未配置数据库时也有 TaskId、任务查询、暂停、继续和取消。TaskStore 是历史与续办的可选适配器。

## 阅读顺序

1. [model.rs](../crates/storage-pool/src/task_center/model.rs)：TaskId、TaskStatus、TaskClass。
2. [registry.rs](../crates/storage-pool/src/task_center/registry.rs)：逻辑任务目录，连接当前执行和观察者。
3. [conflicts.rs](../crates/storage-pool/src/task_center/conflicts.rs)：TaskRules 协议及默认 PoolRules。
4. [scheduler.rs](../crates/storage-pool/src/task_center/scheduler.rs)：规则结果的通用执行、等待关系和隔离。
5. [mod.rs](../crates/storage-pool/src/task_center/mod.rs)：唯一主循环；接收提交、控制和 Driver 事件。
6. [control.rs](../crates/storage-pool/src/task_center/control.rs)：按 TaskId 控制当前执行；暂停/继续不会重建。
7. [recovery.rs](../crates/storage-pool/src/task_center/recovery.rs)：读取持久记录，注册原 Task，调用工厂重建执行。

```text
Pool 请求 / topo 事件
    ↓
TaskCenter：TaskId → TaskRecord
    ├─ PoolRules：判断两件业务之间的关系
    ├─ scheduler：维护关系、发控制请求、判断条件、发 StartPermit
    ├─ registry：统一查询与观察，包括尚未重建执行的任务
    ├─ TaskStore（可选）：历史、恢复参数、检查点
    └─ WorkflowDriver：推进当前执行，报告步骤状态和安全暂停确认
```

Driver 内部的 `ExecutionRegistry` 只按 WorkflowId 保存执行观察。
它不决定哪个业务可以开始，也不分配逻辑 TaskId。

## 身份与接口

`TaskRun::id()` 返回稳定的 TaskId；`execution_id()` 返回本次执行的 WorkflowId，仅供诊断。
暂停/继续不改变这两个 ID；接管保留 TaskId，创建属于新宿主的执行。
WorkflowId 是宿主内的序号，跨宿主可能重复，必须结合 PoolInstanceId 解释。
跨池引用为 `TaskRef { pool: PoolUuid, task: TaskId }`。

```rust,ignore
let mut run = pool.shrink_disk(disk).await?;
let id = run.id();
let task = pool.task(id)?;
let mut observer = pool.observe_task(id)?;
pool.suspend_task(id).await?;
pool.wait_suspended(id).await?;
pool.resume_task(id).await?;
let result = run.result().await?;
```

`TaskSnapshot` 包含逻辑身份、业务分类、状态、错误、准入解释 `conflicts` 和当前等待 `waits`。
`execution: Option<ExecutionSnapshot>` 提供执行状态、进度和 Activity 详情。
恢复出的任务尚无 Future 时，execution 为空，但仍可查询、观察并保留占用。
同一个观察者能看到该 Task 随后绑定的新执行。

`suspend/resume/cancel` 是对当前执行的即时控制；异步 `suspend_task/resume_task/cancel_task`
还会在配置了 TaskStore 时等待历史写入。取消尚未重建的任务也走 `cancel_task`，确认持久取消后释放占用。
`resume_task` 只解除用户暂停，返回 `()`；重建需要自动接管或显式 `retry_task`。
`TaskRun.control` 绑定原执行，不能替代切换后通过 TaskId 重新查询。

## 规则只描述关系

| 决定 | 调度器行为 |
| --- | --- |
| Independent | 不建立等待关系 |
| Wait | 等已有任务以确定结果结束 |
| CancelThenWait | 请求取消已有任务，并等它实际结束 |
| SuspendThenWait | 请求已有任务安全暂停，确认后放行；请求者结束后撤销自己的暂停原因 |
| Reject(reason) | 拒绝新任务，不改变任何已有任务 |

默认 PoolRules 包含池级缩容/topo 关系、节点版本、重复请求和对象重叠规则。
规则先检查全部冲突，准入和容量检查通过后才执行取消或暂停。
对象关系来自 Workflow 声明的完整对象集合，不能靠字符串猜测拓扑。

新增业务通常只修改 PoolRules。独立嵌入者也可实现 `TaskRules`，通过
`TaskCenter::with_rules` 和 `submit_operation` 接入自己的分类。
`scheduler.rs` 不识别 Shrink、Topology、Node 或 Disk，只解释规则结果。
通用机制测试见 `memory_tasks_and_custom_rules_share_identity_control_and_wait_observation`。

## 有状态的等待与唤醒

每次准入建立明确关系，关系保留到相应任务确定结束。`pump` 读取关系与执行事实，
计算当前未满足条件，再同步更新控制和观察；不会重新写一套 topo 判断。

```text
缩容 S 正在迁移
  → topo T1 受理：建立 T1 等 S.SafePause，S 等 T1.Release
  → S 的 Activity 向均衡服务发 Pause，等待确认
  → Driver 报告 S 安全暂停，pump 发出 T1 的 StartPermit
  → T2 受理：再增加一个暂停原因
  → T1 结束：只删除 T1 对应关系，S 仍等 T2
  → T2 确定结束：撤销最后一个系统暂停原因，唤醒 S 的条件检查
  → 条件满足且无用户暂停：继续原执行、原均衡 job
```

TaskCenter 主循环不能 await 暂停确认，否则 Driver 无法继续推进并产生确认。
它只发请求、记录条件，随后继续轮询 `driver.tick()`。状态变化再进入 `reap → pump`。
`reap` 收走已确定结束的执行并释放关系；Uncertain 转成隔离，继续保留关系。
`pump` 根据现有状态放行可运行任务；通知只触发重检，不替代条件本身。

`waits` 是实际执行使用的等待关系视图：

- Completion：等另一个 Task 确定结束。
- SafePause：等另一个执行确认安全暂停。
- Release：当前暂停由哪个 Task 请求，需等它结束。
- Reconciliation：前序任务尚未重建，或结果不确定。

观察者既订阅执行变化，也订阅关系变化。因此多个 topo 只结束其中一个时，即使缩容仍是 Paused，
界面也能收到等待原因变化。业务条件不满足的具体原因仍在 execution.state 的 WaitingConditions 中。

## 单池缩容入口

每池最多一个未结束缩容。排队、用户暂停、条件等待、待恢复和 NeedsCheck 都保留名额；
第二个缩容直接拒绝。确定结束后可提交下一次。

无需保存 ID 的后端接口可以直接调用：

```rust,ignore
let current = pool.current_shrink(); // Option<TaskSnapshot>
pool.suspend_shrink().await?;
pool.resume_shrink().await?;
pool.cancel_shrink().await?;
```

这些方法从同一个 TaskCenter 目录定位任务，没有第二份缩容状态。
没有正在进行的缩容时，查询返回 None，控制返回明确错误。

## 历史与隔离

普通终态记录按 `Config::task_history` 有界保留；已获取的 observer 不随目录淘汰失效。
Activity 记录自动生成，`Config::activity_history` 限制热缓存，total/omitted 说明截断。
watch 是合并的当前状态订阅；完整持久历史通过 TaskStore 查询，见 [持久化](task-persistence.md)。

Uncertain 不释放对象和暂停原因。已有相交等待者得到 Rejected；新的相交请求在准入阶段拒绝，
不会取消别的活动任务或扩大隔离范围。业务确认完成前不能用普通 cancel 清除隔离。
恢复过程和 Activity 控制分别见 [恢复](task-recovery-design.md)、[控制](activity-control.md)。
