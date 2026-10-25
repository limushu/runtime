# TaskCenter：已有 A，来了 B，应该怎样处理

每个 Pool 有一个 TaskCenter，在自己的事件循环中推进 WorkflowDriver。
Task 是管理与观测身份，Workflow 是执行体，Activity 是一次领域调用。持久化与否不改变这套模型。

## 一套规则入口

保留 `SchedulingPolicy<PoolOperation>`，只有一个纯函数：

```rust,ignore
fn on_task(&self, existing: &PoolOperation, incoming: &PoolOperation) -> Conflict;
```

`existing` 是较早接受的任务，`incoming` 是后来到达的任务。
准入检查和运行中的重新判断都调用这个入口，不再拼接 admission、正反 relation 和数值 priority。
默认规则在 [conflicts.rs](../crates/storage-pool/src/task_center/conflicts.rs)，框架执行规则的代码在
[scheduling.rs](../crates/workflow-foundation/src/scheduling.rs)。`TaskCenter::conflict(incoming, existing)`
保留原来的调用顺序，作为默认业务规则的兼容入口；没有第二套规则实现。

| ConflictDecision | 含义 |
| --- | --- |
| Independent | 这条规则允许并行，不添加等待关系 |
| Wait | 新任务等待已有任务结束 |
| CancelThenWait | 新任务接受成功且成为队首后，请求已进入业务体的 current 取消，等待真实终态 |
| SuspendThenWait | 队首请求 current 停稳，确认暂停后放行；队首结束或撤销时交接暂停原因 |
| Reject(reason) | 拒绝新任务，不修改已有任务 |

先检查所有已有任务、容量和运行时接受结果；持久化任务还要等初始记录保存成功。
只有成功接受之后才应用取消和暂停。某条规则拒绝时，其他规则不能提前产生副作用。
规则应是确定性的纯判断；TaskCenter 提供请求、确认和启动许可的执行机制。

## current + queue

对存在协调规则的任务，按 TaskId 创建顺序确定 current 与后续 queue。只有队首可以请求
current 取消或暂停，其余任务等待队首；不再由每个新提交取消全部旧任务。互不相关的工作仍并行。
涉及多个对象的任务，必须在相关队列都到达队首，才能影响已有执行。调度结果从现有 entries
计算，不新增 Task 身份、pending/waiting 生命周期状态或每对象宿主。

持久化接收尚未完成的任务占据排队位置，但不能产生控制副作用；失败后清理该位置。
`check_submission` 只做提交规则检查及初始观测解释，`plan` 统一计算取消、暂停、阻塞与放行，
`pump` 应用结果。已删除准入阶段保存的 replacements 名单。

队首获得启动许可后才成为 current；Driver 首次进入 Workflow 业务体时设置 `has_started()` 并唤醒
调度器。策略取消必须等待这个标记，不能在启动 gate 或运行前的检查处跳过它。用户显式取消
排队任务及宿主关闭仍可结束未启动任务；本保证针对后继任务的替换。

```text
current A；queue [B, C]
    B 请求 A 安全结束；C 不能取消排队中的 B
A 结束 → current B；queue [C]
    B 进入业务体；C 此后可提出取消
    B 的不可中断步骤仍执行并返回结果
    B 在下一个普通控制边界响应取消
B 安全结束 → current C
```

暂停保留旧任务的执行上下文：暂停确认后队首可以执行，旧任务等待它结束。后续队首依次接管
暂停原因，不在交接间隙恢复旧任务。同一个 current 同时只由第一个冲突后继施加控制；这会让
原先共同暂停一个任务的多个后继依次处理，是有意选择的简单队列语义。

规则仍须通过业务测试验证组合，不提供任意自定义规则的死锁自动修复。

## 默认业务规则

| 已有任务 | 新任务 | 处理 |
| --- | --- | --- |
| 缩容 | 缩容 | 拒绝重复缩容 |
| 缩容 | 拓扑 | 暂停缩容，真实停稳后放行拓扑 |
| 拓扑 | 缩容 | 缩容等待拓扑结束 |
| 缩容 | 普通卷操作等非拓扑工作 | 允许并行，由领域校验和 Activity 资源声明保护具体动作 |
| 普通非拓扑工作 | 缩容 | 同上 |
| 同一节点的旧状态变化 | 更新的状态变化 | 取消旧任务，等待收尾 |
| 同一节点的新状态变化 | 陈旧状态变化 | 拒绝 |
| 其他已有任务 | 重复操作 / 对象重叠 / 无关操作 | 分别拒绝 / 等待 / 并行；以规则函数为准 |

拓扑分类沿用节点、磁盘、Tier 和池生命周期操作。Volume 创建/删除、RenamePool 和 Custom 不属于拓扑。
缩容不再自动持有对所有普通任务生效的整个任务生命周期互斥。
这移除了旧的“缩容等拓扑、拓扑等卷、卷等缩容”中的最后一条无必要依赖，不用数值优先级补救。
拓扑队首持有策略暂停原因，后续任务依次接管。用户 resume 只移除用户原因，不解除队首的暂停要求。

效果不确定仍保留隔离：涉及隔离对象或与隔离任务有协调规则的新任务不能越过它。
隔离不是普通暂停，不因 Independent 或用户 resume 而解除。当前没有自动对账/解除隔离接口。

## 一条可追踪的暂停链

```text
RunControl::suspend / TaskCenter 设置策略暂停原因
  -> control.rs 更新请求代次并通知
  -> ActivityContext::control() 随请求到达领域执行端
  -> 领域服务停稳，返回明确结果（或不能中断时等待自然完成）
  -> workflow.rs 的 drive_activity 返回结果，不提前确认暂停
  -> Workflow 消费部分结果、保存检查点
  -> wait_until_runnable 检查没有在途动作，再确认 Task = Suspended
  -> 调度器收到 ACK，放行等待的任务
  -> 用户恢复 / 等待任务结束，移除对应暂停原因
  -> wait_for_readiness 重新检查 RunCondition
  -> Workflow 显式发起下一次普通 Activity
```

执行期间，`driver/execution.rs` 同时推进业务 Future 和控制意图持久化；持久化任务的用户取消
先落盘再投递取消 token。暂停请求可先被领域端观察，安全边界仍需完成持久化和停稳确认。

没有 ActivityControl、register_control 或 set_activity_control，也没有挂起的 Activity 执行实例。
`ActivityExit::Suspended(checkpoint)` 保留现有结果名称，表示这一次调用已经安全结束，只有数据可续作。
ActivityHistory 中这次调用是 Completed；业务是否完成由 ActivityExit 和 Workflow 阶段表达。
框架不自动重试/重放，不把暂停请求当成已完成。完成与暂停竞态时保留实际完成结果。

当前缩容在 `migrate_to_completion` 中明确写出“调用、处理结果、保存、等待、续作”；外层阶段 match
只表达业务阶段推进。不同 Workflow 的停止方式可以由请求及领域服务定义，无需框架回调。

取消同样通过 token 交付。执行中继续等待实际业务结果，等待暂停/条件时则响应取消且不再派发调用。
持久化取消意图确认写入后才向下交付。C 控制接口只确认指令交付，仍须等真实 completion。
不能中断的领域调用可能延迟暂停或取消；不能用超时或丢弃 Future 假造停稳。

## 不可中断的必做步骤

现有调用方式保持兼容，只有显式选择的步骤受保护：

```rust,ignore
ctx.activity(&node, disconnect).uninterruptible().await?;
// 消费断链结果，必要时 save_checkpoint；随后响应已经记录的取消/暂停。
ctx.activity(&vd, rebuild).await?;
```

`uninterruptible()` 从第一次 poll 起覆盖获取资源、派发、服务执行与实际回复。
Task 已进入业务体后，即使取消已经到达，也不会跳过这个调用。服务得到独立的控制 token，
不会从 cancellation 或 control 任一路径收到 Task 的取消/暂停。调用返回真实结果，调用者可以
更新进度和保存检查点；下一次普通 Activity 或 wait_until_runnable 再响应原控制请求。
该选项不会检查 Task 的 RunCondition，适合必须完成的失效/清理步骤；普通调用仍检查准入。

保护不绕过资源锁、隔离、持久化错误、服务关闭、宿主关闭或失主。不可中断不等于必然成功，
错误照常返回；不能确认外部效果时，领域应返回 Uncertain，以隔离后继。已知失败后的恢复措施
仍由业务决定。框架不会自动重试，也不保证跨崩溃 exactly-once。

节点观察，以及节点/磁盘离线的失效和关 IO 前缀已显式标记；等待重建及后续步骤仍可取消。
当前 Node::Observe 失效的是本地 connected/loaded 状态，仓库尚无真实网络断链接口，不能把它
当作远端连接已经关闭。实际断链适配器接入后应放在这个受保护前缀中。

## 观察与持久化

对外读取 `TaskSnapshot`，订阅使用 `TaskObserver`，持久化使用同一 TaskId 的 `SavedTask`。
TaskInfo、TaskRecord、ExecutionView 是内部信息或访问入口，不是新的 Task 身份。

- conflicts 是接受时的规则解释；blockers 是当前等待的任务。
- suspend_requested 和 suspend_acknowledged 分开表达请求与实际确认。
- details 允许 Workflow 上报自己的类型化业务信息。
- Activity 历史只记录实际调用；watch 通知可能合并，快照不是跨字段原子读取，也不是审计日志。
- TaskStore 保存恢复数据，不保存完整 Activity 历史或 Rust 执行栈。

恢复入口在 `driver/recovery.rs::restore`：读取待恢复 Task → 全量校验并重建执行体 →
逐条 CAS 接管记录 → 注册 Task。`Workflow::restore` 只是全量校验阶段的执行体构造函数，
不负责恢复 Task 身份、控制端或调度状态，也不应执行领域动作。
记录接管不是批量事务；中途写失败不会注册任务，重新调用 restore 会重新加载存储。
接管后再次检查宿主有效性；构造整批执行体后登记，登记阶段不再调用 Workflow 声明或执行可失败校验。
注册后的 Task 在首次 tick 前就能查询原 details、objects 和用户暂停请求。
TaskCenter 随后重新计算规则、登记占用，再开放 start gate。
TaskId 保留，用户暂停保留，暂停确认重新产生，策略暂停原因从恢复后的任务集合重建。

## 验证入口

- [规则与控制测试](../crates/storage-pool/tests/scheduling_policy.rs)：接受失败无副作用、真实停稳确认、暂停原因。
- [真实缩容测试](../crates/storage-pool/tests/shrink_workflow.rs)：并行卷操作、拓扑交接、检查点恢复和领域约束。
- [基础控制测试](../crates/workflow-foundation/tests/managed_workflows.rs)：不能中断的调用、取消竞态、完成事实、恢复前检查。
- [演示](../crates/storage-pool/examples/managed_shrink.rs)：token 停稳返回、保存检查点、同一 Task 继续。

- [队列与必做步骤时序](../crates/storage-pool/tests/task_queue.rs)
- [不可中断调用边界](../crates/workflow-foundation/tests/uninterruptible.rs)
