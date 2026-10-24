# TaskCenter：策略、执行和停稳交接

一个 Pool 一个 TaskCenter，在同一个事件循环中推进 WorkflowDriver。Task 是统一管理与观察
对象，Workflow 是执行体，Activity 是领域步骤；持久与非持久流程走同一套路径。

## 能力边界

| 组件 | 负责 | 不负责 |
| --- | --- | --- |
| Workflow | 步骤、续跑 checkpoint、业务条件、自定义 details、是否持久化 | 跨任务队列 |
| SchedulingPolicy | 两个任务的准入决策与运行关系 | 执行领域命令、修改队列 |
| TaskCenter | 准入、排队、暂停原因、等待真实 ACK、启动许可、隔离 | 判断 topo/shrink 等具体业务 |
| WorkflowDriver | Future 推进、Activity、Task 观察、控制、存储 | 定义业务冲突与准入条件 |
| 领域服务及适配器 | 命令、合法性、实际停稳与恢复 | 判断哪个工作流应优先 |

## 策略接口

基础库的 `SchedulingPolicy<D>` 有两个纯判断入口，存储应用使用 `D = PoolOperation`。
组合根通过 `TaskCenter::with_policy(driver, config, policy)` 注入；默认 start 使用
PoolSchedulingPolicy。规则位于 [conflicts.rs](../crates/storage-pool/src/task_center/conflicts.rs)，
通用规划位于 [scheduling.rs](../crates/workflow-foundation/src/scheduling.rs)。

admission(incoming, existing) 决定是否接受新任务，以及是否请求旧任务取消：

| 结果 | 意义 |
| --- | --- |
| Independent | 准入时没有冲突 |
| Wait | 允许排队，具体运行关系由 relation 决定 |
| CancelThenWait | 请求旧任务取消，实际停稳后才交接 |
| Reject(reason) | 拒绝本次提交，不取消其他任务 |

relation(task, other) 决定已接受任务如何同时运行：

| 关系 | 意义 |
| --- | --- |
| Independent | 可并行 |
| Exclusive | 串行，等待已运行任务或更早的排队任务 |
| YieldTo(reason) | task 应让出执行权，other 等待 task 实际暂停后才启动 |

YieldTo 有方向，策略不能形成让出环。规划器不猜测资源、业务优先级或拓扑关系。
准入与运行关系应一致；Reject/CancelThenWait 在完成全部准入检查之后才应用。

默认存储策略保留节点序号、重复请求与对象交接规则，并声明缩容让出给 topo：
先到的 topo 阻止缩容启动；缩容运行期间到达 topo，先暂停缩容，再运行 topo。
多个 topo 分别形成暂停原因，最后一个结束才清除全部策略原因；用户暂停不会被一并清除。
默认策略将节点、磁盘、tier、池生命周期操作归入 topology；Volume 创建/删除和 RenamePool
不归入其中。应用如果需要不同分类，替换策略，不能在事件循环中补业务判断。

## 暂停不是通知送达

暂停请求带代次。TaskCenter 先更新原因，再用新代次的停稳 ACK 重新规划，才打开启动许可。
暂停期间收到新的控制请求使旧 ACK 失效；正在恢复的领域动作必须再次停稳才能重新交接。
用户 resume 只移除用户原因，不会越过仍在运行的 topo。

长 Activity 有两种接入方式，接口仍为 ctx.activity(...).await：

1. 调用保持等待，ActivityControl::suspend 返回真实停稳确认；Workflow 的 run_condition
   重新判断条件后，框架调用这个 Workflow 选择的 resume 适配器。
2. 服务停稳并返回业务部分结果；Workflow 保存 checkpoint 后到达 wait_until_runnable，Task 才确认暂停。
   恢复检查满足后继续下一次 Activity。没有假造统一的“Activity 暂停结果”。

如果服务不能暂停，框架只能等它自然完成。暂停保留 Activity 资源锁：让出关系不允许另一个
动作绕过读写保护；需要交接同一资源时，业务应选停稳后返回的方式，或调整真实的资源边界。
控制命令须能通过服务的并发命令/客户端路径到达，不能把 resume 排在自己占用的资源锁之后。
不确定结果继续隔离对象；新任务不得通过 YieldTo 越过隔离。没有自动解除隔离或自动修复策略环。

## 观察与恢复

task(id)、observe_task(id) 都返回 foundation 的同一个 TaskSnapshot/TaskObserver：

- conflicts：准入时的解释；blockers：当前阻塞者。
- suspend_reasons、suspend_acknowledged：暂停来源与真实确认。
- details：业务附加信息；details_as::<T>() 类型化读取。
- activities：当前宿主的调用历史，默认最多 1024 条，total/omitted 明确累计和淘汰数量。

观察通知可能合并，不能充当可靠事件日志。普通终态历史有界；Uncertain 保留。
Workflow 自声明 Durable 时，Driver 存储 Task input/checkpoint/details/控制意图/终态。
恢复先 driver.restore()，再 TaskCenter::from_restored(...)，一次装回全部占用再开始推进；
恢复保留原 TaskId，重新建立 policy 暂停原因，用户暂停从记录恢复。
Activity 历史和 Rust Future 不持久化。完整约束见
[统一 Task 规格](changes/2026-10-24-unified-task-controls.md)。

## 扩展及验证

新增业务实现 Workflow 与 PoolWorkflow，声明完整 objects；策略对这些描述作纯判断。
为新的关系补双向、重复/陈旧、多个阻塞者和停稳 ACK 的测试。
[scheduling_policy.rs](../crates/storage-pool/tests/scheduling_policy.rs) 用非存储业务策略验证可替换性。
[managed_shrink.rs](../crates/storage-pool/examples/managed_shrink.rs) 演示 topo 抢占与两种迁移暂停方式。
