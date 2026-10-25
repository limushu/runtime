# 当前架构与依赖边界

## 组合结构

```text
PoolManager
└─ Pool（组合根 pool.rs）
   ├─ TaskCenter（一个事件循环）
   │  ├─ 冲突判断、队列、逻辑对象占用与隔离
   │  └─ 直接推进 Workflow Future（嵌入通用执行驱动，无独立 task）
   ├─ DiskService + DiskHost（独立服务宿主）
   ├─ NodeService + NodeHost（独立服务宿主）
   └─ VdService + VdHost（独立服务宿主）
```

Task 管理一个 Workflow 的完整生命周期，Activity 是其步骤。硬盘上线是一个流程，Add/Open/Enable
是内部步骤。PoolHandle/PoolManager 保留类型化业务入口；自行组合应用时可向 TaskCenter
提交 PoolWorkflow，并在构造 TaskCenter 时注入 SchedulingPolicy。策略不进入领域模型。
Pool 没有单独的 runtime 字段，也不使用 RuntimeHandle 向另一条协调队列提交。

## 按业务组织，而不是把同一领域拆散

domains 表示“业务领域分组”，不是声称其中所有文件都是 DDD 纯领域层。

- `domains/{disk,node,vd}/model.rs`：对象、不变量、状态规则，只依赖标准库及其他领域模型。
- `domains/{disk,node,vd}/service.rs`：能力与已确认状态；组织领域校验、数据面操作、元数据提交。
  不实现 ServiceHandler、不使用 ActivityContext、不认识托管自己的 runtime、不维护对象冲突队列。
- `domains/{disk,node,vd}/runtime.rs`：服务托管适配，声明步骤资源并转发能力调用；runtime 依赖 service，
  service 依赖 model，不能反向依赖。
- `domains/mod.rs`：公共身份和 DomainError；错误转换放在最外层 support.rs。
- `ports.rs` / `adapters.rs`：外部接口与模拟实现。取消/IO 是应用能力边界，不进入领域模型。
- `workflows/`：通过 ActivityClient 的类型化能力通道调用步骤，不知道具体服务宿主类型。
- `task_center/`：同一组件维护队列、停稳交接与隔离；conflicts.rs 提供默认存储策略。
  单一规则 on_task(existing, incoming) 的执行位于 foundation/scheduling.rs，事件循环不判断 topo/shrink 业务类型。
- `pool.rs`：组合根，装配 TaskCenter 和领域服务。不把组合逻辑放入模型。

自动测试检查 model 不引用 Tokio/执行框架，service 不依赖 runtime，Workflow 不引用服务宿主。
当前 service/ports 使用执行层 Outcome 和取消令牌，model 返回独立 DomainError。

## 一个循环协调并执行

TaskCenter 的 select 同时处理新请求、停止请求和 driver.tick()。
WorkflowDriver 是一组可轮询的执行状态/Future，本身没有提交队列，也不会 spawn 执行宿主。
defer() 是同步登记，不是发消息给另一个 Runtime。TaskCenter 持有启动许可，冲突消除后放行；
在同一个循环内轮询多个 Workflow，因此不需要 await 整个流程才接收下一条请求。

WorkflowRuntime/RuntimeHandle 仅是基础库给无自有事件循环的示例使用的可选独立封装，
代码位于 standalone.rs，存储 Pool 不构造它。ExecutionView 只共享观察/控制状态，
不调度、不提交、不启动 task。

TaskCenter 检查全部在途冲突及容量。只有成功接收且成为队首才实施控制；策略取消还须等待 current 进入业务体。旧流程和 Activity 实际终态前
不释放逻辑对象。排队任务计入容量、可取消；丢弃调用者/Run 不丢失已接受执行。
不确定结果保留逻辑对象隔离；被隔离的新任务不会取消其他活动任务，被拒绝的后继不扩散隔离。

受控执行不再重复取得 Workflow 对象锁。驱动保留 Activity 资源读写保护、停稳跟踪与资源隔离，
尤其保护 VD 反向引用索引等共享结构。服务内部没有业务对象排队器。
通用独立 submit 仍提供简单对象互斥供示例/测试使用，不能绕过 Pool 正式入口混用。

暂停通过随 Activity 请求传递的 token 协作执行；领域调用停稳返回后，Workflow 保存检查点，
到达安全边界才确认 Task 暂停。没有保留调用的 Activity hook，也没有框架自动重试。
TaskCenter 按 current + queue 应用 on_task 的决定：等待、取消后等待、暂停后等待、拒绝或并行；后续任务不能越过队首控制 current。
缩容与普通卷操作不再因对象重叠自动互斥；Activity 资源声明和领域版本校验保护具体动作。
用户与策略暂停原因分别维护；新调用前重新检查业务准入。
pool.scheduling() 表示业务排队/运行/逻辑隔离；pool.execution() 表示底层执行与资源计数。
pool.task(id).conflicts 是准入时解释，不是实时等待图。
同一 snapshot 的 blockers 是当前阻塞者，details 是 Workflow 提供的附加信息。

## 持久化与恢复

Workflow 声明 Memory 或 Durable，Driver 写入同一个 Task 的存储记录。
恢复通过 Workflow::restore 重建流程及依赖，装回全部调度占用后才放行启动许可；
业务从显式 checkpoint 继续，不能自动恢复 Rust Future 的程序计数器。
TaskId 保留创建时轮次，写入使用当前 owner_epoch 和 revision 做 fencing/CAS。
选主轮次、数据面 fencing、当前 Pool 目录及节点/层级快照由应用提供。
正式入口 `PoolManager::with_task_store(...).activate(recovery)` 恢复领域视图、成员索引及 Task，
配置存储后 PoolHandle::shrink 自动持久化；失主调用 demote 等待真实停稳。
未配置存储的旧 create/create_pool 仍为内存运行。可运行链路见 [主备切换](leader-handoff.md)。
当前控制与规则契约见 [TaskCenter](task-center.md)。

基础库源码按职责划分：`workflow.rs` 提供业务契约和上下文，`control.rs` 提供请求/token/确认，
`driver/execution.rs` 包装 Task 的完整执行和持久化时序，`driver/recovery.rs` 负责接主恢复。
`persistence.rs` 定义存储协议和写入机制；`Dependencies` 与 `Restored` 仅在该模块保留旧路径重导出。
`tasks.rs` 管观测和索引，调度规则的定义与计算均在 `scheduling.rs`。
完整文件职责表见 [基础库说明](../crates/workflow-foundation/README.md)。

## 限制

没有持久化 Activity 历史、确定性重放、自动重试、动态扩展对象集合或真实重建/成员删除。
正式 Pool 入口支持 Task checkpoint 跨宿主恢复；多机选主、共享存储和数据面 fencing 仍需生产适配器接入。
拓扑身份不可变；节点关联盘集合包含已预约但未提交的盘。资源声明仍较保守，同节点的部分
磁盘流程会串行，本轮不改变原有冲突范围。强制 Drop 不能证明远端 IO 已停止。
