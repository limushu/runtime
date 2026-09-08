# Service Runtime：生命周期、任务与观测

状态：`B0.7 已实现`。

本章定义 Pool 内领域 Service 共用的运行组件。它解决的是“服务如何运行和被管理”，而不是
“某个业务下一步应该做什么”。MemberDisk 是第一个适配者，Node、VD、BG 可以复用同一
运行契约，但不要求复用 MemberDisk 的对象冲突策略。

![Service Runtime 契约](assets/service-runtime-contract.svg)

## 1. 边界

Runtime 负责：

- typed request/response channel 与容量背压；
- 一个根 task 统一 poll 多个 handler Future；
- 高优先级控制通道与服务生命周期；
- 请求、Operation、Task 和 Trace 的关联；
- Task 进度、里程碑、阻塞原因、精确取消和完成结果；
- 同 Key 在途工作所需的 Join/Queue/Cancel/Promotion/WaitIdle 机械机制；
- 当前快照、实时结构化事件和可插拔导出；
- 协作停服以及最终强制回收。

Runtime 不负责：

- 判断 DOWN 是否覆盖 UP，或 Shrink 是否必须继续；
- 决定 BG 如何重建、Tier 如何选盘、Node 如何广播；
- 保存领域对象的权威状态；
- 把业务流程改写成 Action DSL；
- 根据请求类型猜测目标服务。

## 2. 一个实例，四类句柄

`ServiceRuntime::spawn(service, config)` 返回 `ServiceInstance<M>`：

| 句柄 | 面向谁 | 职责 |
| --- | --- | --- |
| `ServiceClient<M>` | 业务调用方 | `call(request)`；目标实例已由持有的 Client 明确 |
| `ServiceControl` | Pool/运维控制面 | Pause、Resume、Drain、Stop、CancelTask |
| `ServiceObserver` | TUI、测试、审计适配器 | 当前快照、watch 变更、事件订阅、有限历史 |
| `ServiceTask` | Pool 所有者 | 持有唯一根 Tokio task；Join 或故障隔离 Abort |

Client 不持有业务 Service 对象，也不能绕过 mailbox 直接访问元数据。Control 使用独立有界
通道，并在根循环的 `select!` 中具有优先级，不会被业务队列挤占。Observer 只读。Task 的
所有权必须保存在 Pool 的装配结构中；直接丢弃它会触发 abort，从机制上避免根 task 泄漏。

## 3. 生命周期

![Service 生命周期](assets/service-lifecycle.svg)

| 当前状态 | 接受新业务 | 已接收 Future | 允许的主要控制 |
| --- | --- | --- | --- |
| `Initializing` | 可以进入有界队列，等待初始化完成 | 尚未启动 | Drain、Stop |
| `Running` | 是 | 正常 poll | Pause、Drain、Stop、CancelTask |
| `Paused` | 否，立即返回明确错误 | 继续 poll | Resume、Drain、Stop、CancelTask |
| `Draining` | 否 | 不取消，等待自然完成 | Stop、CancelTask |
| `Stopping` | 否 | 请求协作取消，等待稳定退出 | CancelTask |
| `Stopped` | 否 | 无 | 无 |
| `Failed` | 否 | 无 | 无；由所有者决定重建实例 |

三个关闭语义必须区分：

- `drain()`：停止接收新业务，不取消已接收 Future；全部完成并执行 `shutdown()` 后返回；
- `stop()`：取消 Service 根 token 和所有可取消 Task，等待业务在稳定边界退出，再执行
  `shutdown()`；
- `ServiceTask::abort()`：直接 drop 根 Future，只用于超时、故障隔离或进程卸载的最后手段。

初始化和关闭 hook 都可以读取 `ServiceContext::cancellation()`。因此 Stop 能协作结束阻塞的
初始化；不响应取消的第三方调用仍由上层超时后执行强制 abort。

根循环捕获 Service handler panic，并将实例切换为 `Failed`、清空在途运行投影、记录错误。
普通 handler 返回 `Err` 只表示该请求失败，Service 仍可继续运行。

## 4. Future、Operation 与 Task 不是同一个概念

```text
Request
  -> handler Future                 每个请求都有，由根 task poll
       -> OperationContext          有业务意义的请求才可观测，跨服务保持因果身份
            -> TaskAttempt (0..n)   需要进度、审计或控制时才创建
```

Query 仍是 Future，但通常没有 Operation 事件和 Task。业务工作流也不需要被塞进
`TaskSpec::new(move || ...)`：Runtime 已经在执行 `service.handle(...)` 产生的 Future；业务在
真正需要管理的阶段调用 `context.start_task(...)`，只是给当前工作附加控制与观测，不改变
正常 `async fn` 的书写方式。

`OperationContext` 是稳定的因果身份，包含 OperationId、scope、kind 和 TraceContext；它不
保存业务 phase/status。`TaskAttempt` 是一次临时执行尝试。影响恢复正确性的事实必须提交到
领域 SDB，不能只存在于 Operation 或 Task 记录里。

跨 Service 协作通过目标 Client 显式调用：

```rust
let bg = pool.bg_service();
let reply = bg.call_in(context.operation(), RebuildBg { bg_id }).await?;
```

这里负责通信的是 `bg` Client；`call_in` 只传播因果身份。Task 不负责路由或 RPC。

## 5. 服务适配接口

领域只实现三个最小边界：

```rust
#[async_trait]
impl ManagedService for MemberDiskService {
    type Message = MemberDiskMessage;

    async fn initialize(&self, context: ServiceContext) -> Result<()> { /* 可选 */ }

    async fn handle(
        self: Arc<Self>,
        message: MemberDiskMessage,
        context: RequestContext,
    ) -> Result<()> {
        // 一处显式协议分发；具体工作仍是 self.offline(...).await 等普通方法。
    }

    async fn shutdown(&self, context: ServiceContext) -> Result<()> { /* 可选 */ }
}
```

公开请求实现 `ServiceRequest<Message>`，静态声明 Response，并转换为目标 Service 私有协议。
这不是自动路由：调用方必须先持有准确的 Service Client。内部枚举 match 是协议边界的一处
显式分发，避免全局 Router、类型擦除和隐藏注册表。

## 6. 对象在途任务协调

MemberDisk 需要“同盘一个活动意图”，而 BLK 申请不需要。这个差异由领域选择是否使用
`ObjectTaskCoordinator<K, I, E>`，Runtime 不强迫所有请求进入对象槽。

```text
admit(key, input, domain_decision)
  Idle                 -> Active(ObjectLease)
  相同意图              -> Joined
  冲突意图              -> Pending；可选请求当前 Task 协作取消

active lease finish
  -> 按顺序提升一个仍有效的 Pending
  -> 无 Pending 时唤醒 WaitIdle，并保存最近结果
```

Coordinator 只执行机制。MemberDisk 的 `resolve_conflict` 决定相邻同类事件 Join、不同事件
QueueAndCancel。它保存的是运行槽位，不是 MemberDisk 状态副本，也不创建每盘 Tokio task。
槽位关联 `TaskControl` 后，业务冲突与运维 `cancel_task(id)` 走同一套精确取消路径。

## 7. 取消协议

取消令牌形成一棵树：Service token -> Request token -> ObjectLease/Task token -> 下游调用。
业务不需要在每一行检查 `is_cancelled()`；可取消等待使用 `select!` 或下游接口接收 token，
不可取消边界则故意不接收该 token。

MemberDisk DOWN 展示了两种边界：

1. 给所有可服务 user_dp 设置 DOWN/停 IO 是可靠性边界，不被 UP、Shrink 或 Stop 中途打断；
2. DOWN 已生效后，恢复窗口和 VDM 排空是可协作取消阶段；
3. 新事件只请求取消，旧 Future 稳定返回后 pending 才被提升；
4. 强制 abort 只由 ServiceTask 所有者在关闭超时后执行。

因此 Cancel 表示“请求尽快停在安全点”，不是“立刻 drop 任意 Future”。

## 8. 观测契约

`ServiceSnapshot` 提供：

- lifecycle、Idle/Busy；
- queued/in-flight、accepted/completed/rejected 计数；
- active Task 列表；
- 每个 Task 的 operation_id、trace_id、key、kind、状态、进度、里程碑、blocked_on；
- 最近错误。

`RuntimeEvent` 提供追加式结构化事件：生命周期和活动边沿、请求接收/拒绝/完成、Operation
开始/完成、Task 开始/进度/阻塞/取消/完成、领域对象状态转换。TUI 可以先读 Snapshot，再
订阅 Event 做增量更新，不需要解析日志。

`RuntimeEventSink` 是同步、非阻塞适配器；适配器应只做内存入队，持久化或网络发送由它
自己的 worker 完成。Runtime 捕获 sink panic，观测故障不能改变业务结果。内置 history 是
有限环形缓存，仅供实时诊断和测试，不是审计数据库或业务 WAL。

## 9. 可测试性

组件测试不依赖 sleep 猜测状态，而是通过 Observer、Semaphore 和 Control 精确同步：

- 初始化可见且请求在初始化期间等待；
- Pause 拒绝新请求，Resume 恢复；
- Drain 等待已接收工作，Stop 协作取消；
- 按 TaskId 精确取消并审计原因；
- Query 不产生 Task；Idle/Busy 只在边沿变化；
- 强制 abort 以及丢失 ServiceTask 所有权都不会泄漏 Future；
- 结构化事件可导出；
- handler panic 被隔离并准确投影为 `Failed`；
- MemberDisk 重复 DOWN 合并，UP 冲突取消，状态转换和 Trace 可见；
- 有在途生命周期事件的磁盘不会参与新 BLK 分配。

## 10. 当前限制与后续扩展

- Runtime 是当前单 crate 内的独立模块。第二个真实领域接入后若 API 仍稳定，再考虑拆 crate；
- 内置历史只在内存中。跨 Monitor 主切换的审计存储需要实现外部 `RuntimeEventSink`；
- Service Runtime 不替代 `PoolManager -> Pool -> Domain Client` 多 Pool 装配；该层仍待实现；
- 第一阶段 MemberDisk 修改由一个 `mutation_gate` 串行。以后可以按 Tier/Partition 缩小并行
  粒度，但必须保持 SDB-first、无重复分配和释放顺序不变量；
- 需要恢复的长流程必须从领域 SDB 和现实状态重新收敛，不能反序列化 Future 或 Task。
