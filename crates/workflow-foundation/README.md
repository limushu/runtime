# Workflow Foundation

业务无关的执行框架，不认识 Pool 的 Disk/Node/VD 冲突策略。crate 名称保留，
不是 DDD 领域层或业务共享内核。应用契约和通用技术执行机制位于此 crate。

## 主要类型

| 类型 | 含义 |
| --- | --- |
| WorkflowDriver | 嵌入调用方事件循环的被动执行驱动，不 spawn、不自带提交队列 |
| ExecutionView | 只读/控制句柄，不提交、不调度 |
| WorkflowRuntime | 可选的独立宿主封装，仅供无自有循环的使用者；存储 Pool 不用它 |
| RuntimeHandle | 仅为可选独立宿主提供本地调用句柄，TaskCenter 不持有它 |
| Workflow / WorkflowContext | 流程体、Activity/Signal/Timer、安全边界 |
| Run / RunControl | 类型化结果与执行控制 |
| ActivityClient | 带类型的能力通道，不暴露具体宿主类型给应用 |
| ServiceHandler | 基础设施的托管适配协议，不是 DDD DomainService |
| StartPermit | 应用拥有的一次性启动许可，无冲突判断逻辑 |

接口：[workflow.rs](src/workflow.rs)、[service.rs](src/service.rs)、
[driver.rs](src/driver.rs)、[standalone.rs](src/standalone.rs)、[model.rs](src/model.rs)、[tasks.rs](src/tasks.rs)。

## 应用管理的执行

```rust,ignore
let (run, permit) = driver.defer(workflow)?;
// 应用已经检查并登记协调状态；必要时等待旧任务真正完成。
permit.start();
// 或 permit.reject(error)；丢弃 permit 也不会偷偷启动流程。
```

defer 同步登记一个受控执行，计入容量，返回可观察/可取消的 Run。调用方必须在自己的
事件循环中轮询 driver.tick() 才能推进流程；驱动器自身不会启动任何 Workflow task。业务层自行维护完整对象集合、
排队、占用和不确定结果隔离。Runtime 不重复获得此执行的 Workflow 对象锁。
受控执行在许可到来前不运行流程体；取消可以唤醒它，暂停请求在获准后的安全边界确认。
Runtime 不提供 AdmissionPolicy、业务描述类型擦除或 submit_with_policy。

通用独立示例仍可以 submit(workflow)，Runtime 为该模式提供简单的 Workflow::objects()
互斥。这只是底层机制，没有存储业务规则；Pool 对外不开放此旁路。

## Activity 与服务托管

ServiceHandler 声明请求/回复/查询/视图和 resources。ActivityClient 只保留带类型的调用通道；
应用可以声明客户端类型而不依赖具体基础设施 handler。ServiceClient<S> 是已知宿主类型时的便捷别名。

每个服务有独立宿主；同一宿主可用 FuturesUnordered 等待多个步骤。WorkflowDriver 也通过
FuturesUnordered 推进多个 Workflow；在存储池中，它就在 TaskCenter 的事件循环内被轮询。

Activity 资源由运行时统一读写保护，持有到实际动作停稳；领域能力内部不排队。
多资源原子取得、读共享/写独占、冲突等待者 FIFO。不支持并行 Activity 或动态扩展流程对象。
跨服务的同一资源必须使用同一个 key；框架不会推断业务拓扑。

## 控制与安全语义

- 丢弃 Run 或 result 等待者不会取消已接收执行。signal 使用有界缓冲，timer 使用单调时钟。
- cancel 是协作请求，不是完成确认；Workflow 完成前等待所有已派发 Activity 停稳。
- 已完成不可中断提交可以返回成功，不能把发生的效果伪装成零副作用取消。
- Uncertain/panic 会保留步骤资源隔离，不能被 Workflow 忽略成成功。单纯被隔离的后继
  返回 Rejected，避免扩大隔离。在 defer 模式下，应用负责 Workflow 级对象隔离。
- ActivityContext 是适配层执行上下文，不应进入纯领域对象。
- pause 在 Activity/Signal/Timer 或 safe_point 边界确认；不抢占外部 IO，暂停不会回滚步骤。
  排队准入期间还不能即时确认 Paused。取消/关闭可以直接唤醒暂停。
- client 绑定 Runtime 实例，即使 Pool 名称相同也不能跨实例执行 Activity。
- shutdown 拒绝新工作、请求取消、等待停稳、关闭服务；等待可取消后重试。
  强制 Drop 只保证本地任务回收，不能保证远端 IO 停止。
- 任务索引按 ID 提供观察/控制。普通终态历史有界，Uncertain 保留；watch 可能合并事件。
  业务冲突解释属于 TaskCenter，不再由 Runtime 记录或解释。

没有持久化历史、确定性重放、跨主恢复、自动重试或完整 Temporal 引擎能力。
不可把重新创建同名实例当作恢复，也不提供执行不可信代码的沙箱。

## 验证

在 workspace 根目录：

```sh
cargo test --locked -p workflow-foundation
cargo clippy --locked -p workflow-foundation --all-targets -- -D warnings
cargo run --locked -p workflow-foundation --example two_services
```

[two_services.rs](examples/two_services.rs) 演示两个能力的串联。
DESIGN.md 保留历史阶段记录，当前接口以本页与源码为准。
