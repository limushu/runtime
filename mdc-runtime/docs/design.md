# 设计说明

## 1. 最终抽象

```text
Service = 核心元数据 + Client + handle + async workflow
Task    = 身份/控制/观测 + Future 执行体
Executor = 业务通道 + 控制通道 + TaskSet + 唯一 poll 循环
Router  = ServiceKind -> 类型化 ServiceClient
```

没有 HandlerRegistry、Scheduler、Effect、Directive，也没有每业务一个 Tokio spawn。

![请求决策](diagrams/task-decision.svg)

## 2. 请求边界

`Service::handle` 是短小同步方法。它返回：

```rust
pub enum HandleResult<T, E> {
    Reply(Result<T, E>),
    Task(TaskSpec<T, E>),
}
```

因此简单 Query 只有一次 mpsc + oneshot；只有需要长期执行、取消、排重和观测的请求才成为 Task。

## 3. Task 所有权

公开结构：

- `TaskMeta`：key、label、visibility；
- `TaskSpec`：meta、冲突策略、Future factory；
- `TaskContext`：当前任务身份、trace、父子调用与取消；
- `TaskTicket`：等待、请求取消；
- `TaskSnapshot/TaskEvent`：TUI 观测。

内部结构：

- `TaskSet`；
- `TaskSlot`；
- `FuturesUnordered`；
- AbortHandle；
- pending replacement。

Service 能定义 Task，但不能直接 poll、删除或篡改 TaskSlot。

## 4. 结构化取消

![取消链](diagrams/cancellation.svg)

正常取消不 drop Future。Task 继续被 Executor poll，直到所有下游 Task 返回终态。

```text
取消信号：Disk -> Rebuild -> BG
终态确认：Disk <- Rebuild <- BG
```

每个 child call 使用子取消作用域：父取消会影响所有后代，子任务单独取消不会误伤父亲和兄弟。

`TaskContext::call` 是取消语义的统一实现点。未来改成 RPC 时，Local Client 的子 scope 可替换为 `CancelOperation(operation_id)` RPC，workflow 不变。

## 5. 同对象串行

TaskSet 以 `TaskKey` 建立对象槽：

```text
Running(old)
    │ replace request
    ├── old -> Cancelling
    └── new -> Queued

old -> Cancelled
    └── new -> Running
```

这保证同一对象的旧任务和新任务不重叠。本地核心元数据不需要用 generation 防止旧 Future 回写；如果将来存在跨进程旧 RPC、重启恢复或多实例写入，再在外部协议中加入 operation fencing。

## 6. 元数据模型

Service 是其核心元数据的所有者：

```text
DiskService    -> DiskMetadata
RebuildService -> RebuildMetadata
BgService      -> BgMetadata
```

元数据锁是 Service 内部实现细节。workflow 只能调用短小同步领域方法，不能取得 guard 或 `&mut Metadata`，因此不会产生持锁跨 `.await` 的后门。

## 7. 服务生命周期

- `Pause`：停止接收新业务，继续 poll 在途 Task；
- `Drain`：处理已经进入队列的请求并等待 Task 自然完成，然后 Paused；
- `Graceful Shutdown`：Drain 后停止；
- `Immediate Shutdown`：唯一允许强制 abort 全部 Future 的正常 API；
- Drop `ServiceGroup`：异常卸载保险，abort 唯一宿主 Tokio Task。

控制通道在 `tokio::select! { biased; ... }` 中优先于 Task 完成和业务通道。

## 8. 观测

`ServiceObserver` 提供：

- Service Lifecycle 与 Idle/Busy；
- 队列请求数和受管 Task 数；
- 当前 TaskSnapshot 列表；
- Task 状态事件：Queued、Running、Cancelling、Completed、Failed、Cancelled、Aborted。

TUI 只订阅 Observer，不读取 Service 元数据。

## 9. 代码导航

```text
service/mod.rs          Service trait 和 HandleResult
service/client.rs       call/submit/Ticket/父子调用
service/control.rs      生命周期与 Task 取消控制
service/group.rs        Pool 级 Service 容器
executor/runtime.rs     唯一 select/poll 循环
executor/task_set.rs    Task 索引、替换、取消、完成
task.rs                 业务可见 Task API
demo/disk_service.rs    Query 直返、Offline Task、Fault 替换
demo/rebuild_service.rs 中游取消传播
demo/bg_service.rs      下游 Task
demo/backend.rs         最末端 I/O 的优雅取消
```

自动化测试覆盖 Query 零 Task、三级取消等待、同对象替换串行和生命周期清理。
