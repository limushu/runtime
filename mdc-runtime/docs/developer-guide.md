# 开发指南

## 1. Service 拥有什么

```rust
pub struct RebuildService {
    metadata: RebuildMetadata,
    router: Router<ServiceKind>,
    tasks: ServiceTaskManager<ServiceKind>,
}
```

核心元数据和任务策略都属于 Service。其他模块只能通过 Router 找到类型化 `ServiceClient`，不能取得 Service 或其元数据。

```rust
impl Service for RebuildService {
    type Key = ServiceKind;

    fn task_manager(&self) -> &ServiceTaskManager<ServiceKind> {
        &self.tasks
    }
}
```

框架提供 `ServiceTaskManager` 的 TaskId、槽位、取消、等待和观测机制。Service 决定何时调用 `create_new_task` 以及使用何种冲突策略；复杂模块可以覆写 `Service::create_new_task`。

## 2. Handler Future 是执行主体

```rust
async fn handle(
    self: Arc<Self>,
    request: RebuildRequest,
    context: RequestContext,
) -> Result<RebuildResponse, RebuildError> {
    match request {
        RebuildRequest::Start(disk) => {
            self.rebuild_workflow(disk, context).await
        }
        RebuildRequest::Query(disk) => Ok(self.query(&disk)),
    }
}
```

Executor 调用 `service.handle()` 取得 Future，并将其放入 `HandlerSet`。Service 的唯一 Tokio 宿主任务通过 `FuturesUnordered` poll 所有 Handler Future；业务代码不需要 `tokio::spawn`，也不需要 Future factory 闭包。

## 3. Task 是可选控制作用域

```rust
let task = self
    .create_new_task(
        &context,
        TaskMeta::new(
            TaskKey::new(format!("rebuild/{disk}")),
            format!("rebuild disk {disk}"),
        ),
        ConflictPolicy::Reject,
    )
    .await?;
```

这一步完成：

- 申请 TaskId；
- 按 TaskKey 准入、排队或替换；
- 建立取消作用域；
- 发布 Task 快照和事件；
- 返回 `TaskContext`。

Task 不保存也不 poll Handler Future。当前实现把 Task 生命周期绑定到创建它的 Handler 请求：Handler 返回时，ServiceTaskManager 根据请求结果发布 Completed、Failed、Cancelled 或 Aborted，并释放对象槽位。

查询不调用 `create_new_task`，因此只产生一个很短的内部 Handler Future，不会进入 Task 观测和排重系统。

## 4. Router 负责跨 Service 调用

```rust
let child_context = context.with_task(&task);

self.router
    .call::<BgService>(
        &ServiceKind::Bg,
        BgRequest::Rebuild(disk.clone()),
        child_context,
    )
    .await?;
```

`RequestContext` 的关键字段：

```text
request_id    当前 Service 内唯一的请求身份
operation_id  整条入口操作共享的身份
trace         trace 传播信息
task          Option<TaskRef>，可选父 Task 信息和取消作用域
```

Router 负责查找 Client、mpsc/oneshot、传播上下文、等待下游结果，以及父取消时请求下游 Task 优雅取消。`TaskContext` 不知道 Router、ServiceKind 或请求协议。

未来切换 RPC 时，只需把 `TaskRef` 转换为可序列化的 task/operation 标识，并由 Router Client 实现取消 RPC；工作流调用形式不变。

## 5. 冲突和替换

默认拒绝同 TaskKey 并发：

```rust
ConflictPolicy::Reject
```

高优先级操作替换旧操作：

```rust
ConflictPolicy::Replace(CancelReason::Preempted {
    by: key.clone(),
})
```

替换过程：

```text
旧 Handler 持有 Running Task
        │ 新 Handler 请求相同 TaskKey
        ├── 旧 Task -> Cancelling
        └── 新 Task -> Queued，新 Handler 在 create_new_task().await 处 Pending

旧 Handler 等待下游终态后返回
        └── ServiceTaskManager 释放槽位并唤醒新 Handler
```

旧、新业务逻辑不会重叠执行。

## 6. 优雅取消

```rust
ticket
    .cancel_and_wait(CancelReason::requested("operator cancel"))
    .await?;
```

取消链：

1. ControlHandle 将取消请求交给目标 Service 的 TaskManager；
2. Task 进入 Cancelling，取消作用域触发；
3. Router 感知父取消，请求下游 Service 取消对应 Task；
4. 最末端 I/O 使用自己的 TaskContext 停止外部动作并等待确认；
5. 下游 Handler 逐级返回；
6. 根 Handler 返回后，TaskTicket 收到最终 Cancelled。

正常取消不 drop Handler Future。只有 `ShutdownMode::Immediate` 或 `ServiceGroup` 异常 Drop 才强制终止宿主 Future。

## 7. Client 调用方式

```rust
// RPC 风格：只关心最终结果
let response = client.call(request).await?;

// 控制风格：若工作流创建 Task，则取得 TaskTicket
match client.submit(request).await? {
    Submission::Reply(result) => { /* 无 Task 的短请求 */ }
    Submission::Task(ticket) => {
        let task_id = ticket.task_id();
        let exit = ticket.wait().await?;
    }
}

// 消息风格：只确认入队
client.send(request).await?;
```

`submit()` 会等待 Handler 创建首个 Task 或直接完成；`send()` 不等待分类，适合真正的 fire-and-forget 请求。

## 8. Code review 清单

- `handle` 是否是自然的 async 成员方法？
- 查询是否不创建 Task？
- 长流程是否只在确实需要排重、取消或观测时创建 Task？
- Service 是否持有自己的 `ServiceTaskManager`？
- 跨模块是否只通过 `router.call()`？
- `TaskContext` 是否没有承担通信职责？
- 是否存在工作流内部的 `tokio::spawn`？
- 相同对象是否使用稳定 TaskKey？
- Replace 是否等待旧 Handler 真正退出？
- 最末端 I/O 是否响应 TaskContext 的取消？
- 元数据锁是否保持私有且不跨 `.await`？
