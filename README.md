# MDC Runtime

一个面向进程内逻辑微服务的 Rust 运行时：每个 Service 只有一个 Tokio 宿主任务，由它并发 poll 该服务的全部 `handle` Future。

核心关系：

- `handle Future` 是请求的执行主体；
- Service 持有自己的核心元数据、Router 和 `ServiceTaskManager`；
- Task 是 Handler Future 内按需创建的调度与控制作用域，不持有 Future；
- Router 负责跨 Service 调用，并传播 `RequestContext` 中可选的 `TaskRef`；
- Executor 只负责业务/控制通道、Handler Future 的 poll 和服务生命周期。

![运行结构](docs/diagrams/runtime.svg)

## 最短示例

`handle` 是普通异步成员方法。查询不创建 Task：

```rust
async fn handle(
    self: Arc<Self>,
    request: DiskRequest,
    context: RequestContext,
) -> Result<DiskResponse, DiskError> {
    match request {
        DiskRequest::Query(disk) => Ok(self.query(&disk)),
        DiskRequest::Offline(disk) => self.offline_workflow(disk, context).await,
    }
}
```

工作流只有在需要排重、取消或观测时才向本 Service 申请 Task：

```rust
async fn offline_workflow(
    self: Arc<Self>,
    disk: DiskId,
    context: RequestContext,
) -> Result<DiskResponse, DiskError> {
    let task = self
        .create_new_task(
            &context,
            TaskMeta::new(
                TaskKey::new(format!("disk/{disk}")),
                format!("offline disk {disk}"),
            ),
            ConflictPolicy::Reject,
        )
        .await?;

    self.router
        .call::<RebuildService>(
            &ServiceKind::Rebuild,
            RebuildRequest::Start(disk),
            context.with_task(&task),
        )
        .await?;

    Ok(DiskResponse::OfflineCompleted)
}
```

注意：跨模块调用属于 Router，不属于 Task。Task 只通过 `RequestContext.task: Option<TaskRef>` 传播身份和取消作用域。

## 调用语义

```rust
// 等待最终业务结果
let result = client.call(request).await?;

// 若 Handler 创建 Task，返回可取消的 TaskTicket；否则等待即时结果
let submission = client.submit(request).await?;

// 只确认消息进入服务队列，不等待 Handler 创建 Task 或完成
client.send(request).await?;
```

## 代码入口

```text
src/
├── service/             Service、Client、Control、ServiceGroup
├── executor/            唯一 poll 循环与 HandlerSet
├── task/                Manager、Context、Task policy
├── router.rs            类型化路由与上下文/取消传播
├── cancellation.rs      父子取消作用域
├── observation.rs       生命周期、Idle/Busy、请求与 Task 观测
└── demo/                Disk → Rebuild → BG 完整示例
```

推荐先读 [开发指南](docs/developer-guide.md)，需要理解内部所有权和状态转换时再读 [设计说明](docs/design.md)。

## 验证

Windows：

```powershell
./scripts/test.ps1
```

Linux/macOS：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo run --example rebuild
```
