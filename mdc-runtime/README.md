# MDC Runtime

一个面向进程内逻辑微服务的 Rust 运行时：每个 Service 只有一个 Tokio 宿主任务，由它统一 poll 该服务的全部业务 Future。

业务开发者只需要理解四个概念：

- `Service`：持有自己的核心元数据和依赖；
- `handle`：同步判断请求应立即回复，还是生成受管 Task；
- `TaskSpec`：Task 的身份、冲突策略和业务 Future；
- `TaskContext::call`：跨 Service 调用，并自动完成父子取消传播。

![运行结构](docs/diagrams/runtime.svg)

## 最短示例

查询直接回复，不产生 Task：

```rust
fn handle(
    self: Arc<Self>,
    request: DiskRequest,
    _context: RequestContext,
) -> HandleResult<DiskResponse, DiskError> {
    match request {
        DiskRequest::Query(disk) => {
            HandleResult::ok(DiskResponse::Snapshot(self.metadata.query(&disk)))
        }

        DiskRequest::Offline(disk) => {
            let meta = TaskMeta::new(
                TaskKey::new(format!("disk/{disk}")),
                format!("offline disk {disk}"),
            );

            HandleResult::task(TaskSpec::new(meta, move |task| {
                self.offline_workflow(disk, task)
            }))
        }
    }
}
```

工作流是普通 Service 成员方法，可以直接顺序 `.await`：

```rust
async fn offline_workflow(
    self: Arc<Self>,
    disk: DiskId,
    task: TaskContext,
) -> Result<DiskResponse, DiskError> {
    self.metadata.begin_offline(&disk)?;

    task.call(&self.rebuild, StartRebuild { disk: disk.clone() })
        .await?;

    self.metadata.complete_offline(&disk)?;
    Ok(DiskResponse::OfflineCompleted)
}
```

`TaskContext::call` 在父任务取消时会取消下游 Task，并等待下游返回终态。正常取消不会 drop Future；只有 `ShutdownMode::Immediate` 和容器异常 Drop 才强制 abort。

## 代码入口

```text
src/
├── service/             Service trait、Client、Control、ServiceGroup
├── executor/            唯一 poll 循环与内部 TaskSet
├── task.rs              业务可见的 TaskSpec/TaskContext
├── cancellation.rs      父子取消作用域
├── observation.rs       生命周期、Idle/Busy、Task 快照与事件
├── router.rs            类型化本地路由
└── demo/                Disk → Rebuild → BG 完整示例
```

推荐先读 [开发指南](docs/developer-guide.md)，需要理解内部保证时再读 [设计说明](docs/design.md)。

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
