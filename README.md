# Pool Control Plane Runtime

这是 Monitor Pool 管控面的 Rust 基础运行组件与 MemberDisk 纵切面。实现目标不是把业务
写进框架，而是把所有领域都会重复遇到的执行、生命周期、取消与观测机制做成一个小而完整
的公共层，让领域开发者继续编写普通 `async fn`。

## 当前实现

- 一个 Service 只有一个根 Tokio task；请求 handler 是由根 task 统一 poll 的 Future，不为
  每个对象递归 `spawn`；
- 每个实例暴露四个职责明确的句柄：typed `client`、高优先级 `control`、只读
  `observer`、具有强制回收语义的根 `task`；
- 每个 Service 只定义一组 `Request`、`Reply` 和领域 `Error`；通用 Envelope 持有唯一的
  `oneshot`，`CallError` 将生命周期/通信错误与领域错误分开；
- handler panic 和强制 abort 都会显式结束已接收 Request/Operation；前者返回
  `HandlerPanicked` 并使服务进入 `Failed`，被强制丢弃的在途请求返回 `RequestAborted`；
- 生命周期完整覆盖 `Initializing -> Running <-> Paused -> Draining/Stopping ->
  Stopped`，初始化或关闭失败进入 `Failed`；
- Query 可以只作为普通 Future 执行；只有需要审计、进度或精确取消的工作才在 handler
  内部创建 `TaskAttempt`；
- `ObjectTaskCoordinator` 只提供同 Key 的 Join、Queue、Cancel、Promotion 与 Idle waiter
  机制，Key 和冲突规则仍由具体领域决定；
- 快照和结构化事件同时提供服务状态、请求计数、Task 进度、阻塞原因、Trace、取消及对象
  状态转换，可直接供 TUI 或外部审计适配器消费；
- `MemberDiskService` 已使用这些机制实现 DOWN、UP、Shrink、可靠 DOWN 边界、冲突协作
  取消、BLK 分配与 SDB-first 内存发布。

## 最小使用方式

```rust
let running = member_disk_service.spawn(128);

// 领域 facade：目标实例已经明确，私有 Request/Reply enum 不会泄漏给 Pool 工作流。
running.client.submit(member_disk_event).await?;
let disk = running.client.get(disk_id).await?;
let allocation = running
    .client
    .allocate_blks(AllocateBlks::new("tier-ssd", 3))
    .await?;

// 观测通道：无需解析日志。
let snapshot = running.observer.snapshot();
let events = running.observer.history();

// 控制通道独立于业务流量并优先处理。
running.control.pause().await?;
running.control.resume().await?;
running.control.drain().await?;
running.task.await?;
```

`drain` 不取消已接收工作；`stop` 请求协作取消后等待 Future 稳定退出；`task.abort()` 是故障
隔离用的强制 drop。若根 `task` 所有权被直接丢弃，运行时也会自动 abort，防止服务泄漏。

## 代码入口

- 公共运行时：[`src/runtime/`](src/runtime/)
- Service Host 与四类句柄：[`src/runtime/service.rs`](src/runtime/service.rs)
- 生命周期与控制面：[`src/runtime/lifecycle.rs`](src/runtime/lifecycle.rs)
- Task、Trace 与结构化观测：[`src/runtime/task.rs`](src/runtime/task.rs)、
  [`src/runtime/observation.rs`](src/runtime/observation.rs)
- 对象在途任务协调：[`src/runtime/object_task.rs`](src/runtime/object_task.rs)
- MemberDisk 领域实现：[`src/member_disk/`](src/member_disk/)
- 完整运行时设计：[`docs/09-service-runtime.md`](docs/09-service-runtime.md)
- Pool 管控面理论基线：[`docs/07-pool-control-plane-theory.md`](docs/07-pool-control-plane-theory.md)

## 验证

```bash
cargo fmt --all --check
cargo test
cargo clippy --all-targets -- -D warnings
```

运行时组件测试位于 [`tests/runtime_component.rs`](tests/runtime_component.rs)，MemberDisk 的
业务、冲突与端到端观测测试位于
[`src/member_disk/service_tests.rs`](src/member_disk/service_tests.rs)。
