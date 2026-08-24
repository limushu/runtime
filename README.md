# MDC Service Runtime

这是本仓库当前的**主实现**：一个面向分布式存储控制面的轻量 Rust Service/Workflow 运行时。

它只要求业务开发者理解四个词：

- **Service**：拥有私有业务状态的长生命周期容器；
- **Message**：Service 之间唯一的输入；
- **Task**：由 Service 统一 poll 的一个业务 Future；
- **Router**：按 `ServiceKind` 发送 Message，不暴露另一个 Service 的对象。

框架内部没有要求业务实现的 Scheduler、Policy、Reducer、Effect、Directive 或 Engine trait。策略就是普通 Rust 状态和自由 handler 函数；workflow 就是普通 `async fn`。

![运行容器](docs/diagrams/02-runtime-container.svg)

## 先看开发者要写什么

一个 feature 固定为四类文件：

```text
rebuild/
├── install.rs      # 唯一的 Message -> handler 映射
├── handlers.rs     # 同步决策：读写 State，按需提交 Task
├── workflow.rs     # 普通 async fn，只写业务步骤
└── protocol.rs     # 通常放到服务或应用共享协议中
```

注册只写一次：

```rust
pub fn install(blueprint: &mut DemoBlueprint) -> Result<(), RuntimeError> {
    register_handlers!(blueprint.rebuild, {
        StartRebuildRequest   => handle_start,
        SuspendRebuildRequest => handle_suspend,
        ResumeRebuildRequest  => handle_resume,
        CancelRebuildRequest  => handle_cancel,
        DiskResolved          => handle_disk_resolved,
        BgFinished            => handle_bg_finished
    })
}
```

Service 内没有另一份硬编码 `match`。外部请求和 Future 完成后的内部消息都进入这张表。

handler 只有遇到异步边界才提交 Task：

```rust
pub fn handle_start(service: &mut RebuildService, request: StartRebuildRequest) {
    // 1. 普通 Rust：检查对象状态、合并 BG owner、修改调度上下文
    // 2. 需要查 MDC 时，把普通 async fn 交给框架
    service.run(
        TaskKey::new(format!("resolve/{}", request.disk)),
        "resolve disk to BGs",
        TaskVisibility::Internal,
        move |_| workflow::resolve_disk(catalog, disk),
        move |outcome| DemoMessage::DiskResolved(DiskResolved { disk, outcome }),
    );
}
```

`workflow::resolve_disk` 不注册、不持有 Service，也不 `tokio::spawn`。

## Demo 对应的真实业务

Demo 不是 RAID object 模型，而是你描述的 MDC 模型：

1. 硬盘由 1G BLK 构成；
2. 不同硬盘上的 BLK 组成 BG；
3. RebuildService 根据 `disk_id` 查询受影响 BG；
4. 多个硬盘可以共同依赖一个 BG，但该 BG 只重建一次；
5. RebuildService 维持固定并发窗口，完成一个 BG 就补一个；
6. BgService 执行 `remap -> 节点重建 -> BG 元数据提交`；
7. 每个硬盘只有一个发起者 ticket，在它依赖的全部 BG 完成后返回；
8. 对外只有一个 pool rebuild campaign，disk/BG task 仍在内部真实存在并可审计。

运行：

```powershell
cd kube-managed-future-runner/mdc-runtime
./scripts/test.ps1
```

该脚本会依次执行格式检查、编译、全部测试和端到端 demo，并自动处理中文 Windows 用户目录导致的 MinGW sysroot 问题。

Linux/macOS 或 Rust 工具链路径仅含 ASCII 时，直接运行：

```bash
cargo test --all-targets
cargo run --example rebuild
```

Demo 入口是 [`examples/rebuild.rs`](examples/rebuild.rs)，业务实现从 [`src/demo/blueprint.rs`](src/demo/blueprint.rs) 开始读。

## 推荐阅读顺序

1. [`docs/developer-guide.md`](docs/developer-guide.md)：照着新增一个业务；
2. [`src/demo/disk_offline/install.rs`](src/demo/disk_offline/install.rs)：理解一次安装；
3. [`src/demo/rebuild/handlers.rs`](src/demo/rebuild/handlers.rs)：理解对象策略与窗口调度；
4. [`src/demo/rebuild/workflow.rs`](src/demo/rebuild/workflow.rs)：理解自由 workflow；
5. [`docs/design.md`](docs/design.md)：再看运行时内部和五视图。

## 当前边界

本交付实现了内存态调度、精确取消、Service 生命周期、优先控制通道、Idle/Busy 与 Task 观测。生产接入仍需补 MDC 持久化、重启恢复、fencing token、retry/backoff 和跨节点 leader/lease；这些能力不应改变业务开发接口。
