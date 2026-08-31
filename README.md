# Pool Control Plane

面向分布式存储 Monitor/Pool 管控面的 Rust 架构与运行时验证仓库。仓库只保留当前实现及其架构基线，根 Cargo Workspace 是唯一代码入口。

## 开发者看到的模型

领域开发只需要维护三类代码：

1. 完整领域对象及其 SDB 数据；
2. 纯状态机：`(当前状态, 事件) -> Transition::to(新状态).ensure(工作流)`；
3. 普通 `async fn` 工作流，通过目标领域的命名 facade 协作。

Actor、每对象串行化、重复请求合并、冲突工作流协作替换、oneshot、Future poll、Task 与 Trace 都隐藏在 `control-runtime` 内部。一个 Service 只有一个根 Tokio task，不会为每个领域对象创建 task 或 mailbox。

## 当前入口

- [`crates/foundation/control-runtime/`](crates/foundation/control-runtime/README.md)：业务无关的 Service Runtime、隐藏 ActorCell 和公共状态机原语；
- [`crates/pool-control-plane/`](crates/pool-control-plane/README.md)：Monitor 级 `PoolManager`、独立 `Pool`、领域对象、状态机和工作流；
- [`pool-control-plane-baseline/README.md`](pool-control-plane-baseline/README.md)：架构与业务基线；
- [`pool-control-plane-baseline/07-pool-control-plane-theory.md`](pool-control-plane-baseline/07-pool-control-plane-theory.md)：理论模型；
- [`pool-control-plane-baseline/08-package-and-workspace-layout.md`](pool-control-plane-baseline/08-package-and-workspace-layout.md)：代码边界。

## 当前结构

```text
crates
├── foundation/control-runtime    # 通信、执行、取消、观测和隐藏 Actor
└── pool-control-plane            # Pool 业务 crate
    └── src
        ├── pool_manager.rs       # Monitor 级多 Pool 注册与事实路由
        ├── pool/                 # 一个独立 Pool 的元数据和装配根
        ├── ports.rs              # SDB 等基础设施端口
        └── domains
            ├── member_disk       # 完整对象、状态机、工作流
            ├── virtual_disk      # VD/BG 能力原型
            └── pool_node         # Pool 在 user_dp 上的能力原型
```

领域使用 Rust module 表达知识和私有元数据边界。跨领域调用通过显式 Service facade 完成；请求本身不携带隐藏路由信息。

## 验证

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/verify_architecture.py
```
