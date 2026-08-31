# Pool Control Plane

面向分布式存储 Monitor/Pool 管控面的 Rust 架构与运行时验证仓库。

仓库只保留当前实现及其架构基线，根 Cargo Workspace 是唯一代码入口。

## 当前入口

### 1. 实现入口

- [`Cargo.toml`](Cargo.toml)：唯一生产 Workspace；
- [`crates/foundation/control-runtime/`](crates/foundation/control-runtime/README.md)：业务无关 Service/ObjectSlot Runtime；
- [`crates/pool-control-plane/`](crates/pool-control-plane/README.md)：Monitor 级 `PoolManager`、独立 `Pool` 实例、领域对象和 Workflow。

验证命令：

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/verify_architecture.py
```

### 2. 架构与业务基线

团队设计和后续实现的权威入口：

- [`pool-control-plane-baseline/README.md`](pool-control-plane-baseline/README.md)
- [`pool-control-plane-baseline/07-pool-control-plane-theory.md`](pool-control-plane-baseline/07-pool-control-plane-theory.md)
- [`pool-control-plane-baseline/08-package-and-workspace-layout.md`](pool-control-plane-baseline/08-package-and-workspace-layout.md)

这里定义 Pool、MemberDisk、Tier、VD/BG、PoolNode、SDB 权威、领域 Workflow、协作取消、逻辑 Actor、ObjectSlot 和 Cargo 包边界。

## 当前结构

Workspace 结构：

```text
crates
├── foundation/control-runtime    # 可复用的业务无关机制
└── pool-control-plane            # Pool 业务 crate
    └── src
        ├── pool_manager.rs       # Monitor 级多 Pool 注册与事实路由
        ├── pool/                 # 一个独立 Pool 的元数据和装配根
        ├── ports.rs              # SDB 等基础设施端口
        └── domains
            ├── member_disk       # 完整实体、逻辑 Actor、状态图和工作流
            ├── virtual_disk      # VD/BG 能力原型
            └── pool_node         # Pool 在 user_dp 上的能力原型
```

领域使用 Rust module 表达知识和私有元数据边界，不再为每个领域创建 Cargo package。跨领域调用通过显式 Service facade 完成；请求本身不携带隐藏路由信息。

当前纵切面已经验证：Pool CRUD 与冷恢复、DiskMap 事实的多 Pool 定位、MemberDisk 核心元数据与位图、逻辑 Disk Actor、对象意图替换和协作取消。下一阶段在相同边界内补齐 Tier、VD/BG、Node 与真实 SDB 适配器。
