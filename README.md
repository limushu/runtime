# Pool Control Plane

面向分布式存储 Monitor/Pool 管控面的 Rust 架构与运行时验证仓库。

仓库只保留当前实现及其架构基线，根 Cargo Workspace 是唯一代码入口。

## 当前入口

### 1. 实现入口

- [`Cargo.toml`](Cargo.toml)：唯一生产 Workspace；
- [`crates/foundation/control-runtime/`](crates/foundation/control-runtime/README.md)：业务无关 Service/ObjectActor Runtime；
- [`crates/pool-control-plane/`](crates/pool-control-plane/README.md)：全部 Pool 领域、Workflow 和一个 Pool 的装配根。

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

这里定义 Pool、MemberDisk、Tier、VD/BG、PoolNode、SDB 权威、领域 Workflow、协作取消、ObjectActor 和目标 Cargo 包边界。

## 当前结构

Workspace 结构：

```text
crates
├── foundation/control-runtime    # 可复用的业务无关机制
└── pool-control-plane            # Pool 业务 crate
    └── src/domains
        ├── member_disk           # Rust mod
        ├── virtual_disk          # Rust mod
        └── pool_node             # Rust mod
```

领域使用 Rust module 表达知识和私有元数据边界，不再为每个领域创建 Cargo package。
下一阶段在 `pool-control-plane` 内增加 Tier、PoolCore、SDB Repository Port 与冷备恢复协议。
