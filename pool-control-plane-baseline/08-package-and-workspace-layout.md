# Pool 管控面 Workspace 与模块边界

状态：已确认并落地。

## 1. 决策

生产 Workspace 只保留两个核心 library crate：

1. `control-runtime`：完全业务无关的进程内 Service Runtime；
2. `pool-control-plane`：整个 Pool 管控面业务。

MemberDisk、VirtualDisk、Tier、PoolNode、PoolCore 等 Domain 是
`pool-control-plane` 内的 Rust module，不是独立 Cargo package。

这是一次有意的减法。早期 `domain/api + domain/service` 方案能够强化编译期依赖，
但对当前单仓库、单进程、共同发布的 Monitor 业务没有足够收益，反而增加：

- Cargo manifest 和 crate root 数量；
- Request/Reply 与实现之间的导航距离；
- 为跨 crate 使用而扩大 `pub` 可见性的压力；
- 普通开发者理解和修改一个领域 Workflow 的成本。

## 2. Package、crate 和 module

```text
Cargo package
└── src/lib.rs          library crate 的默认根文件
    └── mod domain      crate 内部 Rust module
```

`lib.rs` 不是与 crate 对立的概念。一个包含 `Cargo.toml` 和 `src/lib.rs` 的目录，
就是一个 library crate。当前选择是“整个 Pool 业务一个 crate、每个领域一个 mod”。

## 3. 目标目录

```text
kube-managed-future-runner/
├── Cargo.toml
├── Cargo.lock
├── crates/
│   ├── foundation/
│   │   └── control-runtime/
│   │       └── src/
│   │           ├── lib.rs
│   │           ├── protocol.rs
│   │           ├── context.rs
│   │           ├── router.rs
│   │           ├── observation.rs
│   │           ├── state_cell.rs
│   │           └── service/
│   │               ├── mod.rs
│   │               ├── contract.rs
│   │               ├── container.rs
│   │               └── object_actor.rs
│   │
│   └── pool-control-plane/
│       └── src/
│           ├── lib.rs
│           ├── kernel/
│           │   └── mod.rs
│           ├── domains/
│           │   ├── mod.rs
│           │   ├── member_disk/
│           │   │   ├── mod.rs
│           │   │   ├── protocol.rs
│           │   │   ├── machine.rs
│           │   │   └── service.rs
│           │   ├── virtual_disk/
│           │   ├── pool_node/
│           │   ├── tier/             # 后续
│           │   └── pool_core/        # 后续
│           └── pool_runtime.rs
└── scripts/
    └── verify_architecture.py
```

![Workspace 与模块边界](assets/package-workspace-layout.svg)

图源：[package-workspace-layout.puml](diagrams/package-workspace-layout.puml)

## 4. `control-runtime` 的职责

`control-runtime` 是唯一值得独立复用和隔离的基础 crate。它提供：

- 每个 Service 一个根 Tokio task；
- 业务通道与控制通道；
- `FuturesUnordered` 统一 poll Workflow Future；
- ObjectActor 的 `Idle/Pending/Running/Cancelling` 准入；
- Router 和类型化 oneshot 调用；
- Operation/Call/Task 因果上下文；
- 结构化取消、Service 生命周期和观测；
- 不允许锁 Guard 跨越 `.await` 的 `StateCell`。

它不能出现 Pool、Disk、Node、VD、BG、BLK、Tier、Rebuild、SDB 等业务知识。

内部原来的 `runtime.rs` 已改为 `service/container.rs`。原因是整个 crate 已经是
Runtime，而该文件实际只负责一个 Service 的执行容器，旧命名重复且掩盖职责。

## 5. `pool-control-plane` 的职责

`pool-control-plane` 保存全部 Pool 业务知识：

- 稳定业务 ID 和值对象；
- Domain 私有核心元数据；
- Domain Request/Reply/Event；
- 状态图和领域不变量；
- 自然 `async fn` Workflow；
- 一个 Pool 的 Service 创建、Router 注册、生命周期和外部业务入口。

它依赖 `control-runtime`，反向依赖永远不允许。

## 6. Domain module 约定

一个领域的推荐最小结构：

```text
domains/member_disk/
├── mod.rs
├── protocol.rs
├── machine.rs
├── service.rs
└── workflows/          # 只有 service.rs 过大时再创建
```

不要为了目录对称创建空文件。简单领域可以只有 `mod.rs + protocol.rs + service.rs`。

### `protocol.rs`

保存跨领域调用方需要理解的：

- Request/Reply；
- DomainEvent；
- 不可变 View；
- 调用方必须处理的稳定业务结果。

不能暴露可变元数据、锁、Repository 实现或 SDB Key 布局。

### `machine.rs`

只保存纯状态迁移：

```text
(当前业务状态, 外部/内部事件, 当前对象活动) -> (新状态, 调度效果)
```

它不持有 Router、锁、Future 或 task。

### `service.rs`

保存：

- 私有领域元数据；
- `impl Service`；
- 准入决策到状态图的适配；
- 自然 async Workflow；
- 跨领域 Router 调用。

## 7. 可见性就是边界

领域核心元数据保持私有：

```rust
struct MemberDiskMetadata {
    // 只有 member_disk module 能直接解释和修改
}

pub(crate) struct MemberDiskService {
    metadata: StateCell<MemberDiskMetadata>,
}
```

其他领域只能使用协议并经 Router 调用：

```rust
use crate::domains::virtual_disk::protocol::VirtualDiskRequest;

self.router
    .call(&context, VirtualDiskRequest::EvacuateMemberDisk(disk))
    .await?;
```

同一个 crate 消除了 Cargo 循环依赖，但不代表允许直接访问其他领域元数据。
边界由模块私有性、Router 调用约定、测试和代码审查共同保持。

## 8. Domain、Service、Task 不等价

```text
Domain module
    业务知识和源码所有权边界

Service struct
    Domain 对外提供的运行能力

ServiceContainer
    Runtime 执行和控制一个 Service 的容器

Tokio root task
    ServiceContainer 的根执行载体

Workflow Future
    Service 上运行的一次业务收敛流程
```

把 Domain 改成 module 不会改变每个 Service 独立根 task 的运行模型。

## 9. 什么时候才升级成独立 crate

只有出现至少一项真实需求时才考虑把 module 提升为 crate：

1. 被 Pool 之外的多个产品复用；
2. 独立发布、版本兼容或 feature 管理；
3. 被多个进程或二进制以不同组合链接；
4. 构建时间已经证明需要物理隔离；
5. 模块边界长期被破坏，且轻量检查无法约束。

“它是一个重要领域”本身不是拆 crate 的理由。

## 10. 当前实现状态

- 已完成：`control-protocol` 合并进 `control-runtime::protocol`；
- 已完成：`runtime.rs` 重组为 `service/contract + container + object_actor`；
- 已完成：Pool Kernel 合并为 `pool-control-plane::kernel`；
- 已完成：MemberDisk、VirtualDisk、PoolNode 合并为 Domain modules；
- 已完成：PoolRuntime 与全部状态图/场景测试迁移；
- 已完成：生产 Workspace 收敛为两个 crate；
- 待实现：Tier、PoolCore、SDB Port、冷备恢复与 reconcile。

## 11. 验证

```text
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/verify_architecture.py
```

架构脚本同时拒绝：

- `control-runtime -> pool-control-plane` 反向依赖；
- `pool-control-plane/src/domains` 中出现嵌套 `Cargo.toml`。
