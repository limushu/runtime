# Storage Pool Control Plane

每个 Pool 一个 TaskCenter，在同一个事件循环内协调并推进完整业务 Workflow。
领域按业务模块组织，model、service、runtime 放在一起，内部保持单向依赖。

```text
crates/
├─ workflow-foundation/       # 可嵌入的执行驱动、能力通道、执行控制
└─ storage-pool/src/
   ├─ pool.rs                # PoolManager / Pool / 对外业务入口
   ├─ task_center/           # 协调、排队、占用、Workflow 推进
   ├─ workflows/             # 完整业务流程
   ├─ domains/
   │  ├─ disk/               # model.rs / service.rs / runtime.rs
   │  ├─ node/               # model.rs / service.rs / runtime.rs
   │  └─ vd/                 # model.rs / service.rs / runtime.rs
   ├─ ports.rs               # 元数据与数据面接口
   └─ adapters.rs            # 内存/模拟实现
docs/                        # 架构、开发说明、验证与历史归档
scripts/verify.ps1           # workspace 验证入口
```

## 从哪里开始

- [架构与依赖边界](docs/architecture.md)
- [如何新增业务冲突规则](docs/task-center.md)
- [存储业务与安全限制](crates/storage-pool/README.md)
- [通用执行基础](crates/workflow-foundation/README.md)
- [构建与验证](docs/verification.md)
- [TaskCenter 重构规格与证据](docs/changes/2026-10-15-task-center.md)

```sh
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo run --locked -p storage-pool --example pool_lifecycle
cargo run --locked -p storage-pool --example task_center
```

本轮移除策略注入/规则引擎和旧分层目录，不保留第二套业务协调实现。旧代码可从 Git 历史恢复。
当前仍是内存模型与模拟适配器，不是生产存储系统；没有持久历史、崩溃恢复或真实磁盘 IO。
