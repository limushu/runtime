# Storage Pool Control Plane

存储池应用编排与通用 Workflow 运行基础。当前只有一套有效实现，使用 Cargo workspace
统一构建；原有 service-sdk-next 和根 src 已退役。

```text
crates/
├─ workflow-foundation/   # 业务无关：执行、准入、取消/暂停、占用、任务观测
└─ storage-pool/          # 存储池业务
   └─ src/
      ├─ app/            # Pool、协调规则、业务 Workflow
      ├─ domain/         # Disk / Node / VD 对象、领域能力和端口
      └─ infrastructure/ # 模拟元数据与数据面适配器
docs/                    # 当前架构、验证与变更记录；archive/ 为历史材料
scripts/verify.ps1       # 统一验证入口
```

依赖方向：`storage-pool → workflow-foundation`。Pool 协调服务决定业务冲突策略，
Runtime 执行准入与稳定交接；领域服务拥有状态，Workflow 调用其 Activity 能力。

## 开始阅读

- [当前架构与目录边界](docs/architecture.md)
- [存储池业务入口与限制](crates/storage-pool/README.md)
- [通用 Workflow 基础接口](crates/workflow-foundation/README.md)
- [构建与验证](docs/verification.md)
- [本次目录迁移与恢复说明](docs/changes/2026-10-15-workspace-layout.md)

## 在根目录运行

```sh
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo run --locked -p storage-pool --example pool_lifecycle
cargo run --locked -p storage-pool --example coordination_rules
```

根目录保留唯一 Cargo.lock；测试、示例留在所属 crate 内，避免形成第三套业务入口。
现有公开 crate 名称和模块兼容导出不变。

当前使用内存/模拟适配器，不是生产存储系统。暂停恢复限于当前进程，尚无持久历史或
崩溃重放；重建/成员移除等占位步骤仍明确失败，不在目录迁移中假装补齐。
