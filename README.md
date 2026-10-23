# Storage Pool Control Plane

每个 Pool 一个 TaskCenter，在同一个事件循环内协调并推进完整业务 Workflow。
领域按业务模块组织，model、service、runtime 放在一起，内部保持单向依赖。

```text
crates/
├─ workflow-foundation/       # 执行驱动、Activity、执行控制与资源保护
└─ storage-pool/src/
   ├─ manager/               # Pool 句柄索引、公平事件分发与汇聚
   ├─ pool/                  # 所有业务入口、成员目录、生命周期与组装
   ├─ task_center/           # 准入、冲突、排队、观察、历史与续办
   ├─ workflows/             # 完整业务步骤
   ├─ domains/               # disk / node / tier / vd / balance
   ├─ metadata/              # 统一元数据仓储 + 可替换的事务 KV
   ├─ ports.rs               # 数据面与均衡服务契约
   └─ adapters/              # 模拟实现与测试适配
```

## 从哪里开始

- [架构、职责和阅读顺序](docs/architecture.md)
- [元数据模块：嵌入自己的 KV 数据库](docs/metadata.md)
- [如何新增业务冲突规则](docs/task-center.md)
- [Activity 暂停钩子与继续条件](docs/activity-control.md)
- [完整缩容工作流与均衡模块接入](docs/shrink-workflow.md)
- [存储业务与安全限制](crates/storage-pool/README.md)
- [通用执行基础](crates/workflow-foundation/README.md)
- [构建与验证](docs/verification.md)
- [TaskCenter 重构规格与证据](docs/changes/2026-10-15-task-center.md)

```sh
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo run --locked -p storage-pool --example pool_lifecycle
cargo run --locked -p storage-pool --example task_center
cargo run --locked -p storage-pool --example shrink_disk
cargo run --locked -p storage-pool --features kv-redb --example metadata_kv -- /tmp/runtime-demo.redb
```

默认使用内存适配器；元数据可注入自己的 KV，或启用 `kv-redb` 持久化。
任务历史与指定任务续办由可注入的 TaskStore 提供。元数据和任务历史可以共用物理数据库。
数据面与均衡示例仍是模拟，不执行真实磁盘 IO，也不提供跨主租约或任意 Future 的崩溃恢复。
