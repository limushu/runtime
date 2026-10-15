# 当前架构与目录边界

仓库是虚拟 Cargo workspace，根目录不再承载第三份业务实现。

```text
runtime/
├─ Cargo.toml / Cargo.lock       # 唯一 workspace 与依赖锁
├─ crates/
│  ├─ workflow-foundation/       # 业务无关的执行基础
│  │  ├─ src/                   # Workflow、服务宿主、准入、占用、任务观测
│  │  ├─ tests/                 # 基础语义集成测试
│  │  └─ examples/              # 无存储业务的用法示例
│  └─ storage-pool/              # 存储池业务
│     ├─ src/
│     │  ├─ app/                # Pool 装配、请求、协调策略与具名 Workflow
│     │  ├─ domain/             # disk / node / vd 对象及领域能力、外部端口
│     │  └─ infrastructure/     # 元数据、数据面适配器
│     ├─ tests/                 # 业务、交接与可观测性验证
│     └─ examples/              # 生命周期、缩容命令、协调规则
├─ docs/                       # 当前导航、架构、验证、变更记录
│  └─ archive/legacy/           # 历史理论、草案与配图，不参与构建
└─ scripts/verify.ps1           # workspace 统一检查入口
```

依赖方向：`storage-pool → workflow-foundation`，基础库不得反向依赖存储对象。

- PoolManager 管路由与生命周期，Pool 是组合和隔离边界。
- PoolWorkflowCoordinator 定义应用协调策略；Runtime 执行准入、取消、等待停稳等机制。
- Workflow 表达完整业务目标；领域读写步骤经 Activity，外部观察通过 Query。
- 各领域拥有自己的实体与状态，通过显式端口访问外部能力，不自行维护冲突排队器。
- 当前领域服务实现基础库的 DomainService 契约，尚未进一步拆分纯领域与执行适配层。

本轮只整理物理目录、构建配置与导航，不改公开 Rust crate 名称、模块兼容导出和业务语义。
保留分支已有的配置化规则、任务观察/控制、节点与磁盘交接及占位失败语义。
没有为了目录美观额外拆出 types、ports 等小 crate，也没有添加未实现的生产应用目录。

旧 service-sdk-next、根 src 和旧根 tests 已移除，不在仓库内保留第二套可编译实现。
旧源码可从 Git 历史恢复；迁移前未提交文件另存于仓库外本地备份。
