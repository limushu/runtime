# pool-control-plane

整个 Pool 管控面业务只有一个 crate。Monitor 级 `PoolManager` 管理多个独立 `Pool`；MemberDisk、VirtualDisk、PoolNode 等领域是这个 crate 内的 Rust module。

## 当前纵切面

```text
PoolManager
└── PoolRegistry: PoolId -> Arc<Pool>
    └── Pool
        ├── PoolMetadata + ControlPlaneStore
        ├── MemberDiskService
        │   └── MemberDiskId -> MemberDisk
        ├── VirtualDiskService
        └── PoolNodeService
```

- `PoolManager` 根据物理盘归属把 DiskMap 标准事实路由到对应 Pool；普通盘强制 `Exclusive`，只有 `SharedCache` 才允许多目标。
- `Pool` 是业务对象和生命周期边界；通用 Runtime 只是其内部 Service 的执行机制。
- `MemberDisk` 是唯一业务对象，同时拥有持久化决策字段与当前 DiskMap 物理观测；UA/DA/DI/UI/Removed 只是对象字段的运行投影。
- `machine.rs` 只写 `(状态, 事件) -> Transition`；`change(...)` 描述对象字段变化，`ensure(WorkflowKind)` 声明期望工作流。
- `service.rs` 只用 `HashMap<MemberDiskId, MemberDisk>` 保存对象，并定义自然 `async fn` 工作流；没有额外的 Record、Snapshot 或业务 Actor 类型。
- ActorCell、对象互斥、相同 Workflow 合并、冲突 Workflow 协作替换和 Future poll 全部位于 `control-runtime` 私有实现。
- 对外只暴露 `MemberDiskService::apply_physical/allocate/release/get/...` 等明确能力；命令枚举、channel 和 oneshot 留在模块内部。

开发一个新领域时，不新增 `actor.rs`，也不为每个对象 spawn task。先写一个完整对象，再写纯状态机，最后写普通异步工作流。逻辑上的 Actor 等于“该对象 + Runtime 私有执行状态”，但后者不复制业务字段，也不进入开发者 API。
