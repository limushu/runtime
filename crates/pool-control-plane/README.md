# pool-control-plane

整个 Pool 管控面业务只有一个 crate。Monitor 级 `PoolManager` 管理多个独立 `Pool`；MemberDisk、VirtualDisk、PoolNode 等领域是这个 crate 内的 Rust module。

## 当前纵切面

```text
PoolManager
└── PoolRegistry: PoolId -> Arc<Pool>
    └── Pool
        ├── PoolMetadata + ControlPlaneStore
        ├── MemberDiskService
        │   └── MemberDiskDirectory<MemberDiskId, MemberDisk>
        ├── VirtualDiskService
        └── PoolNodeService
```

- `PoolManager` 根据物理盘归属把 DiskMap 标准事实路由到对应 Pool；普通盘强制 `Exclusive`，只有 `SharedCache` 才允许多目标。
- `Pool` 是业务对象和生命周期边界；通用 Runtime 只是其内部 Service 的执行机制。
- `MemberDisk` 是完整业务对象，拥有 SDB Record 与当前 DiskMap 物理观测；UA/DA/DI/UI/Removed 只是二者的运行投影。
- `machine.rs` 只写 `(状态, 事件) -> Transition`，用 `ensure(WorkflowKind)` 声明期望工作流。
- `service.rs` 保存对象目录和自然 `async fn` 工作流，通过 `PoolNodeService`、`VirtualDiskService` 等命名 facade 跨领域调用。
- ActorCell、对象互斥、相同 Workflow 合并、冲突 Workflow 协作替换和 Future poll 全部位于 `control-runtime` 私有实现。
- 对外只暴露 `MemberDiskService::apply_physical/allocate/release/get/...` 等明确能力；命令枚举、channel 和 oneshot 留在模块内部。

开发一个新领域时，不新增 `actor.rs`，也不为每个对象 spawn task。先写完整对象，再写纯状态机，最后写普通异步工作流。
