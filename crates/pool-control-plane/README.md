# pool-control-plane

整个 Pool 管控面业务只有一个 crate。Monitor 级 `PoolManager` 管理多个独立 `Pool`；
MemberDisk、VirtualDisk、PoolNode 等领域是这个 crate 内的 Rust module，而不是独立 Cargo package。

模块负责业务知识和元数据所有权；`control-runtime` 负责进程内 Service 容器、
对象准入、Workflow poll、结构化取消和观测。只有出现独立发布或复用需求时，
才把某个领域提升为单独 crate。

## 当前纵切面

```text
PoolManager
└── PoolRegistry: PoolId -> Arc<Pool>
    └── Pool
        ├── PoolMetadata + ControlPlaneStore
        ├── MemberDiskService
        │   └── MemberDiskDirectory<MemberDiskId, MemberDiskActor>
        ├── VirtualDiskService
        └── PoolNodeService
```

- `PoolManager` 根据物理盘归属把 DiskMap 的标准事实路由到一个或多个 Pool；普通盘强制 `Exclusive`，只有 `SharedCache` 才允许多目标，二者不能混用。
- `Pool` 是业务对象和生命周期边界；通用 Runtime 只是其内部每个 Service 的执行机制。
- `MemberDiskRecord` 保存身份、Pool/Tier/故障域、容量、分配状态、成员状态和 BLK 位图；`PhysicalState` 是 DiskMap 观测，不冒充 SDB 决策。
- `MemberDiskActor` 是每盘一个的逻辑 Actor，但不创建每盘 Tokio task。它拥有完整实体和状态图决策。
- `ObjectSlot` 由框架实际维护每个对象的在途执行意图，执行 `Start/Join/Queue/Replace`；它不拥有 MemberDisk 元数据。
- 对外只暴露 `MemberDiskService::apply_physical/allocate/release/get/...` 等明确能力。命令枚举、channel 和 oneshot 都留在模块内部。
