# Service SDK Next

当前正式入口是 **PoolManager → Pool → ProcessManager → 领域能力**。
ProcessManager 已在 [src/process_manager/mod.rs](src/process_manager/mod.rs) 实现，
不是 examples 中的原型，也不是 MemberDiskService 的别名。

## 结构与所有权

```text
PoolManager（路由、根任务所有权）
├─ Pool A：独立 Runtime 根 task
│  ├─ ProcessManager：流程目录、准入/取消策略、整体结果
│  ├─ DiskService：MemberDisk 状态、BLK、资源互斥、提交
│  └─ VdService：排空端口适配、稳定取消、排空凭据
└─ Pool B：同样结构，独立状态和运行控制
```

每个 Pool 一个根执行单元，不是每盘或每个 Manager 一个 task。
具名 Workflow 与它调用的领域 async 能力共用这个根 Future driver。
Runtime 模块提供可复用的执行机制；流程目录作为 ProcessManager 的实例字段被拥有，
不是全局 Runtime 的业务状态。磁盘目录只有 DiskService 一份，Activity 只是调用边界。

| 正式文件 | 阅读问题 |
| --- | --- |
| [pool.rs](src/pool.rs) | Pool 如何装配？PoolManager 如何路由、拒绝重复、停止并移除？ |
| [process_manager/mod.rs](src/process_manager/mod.rs) | 操作如何接收、排队、取消、汇总？ |
| [process_manager/workflows.rs](src/process_manager/workflows.rs) | 离线、上线、缩容如何顺序 await 领域能力？ |
| [member_disk/service.rs](src/member_disk/service.rs) | DiskService 如何保护能力调用与排空凭据？ |
| [member_disk/admission.rs](src/member_disk/admission.rs) | 不经过 ProcessManager 时，领域资源如何仍受保护？ |
| [member_disk/effects.rs](src/member_disk/effects.rs) | 预校验、provider 调用、SDB 确认和内存发布如何完成？ |
| [vd.rs](src/vd.rs) | 排空端口如何返回不可伪造的本地凭据？ |
| [runtime/workflow.rs](src/runtime/workflow.rs) | 通用执行记录、协作取消、Activity gate 与结果缓存如何实现？ |

## 调用方式

```rust,ignore
let pool = Pool::new(
    "pool-a", members, metadata, nodes, virtual_disks, PoolConfig::default()
)?;
let mut manager = PoolManager::default();
let handle = manager.insert(pool, ServiceConfig::default())?;

let result = handle.client.call(ProcessCommand::Shrink(disks)).await?;
manager.remove("pool-a").await?;
```

完整可运行代码见 [examples/pools.rs](examples/pools.rs)。
也可使用 `Pool::start` 直接获得 `client / control / observer / task`，
此时调用方负责根任务所有权。PoolManager 本身不是巨型业务 Actor，它不保存磁盘/BG 状态。

`submit` 只确认内存接收；`call` 返回最终结果。批量磁盘结果保留输入顺序和重复项。
`ProcessQuery::Get/List` 读取已提交成员；`Work` 返回执行状态/结果；
`InFlightCommands` 从执行目录派生批量进度。快照携带 Pool ID、磁盘/VD 恢复阻塞和操作结果。
普通终态保留最近 256 项，恢复阻塞不自动淘汰；没有持久化保证。

## 两种冲突分别处理

**流程冲突归 ProcessManager：** 同盘生命周期一次一个，重复输入排队，
相反意图请求旧流程协作取消并等待停稳；已知盘 DOWN 取消本 Pool 所有在途 Shrink。
这些业务策略不放入通用 Runtime。另一 Pool 的相同 ID 不参与该判断。

**资源冲突归领域：** DiskService 用每盘修改占用与空间操作独占保护能力。
Allocate/Release 在同一个领域占用内完成整份计划的校验和提交，避免重复分配；
不同盘的普通修改可并发。只在实际领域动作期间持有资源，不在恢复等待或 VD 排空期间
持有全局空间锁。所以 A 盘等待排空时，B 盘仍可以分配。

所有可变目录访问只持短同步锁，不跨 await。资源预约使用独立异步锁，
允许跨 provider await，以保证提交安全；二者不是同一种锁。
通过 `Pool::disks()` 获得的领域 facade 同样经过此保护，不能绕开准入修改对象。

## 安全边界

- 提交顺序保持 `预校验 → SDB 确认 → 发布内存`。查询不领先于已确认提交。
- 元数据和节点端口的 `is_retryable` 默认 false，只有可证明安全的失败才重试。
  不明结果或协议异常进入恢复阻塞；Workflow panic 由 Runtime 隔离。
- 领域侧也保留阻塞：直接能力调用在副作用期间 panic 或被 drop，不会仅靠释放锁放行后续写。
  批量关盘 leader 被单独丢弃时，所有跟随者都有明确结果，不会无限等回复。
- BLK 部分成功返回 `PartialSpace { allocated, committed, cause }`；不把错误解释为零副作用。
- 排空流程是 `disable_allocation → prepare_drain → VdService::evacuate → remove(proof)`。
  凭据绑定 Pool 实例、盘和领域版本；跨实例或过期凭据在任何移除副作用之前被拒绝。
  凭据是本地版本校验，不代替生产 VD 的持续引用屏障，也不持有跨域事务锁。
- provider 必须绑定正确 Pool。共享一个未做命名空间隔离的远程 provider，不会因本地 Pool ID
  不同而自动安全；真实 SDB、节点、BG fencing 仍需生产适配。
- 停服先取消应用流程，并取消领域预约中的操作；等二者停稳才结束根任务。
  已确认的最终提交仍返回成功。调用方丢弃回复不撤销已接收流程。

## 生命周期

Pause 拒绝新流程、阻止新的 Activity/领域预约；已开始的安全边界继续。
Resume 继续；Drain 允许已接收流程继续进入领域动作直到结束；
Stop 取消并等待领域动作停稳，之后保留的领域 facade 也拒绝新副作用。

PoolManager::remove 在停止等待期间保留路由和 task 所有权，取消这次管理调用
不等于强杀 Pool；之后可以继续 remove。Drop PoolManager 或 abort 根 task 是强制回收，
不能证明外部副作用已经停止。恢复阻塞只能经外部对账后重建，不能通过删记录自动解除。

## 迁移与明确未实现项

原 `MemberDiskService::new(...).start(...)` 改为 `Pool::new(pool_id, ...).start(...)`；
管理请求/查询/结果移至 process_manager，命名为
`ProcessCommand / ProcessQuery / ProcessReply / ProcessSnapshot`。
原 member_disk/workflows.rs 已移动，不再让领域服务编排完整生命周期。

已实现磁盘纵切面的应用/领域分层、Pool 路由和隔离。
VdService 只是已有排空协议的正式适配，没有 BG 队列、共享需求合并或完整 VD 状态模型。
Node 能力仍是 PoolNodes 显式端口，尚无具体 NodeService/TopologyManager。
没有持久化历史、确定性重放或切主恢复；生产背压、执行限额和历史持久化也仍需补齐。

本地未提交的独立实验不属于此正式库实现。

## 验收和运行

[范围与验证证据](../docs/proposals/2026-10-14-pool-process-manager.md)。

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo run --example pools
cargo run --example member_disk
```

本机使用已有 GNU 工具链及 ASCII sysroot：
`RUSTFLAGS=--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu`。
只借鉴 Workflow/Activity 的职责分离，不引入 Temporal 服务。
