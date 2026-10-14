# Service SDK Next

当前模型：**Pool 内通用 Workflow 管理者 + 各领域独立 Runtime**。
本次推倒旧的“磁盘专用 ProcessManager / 领域内部资源预约 / Pool 共用 driver”设计。

## 从这里读

```text
PoolManager：路由和生命周期
└─ Pool：实例隔离与组合
   ├─ ProcessManager + Runtime：通用流程推进、对象集合互斥、取消、结果
   ├─ DiskService + Runtime：独立消息队列、执行、查询、磁盘状态和提交
   └─ VdService + Runtime：独立消息队列、执行、排空能力
```

不是一个 Pool 一个 driver；当前每 Pool 三个独立根 task。
它们可以运行在同一个 Tokio executor 上；独立 Runtime 不等于独占线程。
新增领域沿用 ActivityHandler/ActivityService，不需要修改 ProcessManager。
新增 Workflow 是普通 async 函数，不需要扩展磁盘命令枚举。

| 文件 | 职责 |
| --- | --- |
| [process_manager/mod.rs](src/process_manager/mod.rs) | 接受任意 WorkflowTask；统一准入、执行目录、FIFO、取消、恢复阻塞 |
| [process_manager/objects.rs](src/process_manager/objects.rs) | 对象命名空间、集合/单对象、共享/独占 |
| [process_manager/context.rs](src/process_manager/context.rs) | Workflow 上下文、暂停/计时、Activity 执行许可与停稳追踪 |
| [runtime/activity.rs](src/runtime/activity.rs) | 每个领域独立的消息入口和 Future driver；无对象冲突队列 |
| [member_disk/service.rs](src/member_disk/service.rs) | 领域状态、类型化能力、预校验、提交后发布 |
| [vd.rs](src/vd.rs) | 独立 VD 服务及排空端口适配 |
| [workflows.rs](src/workflows.rs) | 磁盘离线、上线、缩容定义，属于应用流程而非管理者 |
| [pool.rs](src/pool.rs) | 装配三个 Runtime，路由、先收敛 Workflow 再停止领域 |

## 通用提交与具体步骤

```rust,ignore
// manager 不认识这个名称，也不需要知道磁盘命令类型。
let disks = handle.disks.clone();
let task = WorkflowTask::new(
    "allocate",
    vec![ObjectClaim::exclusive(ObjectKey::collection("disk"))],
    move |mut ctx| async move {
        let blocks = disks.allocate(&mut ctx, 4).await?;
        // 消费类型化结果；也可以通过应用的 oneshot 返回业务输出。
        Ok(())
    },
);
handle.client.call(ProcessCommand::Start(task)).await?;
```

`submit(Start(task))` 返回接收的 CommandId；`call(Start(task))` 等待整个流程及其
已派发 Activity 停稳。`Cancel(id)` 请求协作取消，不代表取消已经完成。
`query(())` 查询执行状态和最终错误；磁盘查询使用 `handle.disks.get/list`。
业务步骤得到类型化结果/错误；管理者只记录通用成功/失败，不引入领域结果大枚举。
如用应用输出通道，读到输出不等同于 Workflow 已终结，应同时等待管理者完成。

## 对象互斥只在一个地方

Workflow 开始前声明完整对象集合；管理者一次性取得全部对象，持有到整个流程结束。
同对象冲突 FIFO，不同对象并发；多个共享读可并发，独占写与读写均冲突。
单对象与所属集合冲突；不同命名空间、不同 Pool 不冲突。
第一版不动态扩锁，不自动抢占，不内置 DOWN/UP/Shrink 业务优先规则；
调用方可以显式取消旧 Workflow，再等待对象交接。

领域没有 Admission、每盘异步锁、空间占用队列。
领域 Activity 声明自身需要的对象；客户端只校验当前 Workflow 的许可是否覆盖，
以及是否属于同一 Pool 实例，不再竞争对象锁。同步目录 Mutex 仅保护短内存访问，
不跨 await。只读查询可看到已提交状态，不保证跨多次查询的一致性快照。

当前 Allocate/Release 保守要求整个 disk 集合独占。
因此它会等待同 Pool 的磁盘生命周期 Workflow（包括恢复计时/排空）结束。
这与被推翻版本的“等待期间仍允许其他磁盘分配”不同；将来可以细化对象集合。
跨领域流程必须声明所有实际受影响对象；目前 VD 仅提供以磁盘为目标的排空端口，
未来 BG 级能力必须声明对应 BG 对象，不能假定不同磁盘就天然没有 BG 冲突。

## Activity 与安全取消

Workflow 顺序 await 类型化 Activity；实际副作用在领域自己的 Runtime 内执行，
结果通过消息返回。这借鉴 [Temporal Workflow](https://docs.temporal.io/workflows) /
[Activity](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/activities/workflow-activity.mdx)
分工及[协作式取消](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/workflow/cancellation-and-termination.mdx)；
对象锁是本项目语义，不是声称 Temporal 自带这些锁。

- 取消传递到 Activity，等待领域返回稳定结果；已确认的最终提交可以成功。
- 许可随已派发请求一起追踪，而不是随调用者的等待 Future 生存。
  Workflow panic、丢弃等待 Future、提前返回，也必须等后台 Activity 停稳再交接对象。
- 同一 Workflow 上下文不可 Clone；步骤借用可变上下文。不支持并发发起多个 Activity；
  丢弃前一次等待后，其步骤未结束之前不能再发下一步。
- 未知副作用、Activity/Workflow panic 保留对象恢复阻塞。即使 Workflow 忽略 Activity
  的未知错误，也不能清除阻塞。被拒绝的等待者不会扩大恢复阻塞。
- 领域仍校验不变量，按“预校验 → provider 确认 → 发布内存”执行；
  只有明确未提交且可安全重试的错误才重试。不对整段 Workflow 自动重试。
- BLK 部分成功保留 PartialSpace 类型化进度；排空凭据绑定 Pool 实例/盘/版本。
  provider 必须提供持续引用屏障和正确的 Pool 外部命名空间。

## 生命周期和边界

`handle.control` 只控制 Workflow 管理者；`disk_control / vd_control` 分别控制领域。
暂停管理者阻止下一个步骤，但已经发出的 Activity 继续执行和提交。
暂停领域时，它拒绝新调用；不会自动重试或无限等待。
Pool shutdown 让已暂停领域恢复处理已接收请求，然后请求 Workflow 取消并等停稳，
最后停止所有领域。取消 shutdown/remove 的等待不会丢失 task 所有权，可以重试。

强行 Drop PoolInstance/PoolManager 或 abort task 是强制回收，不能证明远端副作用终止。
已持有的业务 client 不会自动路由到重建的同名 Pool。恢复阻塞没有“删记录即解锁”接口；
生产重建前必须外部对账，不能将本地重建当作安全恢复机制。

普通终态保留最近 256 项；恢复阻塞保留。队列背压目前只有有界消息通道，
没有总在途流程/历史容量上限。一个 ExecutionScope 只能对应一个管理者；
生产写路径必须全部通过这个管理者，不能在外部直接调用 provider。

## 本次没有声称完成

没有接入 Temporal 服务；没有历史持久化、确定性重放、切主恢复、Signal/Update、
自动补偿、Activity 幂等键协议、生产限流或完整 BG/Node/Topology 领域实现。
VdService 具有独立运行边界，但业务仍是排空端口适配，不是完整 BG 调度器。

## 验证

[重建范围与实测证据](../docs/proposals/2026-10-14-generic-workflow-manager.md)。

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo run --example pools
cargo run --example member_disk
```

本机可用 GNU 工具链及 ASCII sysroot：
`RUSTFLAGS=--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu`。
