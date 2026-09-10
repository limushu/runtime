# Service SDK Next

这个目录验证一套尽量朴素的进程内领域服务模型：SDK 只管理服务运行，业务代码仍然是
可以顺着读下去的 `async fn`。它不依赖 poll 轮次收集业务数据，也不会为了批量操作偷偷
注册回调。

## 两层职责

```text
Service SDK
  ├─ Query / Command / Control 三条通道
  ├─ 一个根 Tokio task
  ├─ poll 已确认执行的 Future
  ├─ Pause / Resume / Drain / Stop
  └─ Execution 观测

MemberDiskService
  ├─ 私有 MemberDisk 元数据
  ├─ Query 与批量 Command
  ├─ 同盘操作的 Join / Cancel / Wait
  ├─ SDB-first 提交
  ├─ offline / online / shrink 工作流
  └─ 业务 Task 观测
```

SDK 不知道 Disk、Node 或 BG。`MemberDiskService` 也不实现自己的 select 循环。它只把
一个普通 Future 返回给 SDK，SDK 的根任务负责持续 poll。

## 开发者首先看到的代码

命令入口只负责选择业务流程：

```rust
match command {
    MemberDiskCommand::DiskDown { disks, observed_at } => {
        self.run_disks(OperationKind::Offline, disks, Some(observed_at), &context).await
    }
    MemberDiskCommand::DiskUp { disks } => {
        self.run_disks(OperationKind::Online, disks, None, &context).await
    }
    MemberDiskCommand::Shrink { disks } => {
        self.run_disks(OperationKind::Shrink, disks, None, &context).await
    }
}
```

真正的批量边界写在工作流里，而不是由运行时猜测：

```text
offline(disks)
  1. set_disks_down(disks)           一次网络请求 + 一次 SDB 提交；整体不可取消
  2. join_all(isolate_disk(disk))    每盘独立推进，统一等待全部结果

online(disks)
  1. open_disks(disks)               一次网络请求，保留逐盘结果
  2. push UP(opened_disks)            只发送打开成功的盘
  3. commit(Online, opened_disks)     一次 SDB 请求，成功后更新内存

shrink(disks)
  1. commit(RequestShrink, disks)
  2. commit(DisableAllocation, disks)
  3. join_all(evacuate(disk))
  4. set_disks_down(evacuated_disks)
  5. commit(Remove, disks)
```

`join_all` 只是组合子 Future。它不会创建 Tokio task；SDK 仍然只有一个根 task，根 task
poll 顶层 Command Future，顶层 Future 再 poll 各盘的隔离 Future。所有已经开始的盘都会
得到结果，不会因为其中一盘失败就丢弃其他盘的 Future。

## 冲突和协作取消

批量 Command 可能互相重叠，所以 MemberDisk 按 UUID 保存一个很小的在途表。规则集中在
`operations.rs`：

| 当前操作 | 新操作 | 处理 |
|---|---|---|
| 相同操作 | 相同操作 | Join，复用已有结果 |
| Shrink | Online | Wait；Shrink 完成后 Online 作为 Rejoin |
| 其他冲突 | 新操作 | 设置旧操作的取消 token，等待旧 Future 到稳定边界，再重试新操作 |

DOWN 的“停 IO 并通知 user_dp”是强制安全动作，不检查取消 token。即使这时收到 UP，也先
完成整批 DOWN 网络请求和 SDB 提交，再让旧流程在恢复窗口或 VDM 边界退出。跨领域的
`VirtualDisks::evacuate` 接收同一个 token，必须先收敛在途 BG，再返回取消结果。

这个表同时是 MemberDisk 的业务 Task 来源；SDK 自己只报告 Execution，不把每个 Future
静默包装成业务 Task。

`OperationKind` 不是第二套业务事件。外部输入只有 `MemberDiskCommand`；它只是槽位中保存的
三个值大小的标签，用来比较“当前操作是否与新命令相同”，不会再次驱动工作流或制造
`DownApplied` 一类中间事件。

## 元数据规则

`MemberDiskService::commit` 是唯一写路径：

```text
读取当前对象并校验全部 mutation
  -> 一次提交 SDB
  -> SDB 成功
  -> 将相同 mutation 发布到内存对象
```

锁不会跨过 `.await`。同一块盘的写操作由在途表串行化，不同盘可以并发。Query 只读取
内存投影并返回 DTO，外部无法拿到活的 `MemberDisk`。

## 文件边界

```text
src/service/
  mod.rs          SDK 对业务的 Service trait
  runtime.rs      通道、生命周期、Future 驱动和 Execution 观测

src/member_disk/
  model.rs        MemberDisk、命令、查询和返回值
  ports.rs        SDB、PoolNode、VirtualDisk 能力边界
  service.rs      服务数据、唯一提交入口、Service 实现
  operations.rs   每盘一个在途操作及冲突规则
  workflow/
    mod.rs        批量命令与每盘在途操作的衔接
    offline.rs    下电与逐盘隔离流程
    online.rs     两阶段批量上电流程
    shrink.rs     计划缩容流程
  tests.rs        业务与运行时契约测试
```

## 验证

```powershell
$env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER='rust-lld.exe'
cargo +stable-x86_64-pc-windows-gnu test --manifest-path service-sdk-next/Cargo.toml --all-targets
cargo +stable-x86_64-pc-windows-gnu clippy --manifest-path service-sdk-next/Cargo.toml --all-targets -- -D warnings
```

测试覆盖批量 DOWN 单包、并发逐盘隔离、两阶段批量 UP、部分打开失败、Shrink、Shrink
途中 DOWN 接管、相同操作 Join、强制 DOWN 重试、SDB-first 失败不发布内存，以及服务
Pause/Drain 生命周期。
