# Service SDK Next

当前实现：本地 **Workflow + Activity + Runtime**。业务顺序写成普通 async 函数，
Activity 通过显式 provider 端口完成副作用，Runtime 管理执行和取消。
不引入 Temporal 服务，不提供持久化历史、确定性重放或切主恢复。

本地独立实验不包含在此次重构提交中，也不构成本库的持久化执行保证。

## 先读流程

从 [MemberDisk workflows](src/member_disk/workflows.rs) 开始：

```text
离线：关 IO → 等恢复窗口 → 禁止分配 → 排空 → 移除
上线：校验/打开 → 发布 UP → 提交并发布成员状态
缩容：禁止分配 → 排空 → 移除
空间：预校验整份计划 → 按盘提交 → 汇总已提交结果
```

Workflow 用普通条件分支读取已提交领域状态，使用
`ctx.activity("name", typed_async_call()).await` 表达步骤。
没有 RequestTable、RequestPhase、Action 枚举、执行转发表或逐步 Completion 回调。
完成一个 Activity 后，返回值直接交给调用它的 Workflow。

## 三个边界

| 文件 | 职责 |
| --- | --- |
| [workflows.rs](src/member_disk/workflows.rs) | 离线、上线、缩容、空间操作的顺序和冲突策略 |
| [activities.rs](src/member_disk/activities.rs) | 领域预校验、provider 调用、SDB 确认、内存发布 |
| [runtime/workflow.rs](src/runtime/workflow.rs) | 唯一执行目录、资源准入、协作取消、Pause gate、结果与批量回复 |

[service.rs](src/member_disk/service.rs) 是薄适配层：把外部命令映射为 Workflow，
提供查询，把领域冲突策略交给 Runtime。MemberDisk/VD 仍拥有业务权威状态；
Workflow 的 Waiting/Running/Completed 等只描述执行，不是第二套磁盘状态机。

原有低层 `Service` hooks 保留用于宿主适配和独立示例，不再要求业务为每个
Activity 实现 admit/start/finish 循环。整个服务仍只有一个根 Tokio task，
由 FuturesUnordered 并发 poll；没有每盘 task，也没有另一个 WorkflowScheduler。

## 调用与观测

`MemberDiskService::start` 仍返回 `client / control / observer / task`。

- `client.call(command)` 等待最终业务结果；批量磁盘调用按原输入顺序返回每项结果。
- `client.submit(command)` 等待内存接收确认，不代表持久化或已经执行。
- `query(Get/List)` 读取已提交领域视图。
- `query(Work)` 和 observer 返回 `WorkflowSnapshot`，包含执行 ID、command ID、
  磁盘、状态、当前 Activity 和终态结果；普通终态保留最近 256 项。
- `query(InFlightCommands)` 从同一执行目录派生批量进度，没有独立命令状态机。
- 调用方丢弃回复等待，不会撤销已接收操作。

API 迁移：`RequestSnapshot/RequestStatus` 替换为 `WorkflowSnapshot/WorkflowStatus`；
快照的 `requests` 字段替换为 `workflows`。`Work` 查询现在包含保留的终态，
判断在途执行使用 `result.is_none()`。

## 并发和取消

- 同盘最多一个运行 Workflow；重复和相同意图排队，不覆盖、不静默合并。
- 相反意图请求旧执行取消，等旧 Activity 停稳后才释放占用。
- 已知盘的 DOWN 取消所有在途 Shrink，包括其他盘；这是显式领域策略。
- Allocate/Release 仍与所有磁盘流程互斥；独立磁盘可以并发。
- Activity 不通过 select/drop 中断 provider Future。取消在步骤边界检查；
  长排空通过 token 请求 provider 停稳。
- DOWN 广播与元数据边界不可中断；只对 provider 确认可重试的失败继续重试。
- 最终 Activity 已提交成功时，晚到的取消不把成功改报为取消。
- 首次关盘的并发调用仍由 Activity 内部合并广播/提交；不为组批重建通用 Action 层。
  上线和禁止分配当前逐盘执行，不承诺旧实现的全流程组批性能。

| 控制 | 已接收执行 |
| --- | --- |
| Pause | 拒绝新命令；已启动 Activity 停稳；不开始新 Workflow/Activity |
| Resume | 继续执行 |
| Drain | 拒绝新命令；恢复 Pause gate 并完成已接收工作 |
| Stop | 拒绝新命令；取消等待项，等运行 Activity 稳定返回 |
| Abort / drop task | 强制丢弃本地 Future；不能证明外部副作用停止 |

Stop 不是超时强杀：必达边界或 provider 不返回时可能一直等待。
已完成但需要恢复的执行不阻止停止服务，却持续阻塞该服务实例中的相关资源。

## 提交和错误契约

`预校验 → SDB 确认 → 发布内存` 在 Activity 内完成。共享领域目录只在短同步
临界区读写，锁不跨 await；Runtime 的资源占用保护异步提交期间的互斥。

元数据和节点端口新增 `is_retryable`，默认 false。只有 provider 能证明
**结果已知、可安全重试**时才覆盖它。超时、结果不明和错误响应数量进入
`NeedsRecovery`；panic 被隔离，同样保留资源阻塞。不自动重跑整条 Workflow。

BLK 计划先整体校验，再逐盘提交。重试只针对未提交的盘。部分成功后失败或取消，
返回 `PartialSpace { allocated, committed, cause }`，明确报告已提交的分配/释放；
调用方必须据此对账，不能假设失败代表零副作用。

VD 排空成功必须建立到移除为止有效的引用屏障；取消返回必须代表动作停稳。
这仍是生产 provider 的外部契约，本地内存替身不证明真实存储协议成立。
需要恢复的资源只能经外部对账后重建服务；没有自动解锁接口。
终态缓存有界，但待执行数量、大批量输入和恢复阻塞记录仍需生产级限额。

## 验收和运行

[范围、验收条件与验证记录](../docs/proposals/2026-10-13-local-workflows.md)。

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo run --example member_disk
```

本机 GNU 工具链需要已有 ASCII sysroot 路径：

```powershell
$env:RUSTFLAGS = '--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu'
cargo +stable-x86_64-pc-windows-gnu test --all-targets
```

仅借鉴 [Temporal Workflow](https://docs.temporal.io/workflow-execution) 与
[Activity](https://docs.temporal.io/activities) 的职责边界，不声称兼容其 SDK 或耐久性保证。
