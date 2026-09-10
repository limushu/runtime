# Service SDK Next

这是一个独立实验目录，不修改现有 `src/member_disk`。目标不是建立业务框架宇宙，
而是只回答一个问题：**Pool 内的领域服务应具备哪些统一能力，业务开发者又只需要写什么？**

## 一条边界

```text
外部
  ├─ Query   ───────> 只读查询
  ├─ Command ───────> 准入 -> Future -> 完成
  └─ Control ───────> Pause / Resume / Drain / Stop
                         │
                    Service SDK
                         │
              MemberDiskService（私有数据）
                         │
              显式状态转换 + await 业务动作
```

外部拿不到 `MemberDisk`、对象表或锁。读操作通过 `Query`，写操作通过
`Command`。`ServiceEndpoint` 只是通道的薄封装，不承载业务规则。

## Service 契约

每个领域服务实现同一个 `Service` trait：

| 业务实现 | 含义 |
|---|---|
| `name` | 服务名 |
| `command_key` | 哪个对象拥有本次命令；SDK 据此建立单对象执行槽 |
| `admit` | 业务冲突规则，只回答 Run / Join / Queue / Replace / Complete |
| `handle_query` | 查询私有数据，不创建业务任务 |
| `handle_command` | 普通的 async 业务入口，可直接使用 `await` |
| `task_snapshots` | 业务主动投影它认可的 Task；SDK 不暗中制造业务 Task |
| `initialize` / `shutdown` | 可选的业务启停钩子 |

SDK 默认实现并统一管理：

- 一个根 Tokio task 和一个 `FuturesUnordered` 驱动集合；
- Query、Command、Control 三条语义不同的通道；
- `Initializing / Running / Paused / Draining / Stopping / Stopped / Failed`；
- 每对象一个执行槽，以及合并、排队、替换的机械动作；
- 协作取消：替换命令只设置当前 Execution 的 token，旧 Future 在稳定边界返回后才启动替代者；
- 两层观测：SDK 报告正在轮询的 Execution，业务通过 `task_snapshots()` 报告业务 Task。

这里没有通用状态机 trait，也没有通用 Action DSL。状态机是 MemberDisk 的业务知识，
SDK 只驱动它返回的 Future。

## MemberDisk 如何接入

`MemberDiskService` 自己保存私有 `HashMap<DiskUuid, MemberDisk>`，并实现：

```text
Command
  -> admit：读取同一 disk 的当前 Execution，给出冲突决定
  -> handle_command：显式登记一个领域 Task
  -> reconcile_once：读取已提交的 MemberDisk 状态
  -> 执行一个动作并 await
  -> SDB commit 成功后更新内存
  -> 回到 reconcile_once，直到稳态
```

状态转换按起始状态拆开，见 `src/member_disk/state_machine.rs`。例如普通下盘：

```text
UpActive
  -- DiskDown / 停 IO 并广播 DOWN（必须完成） --> DownActive
  -- 等待恢复窗口（可协作取消）              --> DownInactive
  -- 请求 VDM 排空所有 BG                    --> DownInactive
  -- 无 BG 引用后提交 Remove                  --> Removed
```

上线与下盘冲突时，SDK 设置下盘 Execution 的取消 token。停 IO 这个安全动作不观察
该 token；完成并提交 DOWN 后，统一驱动循环在稳定边界发现取消并返回，然后同一对象槽
才启动 UP。业务开发者不需要在每个小动作中反复写 `is_cancelled()`。

Shrink 是持久化目标：先提交 `RequestShrink`，随后禁止分配、排空 BG、停 IO、移除。
Shrink 途中收到 DOWN 时，DOWN 替换当前 Future，先完成安全停 IO；因为 shrink intent 已
持久化，新 Future 会继续走到 `Removed`。UP 则排在 Shrink 后面，移除完成后按 rejoin 执行。

## Execution 与 Task

二者不能混为一谈：

- **Execution**：SDK 正在 poll 的一次 Query/Command Future，用于看运行时是否忙、是否取消。
- **Task**：业务认为值得展示和审计的工作。MemberDisk 在 `handle_command` 中自行登记；
  简单 Query 不登记。以后一个重建 Execution 内部聚合多个硬盘，也完全可以只投影一个
  “Pool 正在重建”的 Task。

## 运行验证

```powershell
$env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER='rust-lld.exe'
cargo +stable-x86_64-pc-windows-gnu test --manifest-path service-sdk-next/Cargo.toml --all-targets
```

测试覆盖 Query 隔离、相同 DOWN 合并、UP 协作替换 DOWN、Shrink 完整转换、Shrink
途中 DOWN、SDB-first 失败不发布内存，以及 Pause/Drain 生命周期。
