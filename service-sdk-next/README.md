# Service SDK Next

这是一次从业务纵切面反推公共运行机制的实现。目标不是制造通用工作流引擎，而是让领域
Service 保有业务决定权，同时复用生命周期、三通道、Future 驱动和强制回收。

## 一条主线

```text
Command
  │ 保留 CommandId、reply、命令级 cancel
  ▼
拆为 Request ────────────────┐
  │                          │ 同一对象可以排队多个 Request
  ▼                          │ 不覆盖、不静默合并
Waiting ── admit ──> Ready   │
                    │         │
                    │ 状态机只决定下一项 Action
                    ▼         │
             按 Action 汇聚   │  是否批量由 MemberDisk 决定
                    │         │  Runtime 不认识 Action 或批次
                    ▼         │
                 Future       │
                    │         │
                    ▼         │
                 Running      │
                    │         │
            completion/result │
                    ├─────────┘ 未到目标：回到 Ready
                    └──────────> Finished：聚合到原 Command reply
```

这条路径没有旁路。磁盘事件、缩容、申请 BLK 和释放 BLK 都先成为 Command，再按相同的
Request 生命周期推进。Query 不修改状态，直接读取服务拥有的已提交内存视图。

## 边界

| 组件 | 只负责什么 |
| --- | --- |
| Runtime | 控制、查询、命令三通道；生命周期；单根 Tokio task；poll 业务 Future；强制 abort |
| MemberDiskService | 核心元数据、Command/Request 容器、准入、冲突策略、Action 汇聚和完成解释 |
| State machine | `已提交 MemberDisk 状态 + 当前意图 -> 下一项 Action / Done` |
| Actions | 把一种 Action 实现为普通 `async fn`；完成网络和 SDB 边界后返回事实 |
| CommandTable | 保存原始命令、聚合子 Request 结果、只在全部结束后回复调用者 |
| RequestTable | 保存 Waiting/Ready/Running/Retrying/Cancelling 和对象执行占用 |

Runtime 的业务接口故意很小：

```rust
pub trait Service {
    fn query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error>;
    fn receive_command(&mut self, command: IncomingCommand<...>);
    fn admit(&mut self);
    fn start_ready(&mut self, runs: &mut NewRuns<Self::Completion>);
    fn finish_running(&mut self, completion: Self::Completion);
    fn cancel_all(&mut self);
    fn is_idle(&self) -> bool;
    fn snapshot(&self) -> Self::Snapshot;
}
```

`NewRuns::push(async_fn(...))` 隐藏 pin 和类型擦除。框架只得到一个会产生 Completion 的
Future，不知道业务是否组批，也不参与 Action 选择。

## 一轮如何运行

运行循环的固定顺序是：

1. 有界接收当前已经到达的 Query 与 Command，避免持续输入饿死执行；
2. MemberDisk 把 Command 拆成 Request，并全部追加到 Waiting；
3. `admit` 依次检查 Waiting：放行、继续等待、请求当前工作取消，或明确结束；
4. 状态机为 Ready Request 计算下一项 Action；
5. MemberDisk 按 Action 汇聚 Request，并选择批量或逐个启动普通 Future；
6. Runtime 并发 poll 所有 Future；Completion 回来后，MemberDisk 更新已提交事实并决定
   Request 回到 Ready、Retrying，还是 Finished；
7. 对象位置释放后，原来阻塞的 Request 在下一轮再次经过同一个 `admit`。

控制通道在 `tokio::select!` 中优先。每轮主动收包有明确上限。Ready 中已有工作时无需等待
下一条 Command 才能启动。

## MemberDisk 状态机

MemberDisk 只保存业务权威状态，不保存 DiskMap 的实时物理副本：

```text
UA = Up + AllocationActive
UI = Up + AllocationInactive
DA = Down + AllocationActive
DI = Down + AllocationInactive
Removed = 身份仍保留，但不再服务和分配
```

状态决策集中在 `state_machine.rs`。它是纯函数，没有网络、锁、队列或 Future：

| 意图 | UA/UI | DA | DI | Removed |
| --- | --- | --- | --- | --- |
| Offline | CloseIo | 等恢复窗口 / DisableAllocation | Evacuate | Done |
| Online | UA: Done；UI: BringOnline | BringOnline | BringOnline | BringOnline（rejoin） |
| Shrink | DisableAllocation / Evacuate | DisableAllocation | Evacuate | Done |

状态机只说“下一步是什么”。`actions.rs` 才实现 CloseIo、BringOnline、Evacuate 等动作。
一个动作成功后，服务使用最新已提交对象再次决策，不用 `DownApplied` 之类的内部事件把
`await` 已经明确知道的结果重新绕回状态机。

## 准入与冲突

- 同一磁盘同时最多一个 current Request；后续请求完整保存在 Waiting 中。
- 相同意图排队，不静默 Join，因此每个外部 Command 都有自己的确定结果。
- 不同意图到达时，请求 current Request 协作取消；旧 Future 到稳定边界返回后才释放磁盘。
- 一个已知磁盘的 DOWN 会取消所有在途 Shrink Command，包括不关联该磁盘的缩容。这是
  MemberDisk 的全局业务规则，不在 Runtime 中。
- Allocate/Release 第一阶段与所有磁盘状态工作串行，先保证位图安全；后续可以在保持相同
  不变量的前提下缩小到 Tier/Partition 粒度。

取消 token 形成 `service -> command -> request` 的树。Command 可以整体取消，单盘冲突也能
只取消一个 Request。取消只是请求，不会任意 drop Future：

- DOWN 发布和对应元数据提交是不可中断的安全边界；
- 恢复等待和 VDM 排空接收 Request token，在稳定边界返回；
- 如果动作在取消到达前已经提交了最终目标状态，最终事实优先，Request 正常完成；
- `ServiceTask::abort()` 才会强制 drop 本服务拥有的全部 Future。

## 批量是业务选择

外部批量磁盘 Command 先拆成单盘 Request，冲突始终按对象判断。之后 Ready Request 可以被
重新汇聚：CloseIo、BringOnline、DisableAllocation 使用批量网络/SDB 操作；Evacuate 和恢复
等待逐盘运行。另一个 Service 可以采用完全不同的方式，Runtime 不需要修改。

因此批量不是框架概念，也不损害单盘取消、重试和结果关联。

## 元数据规则

Action 只返回 SDB 已经确认的 `CommittedChange`。根 Service 按固定次序处理：

```text
业务预校验 -> 提交 SDB -> Completion -> 发布到内存对象 -> 再次状态决策
```

Query 只能看到服务已经发布的提交视图。锁不会跨 `await`，因为核心元数据只在单根 Service
task 中访问。DOWN 的必达批次会等所有 SDB 项都确认后一次返回 Completion；重试期间内存
可能暂时落后于已经成功的部分记录，但不会领先于 SDB，切主仍以 SDB 重建。

Allocate/Release 可能跨多个 MemberDisk 记录，又没有跨记录事务。本实现逐盘推进，并把已
成功部分记录在当前 Request 上：后续失败只重试未提交余量，不会重复使用已经分配的 BLK。
测试覆盖了“第一盘成功、第二盘失败一次、仅第二盘重试”的情况。

## 生命周期与句柄

`MemberDiskService::start` 返回一个完整 Service 套件：

- `client`：`submit` 等接受确认，`call` 等最终结果，`query` 直接查询；
- `control`：Pause、Resume、Drain、Stop；
- `observer`：watch Runtime + 业务快照；
- `task`：唯一根 Tokio task 的所有权，可 `join` 或强制 `abort`。

| 状态 | 新 Command | 已启动 Future | 新 Action |
| --- | --- | --- | --- |
| Running | 接受 | 继续 poll | 可以启动 |
| Paused | 拒绝 | 继续安全收敛 | 不启动 |
| Draining | 拒绝 | 继续 poll | 已接收工作可以继续启动 |
| Stopping | 拒绝 | 发协作取消并继续 poll | 只推进稳定退出 |
| Stopped | 拒绝 | 无 | 无 |

Drop/abort `ServiceTask` 是最终故障隔离手段；Drain/Stop 则等所有已接收工作得到明确结果。

## 文件导航

| 文件 | 阅读问题 |
| --- | --- |
| `src/runtime/mod.rs` | 公共循环如何安排三通道、生命周期与 Future？ |
| `src/runtime/api.rs` | client/control/observer/task 如何对外暴露？ |
| `src/runtime/runs.rs` | 普通 Future 如何被隐藏 pin 并统一 poll？ |
| `src/member_disk/model.rs` | MemberDisk 拥有哪些核心数据和合法修改？ |
| `src/member_disk/protocol.rs` | 外部 Command、Query、Reply 与观测快照是什么？ |
| `src/member_disk/commands.rs` | 原始 Command 如何保留、取消、聚合和回复？ |
| `src/member_disk/requests.rs` | 每个 Request 如何排队、占用对象、重试和完成？ |
| `src/member_disk/state_machine.rs` | 当前状态与意图如何决定下一项 Action？ |
| `src/member_disk/actions.rs` | 每项 Action 实际执行哪些网络与持久化步骤？ |
| `src/member_disk/service.rs` | 上述组件如何组成一个完整领域 Service？ |
| `src/member_disk/tests.rs` | 批量、排队、分层取消、生命周期和部分提交如何验证？ |
| `examples/member_disk.rs` | 调用方如何启动服务并执行业务？ |

## 验证

Windows 当前环境缺少 MSVC linker，使用已安装的 GNU Rust toolchain：

```powershell
$env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER='rust-lld.exe'
cargo +stable-x86_64-pc-windows-gnu test --all-targets
cargo +stable-x86_64-pc-windows-gnu clippy --all-targets -- -D warnings
cargo +stable-x86_64-pc-windows-gnu run --example member_disk
```

当前验证：8 项契约测试通过，所有 target 编译通过，Clippy `-D warnings` 通过。
