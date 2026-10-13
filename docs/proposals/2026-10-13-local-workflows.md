# Local Workflow + Activity 重构

日期：2026-10-13；验证于 2026-10-14 完成。状态：已实现本地执行范围。

## 范围

以普通 async Workflow 表达 MemberDisk 离线、上线、缩容和空间操作。Activity
通过显式 provider 接口执行副作用，保护领域不变量。Runtime 统一管理执行记录、
对象占用、取消和批量结果，不再让 MemberDisk 维护 CommandTable/RequestTable
以及 Action 枚举与逐步 Completion 分发。

保留一个 Service 根 Tokio task 和现有 client/control/observer/task 外壳。
原有 ProcessManager/temporal_process 未提交示例保留，作为独立实验，不进入新模型。
不引入 Temporal 服务，不实现事件历史、确定性重放、持久化执行或切主恢复。

## 验收条件

- 直接阅读 workflow.rs 即可理解流程顺序；无 RequestPhase 或 Action 分发流水线。
- 同盘最多一个执行；相同意图排队，不覆盖输入；相反意图协作取消并等停稳。
- 已知 DOWN 取消所有在途 Shrink；不同盘可并发，空间操作保持全局互斥。
- 领域预校验 -> SDB 确认 -> 发布内存；查询不能看到尚未提交的修改。
- DOWN 安全边界不可中断；取消后的已提交终态仍返回成功。
- BLK 部分成功只重试未提交部分；取消/失败返回已提交进度，不能隐瞒部分分配。
- 提交结果不明、协议异常或 panic 阻塞受影响资源，不自动重试或放行下一操作。
- 批量调用逐项保留输入顺序与结果；submit 后可查询执行终态。
- Pause 阻止新的 Workflow/Activity，已开始的 Activity 继续到安全边界；
  Drain 完成已接收工作，Stop 协作取消，Abort 明确不保证外部副作用停止。
- Rust 测试、Clippy、示例与 JurisDesk npm run verify 通过。

## 边界说明

Workflow 的内存执行状态不替代 MemberDisk/VD 的权威领域状态。领域对象只通过
Activity 修改，锁不跨 await。领域冲突优先级仍由业务策略提供，不硬编码到 Runtime。
本地执行结果仅存内存；NeedsRecovery 只能经外部对账后重建服务，不提供自动解锁。
真实存储的 fencing、排空引用屏障和错误分类必须由生产 provider 实现；本原型测试
不能证明这些外部协议已经成立。

## 实现摘要与验证

- 删除旧 actions.rs、commands.rs、requests.rs、state_machine.rs；重写 service.rs 为薄适配。
- 新 workflows.rs 表达离线/上线/缩容/空间操作；activities.rs 封装提交与领域视图。
- Runtime Workflows 保存唯一执行目录；批量只聚合回复，命令进度由目录派生。
- 保留单根任务、三通道、Pause/Drain/Stop/Abort；新增 Activity gate 和进度唤醒。
  修复通道关闭后空 recv 饿死在途 Future 的问题。
- 元数据/节点错误默认不自动重试，provider 通过 is_retryable 显式证明安全；
  协议异常、panic 和不明结果进入 NeedsRecovery。未启动操作的拒绝不会扩大资源阻塞。
- BLK 部分成功返回 PartialSpace；历史最多保留 256 个普通终态，恢复阻塞不淘汰。
- 兼容性变化：RequestSnapshot/RequestStatus 与 requests 字段改为 Workflow 命名；
  Work 查询含终态结果。首次关盘保持合批，上线/禁止分配改为逐盘 Activity。
- 不修改独立示例、用户的 Cargo.toml 依赖改动或无关 src/runtime/driver.rs。

验证证据（2026-10-14，本机 GNU Rust 工具链）：

- cargo test --all-targets：正式库 21 项通过，已有 ProcessManager 8 项和持久化示例 10 项通过。
  后两组属于既有实验的回归，不计作本库持久化能力。
- cargo clippy --all-targets -- -D warnings：通过。
- cargo run --example member_disk：通过；观察到两盘初始 DOWN 合批与明确终态。
- rustfmt --edition 2024 --check src/lib.rs：通过。
- JurisDesk 根目录 npm run verify：通过（Harness、lint、TypeScript 与生产构建）。

运行使用 RUSTFLAGS=--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu。
另一个会话正在验证持久化示例，为避免 Cargo 锁冲突，本次使用临时 CARGO_HOME
（registry 路径联接到已有缓存）与 --target-dir target/workflow-refactor；未修改全局配置。
用已配置的本地代理补齐既有示例依赖后，最终检查全部以 --offline 运行。

测试覆盖：批量顺序/重复输入、未知盘不取消空间操作、同盘稳定取消/异盘进展、
DOWN 对 Shrink 的全局优先级、提交可见性、Pause 步骤边界、Stop 等待最终提交并保留成功、
根任务 abort、通道关闭、BLK 部分提交重试/失败/释放、未知结果/panic 的资源阻塞，
以及超过历史容量的大批量仍完整返回结果。
