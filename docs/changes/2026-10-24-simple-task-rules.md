# 单一调度规则与 token 停稳返回

## 目标与边界

保留已有 Task / Workflow / Driver / Service 命名、组合根和持久化接口。
减少业务读者需要拼接的概念；不新增通用工作流引擎、规则 DSL、自动重试或自动解环。

## 验收标准

| 要求 | 实现与验证入口 |
| --- | --- |
| 已有 A、来了 B，只有一套规则 | SchedulingPolicy::on_task；取消 relation / priority / admission 组合 |
| 规则明确产生等待，无全局 FIFO 屏障 | 调度器按 TaskId 扫描，只应用规则决定；缩容与普通卷操作可并行 |
| 新任务失败不能影响旧任务 | scheduling_policy 的持久化失败、其他任务拒绝测试 |
| 拓扑必须等缩容真实停稳 | shrink_workflow 三任务交接测试，普通卷与拓扑的显式冲突仍等待 |
| 只有 token 控制路径 | 移除 ActivityControl、set_activity_control、register_control、ActivityState::Suspended |
| 调用返回后再确认 Task 暂停 | Workflow 保存检查点并进入 wait_until_runnable；可读链路见 task-center.md |
| 恢复重新准入，取消等待真实收尾 | managed_workflows、shrink_workflow 的条件变化、暂停/取消竞态测试 |
| 部分结果与实际完成不同 | Completed 不重做；Suspended 是调用终态，业务明确续作 |
| 多暂停原因独立 | 多 maintenance、用户暂停、请求代次变化和暂停中取消测试 |
| 同一 Task 的观测与恢复不改模型 | TaskSnapshot / TaskObserver / SavedTask 保持；注册时恢复 details / objects，首次轮询前可查询 |
| 恢复步骤在入口直接可读 | restore 分为读取、全量准备、接管、注册；后续记录校验失败不提前写入，接管失败不注册任务 |
| 控制文件职责明确 | 删除 workflow/control.rs；RunControl 与请求/确认集中到 control.rs，执行等待留在 WorkflowContext |

## 实现摘要

默认策略的所有规则集中在 conflicts.rs。保留 TaskCenter::conflict 原入口和 ConflictDecision 名称，
增加 SuspendThenWait，SchedulingPolicy 只有 on_task(existing, incoming)。State 先全量检查、确认接受，
再应用暂停/取消。框架不从对象重叠推断额外依赖；默认存储规则仍对普通拓扑交接显式使用对象范围。
Uncertain 隔离继续覆盖对象重叠及协调关系，不当作普通暂停释放。

control.rs 集中 RunControl、请求、token 和确认；控制意图持久化归入 driver/execution.rs。
WorkflowContext 在 workflow.rs
等待调用返回并在安全边界确认暂停，删除 workflow/control.rs。RunCondition 位于 Workflow 契约旁，
ActivityExit 位于领域调用契约旁，根路径导出保持兼容。
领域服务从请求上下文读取 token，不向框架注册控制函数。缩容的 migrate_to_completion 方法显式
表达停稳返回、保存、等待、续作；外层阶段 match 仅推进业务阶段。没有自动 redo。
示例 managed_shrink 同步改为唯一的停稳返回模式。

## 兼容性

普通 ctx.activity 调用、Workflow、RunControl、TaskSnapshot、SavedTask 与存储格式保持。
自定义策略把原 admission/relation/priority 合并为 on_task；使用 hook 的适配器需改为监听 token，
停稳返回业务结果。ActivityExit::Suspended 名称保留，避免无必要的业务协议重命名。

旧运行图仍是 53fae88 快照，保留用于历史对照并显式标注；当前控制链和规则以 task-center.md 为准。
本次不重新设计动画；基础设施文件按上述职责整理，不另建运行时或任务管理层。

## Task 恢复的代码组织

driver/recovery.rs 的入口直接表达 load_pending_tasks → prepare_tasks → claim_tasks →
register_restored_tasks。异构 Workflow 的类型擦除只留在恢复模块底部，不再放入 persistence.rs。
PreparedRestore 仅是这一轮恢复的临时数据，不是新的 Task 类型或管理载体。
Workflow::restore 保留名称，仅重建执行体；Driver 负责恢复同一 Task 的身份、控制和启动许可。

全部记录准备成功才开始逐条 CAS 接管，全部接管成功才注册。接管不是批量事务，部分写入失败
不回滚已写记录；保持新任务入口关闭，重试重新读记录。这没有把整个恢复过程宣称为原子事务。
恢复注册同步装载原 details / objects，修复此前需要首次轮询才能看到这些数据的问题。
旧暂停确认不复制，仍需新执行体到达安全边界。

## 2026-10-25 职责整理与注册边界

在已有命名和公开接口基础上完成以下调整：

| 验收点 | 实现 |
| --- | --- |
| 存储不定义运行时装配对象 | Dependencies 移到 workflow.rs，Restored 移到 driver/recovery.rs；persistence 仅保留旧路径重导出 |
| 业务接口与 Task 执行包装分开 | Run、Tracker、ActivityGuard、Completion、execution 和 sync_controls 归入 driver/execution.rs |
| 规则定义与规则计算同属一个模块 | ConflictDecision / Conflict / ConflictMatch 移到 scheduling.rs，根导出保持 |
| 注册不会重复执行可失败声明 | 预检完成后保留 Workflow，接管后构造所有执行体，再执行无 Result、无业务回调的注册循环 |
| 接管期间失主不暴露任务 | 存储 IO 前后检查宿主有效性，失主返回 Unavailable，下一主可继续恢复同一 TaskId |
| 存量业务调用方式保持 | 普通提交、defer、activity、观测、恢复接口和 SavedTask 格式不变 |

移入 execution 的控制写入 Future 与业务 Future 仍并行轮询，保留先前的死锁修复。
没有新增 Activity 种类、执行宿主、任务管理器或自动恢复重试。
恢复批次的存储接管仍为逐条 CAS，不承诺跨记录事务或对并发观测者的原子批量快照。
主身份的本地检查不替代外部领域操作的 fencing。

## 验证证据

2026-10-24，在本机 GNU Rust 工具链（--locked --offline）完成：

- `./scripts/verify.ps1 -Toolchain stable-x86_64-pc-windows-gnu -Offline`：通过。
  包含 rustfmt、143 个 workspace 单元/集成测试、全部 example 编译、文档测试与 Clippy `-D warnings`。
- 6 个示例实际运行通过：two_services、pool_lifecycle、shrink_commands、task_center、managed_shrink、shrink_workflow。
  managed_shrink 经过 WaitingConditions 后以同一 TaskId 完成两次调用；真实缩容示例停稳交接后移除目标盘。
  pool_lifecycle 仍明确报告未实现的离线重建占位，不伪造成功。
- 外围 JurisDesk `npm run verify`：结构检查、lint、TypeScript 和生产构建通过。
- 历史图示验证脚本通过：404 帧/9 条旧版轨迹与六图检查；只更新历史版本提示，没有把旧动画宣称为新机制。
- `git diff --check`：通过。

新增竞态测试首轮暴露了测试自身提前丢弃进度接收端的问题，修正测试生命周期后全套通过。
真实 C 适配器和生产 KV 未接入；上述验证使用仓库模拟后端与明确的手动停稳回执。

同日补充文件职责与 Task 恢复整理后再次验证：

- 完整 Rust verify 脚本通过：146 个单元/集成测试、example 编译、文档测试、rustfmt 和 Clippy。
- 新增三项恢复回归：首次轮询前观测及启动门控、全批校验失败不提前接管、部分接管失败无注册且重试保留 TaskId。
- `managed_shrink` 与 `shrink_workflow` 再次实际运行通过，覆盖停稳、重新准入、续作和最终移除。
- 外围 `npm run verify` 与 `git diff --check` 通过。
- 启动门控测试首轮误把一次 tick 返回视为执行完成；tick 可以返回内部事件，改为持续驱动并检查业务未启动，随后通过。

2026-10-25 职责整理完成后的验证：

- 完整 Rust verify 通过：148 个单元/集成测试、所有 example 编译、文档测试、rustfmt、Clippy `-D warnings`。
- 两项新增回归通过：整批恢复不重复调用可失败声明；接管期间失主不注册任务且下一主保留原 TaskId 恢复。
- 恢复测试显式使用 `persistence::Dependencies` 和 `persistence::Restored`，验证旧导入路径仍兼容。
- `managed_shrink` 和 `shrink_workflow` 实际运行通过；暂停交接、恢复准入、两次迁移调用和最终移除行为保持。
- 外围 `npm run verify` 与 `git diff --check` 通过。本轮没有修改存储池业务 Workflow。
