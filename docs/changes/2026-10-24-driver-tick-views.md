# Driver tick 分支与三任务等待环

## 基线与代码阅读

已快进拉取 `40df97b`、`c067838`、`53fae88`，本次以 `53fae88` 为图示基线。
本次只更新图示、教学轨迹及验证脚本，没有改动 Rust 实现。

- `ShrinkWorkflow` 使用 `loop + match ShrinkStage` 推进，调用现有 Disk / VD / Balance 服务。
- `BalanceHost` 通过独立控制接口提交 suspend/cancel，继续等待原 C completion；
  `ActivityExit::Suspended` 表示原调用已经停稳结束，只保留可序列化的续作数据。
- Workflow 更新进度（Durable 还需保存检查点）后进入 `wait_until_runnable` 确认暂停；
  恢复时检查 `RunCondition`，然后用同一 TaskId 发起下一次迁移 Activity。
- 原死锁是调度等待环，而不是 Rust 互斥锁死锁：运行缩容 A 为 topo B 让位，
  B 按旧 FIFO 等早到的卷任务 C，C 又等 A。旧演示只覆盖 A/B，漏掉了这个组合。
- 当前默认策略的 priority 为 topo=2、其他=1、shrink=0；同级 FIFO。
  它移除 B 等 C 的边，但不允许跳过 A 的停稳 ACK，也不抢走运行中的 Exclusive 占用。
  自定义策略仍需保证 priority 与 YieldTo 一致，不承诺通用的等待环自动检测。

源码入口：
[Driver](../../crates/workflow-foundation/src/driver.rs)、
[通用调度](../../crates/workflow-foundation/src/scheduling.rs)、
[默认策略](../../crates/storage-pool/src/task_center/conflicts.rs)、
[缩容流程](../../crates/storage-pool/src/workflows/shrink.rs)、
[BalanceHost](../../crates/storage-pool/src/domains/balance/runtime.rs)、
[死锁回归](../../crates/storage-pool/tests/shrink_workflow.rs)。

## 验收标准与实现

| 要求 | 图示表现 |
| --- | --- |
| 两个工作分支各自可辨认 | ① events.recv、② running.next 并排；不画成顺序执行的两个 worker |
| 区分 Pending 与返回 | Activity 等待只使 Future Pending；整个 Workflow 完成才返回 Some(TaskId) |
| 明确资源分支行为 | 消费一条事件 → table.event → publish → None；外层继续 reap/pump |
| 展示通知分支 | wake.notified 单独列出；控制/ACK 或准入确认通知外层重算 |
| 区分两种唤醒 | Permit/服务回复 Waker 只唤醒等待链，不能等同 tick 返回 None |
| 同步当前 API | RunCondition、Readiness::Blocked、wait_until_runnable、Suspending/Suspended |
| 解释死锁修复 | 新增三任务场景，展示旧环、优先级移除等待边、真实停稳及交接 |
| 保持现有入口可用 | 动画与静态指南原 URL 不变，仍可离线打开 |

动画共 7 个场景入口、8 组轨迹、346 个不可变事件。画布上方的分支栏随着事件高亮，
同步说明当前是 poll、普通唤醒，还是分支返回。多个同步动作可拆为教学事件，
这些帧不是精确 tick 计数，也不声称每个合法执行都会发生相同交错。

静态指南第二图重画 select 分支，第三图改用当前缩容的停稳返回契约；
第四图继续保留框架两种契约的比较，明确当前缩容采用哪一种。

静态预览：[tick 资源分支](../diagrams/task-center-simulation/tick-preview.png)、
[三任务优先级交接](../diagrams/task-center-simulation/priority-preview.png)。
动画支持「放大阅读」，放大后在画布内滚动查看组件。

## 验证

```sh
python scripts/render-runtime-lab-graph.py
python scripts/render-unified-task-guide.py
node scripts/verify-task-center-simulation.mjs --browser
node scripts/verify-unified-task-guide.mjs --render
cargo test --locked --offline -p storage-pool --test shrink_workflow
```

- 动画轨迹检查覆盖三任务等待边变化、ACK 前不放行、空 Balance 资源集合、
  8 次缩容 Activity、原 Task 身份，以及事件分支/通知/整体完成的区别。
- Chrome 逐事件核对状态与分支说明，检查播放器、暂停、关键点、拖动、键盘、
  等待环面板、SVG 文字边界、移动端、减少动态效果与离线行为。
- 六张静态图重新生成 SVG/HTML/manifest/PNG，并通过交互、打印和文字边界检查。
- 本机 GNU 工具链执行缩容测试：14 项全部通过，包含
  `topo_waits_for_native_stop_then_resumes_same_task_without_fifo_cycle`。
- 外围 `npm run verify` 通过：Harness、lint、TypeScript 与生产构建。

入口：[运行动画](../diagrams/task-center-simulation/index.html)、
[组件指南](../diagrams/unified-task-guide/index.html)、[运行导读](../runtime-guide.md)。
