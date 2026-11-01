# 按池查看 Workflow：界面简化与完成语义纠正

## 目标 / 验收

以业务阅读顺序取代四个并列技术页签、全局 KPI 和右侧检查器：

1. 池页同时显示**当前 Workflow**和**历史 Workflow**。不跨池混排，空分区明确说明。
2. 进入 Workflow，显示该 Workflow 的结果、当前执行批次、已记录的步骤数量、每项结果。
3. 进入步骤，显示结果说明、进度、错误、资源和内部调用。审计时间线及 JSON 留在二级入口。

两个任务分区各自分页，Tab 切换焦点；回退保持选择。刷新、搜索、冻结不执行业务控制操作。
键盘 Enter 或终端鼠标单击都可下钻；最小 64×20，中文字符宽度和弹窗覆盖有测试。

## 为什么旧界面的 DONE 容易产生矛盾

用实际 `leader_handoff` 日志定位到以下链路：

- 旧主 epoch 11 的 `balance.evacuate` 最后进度是 **2/20**。
- `BalanceHost` 返回 `Ok(ActivityExit::Suspended(checkpoint))`，含义是一次调用安全结束并交回恢复数据。
- `ActivityObservation::settled` 对 `Ok(_)` 设置 `ActivityState::Completed`。
  对 `ActivityKind::Call`，这确认的是**调用返回**，不是完整业务目标已完成。
- 同一 Workflow 在新主 epoch 12 恢复，新的调用达到 **20/20**，后续校验/移除完成，Workflow 最终成功。
- 原 UI 把调用 Completed 和逻辑步骤 Completed 都画为 DONE，并把两个批次所有调用平铺在同一页。

本次修正观测解释与查询投影，不改变领域返回值、调度或主备恢复算法：

| 记录 | 当前界面含义 |
| --- | --- |
| Workflow Completed | 工作流成功结束 |
| Step Completed | 逻辑步骤成功 |
| Call Completed | 调用正常返回，**不据此宣称业务目标完成** |
| Failed / Cancelled / Uncertain | 失败 / 取消 / 待核对，不统称“完成” |
| 已结束批次仍有非终态步骤 | 末态未确认；不补造终态 |
| Workflow 上报成功而同批步骤未闭合 | 显式提示记录矛盾，保留原始状态供核对 |

默认只显示最新执行批次。e 可查看旧批次，页面分别标出 Workflow 最终结果和该批次结果。
迟到的旧批次事件仍可审计，但不会覆盖新批次当前状态或回滚池 owner epoch。

## 计数口径与尚未实现的能力

主列表只计顶层逻辑 Step 和无父步骤的历史 Call；子调用在父步骤详情列出，不重复计数。
如果缺失父记录，子调用仍保留为顶层可见记录，不静默丢弃。
未静态登记完整步骤计划时，只展示“已记录 N 项”，**不是完整流程 N 步或 N/N 完成率**。
重试不增加逻辑步骤数。跨恢复批次的步骤没有稳定业务步骤键，不能按名称合并为一套完整步骤。

目前存储池既有 Workflow 主要直接使用 `ctx.activity()`，审计里是 Call，不是逻辑 Step。
UI 明示“只记录了服务调用”，并使用“调用返回”结果；不会伪造业务步骤结果。
基础库 `ctx.run_activity()` 产生真正的 Step / 子 Call 层次，界面和测试支持此层次。
把每个存量业务 Workflow 都改成显式逻辑 Activity、提供计划与稳定跨批次步骤键，是后续业务建模工作，
本次没有偷偷重写业务流程。旧记录也没有保存任意业务返回 payload，详情明确说明这一点。

## 运行

```powershell
node tools/task-center-tui/index.mjs D:/Temp/实际文件.audit.jsonl --pool handoff
node tools/task-center-tui/index.mjs D:/Temp/实际文件.audit.jsonl --once --width 100 --height 30
```

默认进入有当前任务的池；全部已结束时进入历史最多的池，之后不因新记录自动换池。
旧启动参数 `--tab pools/workflows` 兼容为池页；全局 Activities / Events 页已取消。

| 键位 | 操作 |
| --- | --- |
| Enter / 左键点击 | 打开 Workflow / 单步详情 |
| Esc / b | 返回；弹窗中关闭弹窗 |
| Tab | 池页当前/历史分区切换 |
| p / e | 切换池 / 执行批次 |
| ↑↓ / j k / PgUp PgDn | 选择、翻页或滚动详情 |
| / | 搜索当前层级，Esc 撤销编辑 |
| a / r | 审计时间线 / 原始记录 |
| Space | 冻结视图，继续收集记录 |
| h / q | 帮助 / 退出 |

## 实现与验证

`app.mjs` 管三层导航和池范围；`screen.mjs` 管简洁布局；`terminal.mjs` 保留经过测试的字符帧绘制。
`semantics.mjs` 统一结果和计数解释；`model.mjs` 独立维护各批次状态；`mouse.mjs` 解析 SGR 鼠标协议。
读写隔离与持久化边界沿用[审计说明](2026-11-01-persistent-observation.md)，不新增网络服务或业务控制。

```powershell
node --test tools/task-center-tui/model.test.mjs tools/task-center-tui/ui.test.mjs
```

21 项测试通过：当前/历史同时可见、层级回退、子调用计数排除、2/20 返回不冒充成功、最新/历史批次、
旧事件迟到、记录矛盾提示、旧 Call-only 记录、空池、分页/搜索/稳定选择、冻结、审计、
64/80/120/160 列与小窗口、中文/emoji、鼠标分段消息与点击、损坏/追加/截断读取。
外围 `npm run verify` 通过且无 lint 警告。Windows PTY 用真实交接日志实测鼠标点击打开缩容 Workflow、
默认定位最新批次、进入单步详情及退出恢复终端/关闭鼠标跟踪。此次只改客户端与文档，未修改 Rust 执行逻辑。
