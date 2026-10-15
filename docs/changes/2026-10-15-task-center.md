# Pool TaskCenter 重构

## 目标与验收

- 每个 Pool 有一个具体 TaskCenter；任务协调状态与业务冲突判断在同一组件，不再注入策略 trait 或配置规则引擎。
- 冲突判断有单一可读入口，明确区分无冲突、等待、取消后等待、拒绝；先检查全部在途任务，再实施取消。
- TaskCenter 在同一个事件循环管理队列、对象占用并推进 Workflow；执行驱动不单独 spawn，也不通过 RuntimeHandle 提交到第二个循环。
- 取消必须等待 Activity 停稳；不确定结果保留对象隔离；排队任务可取消，互不冲突任务可并发。
- 按业务聚合 domains/{disk,node,vd}/{model,service,runtime}.rs；model 不依赖 workflow-foundation、Tokio、ActivityContext，service 不依赖托管自己的 runtime。
- 保留完整磁盘上线 Workflow、节点交接、字段级元数据提交和既有安全语义。
- 移除旧策略 API 并迁移示例/测试，增加冲突扩展说明和自动边界检查。

## 范围

本轮不是持久化工作流引擎；暂停恢复仍限于进程内安全边界。不增加自动重试、真实磁盘 IO 或尚未实现的缩容能力。
不修改用户未跟踪的历史设计文档。

## 实现与验证

最终按讨论后的单循环、垂直业务模块方案实现：

- `src/task_center/mod.rs` 同一个 select 内处理提交、停止与 driver.tick()；没有第二个 WorkflowRuntime 宿主。
- `src/task_center/conflicts.rs` 是唯一业务规则入口。Independent / Wait / CancelThenWait / Reject 四种结果都有明确含义。
- 新任务通过全量检查并成功登记后才取消旧任务；已有对象隔离也会阻止无效替换误取消其他活动任务。
- `domains/{disk,node,vd}` 内部按 model → service → runtime 单向依赖，Workflow 使用类型化能力通道。
- 基础库 `driver.rs` 不 spawn；`standalone.rs` 仅保留通用示例使用的可选独立封装。Pool 不依赖该封装。
- 原来的 app/domain/infrastructure 物理目录和策略引擎移除，旧版本可从 Git 历史恢复。未跟踪历史提案未提交。

验证（2026-10-15）：

- workspace 全目标测试 84 项通过：存储池 44（含 TaskCenter 7），执行基础 40。
- TaskCenter 测试重复 20 轮通过，含多线程冲突检查与队列推进测试。
- 格式检查、文档测试检查（当前无 doctest 用例）、Clippy `-D warnings` 通过。
- two_services、pool_lifecycle、task_center、shrink_commands 四个示例运行通过。
- Markdown 本地链接检查通过；外围 JurisDesk 的 `npm run verify` 通过。
- 自动边界测试检查 model/service/runtime 方向、Workflow 不引用宿主、Pool 不创建独立 WorkflowRuntime。
- 被动驱动测试确认：仅创建并放行流程不会自行执行，必须由所属循环轮询 driver.tick()。

本次移除了可配置/可注入策略 API，测试相应改为具体业务矩阵与协调状态机验证；
取消停稳、暂停恢复、节点交接、未知结果隔离、容量限制和多领域业务回归仍保留。
持久化恢复、自动重试、真实重建/成员移除仍未实现；原有保守对象集合未缩减。
