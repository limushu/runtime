# 运行现场：持续等待、宿主与事件分派

本次按运行图评审意见重新组织动画。Rust 基线仍为 `53fae88`；不修改 Rust 行为。

## 验收标准

| 要求 | 实现 |
| --- | --- |
| await 期间仍能找到等待者 | 每个 Task 的等待点持续标记，另一条请求活动时不会清除 |
| 看清 Future 的宿主 | TaskCenter 宿主边界；Tier、Node、Disk、VD、Balance 分别列为独立 Tokio task |
| 两个工作分支上下排列 | running.next 在上，events.recv 在下；说明视觉排列不改变 biased 检查顺序 |
| receiver 显示具体事件 | 框内显示 Event 类型、请求别名及资源/访问模式；侧栏显示完整处理结果 |
| 消息和判断不能混淆 | Acquire / Release / Withdraw 分派与 pump 的 Permit / Waiting / Rejected 分开展示 |
| 通道不是执行宿主 | Broker/mpsc 和 ActivityClient 用管道形，Future 用叠层，LockTable 用状态表 |
| 配色区分运行和等待 | 白底蓝灰；蓝色表示当前推进，琥珀虚线表示仍存在的 Pending Future |

## 与代码对照

[locks.rs](../../crates/workflow-foundation/src/locks.rs) 中 Event 为：

- Acquire(Request)：进入 waiting，然后 pump 检查 held、较早等待者和 fences。
- Release { id, uncertain }：移除 held，必要时保留 fence，再 pump。
- Withdraw(id)：删除 waiting 中的请求，再 pump；不能释放其他正在执行的持有者。

“业务 / 冲突”不是 events.recv 的两个消息分支。业务 Submission 由 TaskCenter 的提交入口接收；
资源冲突由 LockTable 判断。Acquire 的 Kind 有 Workflow / Activity 两种，
独立 Driver 模式使用 Workflow claims；Pool 模式的逻辑占用由 TaskCenter 管理，
本演示中的步骤资源请求使用 Activity claims。

Broker 封装 mpsc 发送端、请求编号，以及 acquire 的 Permit oneshot 等待。
它不拥有执行循环，也不直接调用 LockTable。
[ActivityClient](../../crates/workflow-foundation/src/service.rs) 则发送领域 Execute，等待另一条结果 oneshot。
这两种通道和两个等待位置分别标注。

图内 TaskCenter 的琥珀色边界表示它持有 Pending Workflow Futures，不表示它的线程阻塞。
服务上的 Pending 表示其执行 Future 正等待领域 IO/回调；宿主仍可轮询其他就绪消息。
当前事件高亮和持续等待标记可以同时存在。

## 教学状态与范围

- 8 个场景入口、9 组轨迹、404 个事件快照。
- 每个 Task 保存当前 Activity 的命令、领域、资源请求及执行阶段，用于导出等待点；
  它们都是教学状态，不是读取真实 Rust 栈或运行日志。
- 请求编号 A-1 / B-1 是便于追踪的别名；实际 Broker 使用自增 u64。
- 领域等待标注到 ServiceHandler.execute 的异步操作，不伪造某个外部适配器的具体栈帧。
- 新增“资源事件与等待”：应用策略允许业务独立任务同时运行，底层 Activity 仍争用 R1。
  展示 Acquire 冲突、Withdraw 只删除等待项、Release 后 pump 授权，以及 fence 拒绝新请求。
- 同步的释放、pump、Permit 唤醒可以拆为说明帧，不代表额外的异步边界。
- 原业务 FIFO、topo 暂停、三任务等待环等场景保留，与资源锁竞争分开展示。

## 验证

```sh
python scripts/render-runtime-lab-graph.py
node scripts/verify-task-center-simulation.mjs --browser --screenshot-waits await-preview.png
```

验证覆盖：等待者跨其他请求的事件保持、独立服务宿主、Read 访问模式、
Acquire/Release/Withdraw payload、两个排队层的区别、取消等待不释放 A 的资源、
Release 授权 C、隔离不执行 D 的 Activity、分支纵向布局、receiver 原位事件显示，
以及播放、暂停、跳转、键盘、放大、移动端、离线和 SVG 文字边界。

入口：[运行动画](../diagrams/task-center-simulation/index.html)、
[等待现场预览](../diagrams/task-center-simulation/await-preview.png)。


验证结果：上述 404 个事件的语义断言和 Chrome 交互检查全部通过；
已查看最终等待现场截图。外围 npm run verify 通过（Harness、lint、TypeScript、生产构建）。
本次只修改图示与文档，没有重跑或更改 Rust 测试。
