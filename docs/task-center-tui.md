# TaskCenter TUI 建议：沿用现有运维平台框架

设计面向已有终端界面，复用真实 Task / Activity 审计接口；本文件不是已实现的 TUI。
截图中的主问题是宽表把状态、进度、对象、等待放在同一行，步骤详情继续展开技术字段，
难以辨认当前动作、阻塞与历史结果。
以现有平台为约束：保留顶部栏目、Pool 总览、逐层展开面板、面包屑和底部快捷键，
仅改进 Pool 下任务列表和 Workflow/Activity 详情；不新增独立 TaskCenter 应用或全局导航。

## 现有导航与展开层次

```text
集群状态 → Pools → pool1 → 任务列表 → Workflow #184 → Activity #4
                     └→ 服务详情（沿用现有入口）
```

进入任务页自动继承当前 Pool；任务 ID、控制命令和观察订阅均以该池为作用域。
Enter 沿现有框架继续展开，Esc 返回一层并恢复父层的选择、筛选和滚动位置。
顶部 1/2/3 栏目与全局退出行为保持原有约定；快捷键以当前面板底栏为准。

## Pool 下的任务列表

```text
 X-Ray · 运维平台             1 集群状态  2 系统资源  3 运行调度
 Pools > pool1 > 任务列表
 Pool     State     VNode     Tier     Cap     Alarm     Svcs
 pool1    Normal    84        2        0%      0         …
   ┌─ Tasks: pool1 ────────────────────────────────────────────┐
   │ Running 2 · Waiting 1 · Uncertain 1       / 搜索          │
   │ 当前任务                                                 │
   │   ID   Workflow   状态       当前步骤/等待原因    对象    │
   │ > 184  磁盘上线   Running    disk.open            disk/d7 │
   │   183  创建卷     Waiting    等待资源             vd/v3   │
   │──────────────────────────────────────────────────────────│
   │ 历史任务                              状态: 全部         │
   │   181  删除卷     Uncertain  需核对外部效果        vd/v1   │
   │   179  自检       Completed 已完成                probe   │
   │                                                          │
   │ Enter 展开  Tab 切换区域  / 搜索  Esc 返回                │
   └──────────────────────────────────────────────────────────┘
```

沿用现有 Running / History 分区。当前任务区按内容分配高度，无任务时缩为一行提示，
把空间让给历史区；两区都有数据时保留各自最小可读高度并独立滚动。
概要计数与列表来自同一 Pool 和筛选范围；Uncertain 是单独的终态问题计数，不算作运行中。
未选中任何任务时不展示空的步骤详情框。

## 在现有框架内展开 Workflow

```text
 Pools > pool1 > 任务列表 > #184 磁盘上线
   ┌─ Workflow #184 ──────────────────────────────────────────┐
   │ Running · disk/d7    当前：disk.open · 执行 3.7s          │
   │ 已完成 3 步 · 正在执行 1 步                              │
   │                                                          │
   │   #  Activity       Service   状态       执行耗时         │
   │   1  tier.inspect   tier      Completed    2ms            │
   │   2  node.inspect   node      Completed    1ms            │
   │   3  disk.add       disk      Completed   24ms            │
   │ > 4  disk.open      disk      Running      3.7s          │
   │                                                          │
   │ 选中步骤：disk.open    资源：disk/d7 [write]               │
   │ 开始：10:42:03.120     结束：—    错误：—                  │
   │ Enter 步骤详情  Esc 返回任务列表                          │
   └──────────────────────────────────────────────────────────┘
```

该示例只列出已经发起的 Activity。没有静态计划时，不显示未执行的 disk.set-io，也不声称“完成 3/5”。
列表显示 Workflow 名称、状态、当前阶段和一个主要对象；完整对象、时间戳、错误放在详情。
业务可读名称优先，完整 Rust 类型路径放在详情的技术信息区域；路径截断需可展开。

小终端优先隐藏次要列，沿原有面板机制展开详情，不强制增加左右分栏。
默认状态与异常优先排列，选中行固定身份，不随刷新跳到另一任务；新任务到达只提示数量。

## 详情的固定阅读顺序

1. 概要：池名、短任务 ID、Workflow 名称、终态/当前状态及目标对象。
2. 当前动作/等待原因：正在执行哪一步、等待资源/信号/定时器，或等待停稳。
3. Activity 时间线：序号、可读名称、状态、实际执行时长；上下键/Tab 可进入选中。
4. 选中步骤：资源声明、创建/开始/结束时间、完成状态和失败原因。
5. 折叠的技术详情：Pool ID/UUID、实例代次、完整 TaskRef、类型路径和全部对象。

正常任务默认不展开所有技术字段。失败/Uncertain 自动聚焦相应步骤，错误正文支持换行与滚动。
相同名称的循环步骤按 ActivityId.sequence 区分，不合并成一行。
目前 ActivitySnapshot 保留完成状态与失败原因，不保存任意 Activity 返回值；
若要展示业务结果摘要，需要业务显式提供经过筛选的摘要，不能在 UI 中假定已有结果 payload。

## 状态的准确表达

| UI 状态 | 含义与展示 |
| --- | --- |
| Running | 蓝/青色圆点，显示当前 Activity |
| Waiting | 黄色时钟，并具体说明 resources / objects / signal / timer |
| Pause requested | 请求已发出，还没到安全边界 |
| Paused | 已确认暂停，可以 Resume |
| Cancel requested | 执行可能还在继续，显示“等待实际停稳” |
| Cancelled | 仅在已确认取消的终态显示；对应 Failed(Cancelled) |
| Completed | 绿色勾，当前动作区域改为结果摘要 |
| Failed | 红色叉，直接展开失败原因 |
| Uncertain | 橙色感叹号，“需核对外部效果”，不提供普通 Retry 快捷操作 |

颜色同时搭配符号和文字，支持无色/ASCII 模式。
冻结显示仅停止重绘，并持续接收后台更新；暂停是对实际 Workflow 的请求，两者不能共用文案。
取消操作打开确认条，展示明确的 Pool/Task/对象后再发送取消；发送成功不改成 Cancelled 终态。
这些操作接入当前框架已有动作与快捷键机制，键位冲突时以全局约定为先，不覆盖原有导航。
历史任务禁用暂停、恢复、取消。终态与最近一次控制请求分开显示。

## 历史任务

History 保留为任务列表的原有分区，选择历史任务后按相同层次展开时间线。
支持在当前 Pool 内按 Workflow、对象、终态和名称搜索；跨 Pool 先返回现有 Pool 列表选择目标。
默认显示异常和最近结束任务，成功历史可折叠，避免持续运行的系统被成功记录淹没。

历史页明确显示数据范围：当前进程内存历史、Task 上限、Activity 上限。
ActivityHistory.omitted 非零时，时间线上方展示“较早 N 步已淘汰”，不伪造完整审计轨迹。
Task 被淘汰、池已删除、接口不可用须分别显示；已有 Observer 仍可保留自身终态详情。
未实现 Workflow 接受/开始/结束时间前，不显示由 Activity 时间拼出的“工作流总耗时”。
单步执行耗时由 started_at 到 finished_at/当前时间得到；等待耗时与执行耗时分开。
当前时间戳是 SystemTime，时钟回拨时显示不可用，不把负值截成看似准确的零耗时。

## 等待与冲突说明

WaitingResources 可以从当前 Activity 的 resources 展示“等待 disk/d7 写权限”。
WaitingObjects 展示“等待对象准入”；精准的当前阻塞者需要额外的实时等待图接口。
现有 conflicts 是提交时的准入解释，应标记为“准入记录”，不能把它直接写成当前持锁者。
watch 可能合并更新；它能刷新保留的 Activity 结果，不能凭空生成每次暂停/恢复等事件日志。
未来增加追加式状态事件后，再提供完整 Audit Events 页。

## Fanout 详情的接入位置

第一版聚焦现有 Pool 任务入口，不新增全局 Fanout 栏目。
后续广播汇聚详情接入平台既有的事件/调度入口，具体归属核对现有代码后确定。
当前 Pool 内的关联任务仍通过原任务列表查看；跨池事件详情展示以下内容：

```text
 Event 8df2…   Node n2 disconnected   epoch=4 seq=120
 Targets 6   Completed 3   Not relevant 2   Pending 1

 pool-a   ✓ completed     #184    Enter → workflow timeline
 pool-b   ◷ accepted      #87     waiting for previous activity
 pool-c   — not relevant          no workflow created
 pool-d   ✓ completed     #52
 pool-e   — not relevant
 pool-f   ✓ completed     #13
```

Decision 与 Completion 明确分开；Deadline 到达保留所有逐池结果与待完成清单。
Backpressured/Unavailable 不按“不相关”统计；重试只重投尚未接受的目标，不能重发已运行的写操作。
离开页面或停止等待不取消各池已经接受的任务。

## 接口与实现建议

- 复用现有 TUI 技术栈、组件、主题和事件路由。若原框架使用 Ratatui/Crossterm，直接扩展现有面板；
  不能因本方案重新引入一套终端框架或独立事件循环。
- UI 只消费 ViewModel，绘制函数不执行 IO、不持 Registry 锁，不解析调试文本获取状态。
- 列表需要轻量 TaskSummary（阶段、当前步骤、状态、主要对象）；避免每次刷新复制所有 Activity 历史。
  这项摘要接口尚需补充，现有 manager.tasks() 返回完整任务快照。
- 只对当前选中的任务订阅 observe_task；切换选中只改变订阅，不发送业务取消。
- 高频更新合并到固定渲染节奏（例如 10 Hz），但数据模型保留服务器给出的所有在界限内步骤。
- 焦点与选择绑定完整 TaskRef（PoolUuid + instance + WorkflowId），池名和短 ID 仅用于显示。
- 搜索、筛选、冻结显示都在客户端处理；业务控制通过对应 PoolHandle 或 Manager 的作用域校验接口发送。
- 本仓库当前可见的是运行库和命令行示例，未发现截图对应的 TUI 源码；具体面板类型、快捷键与路由名，
  接入时以真实平台代码为准，本文不假定已经找到或修改了该实现。

先改进现有任务面板、Workflow 步骤表和历史筛选，再接入广播汇聚详情。
工作流创建继续沿用平台已有业务入口，观察面板聚焦执行与审计。
