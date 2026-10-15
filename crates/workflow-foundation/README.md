# Workflow Foundation

这是 `crates/workflow-foundation` 中的业务无关基础库，与业务库共用根 workspace 和
Cargo.lock。**不依赖旧 crate，不包含 Disk、VD、Node 或 PoolManager 业务。**

目标是先固定三个边界：Workflow 组织业务过程；Pool 范围的 Coordinator 推进执行、
集中仲裁对象；领域服务拥有状态和业务规则，通过 Activity 提供能力。

## 先读接口，再看运行机制

| 文件 | 内容 |
| --- | --- |
| [workflow.rs](src/workflow.rs) | Workflow、WorkflowContext、类型化 Run、Signal/Timer/Activity 等待 |
| [service.rs](src/service.rs) | DomainService、ActivityContext、绑定 Pool 的 ServiceClient 与独立执行宿主 |
| [model.rs](src/model.rs) | 对象、资源访问、身份、结果分类、状态和配置 |
| [coordinator.rs](src/coordinator.rs) | 一个 Pool 的组合入口、流程准入、运行与退出 |
| [locks.rs](src/locks.rs) | 内部统一对象仲裁，不向业务暴露锁操作 |
| [admission.rs](src/admission.rs) | 类型化应用准入策略：正常排队、拒绝、请求取消旧执行 |

```text
Coordinator（一个 Pool 实例）
├─ Workflow execution：过程、输入/输出、等待、取消
├─ 统一对象准入：流程占用 + Activity 资源占用
└─ 注册的领域服务
   ├─ Service A：状态 + 能力 + 自己的执行宿主
   └─ Service B：状态 + 能力 + 自己的执行宿主

Workflow → Context.activity → 统一资源准入 → 目标服务 → 结果 → Workflow
```

当前宿主用 Tokio 驱动协调者和各服务，服务内用 FuturesUnordered 同时等待多个步骤。
这不是“一对象一 task”模型；task 数量不是业务抽象。
核心不依赖旧 SDK。workspace 整理后旧实现已移出仓库，未提交工作保存在仓库外备份。

## 调用形状

```rust,ignore
let mut pool = Coordinator::start("pool-a", Config::default())?;
let source = pool.register("source", Source)?;
let sink = pool.register("sink", Sink::default())?;

let mut run = pool.submit(Transfer { item: 7, source, sink }).await?;
run.control.signal(Signal::new("continue", "")).await?;
let result = run.result().await?; // 类型由 Workflow::Output 决定
pool.shutdown().await?;
```

流程体就是自然的 async 代码：

```rust,ignore
let value = ctx.activity(&self.source, self.item).await?;
ctx.next_signal().await?;
ctx.sleep(delay).await?;
ctx.activity(&self.sink, value).await
```

可运行示例：[two_services.rs](examples/two_services.rs)。Source 提供字符串，Sink 校验并保存；
它们只是抽象模型的验证对象，不是预先封装的存储业务。

## 两种范围，一个协调者

- `Workflow::objects()` 声明相互排斥的逻辑操作对象，持有到执行和所有已派发步骤结束。
- `DomainService::resources(request)` 声明这次动作实际读取/修改的资源，只持有到动作停稳。
- 两者是独立锁类别。同一个 ObjectKey 出现在两类中，不表示它们互相阻塞；
  业务应明确区分“操作生命周期”与“实际数据修改”。
- 流程对象全部一次取得；每次 Activity 的资源集合也一次取得。支持步骤资源读共享、
  写独占和同类冲突请求 FIFO；无关请求不被队头阻塞。
- 不支持动态扩展流程对象、嵌套资源占用、并行 Activity。Context 不可 Clone，
  下一步必须等待上一步真正结束。避免首版引入跨阶段的循环等待。
- 如果若干跨服务步骤必须作为一个不可交错的业务过程，相关 Workflow 必须共同声明
  同一个逻辑对象；步骤资源锁本身不提供跨 Activity 事务。
- 资源身份由领域契约定义。所有会修改相同数据的能力（即使来自不同服务）必须声明
  相同资源键。框架不推断实体关系、集合覆盖、父子对象或真实存储事务。
- 领域不自己仲裁对象冲突，但必须校验版本、前置条件、不变量和提交结果。

## 执行、取消和错误

`submit` 确认内存接收，返回 Run；`result` 等待类型化最终结果。
丢弃 Run 或取消等待 result 不取消已接收流程。cancel 是请求，不是已经停止的确认。
Signal 使用有界缓冲，可提前到达；消息格式由 Workflow 校验。Timer 是本地单调计时。

ActivityId = WorkflowId + sequence；它不随着响应观察者变化。尚无自动重试或 attempt。
领域返回 `Failed` 表示已知失败，`Uncertain` 表示可能已生效，二者不能互换。
未知结果/panic 保留资源恢复阻塞；单纯被阻塞的后继请求返回 Rejected，不扩大阻塞。

Activity 的执行许可随请求进入服务。丢弃其响应 Future、Workflow 提前返回或 panic，
协调者仍等待该步骤实际结束。未知错误即使被 Workflow 忽略，也不能变成成功。
取消会传到领域；领域必须只在动作已停止或完成不可中断边界后返回。
最终提交已成功时允许返回成功；不会因为同时收到取消而伪装成零副作用。

同一 Pool 的领域 client 不能被其他 Pool 实例使用，即使二者名字相同。
查询读取领域已提交视图，不获取 Workflow 占用，也不提供跨域一致快照。

## 生命周期与容量

### 应用准入策略（2026-10-15）

`CoordinatorClient::submit_with_policy(workflow, descriptor, policy)` 允许应用实现
`AdmissionPolicy<D>`；基础库不认识描述中的业务概念。纯同步策略对每个在途描述返回
Queue / Reject / CancelExisting。所有决定在协调者准入点串行检查；只有容量允许且
没有拒绝才接收新工作并请求取消旧工作。异常策略拒绝该次请求，不终止旧工作。
CancelExisting 不解除对象占用：新流程仍须等待旧步骤实际停稳；未知结果仍会阻塞。
应用必须让描述与 Workflow 对象声明一致，并通过固定应用入口使用同一描述/策略；
无策略或不同描述类型的执行只接受普通对象仲裁。本库不提供结果合并或业务优先级。
业务用法见相邻 [storage-pool](../storage-pool/README.md)。

### 协作暂停与恢复（2026-10-15）

`RunControl::pause()` 请求在下一个安全边界暂停，`wait_paused().await` 等待 Paused
确认，`resume()` 在同一执行继续。请求与确认不同：正在执行的 Activity 不会被强停；
最后一步已成功时可能直接完成。暂停保留逻辑对象，占用直到流程结束；已完成步骤的
资源照常释放。资源等待可撤回以响应暂停。流程对象准入的等待尚不支持即时暂停确认。

Activity / Signal / Timer 前会检查暂停；自定义循环应调用 `safe_point().await`。
同步 `checkpoint()` 只检查取消。暂停不回退已完成步骤；Signal 有界缓存，Timer 按
原来的单调时钟截止时间继续计时，恢复后才返回。Cancel 和 shutdown 可直接唤醒暂停，
不需要先 resume。无暂停请求/已恢复/终态时等待暂停返回错误，不无限等待。
这是进程内控制，不是快照、持久化执行、崩溃后恢复或 Temporal 的完整暂停语义。

### 关闭

Coordinator 持有领域宿主的所有权。正常 shutdown：拒绝新流程 → 请求取消 →
等待流程和 Activity 停稳 → 停止领域宿主。shutdown 的等待可以取消后重试。
服务也能单独 shutdown，不改变其他服务/协调者的生命周期。

Config 限制请求通道容量、Signal 容量/字节数和总在途流程数；达到流程上限返回 Busy。
已完成执行由 Run handle 观察，不在协调者保留无限历史。恢复阻塞不会自动淘汰。
强制 Drop 会回收本地任务并报告不确定结果，但不能保证远端副作用停止；禁止将重建
同名 Pool 当作恢复。生产环境需要另行设计主代次 fencing、核对和持久化协议。

## 本版明确不做

持久化历史/确定性重放、跨主恢复、自动重试、子 Workflow、动态扩锁、优先级抢占、
多阶段事务、完整业务领域、通用 Actor 框架。也不提供不可信代码沙箱。
实际网络 IO、阻塞调用与停止语义由领域实现负责，不能在 handler 中阻塞执行线程。

借鉴的是 Temporal 的 [Workflow](https://docs.temporal.io/workflows) 与
[Activity](https://docs.temporal.io/activities) 分离，不是宣称本库具有 Temporal 的持久化能力。

## 验证

```sh
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
cargo fmt --check
cargo run --locked --example two_services
```

范围与验收见 [DESIGN.md](DESIGN.md)。

## 任务索引、观测与命名准入说明

`CoordinatorClient` 提供 `tasks/task/observe_task` 和按本实例 WorkflowId 的
`suspend/wait_suspended/resume/cancel`。接受提交时即登记控制句柄，丢弃 Run 不丢失可观测性。
任务快照报告状态、对象、阶段/进度和准入说明；只读 TaskObserver 同时订阅状态与进度。
Workflow 可调用 `report_progress`；该记录不参与调度或业务决策。
`AdmissionPolicy::evaluate` 默认包装 decide，也可返回带规则名的 AdmissionEvaluation，
Runtime 只评估一次并记录已有任务 ID 与决定。失败/Busy 的提交不产生已接收任务记录。

普通终态按 Config::task_history 有界保留，Uncertain 记录保留。watch 可能合并变化；
任务 ID 是实例内身份、历史只在内存、准入说明不是实时等待图。生产持久化、跨主身份、
远程控制和鉴权不在此接口中实现。
