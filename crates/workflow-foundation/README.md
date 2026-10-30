# Workflow Foundation

业务无关的执行框架，不认识 Pool 的 Disk/Node/VD 冲突策略。crate 名称保留，
不是 DDD 领域层或业务共享内核。应用契约和通用技术执行机制位于此 crate。

## 主要类型

先按职责读源码，公开类型名称和调用方式保持不变：

| 文件 | 负责什么 | 阅读入口 |
| --- | --- | --- |
| `identity.rs` | 同一个 Task 的身份、轮次和流水 | TaskId / TaskIds |
| `tasks.rs` | 任务观测、订阅和索引；引用调度解释 | TaskSnapshot / TaskObserver / TaskRegistry |
| `scheduling.rs` | 规则契约和调度计算 | ConflictDecision / SchedulingPolicy / plan |
| `control.rs` | Task 控制端：请求、token、确认 | RunControl → Controls / ControlToken |
| `workflow.rs` | 业务契约、依赖绑定和上下文，等待安全边界及 Activity 结果 | Workflow / Dependencies / WorkflowContext |
| `service.rs` | 领域调用契约和服务宿主 | ActivityContext / ActivityExit / ServiceHandler |
| `driver.rs` | 注册与轮询执行体，处理完成和资源事件 | enqueue_with / tick |
| `driver/execution.rs` | 单个 Task 的执行包装：创建句柄、等待启动、控制落盘、运行、收尾和终态落盘 | Run / execution / sync_controls |
| `driver/recovery.rs` | 从存储恢复 Task，保持原 ID，暂不放行业务执行 | restore → load_pending_tasks → prepare_tasks → claim_tasks → register_restored_tasks |
| `persistence.rs` | Task 存储结构、后端契约和日志写入 | SavedTask / TaskStore / Journal |
| `persistence/file.rs` | 文件存储后端 | FileTaskStore |
| `locks.rs` | 资源申请、排队、释放和不确定结果隔离 | Broker / LockTable / Permit |
| `standalone.rs` | 可选的独立事件循环宿主 | WorkflowRuntime / RuntimeHandle |
| `model.rs` | 公共值类型、错误、状态和配置 | ObjectKey / Outcome / RunState / Config |

恢复的 `prepare_tasks` 调用 `Workflow::restore` 重建执行体并检查身份、对象和输入。
全部检查通过才逐条接管记录；全部接管成功才注册 Task，调用方随后重建调度关系并打开 start gate。
接管完成后再次检查宿主有效性，批量构造尚未执行的 Future，再登记任务。
登记阶段没有存储 IO、Workflow 回调或返回错误的校验，不会因重新调用 persistence() 而留下半批任务。
记录接管是逐条 CAS，不是批量事务；写入中途失败时不注册任务，重试重新读取记录。
Task 注册后立即可查询保存的 details、objects 和用户暂停请求，无需等待第一次 tick。
暂停确认必须由新执行体到达安全边界后重新产生，不能从旧宿主复制。

`Restored` 定义在恢复模块中，表示恢复后的句柄和启动许可。`Dependencies` 定义在 Workflow 契约旁。
两者均从 crate 根导出，原 `persistence::Restored` / `persistence::Dependencies` 路径保留为兼容重导出。
这些私有执行模块共同实现同一个 Driver，不会创建额外任务管理器或事件循环。

| 类型 | 含义 |
| --- | --- |
| WorkflowDriver | 嵌入调用方事件循环的被动执行驱动，不 spawn、不自带提交队列 |
| ExecutionView | 只读/控制句柄，不提交、不调度 |
| WorkflowRuntime | 可选的独立宿主封装，仅供无自有循环的使用者；存储 Pool 不用它 |
| RuntimeHandle | 仅为可选独立宿主提供本地调用句柄，TaskCenter 不持有它 |
| Workflow / WorkflowContext | 流程体、Activity/Signal/Timer、安全边界 |
| Run / RunControl | 类型化结果与执行控制 |
| TaskId / TaskSnapshot / TaskObserver | 唯一的任务身份、快照和观察 |
| Persistence / TaskStore | Workflow 声明的存储方式与 Task checkpoint 后端 |
| SchedulingPolicy / Relation | 应用定义的准入决策与调度关系，驱动不解释业务类型 |
| ActivityClient | 带类型的能力通道，不暴露具体宿主类型给应用 |
| ServiceHandler | 基础设施的托管适配协议，不是 DDD DomainService |
| StartPermit | 应用拥有的一次性启动许可，无冲突判断逻辑 |

接口：[workflow.rs](src/workflow.rs)、[service.rs](src/service.rs)、
[driver.rs](src/driver.rs)、[standalone.rs](src/standalone.rs)、[model.rs](src/model.rs)、[tasks.rs](src/tasks.rs)。

## 应用管理的执行

```rust,ignore
let (run, permit) = driver.defer(workflow)?;
// 应用已经检查并登记协调状态；必要时等待旧任务真正完成。
permit.start();
// 或 permit.reject(error)；丢弃 permit 也不会偷偷启动流程。
```

defer 同步登记一个受控执行，计入容量，返回可观察/可取消的 Run。调用方必须在自己的
事件循环中轮询 driver.tick() 才能推进流程；驱动器自身不会启动任何 Workflow task。业务层自行维护完整对象集合、
排队、占用和不确定结果隔离。Runtime 不重复获得此执行的 Workflow 对象锁。
受控执行在许可到来前不运行流程体；取消可以唤醒它，暂停请求在获准后的安全边界确认。
Runtime 不提供 AdmissionPolicy、业务描述类型擦除或 submit_with_policy。

通用独立示例仍可以 submit(workflow)，Runtime 为该模式提供简单的 Workflow::objects()
互斥。这只是底层机制，没有存储业务规则；Pool 对外不开放此旁路。

## Activity 与服务托管

ServiceHandler 声明请求/回复/查询/视图和 resources。ActivityClient 只保留带类型的调用通道；
应用可以声明客户端类型而不依赖具体基础设施 handler。ServiceClient<S> 是已知宿主类型时的便捷别名。

每个服务有独立宿主；同一宿主可用 FuturesUnordered 等待多个步骤。WorkflowDriver 也通过
FuturesUnordered 推进多个 Workflow；在存储池中，它就在 TaskCenter 的事件循环内被轮询。

Activity 资源由运行时统一读写保护，持有到实际动作停稳；领域能力内部不排队。
多资源原子取得、读共享/写独占、冲突等待者 FIFO。不支持并行 Activity 或动态扩展流程对象。
跨服务的同一资源必须使用同一个 key；框架不会推断业务拓扑。

## 控制与安全语义

- 丢弃 Run 或 result 等待者不会取消已接收执行。signal 使用有界缓冲，timer 使用单调时钟。
- cancel 是协作请求，不是完成确认；Workflow 完成前等待所有已派发 Activity 停稳。
- 已完成不可中断提交可以返回成功，不能把发生的效果伪装成零副作用取消。
- Uncertain/panic 会保留步骤资源隔离，不能被 Workflow 忽略成成功。单纯被隔离的后继
  返回 Rejected，避免扩大隔离。在 defer 模式下，应用负责 Workflow 级对象隔离。
- ActivityContext 是适配层执行上下文，不应进入纯领域对象。
- suspend 在安全边界确认：在途 Activity 必须先停稳返回，Workflow 消费结果并保存必要检查点，
  然后在 Activity/Signal/Timer 或 wait_until_runnable 入口确认暂停。
  服务通过请求携带的 token 协作收尾，不抢占外部 IO，暂停不会回滚步骤。
  排队准入期间还不能即时确认 Suspended。取消/关闭可以直接唤醒暂停。
- client 绑定 Runtime 实例，即使 Pool 名称相同也不能跨实例执行 Activity。
- shutdown 拒绝新工作、请求取消、等待停稳、关闭服务；等待可取消后重试。
  强制 Drop 只保证本地任务回收，不能保证远端 IO 停止。
- 任务索引按 ID 提供观察/控制。普通终态历史有界，Uncertain 保留；watch 可能合并事件。
  业务冲突解释由 TaskCenter 写入同一个 TaskSnapshot，基础库不解释业务规则。

## 逐 Activity 观测

每次通过 `WorkflowContext::activity()` 发起的步骤自动进入 `TaskSnapshot.activities`，
不需要在 Workflow 中手工上报开始/结束。每条记录包含稳定的 `ActivityId`（TaskId + 当前宿主内调用序号）、
注册服务名、步骤名、资源声明、当前状态和创建/实际开始/实际结束时间。

```rust,ignore
let task = client.task(id)?; // 运行中或仍保留的历史 task
for activity in &task.activities.entries {
    println!("{} {:?}: {:?}", activity.name, activity.id, activity.state);
}
let mut observer = client.observe_task(id)?;
let updated = observer.changed().await?; // Activity 更新也会唤醒，不依赖 Workflow 状态变化
```

状态为 `WaitingResources -> Queued -> Running -> Completed / Failed(Failure)`。
`Queued` 包含等待服务邮箱容量；取消、拒绝、服务不可用可以在实际开始前结束，
此时 `started_at` 为 None。取消请求不会提前标记完成；调用方丢弃 Activity 等待 Future 后，
记录仍由服务端或 Activity body 的实际执行更新。handler/body panic 或执行中强制终止
记录为 Uncertain，不能误报成功。

`ServiceHandler::activity_name(&request)` 可返回稳定步骤名；默认使用 handler 类型名，
无须修改已有实现。名称不包含请求/回复载荷。存储适配器按命令命名，例如 `disk.open`。
循环中的同名步骤通过调用序号区分；失败后显式再次创建 Activity 也有新序号，框架不自动重试。
`activity_fn` 内部的领域调用和重试共用一条记录，不增加 Activity 序号；记录包括步骤说明、尝试次数、重试原因、上次重试错误和下一次重试时间。

`Config::activity_history` 默认每 task 保留最近 1024 条，必须大于零。
`activities.total` 是累计调用数，`activities.omitted` 明确给出因上限淘汰的条数；当前在途步骤始终保留。
记录随 task 一起按 `Config::task_history` 淘汰，已有 observer/RunControl 仍可读取自身记录。
`task_ids()` 可只列出 ID，避免复制所有步骤历史。

这里记录实际发起过的调用；普通 async Workflow 的未来分支、未发起或跳过的步骤无法自动推断。
watch 仍会合并通知，但合并不会抹掉历史上限内已结束的 Activity 记录。
这里的 Activity 明细只存在当前进程内，关闭后保留在已有句柄中。
Task 摘要可以由 TaskStore 持久化，通过下述 history 接口跨宿主查询。

Activity 历史不持久化。Task 可选持久化 input、checkpoint、details、控制意图与终态，
通过 Workflow::restore 在新宿主重建；不是确定性重放，不自动重试远端操作。
不可把重新创建同名实例当作恢复，也不提供执行不可信代码的沙箱。

## 统一 Task 与可选持久化

已有 Workflow 不必改动：默认 History，未配置 TaskStore 时仅保留内存快照与控制；
配置 TaskStore 后记录摘要历史，但不会自动取得续作能力。
新增流程可以覆盖 persistence() 返回 Persistence::durable(version, &input)，并实现 restore()。
组合根通过 driver.define::<W>(dependencies) 绑定可恢复类型；通过 enable_persistence(store)、
restore() 取得原 Task 的启动许可，安装全部调度占用后放行。Run::accepted() 表示初始记录已确认写入；
直接驱动 Driver 时必须同时轮询 tick()，TaskCenter/RuntimeHandle::submit 会等待该确认。

ctx.report_details(&typed_value) 更新同一个 TaskSnapshot.details，details_as::<T>() 解码；
单份 details 限制 64 KiB。观察通知可合并，不能当事件日志。
ctx.save_checkpoint(&typed_value).await 保存续跑位置与当前 details；report_details 本身不保证落盘。
MemoryTaskStore 只用于测试；FileTaskStore 是持有文件独占锁的单机追加日志示例实现。

TaskIds::for_leader(epoch) 接收选主系统的持久化递增轮次，同一主的所有池共享一个分配器。
with_task_ids(pool_uuid, ids) 绑定不可变池 UUID。失主调用 ids.revoke() 停止新分配并协作取消旧执行。
默认兼容模式 epoch=0，仅限本地，不承诺跨重启单调；恢复任务保持旧 TaskId，更新 owner_epoch。

### 跨切主的 Task 历史

历史与续作使用相同的 TaskId、TaskStore 和 SavedTask。普通 Workflow 不需要实现 restore：

| 声明 | 配置 TaskStore 后的行为 | 切主后执行 |
|---|---|---|
| 默认 `Persistence::History` | 保存任务摘要；受 `Config::record_history` 控制，默认开启 | 不重跑 |
| `Persistence::Memory` | 明确仅在内存中观察 | 不重跑 |
| `Persistence::Durable` | 保存摘要以及 input、checkpoint、控制意图 | 按已注册定义恢复 |

摘要包括对象、应用 metadata、details、阶段/进度、创建/开始/结束时间和结果。
接受记录必须写成功才放行业务；写失败时 accepted 返回错误，业务体不会执行。
执行状态/进度由 watch 合并写入，终态在 result 返回前确认保存。存储不可用会如实返回错误，
不把查询失败当作空历史，也不承诺崩溃前最后一条尚未写入的进度一定保留。

`ExecutionView`、`TaskCenter`、`PoolHandle` 提供相同的异步只读接口：

```rust,ignore
use workflow_foundation::persistence::TaskQuery;
let page = pool.history(TaskQuery {
    limit: 50,
    business: Some("storage.shrink".into()),
    ..Default::default()
}).await?;
if let Some(cursor) = page.next {
    let next = pool.history(TaskQuery {
        before: Some(cursor), limit: 50,
        business: Some("storage.shrink".into()),
    }).await?;
}
let record = pool.history_task(task_id).await?;
```

按稳定 Pool UUID 隔离，按 TaskId 创建顺序倒序，游标排除已返回记录，每页 1..1000 条。
`tasks()/task()` 继续提供当前宿主的实时快照；`history/history_task` 提供已确认写入的记录，
也可查正在运行的任务。内存 `task_history` 淘汰不删除数据库记录；数据库保留期由适配器管理。
历史查询不重建 Workflow，不占调度/conflict 名额，也不能 resume 历史终态。

`SavedTask.recoverable` 明确区分摘要和续作记录。新主只恢复可续作任务；旧主未写终态的普通任务
标为 `StoredOutcome::Interrupted`，保留最后确认的信息，不伪造成功、失败或实际完成时间。
用户取消记录为 Cancelled，已确认业务失败为 Failed，效果不确定为 NeedsCheck。
展示最终结果以 outcome 为准，observation.state 是末次执行观察；可续作任务失主后仍是 Pending。
普通历史无需注册旧 Workflow 类型，旧业务代码移除后依然可以查询。

已有 KV 适配器可继续实现 claim/list/save；新增 get/query/unfinished 提供兼容默认实现。
生产 KV 可分别覆盖为按 TaskId 点查、按池/顺序分页、未结束任务索引扫描。Memory/File 实现
已避免为分页复制全部历史。历史仍受 epoch fencing 与 revision CAS 保护。
旧 SavedTask 未带 recoverable/observation 时，按原先可恢复记录解析，已有 checkpoint 保留。
本版持久化 Task 摘要，未持久化每个 Activity 的完整明细或返回值。

## Task 暂停与 Activity 停稳返回

控制 token 随 ActivityContext 交给服务；服务观察 `control()`，停稳后返回自己的类型化结果。
原 activity.await 返回该结果；Workflow 保存 checkpoint，再调用 wait_until_runnable 确认 Task 暂停。
恢复并通过 RunCondition 后，Workflow 显式发起下一次普通 Activity，TaskId 不变。
必做步骤可使用 `ctx.activity(&client, request).uninterruptible().await`。它从资源获取到真实回复
屏蔽 Task 取消/暂停（包括调用前已请求的取消），返回业务结果后由下一普通边界处理控制请求。
宿主关闭/失主、服务关闭和资源隔离不被屏蔽；不提供自动重试或跨崩溃 exactly-once。
`RunControl::has_started()` 表示 Driver 已进入 Workflow 业务体，调度器在此前不会因后继替换取消它。
没有 ActivityControl hook、Activity 暂停实例或第二种 Activity。
`ActivityExit::Suspended` 是安全结束的一次调用的结果，不是 ActivityState；历史记录为 Completed。
暂停与完成竞态时保留实际结果；取消也必须等在途业务收尾。

### 两条 Activity 路径并行保留

现有接口、缩容工作流和调用方式继续保留。新增入口按步骤显式选择，不自动迁移旧代码：

| 路径 | 步骤协议 | Workflow 得到什么 | 谁处理挂起 |
| --- | --- | --- | --- |
| `run_activity(step)` | `Activity::execute -> ActivityDecision` | `ActivityExit<Output, ()>` | WF 显式处理 Suspended、等待、重新构造步骤 |
| `run_managed_activity(step)` | `ManagedActivity::execute + handle` | 最终 `Output` 或错误 | 执行器内部处理，WF 保持在同一个 await |

原有 `activity(&client, request)`、`activity_body`、`activity_fn` 都不变。
旧示例：[activity_retry.rs](examples/activity_retry.rs)；新示例：[managed_activity.rs](examples/managed_activity.rs)。

新模式把领域返回和控制决定分开：

```rust,ignore
#[async_trait]
impl ManagedActivity for MigrateDisks {
    type Reply = BalanceReply;
    type Output = u64;

    fn name(&self) -> &'static str { "迁移目标盘的数据" }
    fn resources(&self) -> Vec<ResourceAccess> { /* 声明本次执行需要的资源 */ }

    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<BalanceReply> {
        scope.call(&self.balance, self.command()).await
    }
    fn handle(&mut self, result: Outcome<BalanceReply>) -> Outcome<ActivityAction<u64>> {
        Ok(match result? {
            BalanceReply::Completed { moved } => ActivityAction::Continue(moved),
            BalanceReply::Stopped { cursor } => {
                self.cursor = cursor;
                ActivityAction::Suspend(ActivityWait::Runnable)
            }
        })
    }
}

// 这一句覆盖迁移、停稳、挂起、恢复，最后才返回业务结果。
let moved = ctx.run_managed_activity(MigrateDisks { /* 输入和续作状态 */ }).await?;
// WF 在迁移完成后推进下一步，不需要暂停分支或外层重试循环。
```

`handle()` 在本次真实操作停稳之后调用，返回 Continue、Retry、Suspend 或 Cancel；Err 表示失败。
Cancelled / Uncertain 直接终止，不交给 handler 改写。原来的资源隔离和停止确认机制继续生效。

- `Suspend(ActivityWait::Runnable)` 用于 Task 控制请求导致的停稳；恢复重新检查原有 WF 准入条件。
  没有控制请求时不能用它实现业务等待，否则会被拒绝；暂停与恢复先于领域回复到达的竞态允许继续。
- `Suspend(ActivityWait::Condition(check))` 用于业务条件不满足。执行器释放资源后检查只读 RunCondition；
  阻塞时显示 WaitingConditions。条件变化后调用 `RunControl::recheck()` 唤醒，不轮询执行领域操作。
  等待业务条件期间仍响应 Task 暂停、恢复和取消。
- 挂起会结束当前执行段并释放资源，再确认 Task 暂停；逻辑步骤及其字段在同一进程内保留。
  唤醒后重新准入、获取资源并执行。每段仍用原有 ActivityId/观测记录，TaskId 不变；
  一段记录 Completed 表示该段已正常停稳返回，不代表整个逻辑步骤已经完成。
- Retry 的次数和间隔来自 `retry()`。新模式在暂停前后保留已用预算和原定等待截止时间；
  暂停续作不消耗重试名额，也不会重置已用名额。预算耗尽返回失败。
- `ActivityAction::Cancel` 是明确的终态，持久化为 Cancelled，切主恢复不会把它当宿主中断重新执行。

默认只保留进程内的实例状态。需要跨进程续作时，可实现
`checkpoint() -> Outcome<Option<serde_json::Value>>`，明确返回**完整 Task checkpoint**，包括 WF 阶段等恢复信息。
这要求 Workflow 为 Durable；框架在确认挂起前保存，保存失败则不确认暂停，也不继续执行。
它沿用现有 `save_checkpoint` 的替换语义，不能只返回局部字段覆盖其他恢复数据。
恢复仍通过 `Workflow::restore` 以及 `ctx.restored_checkpoint()` 重建步骤，不持久化 Rust Future 或调用栈。

运行 `cargo run --locked -p workflow-foundation --example managed_activity` 可观察新模式完整流程。

### 具名 Activity

`Activity` 描述一次尝试，框架管理整个调用。一个步骤把输入、固定说明、资源和重试预算放在一起，
`execute()` 执行业务操作并解释完整结果。只返回本次决定，不自行编写重试循环：

```rust,ignore
struct CheckDisks {
    client: ServiceClient<Probe>,
    disks: Vec<String>,
}

#[async_trait]
impl Activity for CheckDisks {
    type Output = u32;

    fn name(&self) -> &'static str { "检查目标盘是否就绪" }
    fn resources(&self) -> Vec<ResourceAccess> {
        self.client.resources(&self.disks)
    }
    fn retry(&self) -> Option<Retry> {
        Some(Retry::interval(Duration::from_secs(2)).max_attempts(10))
    }
    async fn execute(&mut self, scope: &mut ActivityScope)
        -> Outcome<ActivityDecision<u32>>
    {
        match scope.call(&self.client, self.disks.clone()).await? {
            ProbeReply::Ready(value) => Ok(ActivityDecision::Complete(value)),
            ProbeReply::Pending { remaining } => Ok(ActivityDecision::Retry {
                reason: format!("还有 {remaining} 个目标盘尚未就绪"),
            }),
        }
    }
}

let exit = ctx.run_activity(CheckDisks { client, disks }).await?;
```

`description()` 可提供固定步骤说明；默认无说明、无资源、无重试预算。
同一次调用的重试使用同一个步骤实例，因此 `&mut self` 可以保留本次调用内的状态。
资源声明在开始时读取，覆盖整个调用；重试期间不能通过修改字段扩大权限。
框架负责重试预算、等待、观测以及尝试间的暂停/取消。`execute()` 中的在途操作仍须协作停稳。
返回 Err 会结束步骤；捕获领域 Uncertain 并返回 Retry，也不能绕过原有隔离机制。

暂停等待返回 `ActivityExit::Suspended(())` 后，Workflow 处理结果并调用 `wait_until_runnable()`。
恢复时可以重新构造步骤，TaskId 保持不变；步骤的 Rust 字段不会自动持久化，也不会自动传给新实例。
需要业务 checkpoint 的长操作继续使用原有 `activity_body` / 领域 Activity 的停稳返回协议。

### Activity 函数适配器

`activity_fn` 是 `Activity` 的便捷适配器，使用同一个执行循环、资源管理和观测机制。


业务可以写成普通 `async fn`，调用处声明步骤信息：

```rust,ignore
async fn add(a: usize, b: usize) -> Outcome<usize> { Ok(a + b) }

let exit = ctx.activity_fn(|| add(2, 3))
    .name("计算结果")
    .description("步骤说明会随 Activity 一起被观测")
    .await?;
```

`.handle_result(...)` 接收完整的 `Outcome<T>`，解释业务数据、提取最终输出，决定完成、重试或失败。
`.retry(...)` 只配置次数和间隔。`||` 是创建每次尝试的函数，一个 Future 本身不能执行两次。
需要调用受框架管理的领域服务时，函数显式接收 `ActivityScope`；不使用隐式线程/任务上下文：

```rust,ignore
async fn check_ready(mut scope: ActivityScope, client: ServiceClient<Probe>) -> Outcome<ProbeReply> {
    scope.call(&client, ()).await
}

fn handle_check_result(result: Outcome<ProbeReply>) -> Outcome<ActivityDecision<u32>> {
    match result? {
        ProbeReply::Pending { remaining } => Ok(ActivityDecision::Retry {
            reason: format!("还有 {remaining} 个目标盘尚未就绪"),
        }),
        ProbeReply::Ready(value) => Ok(ActivityDecision::Complete(value)),
    }
}

loop {
    let client = self.client.clone();
    let exit = ctx.activity_fn(move |scope| check_ready(scope, client.clone()))
        .name("检查目标盘是否就绪")
        .description("暂未就绪时等待后重试")
        .resources(self.client.resources(&()))
        .handle_result(handle_check_result)
        .retry(Retry::interval(Duration::from_secs(2)).max_attempts(10))
        .await?;
    match exit {
        ActivityExit::Completed(value) => return Ok(value),
        ActivityExit::Suspended(()) => ctx.wait_until_runnable().await?,
    }
}
```

- 未配置结果处理策略时，原结果直接返回；仅设置 `.retry(...)` 不会自动重试任何错误。
  策略返回 `Ok(ActivityDecision::Complete(value))` 表示完成，`Ok(ActivityDecision::Retry { reason })`
  表示重试，`Err(Failure)` 表示失败。无需把正常的 Pending 等业务回复伪装为错误。
  策略拥有完整返回值，能把 `T` 转换为另一种最终输出类型；它是同步 `FnMut`，每次结果只处理一次。
  需要异步查询其他信息时，将查询放在业务函数内，再把完整结果交给策略。重复配置 handler 会替换旧策略。
- `Retry::interval` 默认允许最多三次尝试，可用 `max_attempts` 修改，次数包含第一次执行。
  策略要求重试但没有配置预算，或预算耗尽后仍要求重试，均返回带原因的 `Failure::Failed`；
  未就绪的业务回复不会被当作成功。原因保存在 `ActivitySnapshot::retry_reason`，真实错误保存在
  `last_error`（正常业务回复触发重试时为 None）。业务负责确认操作允许重复执行。
- Cancelled 和 Uncertain 在进入处理策略前直接返回，不能被策略改成成功或重试。
  策略发生 panic 同样按 Uncertain 处理，继续沿用现有的资源隔离机制。
- 框架在每次尝试前检查控制信号；重试等待可以立即被暂停或取消唤醒。
  暂停返回 `ActivityExit::Suspended(())`，结束本次 Activity，由 Workflow 处理结果后调用
  `wait_until_runnable()` 确认暂停。恢复创建新 Activity、开始新的重试预算，TaskId 保持不变。
  这里没有业务续作点；跨暂停/恢复需要保留的业务状态由 Workflow 自己保存。
- 正在执行的函数不会被强制丢弃。长函数通过显式参数接收 `ctx.control()`，或通过
  `ActivityScope::control()` / `check_control()` 主动感知控制；框架不能检查函数内部任意 loop。
  领域服务的 token 由 `scope.call` 传递，C 操作仍须调用停止接口、等停稳后返回。
  已经完成的实际结果不会被并发的暂停/取消请求覆盖。
- 所有尝试共用一个 ActivityId、一个资源租约，包括重试间隔。`resources` 声明领域调用资源的并集，
  `scope.call` 不重复加锁，并检查访问权限和 Pool 归属。尝试结束后该次 scope 立即失效。
  函数和每次返回的 Future 必须拥有参数（`Send + 'static`）；客户端/共享输入可以 clone 或使用 Arc。
  这样外层停止等待时，函数仍能完成收尾，资源不会提前释放。
- `.name(...)` / `.description(...)` 同样适用于 `ctx.activity(&client, request)`。
  动态业务进度仍用 `report_progress`；固定步骤信息不需要反复 report。

需要业务循环、自定义暂停 checkpoint 或 `.uninterruptible()` 时，保留显式 body 入口：
`ctx.activity_body(name, resources, |scope| business_function(scope, args))`。
它返回函数原本的结果，业务可以返回 `ActivityExit::Suspended(checkpoint)`，在 Workflow 中保存后
确认暂停。`scope.call` / `check_control` / `sleep` 的原有语义不变。
普通领域调用继续使用 `ctx.activity(&client, request)`，两种入口共用现有执行与资源管理。
若 body 丢弃了未完成的内部调用，框架仍等待停稳并返回 Uncertain；捕获领域 Uncertain 也不能
继续调用或把整体结果改成成功。

完整可运行示例：[activity_retry.rs](examples/activity_retry.rs)。
运行 `cargo run --locked -p workflow-foundation --example activity_retry`，观察 `CheckDisks` 解释
领域结果并要求重试、等待被外部暂停，以及同一个 Task 恢复后重新构造步骤并完成。

完整请求链与代码入口见 [TaskCenter 控制说明](../../docs/task-center.md)。

RunCondition 返回 Blocked 时状态为 WaitingConditions；条件变化后调用 RunControl::recheck 或观察句柄的
recheck(id)，不自动忙轮询。guard 必须只读且允许取消，不能承担远端提交。
suspend/resume/cancel 是请求；暂停确认需 wait_suspended，取消确认需等待终态。
ControlToken::cancellation_reason 区分用户 Requested 与宿主/主轮次撤销 Interrupted。
完整的控制、恢复边界和示例见 [当前规格](../../docs/changes/2026-10-24-simple-task-rules.md)。

## 验证

在 workspace 根目录：

```sh
cargo test --locked -p workflow-foundation
cargo clippy --locked -p workflow-foundation --all-targets -- -D warnings
cargo run --locked -p workflow-foundation --example two_services
```

[two_services.rs](examples/two_services.rs) 演示两个能力的串联。
DESIGN.md 保留历史阶段记录，当前接口以本页与源码为准。

`ActivityExit<T, C>` 为停稳返回的调用提供统一的 Completed / Suspended 结果协议；
取消仍使用 `Err(Failure::Cancelled)`。领域可通过 `ActivityContext::report_progress` 更新任务进度。
完整应用示例见 [缩容工作流](../../docs/shrink-workflow.md)。
