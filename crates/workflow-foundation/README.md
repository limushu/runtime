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

### Activity：业务步骤与执行策略

WF 只等待最终业务结果，重试和挂起不会返回到 WF 外层：

```rust,ignore
let migrated = ctx.run_activity(MigrateDisks { balance, cursor: 0 }).await?;
// 迁移完成后，才进入后续步骤。
```

Activity 提供 `name()`、可选 `description()`、`retry_policy()`、`execute()` 和 `handle()`。
`execute` 负责一次执行，`handle` 解释已停稳的业务回复；框架负责调度决定、观测和控制。

```rust,ignore
#[async_trait]
impl Activity for MigrateDisks {
    type Reply = MigrationReply;
    type Output = u64;
    fn name(&self) -> &'static str { "shrink.migrate-disks" }
    fn description(&self) -> Option<&str> { Some("迁移待移除磁盘上的数据") }
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::exponential(3, Duration::from_millis(100), Duration::from_secs(2))
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Self::Reply> {
        scope.call(&self.balance, self.cursor).await
    }
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<u64>> {
        Ok(match result? {
            MigrationReply::Completed(count) => ActivityAction::Continue(count),
            MigrationReply::Busy => ActivityAction::Retry { reason: "balance busy".into() },
            MigrationReply::Stopped(cursor) => {
                self.cursor = cursor;
                ActivityAction::Suspend(ActivityWait::Resume)
            }
        })
    }
}
```

| 决定 | 框架行为 |
| --- | --- |
| `Continue(value)` | 完成逻辑步骤，向 WF 返回结果 |
| `Retry { reason }` | 检查预算、计算退避、等待后再次 execute |
| `Suspend(ActivityWait::Resume)` | 等待已有 Task 暂停原因解除，并重新检查准入 |
| `Suspend(ActivityWait::Condition(check))` | 等待业务条件；数据改变后通过 `RunControl::recheck()` 唤醒 |
| `Cancel` | 终结 Task，取消不能当作中断后续作 |

**重试策略。** `RetryPolicy::fixed(max_retries, delay)` 使用固定间隔；
`RetryPolicy::exponential(max_retries, initial, maximum)` 使用翻倍且有上限的退避。
默认不重试，只有 handler 明确返回 Retry 才消耗预算。初次执行不计入 max_retries；
暂停后的续作也不消耗预算。次数由框架保存，暂停恢复不会重置，退避截止时间也不会重置。
业务判断可以基于成功回复、错误以及实例字段，业务代码不需要维护公共重试计数器。

**两种暂停方式。** 对本地循环，`scope.wait_until_runnable()` 或 `scope.sleep()` 在安全边界
原地等待，恢复后继续同一次 execute。对 C 侧长操作，领域适配器观察 `scope.control()` 传递的
控制请求，调用 C 的停止接口、等待实际停稳，然后返回类型化回复；handler 保存进程内游标并
返回 Suspend，恢复后框架在同一个实例上再次 execute。两种方式都保持同一个逻辑 ActivityId，
并重新检查 Workflow 的 RunCondition。自定义条件等待与用户暂停同时生效，恢复不能绕过任一条件。

**执行安全。** 所有外部副作用通过 `scope.call()` 调用已有领域服务。资源按每次真实调用
取得并持有到实际停稳，多次调用不构成隐式原子事务；重试和等待期间不占用已结束调用的资源。
框架不丢弃在途 IO 来模拟停止。Cancelled/Uncertain 执行结果不进入 handler，scope 还会记住
服务调用返回的这两类结果，防止 execute 吞掉后返回成功。handler 主动返回 Cancelled 等价于 Cancel。
执行或 handler panic 记为 Uncertain，不能被外层吞掉成为成功。丢弃 Activity 等待者会请求协作取消，
逻辑记录保持 Settling，直到实际子调用结束。

**步骤级观测。** TaskSnapshot.activities 同时包含 `kind = Step` 的逻辑步骤和 `kind = Call`
的真实服务调用，调用通过 `parent_id` 关联步骤。步骤自动记录名称、说明、attempts、retries、
last_error、retry_reason、retry_at、进度和生命周期。attempts 是 execute 的进入次数，
retries 是已批准的重试次数，两者不会把暂停续作混为重试。`scope.report_progress()` 自动使用步骤名；
领域服务的进度也会归入所属步骤。同一逻辑步骤在暂停时显示 Suspended，不会因子调用返回而显示 Completed。

历史上限优先淘汰已结束的子调用。活跃步骤及在途调用始终保留，极小上限下允许暂时多出活跃记录，
结束后重新收敛到配置上限；omitted 明确记录淘汰数量。这些记录和 Activity 实例状态均只存在于当前进程。
此实现不增加历史存储或 Activity checkpoint，已有 Workflow 的显式持久化恢复路径继续保留。
原有 `ctx.activity()` 直接调用方式仍然可用。

独立 demo：[examples/activity_trait.rs](examples/activity_trait.rs)。按 WF → Activity → 模拟领域适配器
的顺序阅读；只演示缩容的迁移步骤，不实际移除磁盘，C 接口使用模拟实现。

```bash
cargo run -p workflow-foundation --example activity_trait
cargo run -p workflow-foundation --example activity_trait -- --cancel
```

默认演示：均衡暂忙 → 策略重试 → 迁移进度 → 暂停 → C 停稳 → 同一 Activity 从游标恢复 → 完成。
`--cancel` 演示在迁移中取消，先等待停稳再终结。末尾同时打印逻辑步骤与实际调用记录。
回归测试见 [activity_trait.rs](tests/activity_trait.rs)。

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

每次通过 `WorkflowContext::activity()` 发起的实际调用自动进入 `TaskSnapshot.activities`；
`run_activity()` 另有覆盖整个逻辑步骤的父记录，
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
记录仍由服务端实际执行更新。handler panic 或执行中强制终止记录为 Uncertain，不能误报成功。

`ServiceHandler::activity_name(&request)` 可返回稳定步骤名；默认使用 handler 类型名，
无须修改已有实现。名称不包含请求/回复载荷。存储适配器按命令命名，例如 `disk.open`。
循环中的同名步骤通过调用序号区分；再次调用也有新序号。直接调用不自动重试，
trait 路径仅在 handler 返回 `Retry` 时重试。

`Config::activity_history` 默认每 task 保留 1024 条，必须大于零；活跃的父步骤和子调用不会被淘汰。
`activities.total` 是累计调用数，`activities.omitted` 明确给出因上限淘汰的条数；当前在途步骤始终保留。
记录随 task 一起按 `Config::task_history` 淘汰，已有 observer/RunControl 仍可读取自身记录。
`task_ids()` 可只列出 ID，避免复制所有步骤历史。

这里记录实际开始等待的逻辑步骤及发起过的调用；普通 async Workflow 的未来分支、未发起或跳过的步骤无法自动推断。
watch 仍会合并通知，但合并不会抹掉历史上限内已结束的 Activity 记录。
历史只存在当前进程内，关闭后保留在已有句柄中；重启后查询需要另外接入持久化存储。

Activity 历史不持久化。Task 可选持久化 input、checkpoint、details、控制意图与终态，
通过 Workflow::restore 在新宿主重建；不是确定性重放，不自动重试远端操作。
不可把重新创建同名实例当作恢复，也不提供执行不可信代码的沙箱。

## 统一 Task 与可选持久化

已有 Workflow 不必改动：默认 Memory，仍有完整 Task 快照与控制。
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
