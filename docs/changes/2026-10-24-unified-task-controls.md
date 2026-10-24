# 统一 Task、可选持久化与协作式暂停

实现分支：`codex/storage-workflow-development`，基于 `654e3ce` 重做，未移植旧实验实现。

## 需求与验收标准

| 需求 | 验收标准 | 实现/验证入口 |
| --- | --- | --- |
| 一个 Task | 所有 Workflow 使用同一身份、快照、观察与控制，内存任务也有 Task | tasks.rs、TaskCenter 直接 re-export |
| 自定义附加信息 | 类型化上报/读取 details，观察者收到更新 | report_details、details_as |
| 自声明持久化 | 默认 Memory；Durable 初始记录写入成功才执行业务 | persistence、accepted、失败存储测试 |
| 切主继续 | 同一 TaskId 恢复 checkpoint/用户暂停，旧 owner 写入失败 | managed_workflows 恢复与 fencing 测试 |
| ID 不回头 | 新轮次 + 池 UUID + 业务 + 共享流水，新轮次大于旧轮次 | identity.rs 与身份测试 |
| 策略可替换 | TaskCenter 主循环没有 topo/shrink 特判 | SchedulingPolicy、非存储业务策略测试 |
| topo 优先 | topo 阻止缩容启动，运行中缩容实际停稳后才放行 topo | 默认策略、停稳交接测试、managed_shrink 示例 |
| 用户控制 | 用户暂停与策略原因独立，观察/挂起/恢复/取消同一 Task | RunControl 与 TaskSnapshot |
| 两种下游契约 | 保留原调用，或返回业务部分结果，不新增 Activity 类型 | managed_workflows 两种契约测试 |
| 恢复先检查 | 未满足条件保持 WaitingConditions，不恢复迁移 | ContinuationGuard 与示例 |
| 上游选择命令 | 不同 Workflow 可为同一服务选择不同控制命令 | workflow_can_choose_different_domain_control_commands 测试 |
| 存量业务不改 | storage-pool/src/workflows 与基线无差异 | git diff 与原有测试 |

## 概念与代码边界

Workflow 承载业务执行，Task 管理该执行的完整生命周期。Run 是结果/控制句柄，SavedTask
是同一 Task 的存储表示，Restored 是恢复交接回执；这些没有新身份或第二个观察中心。
WorkflowId 是 TaskId 的兼容别名，旧 Workflow 的 run/activity 调用不需要修改。
既有 PoolTaskSnapshot 只是多池查询的路由信封（TaskRef + 同一个 TaskSnapshot），
instance 用来拒绝旧 PoolHandle 路由；它不生成任务 ID、不维护第二份任务状态。

TaskCenter 仍是每池一个事件循环，直接轮询 WorkflowDriver，没有额外的 WorkflowScheduler。
基础库提供纯调度关系，应用策略提供业务含义。默认策略在 conflicts.rs；事件循环仅处理关系、
启动许可、控制确认和隔离。策略要求无 YieldTo 环，不提供自动修复环或解除隔离。

新增核心文件：identity.rs、control.rs、scheduling.rs、persistence.rs、persistence/file.rs、
driver/recovery.rs、workflow/control.rs。原 tasks.rs 是唯一 Task 观察定义。
没有新增 pool_tasks、特殊迁移宿主或 Pool 内缩容专用的恢复闭包。

## 身份与选主

TaskId 文本为 `epoch/32位池UUID/business/sequence`。按数值 epoch、全主共享 sequence 排序；
不要按文本字典序判断先后。业务名为 1–96 个 ASCII 字母、数字、点、横线或下划线。
同一主的所有池必须共享一个 TaskIds::for_leader(epoch)。轮次由选主系统持久化且严格递增，
重启不能重用旧轮次。PoolManager::with_task_ids 将分配器传给各池。

默认 epoch=0 是本地兼容模式，不保证跨重启顺序，不能启用 Durable。
失主调用 revoke：停止分配并协作中断旧执行。TaskStore::claim 拒绝旧轮次，save 检查
owner_epoch 和 revision；恢复记录保留创建轮次，只有 owner_epoch 更新。
本地撤销和 TaskStore fencing 不能替代数据面 fencing，应用须阻止旧主真实写入继续生效。

## 持久化与恢复契约

Workflow 返回 Persistence::durable(version, &input)，实现 restore(record, dependencies)。
组合根通过 driver.define::<W>(dependencies) 为可恢复类型绑定构造与本地客户端。
这是 Rust 类型重建所需的通用注册，业务恢复步骤仍归 Workflow；客户端和 Future 不序列化。

启动顺序：取得新主身份及数据面 fencing → 恢复领域状态/绑定客户端 → define →
enable_persistence(store) → restore → TaskCenter::from_restored 装入全部恢复任务。
未知类型、不兼容版本、取消待核对或 NeedsCheck 记录使准入保持关闭，不丢弃旧任务占用。
恢复校验业务名、input/version、objects 不变；当前不自动迁移旧版本。

存储内容：input/version、checkpoint、objects、应用调度 metadata、details、用户暂停、取消
意图、终态、owner_epoch/revision。policy 原因重新计算；Rust 栈、计时器、signal 队列、Activity
历史和类型化 Output 不保存，也没有恢复旧 Run.result 结果的接口。Activity 序号为当前宿主
局部序号，不能用作跨恢复的远端幂等键。恢复始终保留原 TaskId。

report_details 是内存观察更新；save_checkpoint 保存续跑位置与当前 details，终态也保存 details。
checkpoint 只能在先前 Activity 停稳后写入，不表示远端效果与本地记录原子提交。
业务须用稳定 job/操作身份、幂等命令或查询对账处理“动作完成、checkpoint 尚未写入”的窗口。
这不是 Temporal 确定性重放，框架不自动重试远端操作。已有 storage Workflow 仍为 Memory。

Durable 提交等待初始写入，失败时不执行业务，也不对既有任务发出替代取消或抢占暂停。
尚未确认写入的任务不参与运行关系规划；FIFO 针对已确认的任务。用户取消意图先写盘再交付取消令牌，暂停 ACK
也在意图写入后产生。pause/resume/cancel 本身是请求，不是持久化确认；调用者等待 wait_paused、
状态或终态。请求尚未写盘时崩溃，可能丢失未确认的请求。
宿主关闭/失主导致的已停稳中断保存 Pending；用户取消保存 Cancelled；不确定效果保存 NeedsCheck。
中断时遗留的取消意图需要核对，不能直接当成可重执行业务的 Pending。

TaskStore 是可替换后端。MemoryTaskStore 用于测试；FileTaskStore 为单机参考实现，独占文件锁、
追加帧、校验和、sync_all，重开截去不完整尾帧，完整帧损坏拒绝加载；写入异常须重开核对。
它不包含压缩、共享存储共识、目录创建的掉电保障或跨机器复制，不代表生产多主容灾实现。

## 暂停、返回与恢复

两种方式都使用现有 ctx.activity(&client, command).await：

| 情形 | Activity.await | Task 暂停确认 | resume 后 |
| --- | --- | --- | --- |
| 保留调用 | 等待业务结果 | ActivityControl::suspend 实际停稳 | 先执行 Workflow guard，再调用所选 resume 命令 |
| 停稳返回 | 返回领域部分结果 | Workflow 保存进度并到 safe_point | 先执行 guard，再由 Workflow 发起下一次普通 Activity |

服务可用 ActivityContext::register_control 提供默认控制；Workflow 可用 set_activity_control
覆盖下一次调用，让缩容与扩容选择不同命令。这是现有 Activity 的可选能力，不是新 Activity 类型。
控制命令必须能到达服务，不能把 resume 排在自身占用的资源锁之后。

没有控制适配器时，服务可读取 control token，自行停稳后返回部分结果；完全不支持暂停则等自然
完成。TaskCenter 不因请求送达就释放占用。保留调用仍持有 Activity 资源锁，使用相同资源的 topo
仍需等待；此时应选择停稳返回方式，或按真实资源边界声明能力。

ContinuationGuard 由 Workflow 定义，只读且允许取消。返回 Wait(reason) 进入 WaitingConditions，
业务状态变化后 recheck 唤醒检查。它在继续执行边界检查，初始执行也检查；不同阶段可用 Workflow
共享状态表达。它不能替代领域命令提交时的版本/状态校验。
resume 期间又来暂停请求，新的控制代次撤销旧 ACK，须再次停稳后才交接。
每个 topo 和用户分别持有暂停原因；topo 完成不清除用户暂停。控制失败/panic 按 Uncertain
保留隔离，不伪造停稳；取消也等待实际动作结束。

## 接入范围

managed_shrink 是框架接入示例：普通迁移服务/client、Workflow 自声明持久化与 guard、默认 topo
策略、checkpoint 和同一 Task 观察，分别演示两种暂停方式。它用模拟迁移和 MemoryTaskStore，
不包含生产缩容、真实盘移除或 PoolManager 跨主恢复的完整装配。这些需要实际迁移命令、
选主后端、领域重建和业务对账接入。

## 验证记录

- scripts/verify.ps1：125 项 Rust 测试通过；格式、doc-tests、Clippy（-D warnings）通过。
- 新增 11 项 foundation 控制/持久化测试、2 项策略交接测试；保留全部原有业务测试。
- 外围 JurisDesk：npm run verify 通过（harness、lint、TypeScript、Vite build）。
- managed_shrink 两种模式均经过 WaitingConditions；最终 Activity 数分别为 1 和 2，各自 TaskId 不变。
- storage-pool/src/workflows 与 HEAD 无差异；7 个预先存在的用户文档/图文件与备份逐一哈希一致。

命令及本机 GNU 环境见 [verification.md](../verification.md)。
