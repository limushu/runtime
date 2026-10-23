# 当前架构与职责

业务入口统一在 Pool，跨池目录在 PoolManager，调度在 TaskCenter。
执行框架和数据库分别提供基础能力，不参与判断存储业务冲突。

```text
外部事件 / 管理请求
       ↓
PoolManager：按 ID / UUID / Name 找 Pool；公平扇出并汇聚
       ↓
Pool：校验入口、准备对象集合、构造 Workflow
       ↓ 提交
TaskCenter：准入、冲突、队列、暂停协调、历史与续办
       ↓ 推进 Workflow Future
Workflow：按顺序调用 Activity
       ↓
领域服务 / Pool 成员能力：验证、执行、确认结果
       ├── 元数据仓储 → 可替换 KV
       ├── 数据面端口
       └── 均衡服务端口
```

## 代码归属

| 模块 | 职责 | 不应持有或执行 |
| --- | --- | --- |
| `manager/` | 句柄索引、公平事件扇出、跨池物理盘预约索引 | Pool 内部生命周期、Workflow 推进 |
| `pool/` | 所有 PoolHandle 入口、成员目录、生命周期、组装及恢复注册 | 第二套调度器或工作流引擎 |
| `task_center/` | 任务准入与占用、拓扑协调、观察、历史和续办 | 磁盘目录、成员实体或迁移算法 |
| `workflows/` | 一件业务的完整步骤、声明涉及的对象 | Manager Registry、Mutex、领域缓存刷新 |
| `domains/` | 业务模型、领域能力、Activity 托管适配 | Task 队列、PoolManager 内部状态 |
| `metadata/` | 一致读取、版本校验、原子业务变更、可替换 KV | 数据面 IO 或任务调度 |
| `adapters/` | 模拟数据面、模拟均衡器及测试提交日志 | 第二套元数据业务规则 |

`ports.rs` 定义外部数据面和均衡契约。元数据类型定义在 `metadata/`，ports 保留重导出以支持既有接入。

## 阅读顺序

1. `pool/mod.rs`：Pool 与 PoolHandle、查询及业务入口；`bootstrap.rs` 只负责组装。
2. `workflows/shrink_disk.rs`：读取缩容、禁止分配、整盘排空、校验和终结的完整链路。
3. `task_center/conflicts.rs`：何种业务互斥；`scheduler.rs`：何时请求暂停及放行任务。
4. `domains/balance/runtime.rs`：领域 job 的进度、Pause/Resume/Cancel 适配。
5. `metadata/kv.rs`：接入自己的 KV；`repository.rs` 和 `state.rs`：编码与统一业务约束。

TaskCenter 的 `model.rs` 定义不依赖数据库的 TaskId，`registry.rs` 管理全部逻辑任务，
`run.rs` 提供 TaskRun/TaskControl。Driver 内部只持有 ExecutionRegistry。其他文件分别是 `operation.rs`（分类与对象声明）、`observation.rs`（只读视图）、
`submission.rs`（提交与可选审计绑定）、`control.rs`（稳定 TaskId 控制）、`recovery.rs`（自动重建）、
`persistence/`（存储与审计）。`mod.rs` 保留唯一事件循环；它调用同步的 `State::admit`；规则在 PoolRules，等待及唤醒在通用 scheduler，
事件循环不执行数据库 IO，也不包含泛型工作流及持久化绑定的构造细节。

Pool 的 `membership.rs` 封装领域变化后的目录更新；`operations.rs`、`lifecycle.rs`、`shrink_disk.rs`
组织对应业务入口；`takeover.rs` 串联系统接管时的元数据、当前拓扑和 Task 自动恢复。所有业务 Workflow 均位于 `workflows/`，Pool 模块不再混入流程体。

## 状态由谁负责

- **任务协调状态**属于 TaskCenter：排队、对象占用、topo 阻塞、逻辑隔离。
- **执行状态**属于 workflow-foundation：当前 Activity、安全暂停确认、已派发操作结算。
- **磁盘、Tier、VD 及分配事实**属于元数据仓储。领域服务每次从仓储读取，不维护另一份可写缓存。
- **连接和已加载事实**由 NodeService 接受真实拓扑观测；元数据恢复不会捏造这些事实。
- **复制与迁移进度**属于均衡模块，Activity 转发其真实进度。
- **Pool 成员路由与预约**属于 Pool；Manager 只维护需要跨 Pool 排他的物理盘归属索引。

已确认元数据不等于外部 IO 的完整事实。Uncertain 任务继续隔离，成功读回某条记录不会自动解除隔离。

## 执行与控制

TaskCenter 在一个循环内同时接收请求并轮询 `driver.tick()`，不等待一条 Workflow 完成才处理下一条。
WorkflowDriver 推进 Futures，不再做一次业务排队；Activity 资源锁保护实际步骤，保持到停稳。
领域 `runtime.rs` 只是 Activity 的托管适配，不是额外工作流引擎。

缩容只提交一个整盘均衡 job。topo 到来后，TaskCenter 关闭继续门禁，等待 Activity 委托均衡服务确认暂停；
所有 topo 完成后，先重新检查条件，再恢复同一个 job。没有暂停钩子的步骤正常完成后才确认暂停。
用户取消和宿主中断的原因不同，但都需要适配器主动感知 token，不能直接丢弃在途 IO。

最终关闭磁盘、提交 Removed、更新 Pool 目录在一个不可中途暂停的完成步骤内执行。
这样并发取消或 topo 不会落在提交成功和清理目录之间。细节见 [缩容工作流](shrink-workflow.md)。

## 数据库接入与边界

`MetadataStore` 是领域仓储接口；`KvMetadata` 是共用实现，`KvStore` 是底层数据库适配契约。
它要求一致性快照和带条件的原子批量写入。内存和 redb 复用同一套业务规则，见 [元数据接入](metadata.md)。

TaskStore 独立保存历史和可续办 Task，可与元数据共用物理数据库，但不与整个 Workflow 组成长事务。
同一 Task 可以关联多次执行，缩容根据持久意图、真实 placement 和外部 job 核验后继续。
系统获得主权后调用 `PoolManager::open`，池内自动完成恢复及重提交；用户无需提供 TaskId。
当前执行的 suspend/resume 只控制原 Run，不创建 execution；无法安全重建的任务单独报告并保留占用。

当前没有跨主租约、确定性 Future 重放或真实磁盘复制。离线重建等待仍是明确失败的占位能力。
本地强制 Drop 不能证明远端工作已经停止；接管必须沿用系统的所有权交接机制。
