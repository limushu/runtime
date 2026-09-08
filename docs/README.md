# Pool 管控面架构基线

版本：`Baseline B0.7`

状态：理论基线 + MemberDisk 最小纵切面。PoolManager、Pool 装配和其他领域仍待实现。

## 目的

本目录沉淀 Monitor 中 Pool 管控面的统一语言、数据模型、状态权威、架构边界和安全不变量，作为后续服务划分、工作流、并发调度、主备恢复以及代码实现的共同起点。

当前阶段只回答以下问题：

- Monitor 内的 DiskMap、NodeMap、Pool 模块以及 SDB、user_dp、VNODE 的边界是什么；
- Pool 中有哪些核心对象，它们如何关联；
- 每类状态的权威来源是什么；
- 哪些规则在任何工作流和实现中都不能被破坏；
- 哪些架构判断已经确认，哪些仍然只是候选。

当前阶段仍不决定：

- BG 创建、重建、Node 上下线等流程的完整状态机；
- Monitor 切主时在途操作的具体恢复协议。
- Tier/Partition 的最终持久化布局与第二阶段并行分配协议；
- Rebuild 是否拥有独立、可恢复的核心状态并成为独立领域。

## 基线规则

文档中的结论使用三个等级：

| 标记 | 含义 |
| --- | --- |
| 已确认 | 来自当前业务事实，后续不能被实现细节隐式改变 |
| 候选 | 当前合理的架构方向，需要通过后续场景验证 |
| 待决 | 信息不足或需要业务决策，不能提前假设 |

后续每次模型调整应同步更新[决策与待决问题](06-decisions-and-open-questions.md)，避免同一概念在不同文档中产生多个版本。

## 文档导航

1. [统一词汇](00-glossary.md)
2. [系统上下文](01-system-context.md)
3. [领域数据模型](02-domain-data-model.md)
4. [状态与权威来源](03-state-and-authority.md)
5. [Monitor 管控面架构](04-control-plane-architecture.md)
6. [不变量与一致性原则](05-invariants-and-consistency.md)
7. [决策与待决问题](06-decisions-and-open-questions.md)
8. [理论基础论文：对象状态机、领域工作流与收敛控制](07-pool-control-plane-theory.md)
9. [Workspace 与包边界](08-package-and-workspace-layout.md)
10. [Service Runtime：生命周期、任务与观测](09-service-runtime.md)

PlantUML 源文件位于 [`diagrams/`](diagrams/)，渲染后的图片位于 [`assets/`](assets/)。

## 当前基线摘要

- **已确认：** 系统使用一个中心化 Active Monitor 管理所有 Pool，备节点采用冷备方式。
- **已确认：** MDC 不是独立进程；它表示一个 Pool 的核心元数据集合及其内存模型。
- **已确认：** SDB 保存重要业务决策，但 Pool 元数据分散在不同 Key/数据集合中，不是一个整体大对象。
- **已确认：** DDM/DiskMap 是 Monitor 内的全局硬盘管理组件；Pool 将分配给自己的物理盘逻辑化为 MemberDisk。
- **已确认：** NodeMap 是 Monitor 内的全局节点连通性组件，只表达与 user_dp 的网络是否畅通，不表达某个 Pool 在该节点上的服务状态。
- **已确认：** 外部磁盘和节点拓扑变化分别由 DiskMap、NodeMap 标准化并通知 Pool；Pool 不直接消费原始外部拓扑事件。
- **已确认：** 普通物理盘只属于一个 Pool；共享缓存层中的磁盘可以跨 Pool 共享。
- **已确认：** Pool 健康状态来自最差 VD，VD 健康状态来自最差 BG。
- **已确认：** 可靠性优先于并发性能；首阶段允许 Tier 内串行，后续并行必须保持相同安全不变量。
- **已确认：** Monitor 全局层的 `PoolManager` 负责 Pool 生命周期和路由；每个 Pool 是独立业务对象、内存隔离与管控边界，通用 Runtime 只是各领域 Service 的内部执行机制。
- **已确认：** 核心业务状态只属于领域对象；Operation Context 不建立第二套 `phase/status` 业务状态机。
- **已确认：** 业务执行优先写成所属 Service 上的自然 `async fn`；跨服务通信通过明确目标实例的类型化 Service facade；请求不携带隐藏路由身份。
- **已确认：** 每个请求都是 Future，但只有需要审计或命令式并发控制的操作才创建受管 Task；Query 直接执行。
- **当前实现：** `ServiceClient::call(Request)` 统一 typed channel/oneshot；MemberDisk 采用 `Client -> ServiceRuntime root -> MemberDiskService::handle -> executable state table`，不实现通用 ActionFlow DSL。
- **当前实现：** DOWN、UP、Shrink 是同一个 `MemberDiskEvent` 的变体；物理事件保存在每盘 active/pending 槽中而不写入 MemberDisk，Shrink 第一步提交持久化管理意图；不同事件请求当前 step 稳定停止。
- **当前实现：** `ServiceRuntime` 为每个服务实例创建唯一根执行单元，统一 poll 请求 Future，不为每个硬盘创建 Tokio task。
- **当前实现：** `ObjectTaskCoordinator<DiskUuid, MemberDiskEvent, Error>` 保存盘级 active/pending 输入、取消控制和等待者；MemberDisk 以一个普通函数声明冲突策略，执行槽不复制业务状态。
- **已确认：** MemberDisk 决策元数据、DiskMap 输入事实、派生运行状态和在途意图是不同信息，不得用单一状态枚举替代完整实体。
- **当前实现：** 仓库只有一个 `pool-control-plane` crate；`MemberDiskService` 私有持有对象目录，并通过统一的“校验 -> SDB -> 内存”路径修改。
- **当前实现：** `reconcile_once` 直接穷举 `(MemberDiskState, active MemberDiskEvent, shrink_requested)`；分支用普通 `await` 调用一个业务 action，再验证声明的结束状态。`Transitioned` 重新读取权威状态并继续同一事件，`Stable` 结束驱动，不存在 Action enum、执行转发表或隐藏兜底分支。
- **当前实现：** VDM 是 BG 引用关系的权威来源；排空完成不能只相信一次 Future 返回，必须通过 `has_references` 验证。该查询只发生在需要排空的转换中，不会把不可失败的 DOWN 边界耦合到 VDM 可用性。
- **当前实现：** `SetDiskState::Down` 同时表示通知 DOWN 和停止 IO；Shrink 期间 DOWN 会先让当前 VDm step 协作稳定退出，再从对象真实状态继续。
- **当前实现：** BLK 申请是 MemberDisk 领域的 typed Request，按 Tier 和可选故障域选择成员盘，SDB 提交后发布内存，不进入 DiskUuid 生命周期槽。
- **当前实现：** 公共 Runtime 已提供独立业务/控制通道、完整 Service 生命周期、Idle/Busy、可选 Task、Trace、精确取消、结构化快照/事件和根任务强制回收，并由 MemberDisk 纵切面接入验证；第二个真实领域仍用于检验抽象的泛化边界，而不是决定这些机制是否存在。
- **待决：** Tier、PoolCore、Rebuild 的最终服务粒度，以及 Monitor 切主时在途操作恢复协议。

## 基线完成标准

本基线应当能够让后续开发者一致回答：

1. 一个字段是业务决策、外部事实、派生状态还是运行操作状态；
2. 该字段由 SDB、硬件、user_dp 还是 Monitor 内存负责提供权威值；
3. 一个功能属于 Monitor 全局层还是单个 Pool；
4. 一项设计是否破坏 BLK、BG、VD、Pool 之间的核心不变量；
5. 当前结论是已确认事实，还是仍需讨论的候选方案。
