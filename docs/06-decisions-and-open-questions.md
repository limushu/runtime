# 决策与待决问题

## 已确认决策

### D-001 中心化 Monitor

Active Monitor 管理全部 Pool。Monitor 采用冷备，切主后重新加载和构造 Pool 内存结构。

### D-002 MDC 是 Pool 元数据模型

MDC 表示一个 Pool 的核心元数据及内存结构，不是独立进程，也不存在多个 MDC 实例并发写同一个 Pool 的模型。

### D-003 元数据按领域分散存储

Pool Core、MemberDisk/Tier、VD/BG 等数据不保存在一个整体 Key 中。重要决策落地 SDB 后才正式生效。

### D-004 DiskMap 与 Pool 分工

DDM/DiskMap 是 Monitor 内的全局硬盘管理组件；Pool 管理加入本 Pool 后形成的 MemberDisk 及其 Pool 内业务状态。本次架构设计以 Pool 为中心，不展开 DiskMap 内部实现。

### D-005 NodeMap 只表达网络连通性

NodeMap 是 Monitor 内的全局组件，只表达与 user_dp 的网络是否畅通。Pool 在 user_dp 上是否加载和提供服务必须单独建模。

### D-006 拓扑变化由全局 Map 通知 Pool

外部硬盘和节点拓扑变化分别由 DiskMap、NodeMap 吸收并标准化，再通知受影响的 Pool。Pool 不直接依赖原始硬件或网络事件。

### D-007 普通盘独占与共享缓存例外

普通物理盘只能属于一个 Pool。共享缓存层中的磁盘允许跨 Pool 使用，必须单独建模。

### D-008 两维 Entry 状态

BGEntry 的介质可访问性与数据有效性是两个独立维度。磁盘上线不能自动恢复已经失效的数据。

### D-009 健康状态自下而上派生

BG 状态由冗余策略评估，VD 取最差 BG，Pool 取最差 VD。

### D-010 无跨 Key 业务事务

SDB 不提供覆盖 Tier 位图与 VD/BGMap 的统一事务或业务 WAL。系统采用有序持久化，保证 `BG引用集合 ⊆ 已分配BLK集合`。

### D-011 可靠性优先

首阶段接受 Tier 内串行空间修改。未来并行化必须保持相同安全不变量，不能把并发正确性交给业务开发者约定。

### D-012 核心元数据由领域唯一修改

每类核心元数据只有所属领域可以修改。其他领域只能调用业务能力、读取不可变视图或订阅领域事件，不能持有共享可变对象。

### D-013 编排归属于最终业务结果的领域

硬盘隔离等跨领域 Workflow 由拥有最终业务结果的领域编排。被调用领域自治完成内部流程；不引入全局万能 WorkflowService。

### D-014 领域状态、Operation Context 与 Task Attempt 分离

领域对象状态是业务真相。Operation Context 是稳定的因果身份，不定义 `phase/status` 业务状态机；Task Attempt 和 Future 是可失败、可替换、可重建的执行实例。一个 Operation Context 可以关联零到多次 Task Attempt。

### D-015 业务观测采用因果图

系统必须能够从一个历史事件追踪其影响对象、领域 Workflow、Operation Context、状态转换和 Task Attempt。因果关系是允许多原因合并的 DAG，而不是严格父子树；日志和普通 Span 不能单独承担该职责。

### D-016 业务取消由对象状态决定并协作收敛

取消先由领域对象根据当前状态和已提交局部效果决定停止方式，再通过共享取消作用域协调下游安全停止并等待稳定结果。不能把 drop Future 等同于业务取消；局部效果不可逆也不等于父 Workflow 整体不可取消。

### D-017 状态表直接调用 Service 的自然异步方法

业务步骤写成所属 Service 的 `async fn`。`reconcile_once` 在穷举状态表中直接 `await` 这些方法，不经过 Action enum、函数注册表或 `move |task| async move`。每条非稳态规则显式声明起始状态、输入事件、action 和结束状态；action 完成后验证权威后置条件。它只返回 `Transitioned/Stable`；Query 不创建 Task。

### D-020 完整转换契约，按需读取跨领域状态

MemberDisk 本地转换使用 `MemberDiskState + shrink_requested` 作为起止状态。排空转换额外读取 VDM 权威的 BG 引用关系，并要求 action 成功后 `has_references=false`。跨领域状态按转换需要读取，不构造全局大快照；DOWN 通知与停 IO 不依赖 VDM 查询。复合的“排空 + 停 IO + Remove”方法禁止出现，三者是三个独立转换。

### D-018 先具体实现，再提炼机制

MemberDisk 的对象规则和可执行状态表保留在领域内；已从纵切面提取 `ServiceRuntime`、`ServiceClient::call(Request)`、生命周期/观测与 `ObjectTaskCoordinator<K, I, E>`。不建立公共 Action DSL、状态机框架或独立 Runtime crate；业务 Key、冲突策略和状态表仍由领域实现。

### D-019 跨服务通信属于显式 Service 能力

领域之间使用指向明确目标实例的类型化 Service facade。其内部 `ServiceClient<R>` 封装 channel、oneshot 和 Operation Context 传播；请求不携带隐藏的目标 ServiceId，也不通过请求类型自动查找服务。Task 不是跨服务通信能力的所有者。

### D-020 Operation 观测不能成为隐形业务 WAL

Operation 的开始、里程碑、完成和结果可以追加记录并用于 TUI 投影，但不能保存决定业务流程的独立权威状态。影响恢复正确性的中间事实必须进入所属领域元数据。

### D-021 Pool 是业务边界，Runtime 是执行机制

Monitor 的 `PoolManager` 维护 `PoolId -> Arc<Pool>`。`Pool` 拥有 Pool 元数据 CRUD、领域 Service facade 和根执行单元的生命周期；不再使用 `PoolRuntime` 混淆业务对象与通用执行机制。

### D-022 对象执行槽是隐藏机制

MemberDisk 是完整领域对象，不暴露 Actor API。公共 `ObjectTaskCoordinator<DiskUuid, MemberDiskEvent, Error>` 关联 active/pending 事件、TaskControl 和 idle 等待者；它不拥有持久化业务元数据，也不为每个硬盘创建独立 Tokio task/mailbox。MemberDisk 以一个普通决策函数提供冲突含义，槽位机制不解释领域状态或调用下一步业务动作。

### D-023 取消在业务步骤的稳定边界传播

普通业务方法不散落取消轮询。恢复窗口直接等待取消；显式下游能力接收共享 `CancellationToken`，并且只能在下游已经稳定停止或完成不可中断动作后返回。旧 Future 不被直接 drop；不同事件进入 pending 后先等待当前旧 step 稳定退出，再用 pending 事件和完整对象状态重新计算。

### D-024 MemberDisk 信息分层

MemberDisk 的 SDB 决策记录、DiskMap 输入事实、Shrink 管理意图、UA/DA/DI/UI/Removed 投影和运行时 active reconciliation 是不同信息。当前对象只保存已提交 IO 能力与持久化的 `shrink_requested`，不复制 DiskMap `physical_state`，也不保存 Future、取消令牌或运行阶段。物理事实及 `observed_at` 随 active/pending 事件存在。状态图不能取代容量、Tier、故障域、位图和成员关系等核心元数据。

### D-025 REMOVED 保留对象并允许 UP 触发 Rejoin

`REMOVED` 是 MemberDisk 的稳定成员状态，不表示从对象目录删除身份和历史元数据。之后收到物理 UP，MemberDisk 直接执行 Rejoin：在所有当前可服务 Pool 节点打开硬盘并发布 UP，成功后提交 `MemberDiskUpdate::Rejoin`，回到 `UA`。

### D-026 一个类型化 call，响应语义由 Request 决定

目标 `ServiceClient` 已经确定路由，因此外部统一使用 `client.call(request)`。`ServiceRequest<P>` 静态关联 `Response`：MemberDisk 事件返回 `Accepted`，查询返回 `MemberDisk`，BLK 申请返回 `Allocation`。内部协议枚举和 typed oneshot 仍然显式存在，但普通调用者不需要理解或手写它们；当前不引入基于请求类型自动选服务的 Router，也不为隐藏一处清晰分发而引入异步类型擦除。

### D-027 MemberDisk 领域拥有成员、Tier 与 BLK 分配

当前 MemberDisk 领域同时拥有成员目录、Tier/故障域信息和单盘 BLK 位图。BLK 申请因此是 `MemberDiskService` 的 request/response 能力，不拆成独立 SpaceManagerService，也不进入 `DiskUuid` 生命周期槽。第一阶段所有 MemberDisk 修改通过 `mutation_gate` 串行；对象 Guard 在 SDB `.await` 前释放，SDB 成功后再发布内存。后续只有在真实负载和 SDB 条件写契约明确后，才把该 gate 缩小到 Tier 或 Partition。

### D-028 Service 实例具有四种独立句柄

每个 `ServiceInstance` 同时暴露业务 `ServiceClient`、高优先级 `ServiceControl`、只读 `ServiceObserver` 和根所有权 `ServiceTask`。Client 不持有 Service 对象；Control 不与业务流量共享容量；Observer 不修改运行状态；ServiceTask 被 Pool 持有并负责最终 Join/Abort，所有权直接丢失时自动 abort，避免后台 Future 泄漏。

### D-029 Runtime 生命周期与关闭语义

Runtime 实现 `Initializing / Running / Paused / Draining / Stopping / Stopped / Failed`。Pause 只拒绝新请求并继续驱动在途工作；Drain 拒绝新请求且不取消已接收工作；Stop 请求 Service token 和可取消 Task 协作停止并等待稳定退出；Abort 才直接 drop 根 Future。控制通道在根 `select!` 中优先于业务通道。

### D-030 Task 是可选执行尝试，观测不成为业务权威

每个请求都有 handler Future，但 Query 无需 Operation/Task。需要进度、审计或精确控制的业务在已经运行的 handler 内调用 `RequestContext::start_task` 附加 `TaskAttempt`，不使用闭包包装工作流。Service 快照与 RuntimeEvent 提供生命周期、Idle/Busy、队列、Task、Trace、阻塞原因和状态转换；内存 history 与外部 EventSink 都是观测投影，不替代领域 SDB。

## 候选架构判断

以下内容尚未成为最终决策：

1. 共享缓存层由 Monitor 全局资源域管理；
2. PoolView 作为统一只读聚合视图，避免外部直接拼接多个领域状态；
3. Tier、VD/BG、Node 的最终服务实例粒度；
4. 真实 SDB 适配器的条件写和不确定结果确认协议。

## 系统级待决问题

### Q-001 DiskMap 与 Pool 的内部接口边界

- DiskMap 如何向 Pool 提供硬盘身份、容量、介质类型和实际 UP/DOWN；
- 硬盘加入 Pool 的归属关系由谁发起、由谁持久化；
- DiskMap 通知是否有顺序号、代次或可重放能力。

### Q-002 共享缓存层

- 共享缓存盘的权威所有者是谁；
- 多个 Pool 如何分配配额和空间；
- 单盘故障如何传播到使用它的全部 Pool；
- 是否需要独立于 Pool 的全局服务与持久化模型。

### Q-003 Pool 生命周期

- Pool 的创建、恢复、启用、暂停、排空、卸载和删除状态；
- 哪些状态持久化，哪些属于 Monitor 运行生命周期；
- Pool 处于何种生命周期时允许接收业务请求。

### Q-004 Node 成员关系

- Pool 成员节点列表存放在哪里；
- NodeMap 连通性变化如何通知受影响的 Pool；
- DiskMap/NodeMap 通知的排序、合并、重复和丢失语义；
- Node 上线、加载 Pool、Ready 和可服务之间的准确状态机；
- Node 状态变化如何形成 VNODE 的稳定输入。

### Q-005 健康状态体系

- BG、VD、Pool 的完整状态枚举和严重度顺序；
- 不同 VD 类型是否具有不同的健康判定或 Pool 状态权重；
- 状态变化的发布和防抖规则。

### Q-006 服务实例粒度

- 一个 Pool 是否对应一个运行容器；
- Tier、VD、Node 是每个领域一个服务，还是每个 Tier/VD 一个实例；
- 哪些服务必须拥有独立串行化边界。

### Q-007 跨领域操作归属

- 除已确认由 DiskDomain 拥有的硬盘隔离外，Pool 创建、Node 上下线、BG 重建等操作分别由哪个领域拥有；
- 策略、状态机与工作流分别位于哪里；
- Operation Context 与追加式观测记录的最小公共模型。

### Q-008 在途操作与主切换

- 哪些操作中间阶段必须持久化；
- 新主如何识别 user_dp 中已经执行但尚未完成提交的操作；
- 重试、接管和人工介入的边界。

### Q-009 执行与并发模型

- 第二个真实领域是否能在不扩张 `ManagedService` trait 的情况下复用当前契约；
- 生产级 EventSink 的背压、丢弃和持久化策略；
- Service 私有元数据如何从机制上禁止可变访问跨越 `.await`；
- 如何从 Tier 全串行演进到安全并行。

### Q-010 SDB 单 Key 语义

- 单 Key 写入的原子性、顺序性和失败返回语义；
- 是否支持版本号、条件更新或幂等操作标识；
- Monitor 收到超时或不确定结果时如何确认最终状态。

### Q-011 业务观测持久化

- OperationStarted/Finished、Milestone、CausalEdge 和 StateTransitionRecord 存储在 SDB 还是独立观测存储；
- 历史保留、压缩和脱敏策略；
- ServiceSnapshot 的实时订阅协议；
- Operation Context DAG 与 OpenTelemetry Trace/Span 的映射方式。

### Q-012 MemberDisk 可逆排空的精确语义

- UA、DA、DI、UI、REMOVED 的完整进入条件与 SDB 持久化边界；
- 排空停止时 VdDomain 的稳定结果协议；
- 已提交 BG remap、在途 BG 与未调度 BG 分别如何处理；
- Rejoin 失败后的重试、节点部分成功和主切换确认协议。

## 推荐的后续建模顺序

1. Monitor 全局层与 Pool 生命周期；
2. Pool 内领域所有权和服务实例粒度；
3. Pool 与 DiskMap、NodeMap、user_dp、VNODE、SDB 的接口契约；
4. Pool 创建/恢复作为第一个系统级贯穿场景；
5. Node 上下线与拓扑发布；
6. MemberDisk 上下线与 BG 状态传播；
7. BG 分配、释放和重建；
8. 并发、取消、任务和观测运行时。
