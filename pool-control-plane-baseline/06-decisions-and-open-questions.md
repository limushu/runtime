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

### D-017 Workflow 是 Service 上的自然异步方法

普通业务流程优先写成所属 Service 的 `async fn`。业务开发者不创建 `TaskSpec`、不把流程主体包装进 `move |task| async move`，也不手工维护 Future 集合。Query 可以不创建可观测 Task。

### D-018 策略显式、机制统一

领域声明对象键、影响集合以及 Start、Join、Merge、Queue、CancelThenStart、Reject 等冲突语义；Runtime 的 Admission Registry 原子执行准入、在途索引、等待者、取消传播和并发配额。框架不猜测业务语义，普通业务代码不重复实现机制。

### D-019 跨服务通信属于 Router

领域之间使用类型化 `call/submit/query`。Router 封装 channel、oneshot、路由和 Operation Context 传播；Task 不是跨服务通信能力的所有者。

### D-020 Operation 观测不能成为隐形业务 WAL

Operation 的开始、里程碑、完成和结果可以追加记录并用于 TUI 投影，但不能保存决定业务流程的独立权威状态。影响恢复正确性的中间事实必须进入所属领域元数据。

## 候选架构判断

以下内容尚未成为最终决策：

1. Monitor 全局层使用 `PoolManager + PoolRegistry` 管理 PoolRuntime；
2. 每个 Pool 使用一个 PoolRuntime 作为内存和运行隔离边界；
3. PoolRuntime 内部按 PoolCore、TierDomain、VdDomain、NodeDomain 划分职责；
4. 共享缓存层由 Monitor 全局资源域管理；
5. 领域之间通过显式 `call/submit/query` 接口协作；
6. PoolView 作为统一只读聚合视图，避免外部直接拼接多个领域状态；
7. 每个 Service 使用一个根执行单元统一 poll 多个 Workflow Future；
8. Service 私有元数据通过不暴露锁守卫的短临界区 API 访问。

## 系统级待决问题

### Q-001 DiskMap 与 Pool 的内部接口边界

- DiskMap 如何向 Pool 提供硬盘身份、容量、介质类型和实际 UP/DOWN；
- 硬盘加入 Pool 的归属关系由谁发起、由谁持久化；
- DiskMap 通知是否有顺序号、代次或可重放能力。

### Q-002 共享缓存层

- 共享缓存盘的权威所有者是谁；
- 多个 Pool 如何分配配额和空间；
- 单盘故障如何传播到使用它的全部 Pool；
- 是否需要独立于 PoolRuntime 的全局服务与持久化模型。

### Q-003 Pool 生命周期

- Pool 的创建、恢复、启用、暂停、排空、卸载和删除状态；
- 哪些状态持久化，哪些属于 Monitor 运行生命周期；
- PoolRuntime 何时允许接收业务请求。

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

- 业务请求和控制请求的具体通道类型与优先级；
- Service Runtime、Admission Registry、Task Registry 的最小职责；
- 如何在不递归 spawn 的前提下支持并发、取消和观测；
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
- `REMOVED` 表示持久化终态还是 MemberDisk 对象删除。

## 推荐的后续建模顺序

1. Monitor 全局层与 PoolRuntime 生命周期；
2. Pool 内领域所有权和服务实例粒度；
3. Pool 与 DiskMap、NodeMap、user_dp、VNODE、SDB 的接口契约；
4. Pool 创建/恢复作为第一个系统级贯穿场景；
5. Node 上下线与拓扑发布；
6. MemberDisk 上下线与 BG 状态传播；
7. BG 分配、释放和重建；
8. 并发、取消、任务和观测运行时。
