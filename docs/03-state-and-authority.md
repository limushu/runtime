# 状态与权威来源

## 三份系统视图

一个 Pool 的运行状态不是单一副本，而是：

\[
P=(P^{sdb},P^{mem},P^{real})
\]

- \(P^{sdb}\)：SDB 中已提交的重要决策；
- \(P^{mem}\)：Active Monitor 中的当前内存模型；
- \(P^{real}\)：硬件和 user_dp 的实际运行状态。

Monitor 内存模型应表示为：

\[
P^{mem}=Materialize(P^{sdb},Obs_{hardware},Obs_{user\_dp})
\]

不能将其简化为只从 SDB 加载：

\[
P^{mem}\neq Load(P^{sdb})
\]

## 四类状态

### 决策状态

由 Monitor 作出，并以 SDB 成功落地作为生效边界：

```text
Pool配置和阈值
MemberDisk成员关系
空间分配位图
VD配置
BGMap
BGEntry数据有效性
```

\[
DecisionCommitted\iff SDBWriteSucceeded
\]

对 SDB 权威的 MemberDisk 决策字段，当前主必须使用同一条提交路径：

```text
读取并校验 MemberDisk
    -> MetadataService.update_member_disk(DiskUuid, MemberDiskUpdate)
    -> SDB 提交成功
    -> 更新 MemberDiskService 内存目录中的 MemberDisk
```

`MemberDiskService` 的 `HashMap<DiskUuid, MemberDisk>` 是当前主上的已提交内存模型和查询入口；`MetadataService` 只是可靠的 SDB 提交能力，不拥有领域对象，也不接管业务查询。持久化边界只传对象标识和字段级 `MemberDiskUpdate`，不能传整个对象快照做覆盖保存。提交失败时内存不变；提交期间查询只能看到旧提交状态或等待提交完成；SDB 成功后若进程退出，新主从 SDB 重新物化对象。

### 外部事实

来自实际环境：

```text
物理硬盘是否在线
Node是否在线
user_dp是否已加载Pool
数据操作是否真实完成
Monitor与user_dp的网络是否畅通
```

外部拓扑事实进入 Pool 的固定路径是：

```text
硬盘变化 -> DiskMap标准化通知 -> Pool
节点连通性变化 -> NodeMap标准化通知 -> Pool
```

SDB 中的历史记录可以用于审计或辅助恢复，但不能覆盖重新观测到的现实。

### 派生状态

由决策状态和外部事实计算：

```text
BG健康状态
VD健康状态
Pool健康状态
当前可服务Node集合
VNODE所需的Pool服务视图
```

### 执行与观测投影

描述正在发生的过程，但不构成第二套业务状态：

```text
Pool创建/恢复/删除
MemberDisk加入、上下线、退出
Node加载、上下线
BG申请、释放、remap、重建
VD扩缩容
VNODE迁移
```

Operation Context 是稳定因果身份；Workflow、Task Attempt 和 Future 是临时执行实例。开始、里程碑、完成和结果可以记录为追加式观测事件，供 TUI 和历史追溯投影当前进度。

Operation 观测记录不能保存决定流程走向的独立 `phase/status` 真相。影响正确性且不能从 SDB、DiskMap、NodeMap 和 user_dp 重新推导的中间事实必须进入所属领域元数据；否则切主后会出现对象状态与 Operation 状态竞争权威的问题。

业务因果关系单独建模为 Event、Operation Context、Task Attempt 和 StateTransition 构成的 DAG，用于实时 TUI 和历史追溯；它不等同于日志或普通 Trace 树。

## 字段权威表

| 数据 | 权威来源 | Monitor 内存角色 |
| --- | --- | --- |
| Pool UUID、名称、配置、阈值 | SDB | 加载并提供决策上下文 |
| 普通盘所属 Pool | SDB 中的业务决策；物理资源由 Monitor/DiskMap 管理 | 校验并构造 MemberDisk |
| MemberDisk 身份映射、Pool/Tier、介质、容量、故障域 | SDB 中持久化的 `MemberDisk` 决策字段；物理盘基础事实由 DiskMap 校验 | `MemberDisk` 持有的领域实体 |
| MemberDisk 空间位图 | SDB | 分配决策的内存工作集 |
| MemberDisk 物理 UP/DOWN | DiskMap 当前观测 | 仅作为 `MemberDiskEvent` 输入和在途事件保存；不复制进 MemberDisk，冷恢复后由 DiskMap 刷新 |
| MemberDisk Shrink 意图 | SDB 中持久化的管理决策 | `shrink_requested`，直到移除完成或后续明确事件解除 |
| MemberDisk Allocation/Membership | SDB 中持久化的 `MemberDisk` 决策字段 | `MemberDisk` 中的决策字段 |
| MemberDisk 有效 IO 能力 | SDB 中持久化的 `MemberDisk` 决策字段 | `io_state` 只在对应 user_dp 动作成功后提交 |
| DOWN 起始时间 | DiskMap 事件的 `observed_at` | 随 active/pending 事件存在，不进入 MemberDisk |
| MemberDisk UA/DA/DI/UI/Removed | 由 IO 能力、Allocation、Membership 派生 | 只读运行投影，不是第二份核心元数据 |
| MemberDisk 在途 reconciliation | `MemberDiskService::run` 的局部对象槽 | 每盘保存 active 事件、pending 事件、取消令牌和 IdleWaiter，不进入 SDB 业务真相 |
| VD 类型、冗余、ChunkSize | SDB | VD 决策上下文 |
| BGMap | SDB | 逻辑介质映射工作集 |
| BGEntry 数据有效性 | SDB | 与介质事实共同计算 BG 状态 |
| Node 成员关系 | 待确认 | 构造成员集合 |
| Node 网络连通性 | Monitor/NodeMap | 仅作为 Pool 节点判断的输入 |
| Pool 在 Node 上的成员和服务状态 | Pool 元数据与 user_dp 实际状态 | 与 NodeMap 组合后构造可服务节点集合 |
| VNODE 当前实际布局 | VNODE/user_dp | 接收或查询实际视图 |
| BG、VD、Pool 健康状态 | 派生规则 | 计算并对外发布 |
| 共享缓存使用关系 | 待确认 | 构造跨 Pool 缓存视图 |
| Operation Context/里程碑 | 业务观测平面，存储位置待定 | 因果关联和进度投影，不驱动业务决策 |
| Workflow/Task Attempt/Future | 当前由 MemberDisk 私有运行循环承载 | 临时执行，不作为核心事实 |
| CausalGraph | 业务观测平面，存储位置待定 | 实时与历史影响追溯 |

DiskMap/NodeMap 通知本身不是新的权威数据副本；它们是权威全局视图发生变化后，驱动 Pool 更新和决策的事实载体。

## 健康状态传播

BG 状态由所属 VD 的冗余策略对可用 Entry 集合进行评估：

\[
Health(b)=Evaluate(Redundancy(owner(b)),\{e\in entries(b)\mid Usable(e)\})
\]

VD 状态来自最差 BG：

\[
Health(v)=Worst\{Health(b)\mid owner(b)=v\}
\]

Pool 状态来自最差 VD：

\[
Health(P)=Worst\{Health(v)\mid v\in P\}
\]

当前只确认聚合方向。健康状态枚举、严重度顺序以及不同 VD 类型是否存在特殊权重，仍需单独定义。

## 主切换后的状态原则

新主加载时：

1. 从 SDB 恢复已提交的业务决策；
2. 在 Monitor 内重建 DiskMap，重新获取物理硬盘事实；
3. 在 Monitor 内重建 NodeMap，恢复与 user_dp 的网络连通性视图；
4. 从 user_dp 获取各 Pool 的加载、服务和数据面实际状态；
5. 重新计算派生状态；
6. 再决定是否恢复、重做或终止未完成操作。

第 6 步不在当前基线中预设答案。
