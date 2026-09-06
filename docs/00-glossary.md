# 统一词汇

本词汇表是后续架构、接口和代码命名的基线。尚未确认的概念不得通过代码命名提前固化。

| 术语 | 基线定义 | 状态 |
| --- | --- | --- |
| Monitor | 中心化管控进程。Active Monitor 管理系统内所有 Pool；备 Monitor 冷备 | 已确认 |
| MDC | 一个 Pool 的核心元数据及其内存模型，不是独立进程或独立部署服务 | 已确认 |
| Pool | 存储池的业务与一致性管理边界，包含物理介质、逻辑介质和计算节点视图 | 已确认 |
| DDM / DiskMap | Monitor 内的全局硬盘管理组件，负责物理硬盘视图；本基线统一称为 DiskMap | 已确认 |
| Physical Disk | DiskMap 管理的物理硬盘 | 已确认 |
| MemberDisk | 物理硬盘加入 Pool 后形成的逻辑介质成员 | 已确认 |
| Tier | 按介质类型组织 MemberDisk，并在其中建立 Node、Rack、磁盘柜等故障域拓扑 | 已确认 |
| Failure Domain | 冗余布局需要规避的共同故障边界，例如 Node、Rack、磁盘柜 | 已确认 |
| BLK | MemberDisk 上的空间分配单元。通常为 1 GiB；硬盘大于 8 TiB 时为 2 GiB | 已确认 |
| Partition | BLK 位图的持久化组织和竞争单元；一个 Partition 为最多 1024 块盘分别记录 112 bit | 已确认 |
| VD | Virtual Disk，面向上层业务提供逻辑空间的介质对象 | 已确认 |
| BG | BLK_GROUP，VD 的 Chunk 和空间分配单元，由满足冗余与故障域要求的多个 BLK 组成 | 已确认 |
| BGEntry | BG 中对一个 MemberDisk/BLK 的映射，并保存该 Entry 的数据有效性 | 已确认 |
| Node | 可运行 user_dp 的计算节点。它是否属于某个 Pool、该 Pool 是否已加载并可服务，需要由 Pool 级状态表达 | 已确认 |
| NodeMap | Monitor 内的全局节点连通性组件，只表示 Monitor 与 user_dp 的网络是否畅通 | 已确认 |
| PoolNodeState | 某个 Pool 在某个 Node 上的成员关系和服务状态，与 NodeMap 连通性分开建模 | 已确认 |
| Topology Notification | DiskMap 或 NodeMap 将外部拓扑事实标准化后发送给受影响 Pool 的内部通知 | 已确认 |
| user_dp | 数据面业务进程，负责加载 Pool、提供 IO 和执行实际数据操作 | 已确认 |
| VNODE | 计算资源分片与 DHT 寻址使用的虚拟节点概念 | 已确认 |
| SDB | 分布式持久化底座，保存 Pool 的重要业务决策元数据 | 已确认 |
| 共享缓存层 | 允许缓存介质跨多个 Pool 使用的特殊资源层，不遵守普通盘独占 Pool 的规则 | 已确认 |
| 决策状态 | 由 Monitor 作出并以 SDB 成功落地为准的业务状态 | 已确认 |
| 外部事实 | 来自硬件或 user_dp 的实际状态，不能由 SDB 中的历史值替代 | 已确认 |
| 派生状态 | 由决策状态和外部事实计算得到的状态，例如 BG、VD、Pool 健康状态 | 已确认 |
| 领域工作流 | 定义在所属领域 Service 上、以自然 `async fn` 表达的业务过程；不拥有第二套业务状态 | 已确认 |
| Domain Owner | 对一类核心元数据拥有唯一修改权限，并通过业务能力、只读快照和领域事件与外部协作的领域 | 已确认 |
| Operation Context | 一次业务影响的稳定因果身份，携带 owner、scope、cause、取消作用域和 Trace；不是业务状态机 | 已确认 |
| Task Attempt | Runtime 对一个异步单元的一次执行尝试；可以失败、取消或因切主消失后重新创建 | 已确认 |
| Future | Rust 中由运行时 poll 的临时代码对象，不是业务身份或核心事实来源 | 已确认 |
| Managed Task | 业务方法按需提交的对象级执行；Runtime 按 `(ObjectKey, TaskKind)` 启动、合并同类或协作替换异类 | 已确认 |
| Domain Step | 一条 `start_state + event -> action -> finish_state` 转换；直接 `await` 一个 `async fn` action，完成后验证权威结束状态 | 已确认 |
| Service Runtime | 承载控制/业务通道、事件提交、Step Future、Task、生命周期和观测的通用服务容器 | 候选 |
| Causal Graph | 连接 Event、Operation Context、Task Attempt 和状态转换的业务因果有向图 | 已确认 |
| Local Commit Point | 某个局部效果一旦正式提交便不再回滚的边界；不等价于整个父 Workflow 都不可取消 | 已确认 |

## 易混淆概念

### MDC 与 Monitor

Monitor 是运行进程和管控执行者；MDC 是某个 Pool 的核心元数据模型。不能将 MDC 描述为可独立选主的进程级服务。

### Physical Disk、DiskMap 与 MemberDisk

Physical Disk 由 Monitor 内的 DiskMap 管理；MemberDisk 属于 Pool 的领域世界。Pool 不接管 DiskMap 的全部硬盘管理职责，只管理加入本 Pool 后形成的逻辑成员。

### NodeMap 与 PoolNodeState

NodeMap 的 `Connected` 只证明 Monitor 与 user_dp 的网络通道当前畅通，不能证明某个 Pool 已经在该 Node 上加载、Ready 或提供服务：

\[
NodeMap.Connected(n)\centernot\Rightarrow PoolServing(p,n)
\]

Pool 在 Node 上的成员和服务状态必须由 Pool 模块单独管理或从 user_dp 查询确认。

### 原始拓扑事件与 Pool 通知

硬件或网络的原始变化先由全局 Map 消化：

```text
硬盘事实 -> DiskMap -> Pool
节点网络事实 -> NodeMap -> Pool
```

Pool 只处理 Map 提供的标准化拓扑通知，不直接依赖外部事件格式。

### BG 状态与 BGEntry 状态

BGEntry 同时受到介质可访问性和数据有效性的影响。BG 状态不是简单复制最差 Entry 状态，而是由 VD 冗余策略对可用 Entry 集合进行计算。

### 领域服务与微服务

当前讨论的是 Monitor 进程内的领域服务边界。它们可以使用消息通信和独立运行单元，但不等同于独立部署、独立数据库的传统微服务。

### 元数据私有与业务可见

领域核心元数据只能由 Domain Owner 修改。外部不能取得可变引用，但可以通过以下方式协作：

```text
Command/Call：请求领域能力
Query/Snapshot：读取不可变业务视图
DomainEvent：订阅状态变化
```

“不可修改”不等于“不可观察”。

### Event、Workflow、Operation Context、Task Attempt 与 Future

```text
Event：发生了什么
Workflow：自然async业务代码如何编排领域能力
Operation Context：这次业务影响为何发生、属于谁、如何关联
Task Attempt：Runtime本次如何驱动一个异步单元
Future：Task Attempt当前被poll的代码对象
```

Operation Context 不定义 `phase/status` 业务状态机。业务真相在领域对象中；Operation 的运行、完成和进度只是由追加式观测记录投影出来。查询和被状态机直接吸收的重复事件可以不创建可观测 Task。

### 工作流取消与局部不可逆

父 Workflow 可取消，不代表所有已经提交的局部效果必须回滚。例如硬盘排空停止时，已经正式提交的 BG remap 保持不变，尚未开始的 BG 不再调度，在途 BG 到达安全点后返回稳定结果。这是前向恢复，不是事务回滚。
