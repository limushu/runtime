# 不变量与一致性原则

本文件只记录跨流程成立的安全规则，不展开某个具体工作流。

## 可靠性优先

当前基线采用以下原则：

> 宁可降低并发、暂时损失容量或留下可恢复的资源泄露，也不能造成重复分配、有效数据被覆盖或错误元数据成为正式决策。

首阶段允许 Tier 内空间修改串行化；未来并行化只能缩小串行范围，不能改变安全语义。

## Pool 隔离

除共享缓存层外，普通核心对象必须属于唯一 Pool：

\[
PoolOf(MemberDisk)=PoolOf(VD)=PoolOf(BG)
\]

BGEntry 不能跨 Pool 引用 MemberDisk：

\[
e\in BG(P)\Rightarrow MemberDisk(e)\in P
\]

## 核心元数据唯一修改者

对任意核心元数据 \(x\)：

\[
Mutate(x,d)\Rightarrow d=Owner(x)
\]

领域可以暴露不可变快照、查询和事件，但不能暴露共享可变引用。跨领域修改必须调用所有者提供的业务能力。

## 普通物理盘独占

\[
owner(d)=p_1\land owner(d)=p_2\Rightarrow p_1=p_2
\]

共享缓存盘不满足该不变量，必须通过独立的共享资源模型表达，不能通过关闭普通盘校验来实现。

## 引用完整性

每个已提交 BGEntry 必须引用存在的 MemberDisk 和合法 BLK：

\[
Committed(e)\Rightarrow Exists(MemberDisk(e))
\]

\[
Committed(e)\Rightarrow ValidBlk(MemberDisk(e),BlkId(e))
\]

## 空间安全

令：

- \(A\)：Tier 位图中已分配的 BLK 集合；
- \(R\)：所有已提交 BGMap 引用的 BLK 集合。

必须始终满足：

\[
R\subseteq A
\]

允许：

\[
A-R\neq\varnothing
\]

这表示存在空间泄露。

不允许：

\[
R-A\neq\varnothing
\]

这表示 BG 引用了已经可以再次分配的 BLK，存在数据覆盖风险。

由于 SDB 不提供覆盖 Tier 与 VD 多个 Key 的业务事务，跨域元数据修改必须遵守有序持久化：

```text
建立引用：先确认BLK已分配，再建立BG引用
解除引用：先确认BG引用已解除，再释放BLK
```

该原则是系统级基线；具体请求、失败处理和恢复流程后续单独设计。

## 唯一分配

稳定状态下，不同有效 BGEntry 不能引用同一个 BLK：

\[
e_1\neq e_2\land ValidReference(e_1)\land ValidReference(e_2)
\Rightarrow Blk(e_1)\neq Blk(e_2)
\]

业务层必须为并发空间分配提供串行化、版本校验或等价机制。不能只依赖开发者约定。

## 布局合法性

一个 BG 的布局必须满足所属 VD 的冗余和故障域策略：

\[
ValidPlacement(
Redundancy(owner(b)),
FailureDomainPolicy(owner(b)),
entries(b),
Topology(P)
)=true
\]

硬盘故障可以使 BG 降级，但不能反向修改历史事实，使一个从未合法提交的布局变成合法。

## Entry 状态正交性

介质状态和数据状态必须分开：

\[
EntryState=MediaState\times DataState
\]

磁盘重新 UP 不意味着数据自动恢复 VALID：

\[
DOWN\rightarrow UP\centernot\Rightarrow INVALID\rightarrow VALID
\]

## 健康状态派生

\[
BGEntry\rightarrow BG\rightarrow VD\rightarrow Pool
\]

VD 和 Pool 状态不得脱离下层事实被任意独立设置。缓存派生状态时必须能够重新计算并检测偏差。

## 外部事实优先

切主或恢复后：

- DiskMap 中由硬件观测得到的当前物理状态优先于 SDB 中的历史 UP/DOWN；
- user_dp 报告的当前加载和运行状态优先于 SDB 中的历史运行记录；
- 重要配置、分配和 BGMap 决策仍以 SDB 已提交结果为准。

## Node 连通性与 Pool 服务状态分离

NodeMap 连通只表示 Monitor 与 user_dp 的网络畅通，不足以证明 Pool 已加载或可服务：

\[
Connected(n)\centernot\Rightarrow PoolServing(p,n)
\]

Pool 的可服务节点视图必须同时考虑成员关系、NodeMap 连通性和 Pool 在 user_dp 上的实际服务状态。

## 拓扑事实必须经过全局 Map

Pool 不直接消费硬件或网络探测的原始事件：

\[
DiskEvent_{raw}\rightarrow DiskMap\rightarrow PoolEvent
\]

\[
NodeEvent_{raw}\rightarrow NodeMap\rightarrow PoolEvent
\]

DiskMap/NodeMap 负责全局对象身份、当前事实和通知标准化；Pool 负责本 Pool 内的状态转换、策略和工作流。路由层不能替 Pool 作出业务决策。

## 内存可重建

Monitor 内存结构不能成为唯一的核心事实来源：

\[
P^{mem}=Materialize(P^{sdb},Observations)
\]

未来若某类在途操作无法由这些输入安全恢复，就必须显式增加可持久化的操作记录，而不是依赖未完成 Future 的内存状态。

## 领域状态、Operation Context 与 Task Attempt 分离

领域对象状态是业务真相；Operation Context 是因果身份；Task Attempt 是临时执行实例：

\[
DomainState\rightarrow Workflow,
\qquad OperationContext\rightarrow \{TaskAttempt_0,TaskAttempt_1,\ldots\}
\]

Monitor 切主、Task 失败或执行重试不能改变 Operation Context 的因果身份。Operation 不得维护与领域对象竞争权威的 `phase/status` 状态机。影响正确性且无法重新推导的中间事实必须持久化到所属领域；Future、Workflow 栈、Task 和观测记录都不得成为唯一事实来源。

## 业务取消优先于强制终止

取消一个 Workflow 表示改变业务目标，不等于立即 drop Future。领域所有者必须：

1. 根据当前对象状态和已经提交的局部效果决定停止、收敛后停止、继续或拒绝；
2. 向下游调用传播取消意图；
3. 等待下游进入稳定结果；
4. 再把所属领域对象推进到新的稳态。

局部效果越过提交点后不回滚，但这不自动禁止父 Workflow 停止。父 Workflow 可以保留已提交成果、停止剩余工作并执行前向恢复。只有领域对象进入真正终局后，反向需求才需要新的业务流程。强制终止只属于服务关闭或故障隔离机制，之后必须通过领域状态与 Reconcile 恢复。

## 业务策略声明、框架原子执行

领域状态机声明对象下一状态与目标 Workflow。Runtime 依据同一对象的当前目标原子推导 `Start / Join / CancelThenStart`，并维护在途索引、等待者和并发配额；普通对象写操作可以显式选择顺序排队。

框架不猜测业务状态转移，但可以统一执行固定的 `ensure` 语义：同 Workflow 合并、不同 Workflow 协作替换。业务代码不能绕过 Service Runtime 直接启动受管 Workflow；否则同一对象的互斥、合并和取消保证无法成立。

`(ObjectKey, WorkflowKind)` 必须完整标识一个可共享的收敛目标。若两个请求虽然 Kind 相同，但参数或预期结果不可共享，它们必须使用不同的 Kind/Key 或选择顺序 `Enqueue`，不得被错误合并。

## 共享意图的生命期

多个调用合并到同一对象意图时，Workflow 的生命期属于对象意图槽位，不属于第一个调用者。调用者只是结果订阅者：

- 任一订阅者离开不能取消仍被其他订阅者等待的 Workflow；
- 所有订阅者都离开时，Runtime 按请求声明的 orphan policy 决定协作取消或继续收敛；
- 新意图替换旧意图时，由隐藏 ActorCell 请求旧 Workflow 取消，并等待其稳定退出后再启动替代 Workflow。

普通 Workflow 不显式调用 `stable_boundary()`，也不在每一步轮询取消。显式 Service Client 必须等待下游返回稳定结果，并在控制权回到父 Workflow 前统一传播取消；领域执行器只在自身不可再细分的原子工作单元边界解释取消，例如完成当前 BG 后停止补充新 BG。

若一个所属领域的决策已经成功写入 SDB，则内存必须接纳该结果，不能因同时到达的外部观测而拒绝提交并形成 `SDB != memory`。外部观测可以请求替换当前意图，但替代 Workflow 只能在旧意图稳定退出后继续前向收敛。
