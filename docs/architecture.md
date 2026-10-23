# 当前架构与依赖边界

## 组合结构

```text
PoolManager
└─ Pool（组合根 pool.rs）
   ├─ TaskCenter（一个事件循环）
   │  ├─ 冲突判断、队列、逻辑对象占用与隔离
   │  └─ 直接推进 Workflow Future（嵌入通用执行驱动，无独立 task）
   ├─ DiskService + DiskHost（独立服务宿主）
   ├─ NodeService + NodeHost（独立服务宿主）
   ├─ VdService + VdHost（独立服务宿主）
   ├─ TierService + TierHost（独立服务宿主）
   └─ BalanceService + BalanceHost（外部均衡 job 的能力与控制适配）
```

Task 是一次完整 Workflow 执行，Activity 是其步骤。硬盘上线是一个流程，Add/Open/Enable
是内部步骤。业务请求只经 PoolHandle/PoolManager，不能向 Pool 注入任意 Workflow 或策略。
Pool 没有单独的 runtime 字段，也不使用 RuntimeHandle 向另一条协调队列提交。

## 按业务组织，而不是把同一领域拆散

domains 表示“业务领域分组”，不是声称其中所有文件都是 DDD 纯领域层。

- `domains/{disk,node,vd,balance}/model.rs`：对象、不变量、状态规则，身份序列化不引入执行框架。
- `domains/{disk,node,vd,balance}/service.rs`：能力与已确认状态；组织领域校验、数据面操作、元数据提交。
  不实现 ServiceHandler、不使用 ActivityContext、不认识托管自己的 runtime、不维护对象冲突队列。
- `domains/{disk,node,vd,balance}/runtime.rs`：服务托管适配，声明步骤资源并转发能力调用；runtime 依赖 service，
  service 依赖 model，不能反向依赖。
- `domains/mod.rs`：公共身份和 DomainError；错误转换放在最外层 support.rs。
- `ports.rs` / `adapters.rs`：外部接口与模拟实现。取消/IO 是应用能力边界，不进入领域模型。
- `workflows/`：通过 ActivityClient 的类型化能力通道调用步骤，不知道具体服务宿主类型。
- `task_center/`：同一具体组件的协调状态与普通业务判断；conflicts.rs 不是插件策略系统。
- `pool.rs`：组合根，装配 TaskCenter 和领域服务。不把组合逻辑放入模型。

自动测试检查 model 不引用 Tokio/执行框架，service 不依赖 runtime，Workflow 不引用服务宿主。
当前 service/ports 使用执行层 Outcome 和取消令牌，model 返回独立 DomainError。

## 一个循环协调并执行

TaskCenter 的 select 同时处理新请求、停止请求和 driver.tick()。
WorkflowDriver 是一组可轮询的执行状态/Future，本身没有提交队列，也不会 spawn 执行宿主。
defer() 是同步登记，不是发消息给另一个 Runtime。TaskCenter 持有启动许可，冲突消除后放行；
在同一个循环内轮询多个 Workflow，因此不需要 await 整个流程才接收下一条请求。

WorkflowRuntime/RuntimeHandle 仅是基础库给无自有事件循环的示例使用的可选独立封装，
代码位于 standalone.rs，存储 Pool 不构造它。ExecutionView 只共享观察/控制状态，
不调度、不提交、不启动 task。

TaskCenter 检查全部在途冲突及容量。只有成功登记才实施取消；旧流程和 Activity 实际终态前
不释放逻辑对象。排队任务计入容量、可取消；丢弃调用者/Run 不丢失已接受执行。
不确定结果保留逻辑对象隔离；被隔离的新任务不会取消其他活动任务，被拒绝的后继不扩散隔离。

受控执行不再重复取得 Workflow 对象锁。驱动保留 Activity 资源读写保护、停稳跟踪与资源隔离，
尤其保护 VD 反向引用索引等共享结构。服务内部没有业务对象排队器。
通用独立 submit 仍提供简单对象互斥供示例/测试使用，不能绕过 Pool 正式入口混用。

完整缩容由 ShrinkDiskWorkflow 推进。一次排空 Activity 等待独立均衡 job，并转发实际进度；
暂停钩子在适配层发送控制指令并确认停稳。均衡完成后的引用与分配读取权威元数据，
防止领域缓存覆盖已提交的 remap。TaskCenter 协调 topo，均衡器不反向触发缩容状态切换。

暂停仍保留逻辑对象；排队任务只记录暂停请求，获准到达安全边界才确认 Paused。
pool.scheduling() 表示业务排队/运行/逻辑隔离；pool.execution() 表示底层执行与资源计数。
pool.task(id).conflicts 是准入时解释，不是实时等待图。

## 限制

任务历史可通过注入 TaskStore 持久化，指定用户 Task 可按业务检查点续办，见 [接入说明](task-persistence.md)。
没有确定性重放、跨主所有权接管协议、自动重试、动态扩展对象集合或真实重建；空闲无引用成员盘可以显式移除。
拓扑身份不可变；节点关联盘集合包含已预约但未提交的盘。资源声明仍较保守，同节点的部分
磁盘流程会串行，本轮不改变原有冲突范围。强制 Drop 不能证明远端 IO 已停止。
