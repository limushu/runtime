# PoolManager 当前实现

PoolManager 是可克隆的句柄目录；每个 Pool 保留自己的 TaskCenter、领域服务与生命周期。
目录锁仅保护短暂的内存索引操作，不跨异步等待。

## 身份和入口

- `create(CreatePool)` 创建本地 Pool；`create_pool(PoolId)` 是名称默认等于 ID 的便捷入口。
  当前创建只启动内存运行结构，没有外部初始化步骤，因此不生成虚假的初始化 Workflow。
- `with_task_store(...).activate(PoolRecovery)` 是异步持久化启动入口，接主时恢复领域视图和任务。
  配置存储后同步 create 旁路会拒绝。激活失败不发布句柄，降主用 demote 等待停稳。
- `get(PoolSelector)` 支持类型化的 ID、UUID、名称；名称区分大小写。
- `list()` 返回描述符；`PoolHandle::rename(name, expected_revision)` 原子更新名称索引和修订号。
- UUID 与 ID 不复用；删除成功后名称可以复用。旧句柄仍绑定原来的 UUID 和实例。

## Pool 拥有业务

通过 PoolHandle 创建 Tier、显式加入 Node、加入 Disk、创建 Volume，并删除相应子对象。
Node 必须先 attach，才能观察连通事实；连通与 Pool 已加载是不同状态。
磁盘加入前验证 Tier，跨池物理磁盘归属由目录中的预约保护。
外部效果不确定时保留归属，不能让另一池接管仍可能在使用的盘。

删除按依赖顺序进行：Volume 删除确认后释放其分配，磁盘只能在无引用、无分配时移除，
Tier 和 Node 仍有成员盘时不能删除，Pool 非空时不能删除。
释放凭据绑定原 Volume，旧删除重试不能释放后来分配给其他 Volume 的空间。
这不代表后台重建、孤立分配对账或任意在用盘移除已经实现。
新增的数据面清理端口默认拒绝未实现操作，模拟适配器可用于验证。

重命名、子对象操作和生命周期操作通过本池 Workflow 推进，实际读写步骤进入 Activity 审计。
Pool 删除由池拥有的完成逻辑推进；调用方放弃等待不会丢下已接受的删除操作。
已知失败恢复可用门禁，不确定结果保留 `ReconcileRequired` 状态。

## 公平广播和汇聚

`dispatch(TopologyEvent)` 快照目标池，轮换每轮起始池，向所有目标执行非阻塞投递。
某池队列满不会阻止其他池获得处理机会。各池自行判断相关性，无关事件不创建 Task。
尚未确认的成员归属返回暂不可用，不能当作明确不相关。

事件携带事件 ID、源 epoch 和序号；池按对象检查旧版本、同版本冲突及重复投递。
调用方必须提供真实的源版本关系；生成新的事件 ID 不代表同一源事实已经更新。

`gather` 可等待 Decision 或 Completion，并返回完整逐池结果。
Accepted 只表示任务已接受；完成、失败、队列满、不相关、尚未回复分别保留。
超时或放弃汇聚不取消已接受的任务。重试仅重新投递尚未接受且可重试的目标，
不向已经运行的目标重复发送写操作。汇聚不提供跨池事务。

## 生命周期和历史

`unload` 是逻辑卸载：关闭新业务准入，等待此前操作，再确认节点卸载；保留本地服务与句柄。
`load` 要求覆盖全部已加入节点的完整、有效版本事实，恢复节点加载与可用盘状态后再开放准入。
`shutdown` 结束本地运行宿主，不能通过 load 重启，也不等同于外部卸载已经确认。

跨池 TaskRef 包含 Pool UUID、实例 UUID、TaskId；观测和控制均校验作用域。
Manager 汇总本池 Task 快照，选中任务可以订阅 observer；历史与 Activity 均在内存中有界保留。
删除池的运行历史另按 `Config::task_history` 限制保留的池数量；已有 observer 不受目录淘汰影响。

## 当前边界

目录、身份墓碑、事件去重及审计历史都没有跨进程持久化。
Task checkpoint 已接入正式 Pool 的恢复入口；尚无内置 PoolCatalog、选主算法、持久 RequestId 或完整状态事件日志。
新主仍需从外部目录/拓扑权威来源提供 PoolRecovery，不能只靠任务日志恢复所有业务状态。
运行说明及生产接入边界见 [主备切换](leader-handoff.md)。
[设计草案](pool-manager-design.md) 中更完整的持久化契约仍是后续目标，不能当作现有 API。
[TUI 建议](task-center-tui.md) 说明如何呈现当前接口及其限制。
