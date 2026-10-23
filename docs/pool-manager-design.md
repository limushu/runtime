# PoolManager 句柄目录与 Pool 自管理设计

本文保留目标设计，包括尚未实现的持久化及请求契约；当前 API 与限制见 [实现说明](pool-manager.md)。
句柄目录、多索引、Pool 子对象与生命周期、公平扇出、逐池汇聚已经落地；
文中的 PoolCatalog、RequestId 等目标仍未实现，不应将全部示例当作当前 API。
采用轻量句柄目录：PoolManager 管理 Pool 的登记与查找，Pool 管理自身生命周期与子对象。

## 职责划分

```text
API / TUI
  ├─ PoolManager：按 ID / UUID / name 获取 PoolHandle
  │    └─ Registry：身份、别名唯一性、句柄登记
  └─ PoolHandle：创建/删除子对象、加载/卸载/删除自身、任务观测
       └─ Pool：生命周期与组合根
            ├─ TaskCenter：本池准入、冲突、排队、执行
            └─ Node / Tier / Disk / VD：各自的业务能力

DiskMap / NodeMap -> Manager 公平扇出 -> 每个 Pool 判断相关性并处理
```

PoolManager 不需要自己的事件循环、命令邮箱、WorkflowDriver 或 TaskCenter。
第一版使用带短临界区的共享 Registry，PoolManager 本身可克隆。
取出绑定句柄后释放 Registry 锁，再调用 Pool 的异步方法，锁不跨 await。

PoolManager 可提供 create、dispatch、gather、shutdown_all 等便捷方法，方法只转调 Pool 或组合调用结果。
Pool 的创建、加载、删除以及子对象变更，均由 Pool 拥有的操作推进并观测。
不在 Manager 上复制一套 add_disk、create_volume 等业务接口。

Registry 负责跨池唯一性，Pool 负责单池业务不变量。二者都不持有对方的可变领域数据。

## 身份与索引

```rust,ignore
struct PoolDescriptor {
    id: PoolId,
    uuid: PoolUuid,
    name: PoolName,
    revision: u64,
    lifecycle: PoolLifecycle,
}

enum PoolSelector { Id(PoolId), Uuid(PoolUuid), Name(PoolName) }

struct PoolRegistry {
    by_id: BTreeMap<PoolId, PoolEntry>, // 一个对象、一份句柄
    by_uuid: BTreeMap<PoolUuid, PoolId>,
    by_name: BTreeMap<PoolName, PoolId>,
    // 另存创建/重命名的预约、幂等记录以及已删除身份的墓碑。
}
```

- PoolId 保持现有不透明字符串表示，创建后不可变；与名称分离。
- PoolUuid 是专门校验的 128-bit UUID，普通创建由服务生成。
- ID、UUID 均唯一且不复用；name 在同一管理命名空间内唯一，可重命名。
- name 为 1..128 UTF-8 字节，区分大小写，禁止首尾空白和控制字符。
  第一版按字节比较，不做隐式 trim、大小写折叠或 Unicode 等价转换。
- Selector 显式指定索引类型，CLI/HTTP 不根据字符串外形猜测。
- 三个索引的预约、发布、替换和删除在同一 Registry 临界区原子完成。

get() 返回绑定 `(PoolId, PoolUuid)` 的稳定 PoolHandle，不把名称保存在后续路由路径中。
重命名不影响已有句柄；删除确认后 name 可以复用，旧句柄仍指向旧池并返回 Deleted。
实例相关任务控制还需绑定 instance_epoch，避免卸载重载后误命中同号任务。

PoolEntry 可以登记 Creating/Unloaded/Deleting 状态，get() 本身不代表池已经可接受业务。
目录查询可返回状态描述；PoolHandle 在自己的准入入口判断生命周期。

## 对外接口

```rust,ignore
impl PoolManager {
    fn get(&self, pool: PoolSelector) -> Result<PoolHandle, ApiError>;
    fn list(&self, query: PoolQuery) -> Result<Page<PoolDescriptor>, ApiError>;

    // 转调 Pool::create，业务流程和任务所有权属于新 Pool。
    async fn create(&self, req: CreatePool) -> Result<Submitted<PoolHandle>, ApiError>;
    // 对本轮句柄快照公平投递，由各 Pool 决定是否相关。
    async fn dispatch(&self, event: TopologyEvent) -> Result<DispatchReceipt, ApiError>;
    // 汇总各池已有的回复/任务结果，不创建另一层业务执行器。
    async fn gather(&self, receipt: &DispatchReceipt, wait: WaitFor, deadline: Instant) -> FanoutReport;
    async fn shutdown_all(&self) -> Result<ShutdownReport, ApiError>;
}

impl PoolHandle {
    fn descriptor(&self) -> PoolDescriptor;
    async fn rename(&self, req: RenamePool) -> Result<Submitted<PoolDescriptor>, ApiError>;
    async fn load(&self, req: LoadPool) -> Result<Submitted<()>, ApiError>;
    async fn unload(&self, req: UnloadPool) -> Result<Submitted<()>, ApiError>;
    async fn delete(&self, req: DeletePool) -> Result<Submitted<()>, ApiError>;

    async fn attach_node(&self, req: AttachNode) -> Result<Submitted<PoolNode>, ApiError>;
    async fn detach_node(&self, req: DetachNode) -> Result<Submitted<()>, ApiError>;
    async fn create_tier(&self, req: CreateTier) -> Result<Submitted<Tier>, ApiError>;
    async fn delete_tier(&self, req: DeleteTier) -> Result<Submitted<()>, ApiError>;
    async fn add_disk(&self, req: AddDisk) -> Result<Submitted<MemberDisk>, ApiError>;
    async fn remove_disk(&self, req: RemoveDisk) -> Result<Submitted<()>, ApiError>;
    async fn create_volume(&self, req: CreateVolume) -> Result<Submitted<VirtualDisk>, ApiError>;
    async fn delete_volume(&self, req: DeleteVolume) -> Result<Submitted<()>, ApiError>;

    // 仅尝试放入本池有界 inbox；业务相关性随后由本池判断。
    fn try_notify(&self, event: Arc<TopologyEvent>) -> Result<PoolEventReceipt, AdmissionError>;
    async fn on_event(&self, event: TopologyEvent) -> Result<EventDisposition, ApiError>;
    // 保留现有 tasks / task / observe_task / suspend / resume / cancel。
}
```

典型调用是 `manager.get(selector)?.create_volume(request).await`。
不需要让每次池内请求再经过 Manager 转发；所有 PoolHandle 克隆共享该池的生命周期门禁。
Submitted 区分“已接收”与“已完成”，操作在 Pool 内持有，调用方丢弃等待 Future 不丢失操作。

变更请求携带 RequestId；版本敏感更改携带 expected_revision。
先查询幂等记录，再为新请求解析 Selector 并绑定身份；同 RequestId/同内容返回原操作，
内容不同明确冲突。幂等保存期限独立于有界 Task 历史，重试不因名称复用跳转到新池。

## 创建、重命名和删除由 Pool 执行

Pool::create 内部先建立有身份和生命周期控制的 Pool，再启动初始化流程：

1. 通过 Registry 的受限登记能力预约 ID、UUID、name，并登记 Creating 句柄。
2. 由 Pool 自己初始化元数据、领域服务及执行组件，推进本池 bootstrap Workflow。
3. 确认创建提交与初始化成功后进入 Active，完成创建操作。

失败由 Pool 进行清理或进入 ReconcileRequired，并通过 Registry 撤销可安全撤销的预约。
Creating 句柄可观测但不接受普通业务；首次创建任务也属于该池，Manager 没有管理任务执行器。
Registry 只提供 reserve/publish/rename/unregister 等窄接口，不调用领域动作。

rename 同样由 Pool 推进：预约新名称、提交元数据 CAS、原子替换名称索引。
已知提交失败可撤销新预约；Uncertain 保留相关预约，等待核对，不能错误释放名称。

```text
Creating -> Active -> Unloading -> Unloaded -> Loading -> Active
Active / Unloaded -> Deleting -> Deleted
提交或外部效果不确定 -> ReconcileRequired
```

这是管理生命周期，不替代由子对象派生的业务健康状态。
Pool 的卸载/删除操作在自己的协调边界关闭普通准入，并等待已接受的业务停稳。
已有 PoolHandle 的调用同样受门禁约束，不依赖从 Manager 删除索引来阻止新请求。

第一版 delete 使用 Restrict：有活跃子对象、引用、在途预约、未完成清理或隔离时返回原因。
通过屏障后重新校验删除条件，完成外部清理并提交 Deleted 墓碑，最后通知 Registry 摘除活跃句柄。
ID/UUID 墓碑保留，name 在确认删除后释放。卸载只关闭实例，保留目录和资源归属。

实现须处理终结自身的顺序：删除流程等待其他业务停稳，不能调用会等待该删除任务本身结束的
通用 shutdown；停止所需服务并确认业务效果后发布删除终态，再由根宿主完成退出。
注册表摘除不能中途 abort 删除流程。保留的只读 observer 在宿主退出后仍可读取历史。

shutdown_all 只并发请求各 Pool 的正常关闭并汇总结果；关闭所有权不等同于复制业务流程。
Manager 的 Registry 保有活跃句柄和实例所有权，unregister 是受限的终态登记操作，
不能作为用户可调用的删除接口。强制 Drop 仍只能作本地回收，不证明外部 IO 已停稳。

## 子对象和全局资源约束

默认子对象覆盖节点成员、Tier、MemberDisk 和 Volume；BG/BGEntry 随 Volume 管理。
所有创建/删除均经过 Pool 的 TaskCenter 和所属领域服务，声明完整冲突集合并做版本校验。

| 对象 | 创建 | 删除完成条件 |
| --- | --- | --- |
| PoolNode | 显式 attach，随后同步拓扑、按需 load | 无成员盘等引用，确认本池已从节点卸载 |
| Tier | 创建本池配置 | 无成员盘和策略引用 |
| MemberDisk | 预约物理盘归属后建立成员 | 禁止新分配、处理引用、确认停稳与成员退出 |
| Volume | 分配、初始化并提交发布 | 撤销访问、等待 IO 停稳、解除引用，再按凭据回收空间 |

attach/detach 与 add/remove 管理池成员关系，不删除全局物理节点/硬盘。
observe_node 不再隐式创建成员；调用方先 attach。删除不能用移除内存 Map 条目代替业务完成。
BLK 释放必须核对分配所属 Volume 及 allocation generation，避免旧释放请求误释放新分配。
子对象第一版在 Pool UUID 下不复用身份；需要重加入时另行显式引入成员代次。

跨池约束仍需一份共享事实：普通物理盘不能同时属于两个池。
使用共享 DiskOwnershipRegistry 提供原子 reserve/confirm/release 和归属查询，Pool 在成员操作中调用。
它可以与句柄 Registry 放在同一模块，是普通共享数据结构及存储端口，不新增独立宿主或调度器。
池内领域逻辑负责证明何时可释放，全局 Registry 负责确保释放者仍是原归属和原预约代次。

## 公平扇出，Pool 自己判断相关性

标准化全局拓扑通知面向本轮所有已登记 PoolHandle 扇出，Manager 不维护 Node -> Pools
相关性索引，也不根据磁盘归属预先过滤这些通知。DiskOwnershipRegistry 仅维护跨池独占约束。
明确指定目标池的创建 Volume、删除成员等命令继续通过绑定 PoolHandle 调用。

dispatch 在短临界区内取得包含 PoolUuid/实例代次的句柄快照，随后释放锁。
同一通知使用 Arc 共享，向每个池独立的有界 inbox 调用 try_notify，完成所有投递尝试后
返回逐目标 DispatchReceipt。Mailbox 入队成功仅表示 Queued，尚不表示相关或已产生业务 Task。

公平性具体约定：

- 每轮每池一次投递机会，轮换起始位置；大列表按固定预算分批并让出执行权。
- 不串行等待某个池的接收确认或业务完成；单池满队列立即记录 Backpressured，继续其他池。
- 无需为每个池 spawn 一个无限等待的任务，避免全局并发槽被一组慢池长期占满。
- 只重试尚未接受的目标，按轮次重新给予机会；不存在永久优先的池。
- 每池独立设置 inbox 容量与单轮处理预算，保留控制、生命周期与在途 Workflow 的推进机会。

公平表示分发机会及互不阻塞，不保证所有池在同一时刻执行或成功。
达到输入/重试记录上限时明确返回背压，不能宣称有限内存在任意负载下保证最终送达。

每个 Pool 在本地应用入口依据已确认成员、在途预约、退出中的关系与隔离状态判断相关性。
确定不相关返回 NotRelevant，不创建业务 Workflow；有关联则经本池 TaskCenter 正式准入，
返回 Accepted(TaskRef) 或明确的拒绝/待同步状态。相关性判断只是准入判断，执行仍需重新校验权威状态。
普通准入关闭或池尚未加载时不能把无法判断伪装成 NotRelevant，应返回 PendingSync/Unavailable。

Pool 负责基于 EventId、来源对象版本/epoch 的去重、顺序和同版本异值检查，重复事件复用原回复/任务。
并发 dispatch 不能假设入队顺序等于来源顺序；Pool 的版本处理契约决定拒绝旧值或等待缺失事实。
attach 先登记本地成员预约，再同步来源快照和版本交接；新的 Pool 不依赖收到注册前的历史广播。
成员关系变动后，延迟通知不得基于旧关系创建副作用，须校验本地成员代次与来源版本。

DispatchReceipt 固定本轮目标、逐池入队/拒绝结果及投递引用。重试使用同一 EventId 和原目标集合，
已经接受的目标不重复创建工作；新注册池通过来源快照补齐事实。目标池删除或实例更换时
返回 Deleted/StaleInstance，不根据名称重新寻找接收者。NotRelevant 也需保留在约定期限内的去重回复。
普通准入关闭后事实由 Pool 按生命周期进入待同步状态，不旁路门禁启动新业务。
进程内队列不保证重启后不丢失；可靠来源重试、快照同步或持久 inbox/outbox 必须明确选择。

## 在 PoolManager 汇聚结果

Manager 的 gather 是普通异步组合函数：根据 receipt 等待各 Pool 的回复及对应 Task 终态，
复用 Pool 提供的观察/结果能力，不增加 Manager 业务 Workflow 或重复任务状态机。
Pool 保有已接受工作的所有权，调用方取消 gather 不取消各池业务；后续可按 receipt/TaskRef 继续查询。

明确区分三层结果：Queued（已投递）、NotRelevant/Accepted/Rejected（已判断）、
Completed/Failed/Uncertain（相关业务已停稳）。拓扑通知的业务结果使用结构化结果摘要；
跨池查询使用各接口自己的类型化回复，不把任意 Run<T> 输出塞入无类型的结果袋。

WaitFor 第一版只提供 Decision 和 Completion：前者等待每个目标明确回复是否相关及准入情况，
后者继续等待所有相关任务终态。每种对外接口声明采用哪一种，不把入队成功报告成业务完成。
按完成顺序收集回复，并允许界面增量显示，最终报告保留全部逐池状态及已知结果。

只有每个目标都明确 NotRelevant 或达到所要求的成功阶段，才满足该阶段的全部成功条件。
Backpressured、Unavailable、PendingSync、尚未回复等状态均不能当作无关池忽略。
零相关池返回 NoRelevantTargets。部分失败返回结构化部分结果，不丢弃其他池已成功的结果。
截止时间到达返回未完成清单；等待超时不等于业务失败或零副作用，不自动取消/重试已接收动作。

汇聚不提供跨池原子性。若业务要求先 A 后 B、失败补偿或跨池一致性，应显式实现专门的上层业务流程；
不能靠“全部发出然后 await”获得事务语义。通知广播只代表给每个池判断机会，不自动授权每个池执行写操作。

## 存储、观测与落地范围

Registry 是已确认目录及预约的缓存，持久身份、名称唯一性和墓碑由新增 PoolCatalog 端口约束。
Pool 负责创建/删除的提交顺序和对账；Catalog 负责原子唯一性和版本 CAS。
已有 MetadataStore 继续管理池内领域提交。不能持 Registry 锁等待 Catalog IO，
应先建立预约，再提交，最后按预约 token 发布；Uncertain 保留预约。
重新加载已有 Pool 必须恢复元数据和成员归属、同步事实，不将同名 create 当作恢复。

TUI 先枚举 Manager 的 PoolDescriptor，再从各 PoolHandle 订阅任务和 Activity。
任务引用包含 `(PoolUuid, TaskId)`，创建/删除 Pool 的流程同样归属于该池。
Manager 可汇总查询结果，但不拥有另一份任务状态机。
卸载期间保留逻辑 PoolHandle；删除后保留有界只读历史引用，与活跃目录分开淘汰。
当前 Task/Activity 历史仍是进程内有界观测，跨重启查询需另外持久化。

代码改造优先顺序：

1. `manager/{identity,registry}.rs`：三个索引、绑定句柄、唯一性预约。
2. `pool.rs`：Pool 自管理生命周期、bootstrap、门禁及实例代次；Manager create 仅做转调。
3. `manager/{fanout,aggregation}.rs`：公平扇出和结果组合；Pool 判断相关性。共享盘归属仍独立校验。
4. Pool 内补齐 Tier、节点退出、成员盘退出、Volume 删除和空间回收的领域及端口能力。
5. 多池观测聚合、删除后的历史引用与已有 TUI 接入。

当前已具备独立 PoolUuid/PoolName、Tier 实体、逻辑卸载及确认 Volume 删除后的分配释放。
后台重建、孤立分配对账、持久目录与崩溃恢复仍未实现；缺少外部能力时明确拒绝，
测试使用模拟外部适配器验证契约，不伪造删除成功。

验收重点包括三个索引一致、重命名/名称复用不影响旧句柄、同盘跨池预约竞争、成员与拓扑通知并发、
一个池卡住时其他池仍收到通知、队列满后继续扇出、无关池不创建 Task、部分投递重试、
分阶段汇聚、超时保留在途结果、零相关池、重试不重发已接受动作、
已有句柄与删除并发、删除任务不等待自身、失败后不提前释放名称或资源、
多池/多代实例任务不串线，以及终态 Activity 和删除后历史可查询。
实现按 workspace 格式、所有 targets 测试、文档测试和 Clippy 验证。
