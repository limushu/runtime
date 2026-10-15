# Storage Pool — 业务纵切面

基于相邻的 `workflow-foundation`，不依赖旧 SDK。这里开始加入真实的领域概念和可执行
业务编排，但默认适配器**仅模拟**元数据提交和数据面成功，不访问真实磁盘或 SDB。

```text
PoolManager：Pool 路由 / 生命周期 / 普通物理盘唯一归属
└─ Pool：组合与隔离边界，不是一个巨型聚合
   ├─ PoolWorkflowCoordinator：业务操作描述与 PoolConflictPolicy
   │  └─ Coordinator：通用准入机制、Workflow 推进与对象占用（基础库）
   ├─ 具名 Workflow：通过 Activity 调用领域能力
   ├─ DiskService：MemberDisk、BLK 分配
   ├─ VdService：VirtualDisk、BlockGroup、BgEntry 数据有效性
   └─ NodeService：PoolNode、连通事实、已确认加载状态
```

每个领域由基础库的独立服务宿主运行。领域只声明 Activity 资源，不实现自己的对象
排队、互斥或 Workflow 调度。短暂的内存 Mutex 只保护数据访问，不跨 await 持有。

## 分层与依赖

- `app/`：Pool 装配与业务入口、应用请求、Workflow 编排及业务准入策略。
- `domain/`：Disk、VD、Node 各自的实体、业务规则和领域服务；外部能力契约位于 `domain/ports.rs`。
- `infrastructure/`：元数据与数据面契约的适配实现。

应用层调用领域能力并使用 `workflow-foundation` 执行流程；领域层不依赖应用流程或协调策略。
领域服务当前直接实现基础库的 `DomainService` 运行契约。通用 Coordinator 仍属于基础库，
理解存储操作的 PoolWorkflowCoordinator 属于应用层。旧根模块路径保留兼容导出。

## 从哪里读

| 文件 | 职责 |
| --- | --- |
| `src/domain/{disk,node,vd}/model.rs + src/app/requests.rs` | 强类型身份、领域实体、输入、内部空间分配凭据 |
| `src/app/pool.rs` | PoolManager / Pool 装配、类型化业务入口与查询 |
| `src/app/coordination/mod.rs` | Pool 协调服务、操作描述、可独立测试的冲突策略 |
| `src/app/workflows/*.rs` | 每个业务流程独立文件，`mod.rs` 保留导出与公共辅助代码 |
| `src/domain/disk/service.rs` / `src/domain/vd/service.rs` / `src/domain/node/service.rs` | 各领域状态唯一所有者与能力 |
| `src/domain/ports.rs` | 显式 MetadataStore / DataPlane 外部边界及失败契约 |
| `src/infrastructure/adapters.rs` | 带版本校验的内存元数据、无真实 IO 的模拟数据面 |
| `examples/pool_lifecycle.rs` | 两节点两盘 → 镜像卷 → 盘离线/上线 |

API 使用 `PoolHandle` 而不是向调用者开放内部领域命令或任意 Workflow 提交，以免业务
调用绕过完整冲突集合。具名 Workflow 可读，构造由应用入口负责。示例：

```rust,ignore
let mut manager = PoolManager::new(config, metadata, data_plane);
let pool = manager.create_pool(PoolId::new("pool-a")?)?;
pool.observe_node(node.clone(), 1, true).await?.result().await?;
pool.load_node(node.clone()).await?.result().await?;
manager.online_disk(pool.id(), spec).await?.result().await?;
let volume = pool.create_volume(CreateVolume { id, disks }).await?.result().await?;
```

## 首批业务与安全边界

- NodeMap 观测不等于已加载。单节点观测序号单调，重复同值幂等，旧事实忽略，同序异值
  拒绝。断连会撤销当前 loaded 资格；重连必须再次确认加载。此版将显式观测的节点纳入
  Pool；没有单独的节点成员管理工作流。观测排队处理，不是实时网络故障探测器。
- 硬盘上线只有一个 Workflow/Run：InspectNode → Add/确认成员盘 → OpenDisk → 启用 IO
  并提交 Up。Add、Open 都是步骤，不提供 AddDiskWorkflow 或单独 add_disk 入口。
  已有成员盘重上线通过 `pool.online_disk(id)` 进入**同一** Workflow；Add 是幂等确认，
  不重复写入。首次由 manager 路由并预约物理归属；Open 成功不等于整个上线成功。
  节点可服务是本轮上线的前置条件，节点加载有独立业务入口。输入来自可信拓扑，
  模型不验证硬件真实性。打开但未启用 IO 是允许保留的安全中间状态。
- 创建 VD：首版限定**一个镜像 BG、2 或 3 个副本、每副本一个逻辑 BLK、同 Tier 且不同
  Node**。明确指定布局，不声称实现自动放置、真实容量几何或全部 VD 类型。
- 创建顺序：校验节点与盘 → 各盘提交 BLK 分配 → 数据面确认初始化 → 提交 BGMap。
  `BG 引用 ⊆ 已分配 BLK`。分配凭据由 DiskService 私有构造，VD 不接受用户自报 BLK。
- 失败/取消留下的未引用分配不回滚、不复用。没有释放能力；需要后续对账再设计回收。
  重复创建已存在 VD 拒绝；不是跨失败重试幂等接口，失败后重试可能增加保守泄露。
- 离线 Workflow 骨架：检查成员盘 → 失效相关 BGEntry → 关闭 IO 并提交 Down →
  等待重建完成 → 移除成员盘。后两步目前只有领域 Activity 占位，返回明确的
  `Failure::Failed`，不模拟重建成功或成员移除。流程会停在等待重建步骤，盘保持 Down
  和原有成员身份、BLK 分配。真正重建、无引用确认及成员移除提交留待后续实现。
  部分失败允许 Up+Invalid；online 不修改数据有效性。
- 对外副作用成功后 DiskIo 元数据失败，一律 Uncertain；协调者保留阻塞，不能凭旧内存
  状态继续修改。未知提交也保持基础库的恢复阻塞；读视图仍是最后确认状态，不能用来
  证明外部实际状态。没有人工跳过阻塞接口。

## 冲突与状态所有权

不是“所有冲突一律排队”。PoolWorkflowCoordinator 基于通用 AdmissionPolicy 抽象，
用 PoolOperation + PoolConflictPolicy 表达本轮明确的业务选择：

| 新请求 / 已有请求 | 决定 |
| --- | --- |
| 同业务目标重复请求 / 同操作仍在途 | 拒绝重复，不创建第二个 Run，也不假装合并结果 |
| 同盘 Offline / Online | 请求取消旧上线，新离线仍须等待旧流程和 Activity 停稳 |
| 同节点更新的观测 / 较旧观测 | 取消旧观测；旧序号或同序异值的新请求拒绝 |
| 同节点断连观测 / LoadNode | 取消加载并等待停稳，再应用断连事实 |
| Offline / 涉及该盘的 CreateVolume | 排队；本轮不抢断分配/发布流程 |
| 其余冲突 / 不相交对象 | 正常对象准入：前者排队、后者可并发 |

规则纯同步、无 IO，Runtime 在单一准入点对所有在途操作做完整决策。只有新请求能够被
接受、且没有任何拒绝项，才向旧流程发取消；容量不足或策略异常不影响旧流程。
基础库不知道 Disk/VD 业务。规则不是全局优先级抢占，也没有自动解除不确定结果阻塞。

Workflow 声明完整对象集合：创建卷占用 VD、所有目标 Disk 和 Node；上线占用 Disk+Node；
离线占用 Disk；节点变更占用 Node。因此创建、上下线和节点资格变化不能在相关对象上
交错。领域再声明实际 Activity 资源，由同一个 Coordinator 仲裁。

VD 首版使用 Pool 内的全局引用索引写资源，保护批量失效与新引用发布，不在运行时动态
发现/扩展锁。这个短期粗粒度选择可能限制 VD 并发，但不把整段 Workflow 串行化。
不同 Pool 独立；普通物理 DiskId 仍由 PoolManager 全局唯一预约，不能因为 Pool 隔离就
重复拥有同一物理盘。失败/取消后的归属预约保守保留，无解绑接口。

查询返回独立值，不暴露内部可变共享状态。跨域查询不提供原子快照，不提供可能过期的
Pool 健康缓存。成员盘 node/tier 本轮不可变，未来迁移需版本化准入，不能直接扩展预读。

## Query、Activity、取消、暂停与恢复

**区分用途，而不是读写。** `pool.nodes()/disks()/volumes()` 是外部观察 Query，没有
Workflow 身份，不改变执行。Workflow 内决定后续动作的读取通过 `Inspect` /
`RequireAbsent` 只读 Activity，具有 ActivityId、读资源声明、取消和暂停边界；不会
直接 `service.query()`。入口预读只发现不可变拓扑以构建完整对象集合，运行时仍用
Activity 重新检查资格与业务状态；这不构成确定性重放或结果历史。

```rust,ignore
let mut run = manager.online_disk(pool.id(), spec).await?;
run.control.pause()?;             // 仅请求
run.control.wait_paused().await?; // 确认已到安全步骤边界，无在途 Activity
run.control.resume()?;            // 同一个 WorkflowId，从原处继续
run.control.cancel();             // 不需要先 resume；取消优先
let result = run.result().await;  // 实际停稳/完成结果，不是取消请求回执
```

- 自动异步等待：Activity、Signal、Timer 等待期间挂起 Future，不占用阻塞线程。
- 手动暂停：不抢停已开始的 IO；它结束后，在下一 Activity/等待/显式 safe_point 前停住。
  暂停持有整个 Workflow 的对象占用，不持有已结束 Activity 的资源。不让其他业务修改
  到一半的对象。排队等待流程对象时，暂停确认可能也要等对象准入。最后一步已成功
  且流程已结束时，暂停可能来不及生效，wait_paused 会报告已结束，不能假装暂停成功。
- 暂停时 Signal 仍有界缓存，Timer 使用原始单调时钟截止时间；时间继续流逝，但步骤
  不会越过暂停点。恢复不重放已经完成的 Add/Open，不创建新 Run。
- 取消：父流程令牌传给 Activity。取消等待 Signal/Timer/暂停会立即唤醒；正在执行的
  Activity 必须实际停稳后才能返回。不能丢弃 Future 就声称远端停了；最终提交成功
  可以胜过同时到达的取消请求。暂停不是取消，取消也不意味着自动补偿已完成的步骤。
- `checkpoint()` 仅同步检查取消；自定义计算循环需 `safe_point().await` 才支持暂停。
  不提供任意 Rust 指令抢占、硬停 IO 或跨进程恢复。停止/崩溃之后无法 resume 此 Run。

## 外部接口与限制

MetadataStore 仅对已确认提交返回 Ok；已知失败必须证明未提交，否则返回 Uncertain。
MemberDisk 更新是带 expected_revision 的字段级变更，成功提交后才发布内存状态。
DataPlane 必须在操作停稳后返回；不能用超时/取消把可能成功的效果伪装成 Failed。
两个接口均是可信集成边界，基础库不是第三方适配器的安全沙箱。

MemoryMetadata 只在当前进程保存对象和提交日志，**不是持久化**。Node 连通和加载事实
不作为持久决策保存。没有加载已有数据、切主、fencing、历史重放或自动 reconciliation。
关闭后不能在同一 manager 重建同名 Pool；新建 manager 也不代表恢复或跨主所有权。
不删除旧目录。基础库只新增通用准入策略扩展点与协作暂停机制，不引用存储业务。

验收与验证记录见 [DESIGN.md](DESIGN.md)。

```sh
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked --example pool_lifecycle
```

## 节点上下线与协调者取消

```rust,ignore
let offline = pool.offline_node(NodeTransitionRequest { node: node.clone(), sequence: 2 }).await?;
let online = pool.online_node(NodeTransitionRequest { node, sequence: 3 }).await?;
// 协调者取消旧下线；online.result() 等旧 Activity 稳定退出后才可能完成。
```

节点转换序号与 `observe_node` 共享单调序列，表示可信拓扑观测。本轮不是实际电源控制 API。
`OfflineNodeWorkflow` 顺序执行：确认断连事实 → 检查已登记成员盘 → 批量失效数据 →
批量关闭 IO → 等待重建 → 移除成员盘 → 最终下电占位。
`OnlineNodeWorkflow` 确认连接事实、加载 Pool、确认/打开成员盘，再批量启用 IO；不会修复 Invalid 数据。
仅 `observe_node(..., true)` 会取消在途节点下线，但不自动加载 Pool 或启用 IO。

业务取消只由协调者应用 AdmissionPolicy：较新的节点转换替换旧转换；节点下线取消相交的
盘上线/离线与加载；节点上线取消相交的盘离线。所有者释放对象前必须等 Activity 真正停稳。
旧序号不能取消在途新转换。Workflow 内不保存其他 Run、不调用其他 Workflow 的控制方法。
节点和磁盘的影响集合由应用路由登记构造，包含尚未完成首次 Add 的磁盘；登记身份不可变，
失败后保守保留。单盘离线也声明所属节点，保证节点事实变更不能越过它的停稳边界。

DiskService 的 SetIoMany 是一次批量 Activity，并调用 `DataPlane::set_disks_io` 批量端口。
适配器可一次通知多盘；兼容默认实现逐盘执行。回复必须逐盘标明结果且覆盖完整输入，
缺项/重复/错盘结果按 Uncertain 处理。所有确认成功项逐盘提交并发布，即使别的项失败；
外部成功而元数据未确认也按 Uncertain 处理，不自动重试整批。
`DataPlane::wait_rebuild` 是可注入的等待边界，默认明确报未实现；测试适配器验证取消后停稳。
真实重建、成员移除和物理下电仍是空壳，不能将测试的成功/取消当成生产重建能力。

当前节点流程仍声明完整节点/磁盘集合，采用现有完整集合准入；单盘流程增加节点占用会
保守串行化同节点操作。通用的“批次冲突项摘出等待，其余继续”部分准入尚未实现，
这里的领域批量 IO 能力不等同于该调度能力。基础库的释放事件会重新检查等待队列并通过
oneshot 发放许可，唤醒等待 Future，无定时轮询；不确定释放建立恢复阻塞，不放行后继写操作。

Workflow 文件按业务过程拆分：`observe_node.rs`、`load_node.rs`、`online_disk.rs`、
`offline_disk.rs`、`create_volume.rs`、`offline_node.rs`、`online_node.rs`。
各文件同时包含流程参数、操作描述、对象声明和执行步骤；公开类型路径不变。

## 外部 CMD 暂停/恢复示例

运行 `cargo run --manifest-path storage-pool/Cargo.toml --example shrink_commands`。
[shrink_commands.rs](examples/shrink_commands.rs) 使用独立命令通道模拟 Start / Suspend /
WaitSuspended / Resume / Finish 请求；协调者正式任务索引保存 WorkflowId 到执行控制句柄的映射。
发起方只拿 ID，后续指令不依赖最初的 Run。暂停请求与暂停完成分别确认，在途第一块搬移
完成后暂停，恢复继续第二块，保持同一个 WorkflowId。等待确认/结果不会阻塞后续命令。
该示例只有内存内的模拟缩容，不接入真实 IO、网络 CMD、持久任务索引或实际缩容业务。

## 可配置协调规则与任务观测

规则入口是 `app/coordination/rules.rs`。创建 Pool 前选择：

- `manager.with_rules(PoolRules)`：有名称的类型化有序规则表，先匹配者生效；默认排队。
- `manager.with_policy(Arc<dyn AdmissionPolicy<PoolOperation>>)`：复杂业务用普通 Rust 策略实现。

规则包含新/旧操作类别、对象关系和 Queue / Reject / CancelExisting 决定，可单独测试。
默认规则保持之前行为。规则表先检查对象交集和旧节点序号，旧观测不能通过配置取消新转换；
自定义策略是可信应用代码，须自己保持业务约束。任何策略都不能跳过 Runtime 的对象占用、
容量检查、停稳交接和不确定结果阻塞。策略固定后用于新建 Pool，当前不支持在线热更新。

```rust,ignore
let mut rules = PoolRules::default();
rules.rules.insert(0, CoordinationRule::new(
    "maintenance-offline-waits",
    OperationSelector::Class(OperationClass::OfflineDisk),
    OperationSelector::Class(OperationClass::OnlineDisk),
    RuleRelation::SameDisk,
    AdmissionDecision::Queue,
));
let mut manager = manager.with_rules(rules)?;
```

任务索引由基础库在接受提交时建立，不依赖发起者是否保留 Run。以目标 Pool 为范围：

```rust,ignore
let id = run.id();
let snapshot = pool.task(id)?;
let tasks = pool.tasks();
let mut observer = pool.observe_task(id)?;
pool.suspend(id)?;                   // 请求
pool.wait_suspended(id).await?;      // 安全暂停确认
pool.resume(id)?;
pool.cancel(id)?;                    // 请求；最终状态仍需观察
let updated = observer.changed().await?;
```

快照包含 Pool、ID、流程名称、声明对象、执行状态、阶段/进度、暂停/取消请求及准入时
匹配的规则和已有任务 ID。`admission` 是准入时的说明，不是实时阻塞者列表。
Workflow 用 `ctx.report_progress(stage, completed, total)` 报告进度，不把进度作为业务状态。
订阅是 watch 当前值，可能合并中间更新，不是完整事件日志。
`WorkflowId` 只在当前 Coordinator 实例内唯一，不可跨 Pool 或重建实例当作全局业务身份。
普通终态按 `Config::task_history` 保留最近 256 项（可配置为 0），不确定终态保留；
已有 Observer 不因历史淘汰失效。记录在内存中，尚无持久化历史、跨主恢复或远程鉴权接口。

运行 `cargo run --manifest-path storage-pool/Cargo.toml --example coordination_rules` 查看规则与任务快照；
`shrink_commands` 已改用协调者正式任务索引完成外部命令按 ID 暂停/恢复。
