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
| `src/app/coordination.rs` | Pool 协调服务、操作描述、可独立测试的冲突策略 |
| `src/app/workflows/mod.rs` | 节点观测/加载、完整硬盘上线/离线、创建镜像卷 |
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
- 本轮 offline 是**显式保守失效流程**，不是完整物理故障/计划缩容状态机：先提交该盘
  所有 BGEntry Invalid，再关闭 IO 并提交 Down。部分失败允许 Up+Invalid，不允许
  自动声称修复；online 不修改数据有效性。不实现重建、移除或副本补齐。
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
