# Storage Pool

一个 Pool 一个具体 TaskCenter，同一个循环协调并推进 Workflow，独立服务提供步骤能力。
默认适配器仅模拟，不接触真实磁盘或 SDB。

## 阅读入口

| 位置 | 职责 |
| --- | --- |
| [pool.rs](src/pool.rs) | 组合根、Pool 生命周期、对外业务入口 |
| [task_center](src/task_center/mod.rs) | 协调状态、队列、占用和交接 |
| [conflicts.rs](src/task_center/conflicts.rs) | 单一业务冲突判断入口 |
| [workflows](src/workflows/mod.rs) | 完整业务流程，读写步骤均经 Activity |
| [disk/service.rs](src/domains/disk/service.rs) | 磁盘能力、状态与外部效果协调 |
| [domains](src/domains/mod.rs) | 按 disk / node / vd 聚合 model、service、runtime |
| [ports.rs](src/ports.rs) | 元数据/数据面端口及失败契约 |
| [disk/runtime.rs](src/domains/disk/runtime.rs) | 磁盘服务托管协议与调用适配 |

model 不依赖 Tokio 或 workflow-foundation，service 不依赖托管自己的 runtime。每个领域服务独立托管，
不自行管理业务对象冲突。短暂 Mutex 保护内存访问，不跨 await 持有。

## 业务范围

- 节点连通事实不等于 Pool 已加载。观测序号单调，同序异值拒绝，断连撤销 loaded，重连须再加载。
- 磁盘上线只有一个 Run：InspectNode → Add/确认成员 → Open → Enable IO 并提交 Up。
  首次通过 PoolManager 路由，已有盘通过 PoolHandle.online_disk 进入同一流程。
- 创建 VD 当前只支持一个镜像 BG、2 或 3 个副本、同 Tier 不同 Node、每副本一个 BLK。
  先校验、再提交分配、确认初始化，最后发布 BG 引用。分配凭据不能由外部调用者伪造。
- 已分配但未引用的空间在失败/取消后保守保留，不回收、不复用；尚无释放与对账流程。
- 离线：失效引用 → 关闭 IO 并提交 Down → 等待重建 → 移除成员。重建/成员移除仍为
  明确失败的占位能力，不假装业务完成。上线不恢复失效数据，也不释放空间。
- 节点上下线组织批量磁盘步骤；节点交接需等待旧流程及批量动作真正停稳。
- 已生效 IO 后元数据未确认是 Uncertain；不能凭旧视图继续修改。领域视图只表示最后确认状态。
- 拓扑身份在当前模型中不可变。预读只用于稳定路由，影响执行决策的状态必须在 Workflow 内
  通过读 Activity 重新验证；外部观察可以 Query，不承诺跨领域一致快照。

## 使用与观察

```rust,ignore
let mut manager = PoolManager::new(config, metadata, data_plane);
let pool = manager.create_pool(PoolId::new("pool-a")?)?;
pool.observe_node(node.clone(), 1, true).await?.result().await?;
pool.load_node(node.clone()).await?.result().await?;
let run = manager.online_disk(pool.id(), spec).await?;
let id = run.id();
let snapshot = pool.task(id)?;
let mut observer = pool.observe_task(id)?;
pool.suspend(id)?;
pool.wait_suspended(id).await?;
pool.resume(id)?;
pool.cancel(id)?;
```

Task 是完整 Workflow 执行，不是 Activity。重复业务目标在途时拒绝，不产生第二个 Run。
取消是请求，最终状态要观察；不可中断提交成功可以胜过并发取消。

conflicts 字段提供准入时的已有任务 ID、决定、原因；scheduling() 提供业务队列计数，
execution() 提供 Runtime 执行计数。ID 只在当前 Pool 实例内唯一。
普通历史按 Config::task_history 有界保留，Uncertain 保留；observer 不因历史淘汰失效。
暂停只在当前进程安全边界生效，仍保留对象占用，不支持崩溃恢复。

规则扩展见 [TaskCenter 开发说明](../../docs/task-center.md)。本轮删除 with_policy/with_rules、
PoolConflictPolicy、RulePolicy 和选择器引擎，不保留兼容的第二套协调路径。

## 验证

在 workspace 根目录执行：

```sh
cargo test --locked -p storage-pool
cargo run --locked -p storage-pool --example pool_lifecycle
cargo run --locked -p storage-pool --example task_center
cargo run --locked -p storage-pool --example shrink_commands
```

shrink_commands 是执行控制示例，不代表生产缩容已实现。DESIGN.md 仅保留阶段性历史。
