# 可嵌入 KV 的元数据模块

代码入口是 `storage_pool::metadata`。业务代码依赖 `MetadataStore`，共用实现 `KvMetadata`
负责版本校验、业务约束、记录编码和原子变更。接入不同数据库时只实现 `KvStore`，不用重新实现磁盘或卷规则。

```text
磁盘 / Tier / VD 领域服务
          ↓ MetadataStore
      KvMetadata 仓储
          ↓ KvStore
  你的 KV / MemoryKv / RedbKv
```

## 接入已有 KV

```rust,ignore
use storage_pool::metadata::{KvMetadata, KvStore};

let kv: Arc<dyn KvStore> = Arc::new(YourKvAdapter::new(your_database));
let metadata = Arc::new(KvMetadata::new(kv));
let manager = PoolManager::new(config, metadata, data_plane)
    .with_balancer(balancer)?;
```

底层只有两个接口：

```rust,ignore
#[async_trait]
impl KvStore for YourKvAdapter {
    async fn snapshot(&self, prefixes: &[Vec<u8>]) -> Outcome<KvSnapshot> {
        // 在同一个数据库读取快照里扫描所有前缀，返回 key/value 字节。
    }
    async fn transact(&self, tx: KvTransaction) -> Outcome<bool> {
        // 在同一个写事务里验证 tx.checks，再应用全部 tx.writes，最后提交。
    }
}
```

- `KvSnapshot` 是有序的字节键值集合；所有前缀必须来自同一一致性快照。
- `KvCheck.expected = None` 要求键不存在，Some 要求原值完全匹配。
- `KvWrite.value = None` 删除键，Some 写入字节。
- `transact` 返回 true 表示整个事务确认提交；false 表示条件不满足，全部没有生效。
- 已知失败返回普通错误，必须保证没有提交；提交结果不明确返回 `Failure::Uncertain`。

多次独立 get/put 加进程锁无法满足该契约。数据库需提供可实现上述原子性的事务或条件批量写入，
并覆盖不同仓储实例；分布式 KV 的事务大小限制也需要由接入方核对。
仓储只在确认 CAS 冲突时最多重新读取并验证 16 次，原始实体 revision 保持不变。
普通失败和 Uncertain 都不重试，更不会重发数据面 IO。

## 记录与一致性

记录存于 `runtime/metadata/v1/` 命名空间，ID 按 UTF-8 字节转十六进制，避免斜线、中文或前缀碰撞。
这个命名空间应由仓储独占管理，不要绕过仓储直接更新其中的键。

```text
runtime/metadata/v1/
├── pools/<pool-id>/
│   ├── revision
│   ├── tier/<tier-id>
│   ├── disk/<disk-id>
│   ├── volume/<volume-id>
│   ├── allocation/<disk-id>/<blk>
│   ├── deleted-volume/<volume-id>
│   ├── released/<disk-id>/<blk>/<volume-id>
│   ├── retired-tier/<tier-id>
│   └── retired-disk/<disk-id>
└── disk-owners/<disk-id>/
```

每个实体或凭据是一条版本化命名空间内的记录，值使用 JSON 编码。
一次 remap 在同一事务内切换 VD 引用、源/目标磁盘分配和 allocation owner，不能只保存其中一部分。
每池 revision 保护跨记录判断，包括“没有引用”这样的空扫描，防止并发新增引用穿过校验。
物理盘归属键另做 CAS，使不同 Pool 的独立仓储实例也无法同时认领同一块盘。

删除卷时保存原 placement；释放空间时保存原卷的释放凭据。
切换后可以继续清理，旧凭据也不能释放已经分配给新卷的空间。Removed 和退休身份继续保存。

首版每次操作读取单池完整快照，只写发生变化的记录。这个选择优先保证明确的一致性边界；
大规模池可扩展定向读取与索引。凭据和退休记录目前不自动清理，后续清理必须保证旧任务无法复用身份。

## 内存和 redb

`MemoryKv` 和 `RedbKv` 执行同一套仓储规则。`adapters::MemoryMetadata` 只是内存仓储加测试提交日志，
不再维护另一套业务实现。

启用 `kv-redb` 可把元数据和任务历史嵌入调用方持有的同一个数据库：

```rust,ignore
let db = Arc::new(redb::Database::create(path)?);
let metadata = Arc::new(KvMetadata::new(Arc::new(
    RedbKv::from_database(db.clone())?,
)));
let tasks = Arc::new(RedbTaskStore::from_database(db)?);
let manager = PoolManager::new(config, metadata, data_plane)
    .with_task_store(tasks, persistence_config)?;
```

redb 使用独立表，不改动调用方其他表；普通读写在 blocking worker 上执行，写事务使用 Immediate durability。
初始化 open/from_database 应在启动或 blocking 上下文中调用。
其他 KV 不需要启用 redb，也不需要更改 Workflow。

元数据和 Task 历史仍有不同职责：元数据保存已经确认的业务事实，TaskCenter 保存任务意图、执行和审计。
它们不组成一个覆盖整个 Workflow 的长事务。缩容恢复按持久事实和稳定 job ID 核验后继续，
不额外保存一份未参与恢复决策的阶段检查点。

## 读取、恢复与运行示例

Disk/Tier/VD 服务及 Pool 查询统一读取仓储，不保留可切换的领域缓存。
如果一次提交返回 Uncertain，后续成功读取可能看到已提交数据；读取不会解除原任务的隔离，
外部 IO 的实际效果仍需要业务对账。

系统接管既有 Pool 时，确保旧所有者停稳，再调用 `manager.open(原 Pool 身份, 当前完整拓扑)`。
池内依次恢复元数据、成员目录和真实连接/加载状态，随后自动重建可续办 Task。
数据库中的 IO Up 不代表新宿主已取得实时拓扑事实，离线源盘的缩容继续等待条件，
见 [任务持久化](task-persistence.md)。`restore_metadata` 仍可单独用于目录恢复，它本身不创建任务执行。

```sh
cargo test --offline --locked -p storage-pool --all-features --test metadata
cargo run --offline --locked -p storage-pool --features kv-redb --example metadata_kv -- /tmp/runtime-demo.redb
# 再运行一次同一命令，读取既有 Removed 记录和历史 Task。
```

示例使用正式缩容 Workflow，物理 IO 和均衡复制仍由模拟适配器代替。
模块不提供跨主租约、任意 Future 恢复或真实磁盘操作。
