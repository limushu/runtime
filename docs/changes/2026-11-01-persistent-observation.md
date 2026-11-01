# TaskCenter 持久化观测与终端工具

基于 `92ea2b8` 新增只读 TUI 和追加式审计。TUI 展示真实运行记录，不是教学动画。
恢复 checkpoint 与审计记录分开：前者决定业务从哪里继续，后者保留已发生的执行事实。

## 运行

需要 Rust 和 Node.js 22+，TUI 无第三方 npm 依赖。在仓库根目录运行：

```powershell
$env:WORKFLOW_DEMO_HOLD_MS = '30000'
cargo run -p storage-pool --example leader_handoff
```

示例通过正式 PoolManager / TaskCenter 创建两个池，使用模拟数据面，打印审计文件路径。
它在旧主暂停状态保留 30 秒，随后切换到新主并完成缩容。另开终端，使用打印的路径：

```powershell
node tools/task-center-tui/index.mjs D:/Temp/实际打印的文件.audit.jsonl
```

退出示例后，相同命令仍可浏览历史。非交互环境追加 `--once` 输出池总览。
示例不访问真实磁盘，也不实现选举；epoch 由示例显式提供。

键位：上下键或 j/k 选择，Enter 下钻，Esc 返回，Tab 切换全部/当前/历史，`/` 搜索任务，
空格冻结显示，q 退出。冻结显示不停止读取、不暂停业务。全程没有写操作或任务控制命令。

## 展示层级与记录内容

Pool UUID → Workflow TaskId → execution 批次 → 逻辑 Activity / 子调用 → 事件 → JSON 详情。
同一 Activity 的重试、状态和进度逐次保存，不从 watch 刷新差值推断事件。
小终端截断概要，但完整字段可在详情中滚动查看。任务选择绑定身份，不跟随新任务刷新跳动。

审计覆盖：池注册、任务注册/恢复/进入执行/状态/结束、对象、准入解释与阻塞者变化、
用户控制请求、策略暂停原因、步骤/调用创建与状态、重试计数和退避、步骤进度、checkpoint 已保存。
不记录任意请求、返回 payload 或 checkpoint 内容。步骤说明和错误文本必须由业务方排除秘密及客户内容；
文件应按运维数据配置访问权限。

TaskState 是最近一次上报状态，不是心跳或进程存活证明。非终态任务在进程崩溃后也可能留下 Running，
TUI 明确保留这个语义，不伪造终态。切主导致的持久任务中断显示 recoverable，恢复事件接续同一个 TaskId。
每次恢复生成新的 execution UUID，Activity sequence 即使重新从 1 开始也不会覆盖旧记录。
历史查询不依赖 TaskRegistry/ActivityHistory 的内存保留上限，不会重新执行历史任务。

## 接入现有 PoolManager

```rust,ignore
let log = workflow_foundation::audit::AuditLog::new(
    Arc::new(workflow_foundation::audit::FileAuditStore::open("pool-audit.jsonl")?),
    4096,
)?;
let manager = PoolManager::new(config, metadata, data_plane).with_audit(log.clone());
// 可与 with_task_store(...) / with_task_ids(...) 一起使用。
// 接受请求之前完成配置；各池共享一个 writer。
```

`manager.audit_records(after_cursor, limit)` 按游标查询（每页最多 1000 条）；
`manager.audit_health()` 查询写入健康；`manager.flush_audit().await` 等待前序记录持久化。
独立 WorkflowDriver 也支持 `with_audit()`。不配置时保持原来的执行行为。

写入由一个专用线程执行，有限队列避免慢磁盘无限积压。队列满/写失败会使审计健康进入失败状态；
后续普通边界停止推进，结果不再作为正常成功确认，并尽可能追加 EvidenceIncomplete 警告。
首次执行前、服务调用派发前、回复后和最终结果确认前有持久化屏障。
这不是业务与审计的分布式原子事务：崩溃时已派发但未记录结果的调用只能显示未确认，不能猜测成功。

## 文件后端与切主边界

FileAuditStore 使用独立 writer 锁文件，允许只读 TUI 同时打开数据文件；每条追加后同步数据，
屏障同步文件。完整坏记录、游标不连续或过时 owner epoch 拒绝；未完整写出的尾部由 writer 重开时截断。
读者忽略未完成尾部并等待，发现文件替换/截断会停止跟随，要求重新打开，避免混合不同日志。

文件后端验证范围是同机重启、共享同一文件的有序主备交接。**不能把本地文件等同于跨机器高可用存储。**
跨机器需实现 `AuditStore`，接到主备可访问的可靠存储，并执行 epoch fencing、顺序追加与持久化确认；
存储池的选举、旧主外部效果隔离仍沿用现有职责。当前 TUI 直接读文件；远程后端还需相应查询传输适配。

首版文件索引与 TUI 投影在内存中，不提供无限历史容量保证、压缩/归档/保留期、加密、防篡改签名或 RPC 鉴权。
JSONL 是版本化观测格式，不是任意业务数据的保管库。只读分页接口可供后续索引后端复用。

## 验收与验证

- 两个池都可见；空池可见；按池和当前/历史筛选，不跨池混淆。
- 每个已执行步骤与子调用可下钻，淘汰内存记录后文件仍保留其状态变化。
- 同一任务主备交接后保留两个执行批次，旧的已结束任务仍可查询。
- 文件重开恢复历史；并发 writer 被拒绝，TUI reader 不被 writer 锁阻塞。
- 审计故障不返回正常成功，不启动未获得记录确认的外部调用。
- 终端退出恢复 raw mode/光标；输入中的控制字符不会作为终端命令执行。

```powershell
node --test tools/task-center-tui/model.test.mjs
cargo test -p workflow-foundation --test audit
cargo test -p storage-pool --example leader_handoff
./scripts/verify.ps1
```

适用 GNU 工具链的本机命令见 `docs/verification.md`。外围工程另运行 `npm run verify`。

### 本次验证结果（2026-11-01）

- Windows GNU 工具链离线运行完整 `scripts/verify.ps1`：200 项 Rust 测试通过，
  doc tests、格式检查、Clippy `-D warnings` 均通过。
- TUI 投影测试 4 项通过；外围 `npm run verify`（结构检查、lint、TypeScript、构建）通过。
- 实际运行 `leader_handoff` 生成 617 条记录：两个池、18 个历史任务；同一缩容任务的
  epoch 11 / 12 执行批次均保留。`--once` 可读取该文件。
- 交互终端实测池选择、任务搜索、执行批次与事件下钻、JSON 详情、退出恢复终端。
- 审计集成测试验证逻辑 Step 与子 Call 关系、内存淘汰后仍可查询、文件重开、并发读写、
  尾部中断恢复、旧 epoch 拒绝，以及写失败时不执行外部服务调用。
