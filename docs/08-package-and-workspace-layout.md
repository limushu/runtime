# 当前代码布局与演进边界

状态：与 B0.7 Runtime 和 MemberDisk 纵切面一致。

## 1. 当前决策

仓库只有一个 library crate：`pool-control-plane`。通用执行机制已经形成独立的 `runtime`
模块，但暂不拆成新 crate。这样既明确业务无关边界，也避免在只有一个真实领域时提前承担
版本、依赖和发布成本。

不建立通用 Actor API、Action DSL 或按请求类型自动路由的 Router。领域开发者看到的是
自己的 Service、typed Request 和普通 `async fn`；Runtime 负责根 Future poll、生命周期、
Task、取消与观测。

![当前代码布局](assets/package-workspace-layout.svg)

图源：[package-workspace-layout.puml](diagrams/package-workspace-layout.puml)

## 2. 当前目录

```text
kube-managed-future-runner/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs
│   ├── runtime/
│   │   ├── mod.rs
│   │   ├── service.rs
│   │   ├── lifecycle.rs
│   │   ├── observation.rs
│   │   ├── task.rs
│   │   └── object_task.rs
│   └── member_disk/
│       ├── mod.rs
│       ├── model.rs
│       ├── allocation.rs
│       ├── event.rs
│       ├── ports.rs
│       ├── service/
│       │   ├── mod.rs
│       │   ├── client.rs
│       │   ├── runtime.rs
│       │   ├── reconcile.rs
│       │   ├── operations.rs
│       │   ├── allocation.rs
│       │   └── error.rs
│       ├── model_tests.rs
│       └── service_tests.rs
├── tests/
│   ├── public_api.rs
│   └── runtime_component.rs
└── docs/
    ├── *.md
    ├── diagrams/
    └── assets/
```

## 3. Runtime 文件职责

### `runtime/service.rs`

定义 `CallError`、`ServiceReply`、`ManagedService`、`ServiceClient`、`ServiceRuntime` 和
`ServiceInstance`。每个 Service 关联一组 `Request`、`Reply` 和领域 `Error`；通用 Envelope
持有统一 oneshot。每个实例只创建一个根 Tokio task，根循环优先处理控制命令，并用
`FuturesUnordered` poll 已接收 handler Future。业务 Service 不需要自行维护第二个 loop。

### `runtime/lifecycle.rs`

定义生命周期和独立 `ServiceControl`：Pause/Resume/Drain/Stop/CancelTask。Drain 等待已接收
工作自然完成，Stop 发出协作取消，强制终止属于 `ServiceTask`。

### `runtime/observation.rs`

定义 Operation、Trace、Service/Task 快照、结构化事件、`ServiceObserver` 和可选
`RuntimeEventSink`。这些数据是观测投影，不是领域状态权威。

### `runtime/task.rs`

定义 `TaskAttempt`、`TaskControl` 与 `TaskOutcome`。Task 附着在已经执行的 handler Future
上，不包装、生成或路由 Future。Query 可以完全不创建 Task。

### `runtime/object_task.rs`

定义可选的 `ObjectTaskCoordinator<K, I, E>`。它只实现 Join、Queue、Cancel、Promotion、
WaitIdle 及 RAII 清理；领域用一个决策函数赋予“相同、冲突、优先级”等业务意义。

## 4. MemberDisk 文件职责

### 领域根目录

- `model.rs`：`MemberDisk` 实体、已提交字段、状态投影、更新验证和业务不变量；
- `allocation.rs`：BLK、位图、申请 request/response；
- `event.rs`：DiskMap UP/DOWN 与管理 Shrink 输入；
- `ports.rs`：Metadata、PoolNode、VDM 明确能力边界。

### `member_disk/service/`

- `mod.rs`：Service 依赖、已提交对象目录和唯一 `validate -> SDB -> memory` 修改入口；
- `client.rs`：私有 `MemberDiskRequest` / `MemberDiskReply` 协议，以及把统一 Reply 投影为
  `submit/get/wait_idle/allocate_blks` 具体结果的公开领域 facade；对应 `_in` 方法保留跨服务
  `OperationContext`；
- `runtime.rs`：MemberDisk 对 `ManagedService` 的适配、一处协议 match、盘事件对象槽准入、
  Task 附着和状态表循环；
- `reconcile.rs`：穷举 `start_state + event + shrink intent -> action -> finish_state`；
- `operations.rs`：状态表调用的普通异步业务方法；
- `allocation.rs`：Tier/故障域选盘、SDB-first 位图发布，并排除正在做生命周期变更的盘；
- `error.rs`：MemberDisk 状态、端口与取消产生的领域错误；Runtime 的生命周期、通信和协议
  错误由外层 `CallError<MemberDiskServiceError>` 表达。

这些文件共同实现一个 `MemberDiskService`，不产生额外 Service、业务 Actor 或每盘 Tokio
task。`ObjectTaskCoordinator` 相当于隐藏在机制层的对象 mailbox，而不是开发者要继承的
业务框架。

## 5. 两条执行路径

盘事件：

```text
MemberDiskClient::submit(event)
  -> ServiceRuntime business mailbox
  -> MemberDiskService::handle（唯一协议分发）
  -> ObjectTaskCoordinator[DiskUuid]
  -> attach TaskAttempt
  -> reconcile_once
  -> await one MemberDisk business action
  -> verify finish state
  -> reread committed object until stable
```

查询与 BLK 申请：

```text
MemberDiskClient::get(disk) -> direct read Future -> no Task

MemberDiskClient::allocate_blks(request)
  -> attach non-cancellable Task
  -> filter Tier / allocation / fault-domain / active-object guards
  -> MetadataService SDB commit
  -> publish committed bitmap in memory
```

不是所有能力都强行进入 DiskUuid 对象槽。是否需要对象互斥与 Task，由目标 Service 在一处
清晰选择。

## 6. 依赖方向

```text
MemberDisk Model / Value Objects
        <- Event and Ports
        <- Service business operations and state table
        <- MemberDisk Runtime adapter
        <- Runtime mechanisms
        <- typed MemberDiskClient
        <- future Pool assembly
```

Runtime 不能引用 MemberDisk/BG/Node 类型。其他领域不能获得 MemberDisk 可变引用；未来
`Pool` 保存各领域 Client、Control、Observer 和 ServiceTask，跨领域只调用明确目标能力。

## 7. 已实现与未实现

已实现：

- 单根 Future poll、容量背压和最大在途请求数；
- 四类实例句柄、完整生命周期和控制优先级；
- Idle/Busy、请求统计、Task/Trace、进度、阻塞原因、状态转换事件；
- 精确 Task 取消、服务级协作 Stop、Drain 和最终 force abort；
- 根任务所有权丢失时自动 abort，避免 task 泄漏；
- handler panic/根 abort 显式闭合 Request/Operation，Drain/Stop 在 shutdown 和旧队列终态
  拒绝完成后才回复控制 waiter；
- 可选对象槽及 MemberDisk 冲突策略；
- MemberDisk DOWN/UP/Shrink、可靠 DOWN、排空、重试、SDB-first 与 BLK 分配；
- Runtime 契约测试和 MemberDisk 端到端结构化观测测试。

尚未实现：

- `PoolManager -> Pool -> Domain ServiceInstance` 的多 Pool 装配；
- Tier、VD/BG、PoolNode 的真实领域实现和真实 SDB adapter；
- 结构化 Runtime Event 的生产级持久化 adapter；
- Monitor 切主后长流程的领域恢复协议；
- 第二阶段 Tier/Partition 并行分配。

## 8. 何时拆独立 crate

只有同时满足以下条件才把 `runtime` 拆成独立 crate：

1. 至少第二个真实领域使用同一生命周期和根 Future poll；
2. 公共 API 不需要引入业务类型或业务回调 DSL；
3. 拆出后领域工作流仍保持普通 `async fn`；
4. 构建隔离、独立版本或跨项目复用产生实际收益。

## 9. 验证

```text
cargo fmt --all --check
cargo test
cargo clippy --all-targets -- -D warnings
```

仓库根目录还执行 `npm run verify`，保证子项目修改不破坏 JurisDesk 仓库级约束。
