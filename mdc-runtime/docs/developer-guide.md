# 开发指南

## 1. 一个 Service 写什么

```rust
pub struct DiskService {
    metadata: DiskMetadata,
    router: Router<ServiceKind>,
}
```

核心元数据属于 Service。其他模块只能获得 `ServiceClient`，不能取得 Service 或它的元数据。

元数据内部可以使用私有短锁，但只暴露同步领域方法：

```rust
impl DiskMetadata {
    fn begin_offline(&self, disk: &DiskId) -> Result<(), DiskError>;
    fn complete_offline(&self, disk: &DiskId) -> Result<(), DiskError>;
}
```

工作流拿不到锁和 `&mut` 引用，因此无法把锁带过 `.await`。

## 2. `handle` 只做协议分发

```text
Request
  └── handle
        └── 对应的 Service 成员方法
              ├── Reply：查询、校验、读取内存元数据
              └── Task：RPC、等待、长流程、需要取消和观测的操作
```

```rust
impl Service for DiskService {
    type Request = DiskRequest;
    type Response = DiskResponse;
    type Error = DiskError;

    fn handle(
        self: Arc<Self>,
        request: DiskRequest,
        _context: RequestContext,
    ) -> HandleResult<DiskResponse, DiskError> {
        match request {
            DiskRequest::Query(disk) => self.query(&disk),
            DiskRequest::Offline(disk) => self.offline_workflow(disk),
        }
    }
}
```

`handle` 不知道 TaskKey、冲突策略或工作流步骤。新增业务请求时，只在这里增加一条到成员方法的静态路由。

## 3. 工作流自己决定是否创建 Task

查询成员方法直接回复：

```rust
fn query(&self, disk: &DiskId) -> HandleResult<DiskResponse, DiskError> {
    HandleResult::ok(DiskResponse::Snapshot(self.metadata.query(disk)))
}
```

需要受管执行的工作流自行构造 Task：

```rust
fn offline_workflow(
    self: Arc<Self>,
    disk: DiskId,
) -> HandleResult<DiskResponse, DiskError> {
    let meta = TaskMeta::new(
        TaskKey::new(format!("disk/{disk}")),
        format!("offline disk {disk}"),
    )
    .public();

    HandleResult::task(TaskSpec::new(meta, move |task| async move {
        self.metadata.begin_offline(&disk)?;
        task.call(&self.rebuild, StartRebuild { disk: disk.clone() })
            .await?;
        self.metadata.complete_offline(&disk)?;
        Ok(DiskResponse::OfflineCompleted)
    }))
}
```

工作流拥有“是否形成 Task、Task 是谁、与同对象任务如何冲突”的业务决策。Executor 仍独占 Future 的 poll、索引、完成回复和最终回收；所谓工作流管理 Task，不是让业务直接修改 `TaskSet`。

不要为查询创建 Task。立即回复不会进入 TaskSet，也不会产生 Task 观测事件。

## 4. 跨 Service 调用

Router 只存通信 Client：

```rust
let rebuild = self.router.client::<RebuildService>(&ServiceKind::Rebuild)?;

task.call(&rebuild, RebuildRequest::Start(disk.clone()))
    .await?;
```

不要直接调用另一个 Service 的成员方法，也不要在 workflow 内 `tokio::spawn`。

本地实现使用 mpsc + oneshot。未来替换成 RPC Client 时，上层仍保持 `submit/call/cancel_and_wait` 语义。

## 5. 优雅取消

正常取消只有一个入口：

```rust
ticket.cancel_and_wait(CancelReason::requested("operator cancel"))
    .await?;
```

框架行为：

1. Task 进入 `Cancelling`；
2. 取消作用域向所有 child call 传播；
3. `TaskContext::call` 请求下游取消；
4. 等待下游返回 `Completed/Cancelled/Failed/Aborted`；
5. 当前 workflow 才返回；
6. Executor 发布当前 Task 的最终状态。

业务不需要在每个 handler 检查 token。只有最末端的真实 I/O 能力需要定义“如何停止外部操作”，示例见 `demo/backend.rs`。

## 6. 同对象替换

同一个对象使用相同 `TaskKey`。默认冲突返回 `TaskAlreadyRunning`。

高优先级操作需要替换旧操作时：

```rust
fn fault_workflow(self: Arc<Self>, disk: DiskId) -> HandleResult<DiskResponse, DiskError> {
    let key = TaskKey::new(format!("disk/{disk}"));
    let meta = TaskMeta::new(key.clone(), format!("fault disk {disk}"));

    HandleResult::task(
        TaskSpec::new(meta, move |_task| async move {
            self.metadata.mark_faulted(&disk)?;
            Ok(DiskResponse::Faulted)
        })
        .replace_running(CancelReason::Preempted { by: key }),
    )
}
```

新 Task 先进入 `Queued`。只有旧 Task 完成整条优雅取消链后，Executor 才启动新 Task；二者不会重叠执行。

## 7. 调用方式

```rust
// 只关心最终业务结果
let response = client.call(request).await?;

// 需要控制任务
match client.submit(request).await? {
    Submission::Reply(result) => { /* 即时请求 */ }
    Submission::Task(ticket) => {
        let id = ticket.task_id();
        let exit = ticket.wait().await?;
    }
}
```

## 8. Code review 清单

- 查询是否直接 `Reply`，而不是创建空 Task？
- `handle` 是否只做 Request 到成员方法的静态路由？
- 是否由工作流成员方法决定 Reply/Task 以及 Task 策略？
- Service 是否只持有自己的核心元数据？
- 跨模块是否只通过类型化 Client？
- 是否存在 workflow 内部的 `tokio::spawn`？
- 相同对象是否使用稳定 TaskKey？
- 替换任务是否等待旧任务真正退出？
- 真实 I/O 的取消是否等待下游确认？
- 元数据锁是否保持私有且只出现在同步领域方法内？
