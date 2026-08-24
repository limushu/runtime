# 业务开发指南

本文只讲“开发一个业务要写什么”。运行时内部原理放在 [design.md](design.md)。

## 一条规则

先判断代码属于哪一类：

| 问题 | 放置位置 |
|---|---|
| 收到了什么请求？ | Payload / Message |
| 当前对象能不能做、要合并还是互斥？ | State 方法或 handler |
| 要启动、取消哪些异步操作？ | handler 调用 `service.run/cancel_task` |
| remap、RPC、等待临界条件、提交 MDC 怎么串？ | 普通 `async fn workflow` |
| Payload 到哪个 handler？ | feature 的 `install.rs`，只写一次 |
| Future 怎么 poll、如何 Drop、如何观测？ | 框架，业务不实现 |

核心路径是：

```text
Message -> handler -> State 决策 -> service.run(async fn)
                              <- 完成 Message <- TaskSet
```

## 第一步：定义闭合协议

应用用一个穷举 Message enum；宏同时生成稳定的 `MessageKind` 和 payload 提取逻辑：

```rust
define_messages! {
    pub enum PoolMessage => PoolMessageKind {
        StartRebuild(StartRebuildRequest),
        DiskResolved(DiskResolved),
        BgFinished(BgFinished)
    }
}
```

`StartRebuildRequest` 是外部请求，后两个是框架托管 Future 的完成消息。两类消息没有两套路由。

## 第二步：定义 Service 私有 State

State 是**运行期调度上下文**，不是 MDC 的第二份权威数据：

```rust
struct RebuildState {
    disks: HashMap<DiskId, DiskJob>,
    bgs: HashMap<BgId, BgJob>,
    queue: VecDeque<BgId>,
    running: usize,
    window: usize,
    suspended: bool,
}
```

建议把纯规则写成 State 方法，例如 `attach_disk_to_bg`、`take_next_bg`、`detach_disk`；handler 负责把规则结果转换成 `run/cancel/send`。不要把完整 `PoolObject` 用 `Arc<Mutex<_>>` 暴露给所有 Service。每个 Service 只拿它需要的能力，例如只读 Catalog、MetadataWriter、NodeCoordinator。

生产中的数据关系应是：

- MDC：持久、可恢复、带版本/fencing 的事实；
- Service State：从 MDC 和事件构造的内存调度上下文；
- Task：一次异步动作及其运行期身份；
- 重启：先读取 MDC 恢复 State，再按 operation id / generation 恢复或重建 Task。

## 第三步：写自由 handler

handler 在 Service 的唯一宿主 task 上同步执行，所以适合做短小的对象决策：

```rust
fn handle_start(service: &mut RebuildService, request: StartRebuildRequest) {
    if service.state().contains_disk(&request.disk) {
        request.completed.send(Err(AlreadyRunning));
        return;
    }

    service.state_mut().begin_resolve(request.disk.clone(), request.completed);
    let catalog = service.state().catalog.clone();
    let disk = request.disk;

    service.run(
        resolve_key(&disk),
        format!("resolve {disk}"),
        TaskVisibility::Internal,
        move |_| resolve_disk(catalog, disk),
        move |outcome| PoolMessage::DiskResolved(DiskResolved { disk, outcome }),
    );
}
```

handler 不应 `.await` 慢 I/O。需要 `.await` 的逻辑必须成为 Task，这样生命周期、取消和审计才不会绕过框架。

## 第四步：写普通 workflow

workflow 只拿最小依赖或 `TaskContext`：

```rust
async fn rebuild_bg(
    ctx: TaskContext<ServiceKind, PoolMessage>,
    bg: BgId,
) -> Result<(), RuntimeError> {
    let (reply, ticket) = request_channel();
    ctx.send(ServiceKind::Bg, DoBgRebuild { bg, reply }).await?;
    ticket.await??;
    Ok(())
}
```

它可以通过 Router 等待另一个 Service，但不能直接拿另一个 Service 的 State，也不要自行 `tokio::spawn`。因此调用关系是能力调用，不是对象所有权嵌套。

## 第五步：在 feature 内安装一次

```rust
pub fn install(blueprint: &mut PoolBlueprint) -> Result<(), RuntimeError> {
    register_handlers!(blueprint.rebuild, {
        StartRebuildRequest => handle_start,
        DiskResolved => handle_disk_resolved,
        BgFinished => handle_bg_finished
    })
}
```

如果一个 feature 确实同时拥有 global 与 per-pool 两段，可以让这一个 `install(blueprint)` 同时写入两张 Registry：

```rust
register_handlers!(blueprint.global, { NodeOffline => dispatch_to_pools })?;
register_handlers!(blueprint.event,  { PoolNodeOffline => handle_in_pool })?;
```

应用 composition root 只调用 `node_offline::install(&mut blueprint)` 一次。不要再到 GlobalService 和 PoolService 各维护一份 `match`。

独立能力应保持独立 feature：例如 Rebuild 的 `Start/Suspend/Resume/Cancel` 与 DiskOffline 不是同一个协议。DiskOffline 通过 Router 调用 Start，而不是把“盘上下线”塞进 Rebuild 模块。

## 策略到底在哪里

对象冲突、排序、合并、互斥属于 State/handler，不属于通用 Runtime：

```text
Disk event
  -> 读取 disk 对象状态
  -> 规则决定 Ignore / Merge / Cancel old / Start new
  -> handler 输出 run/cancel/send
  -> TaskSet 执行
```

这不是“事件直接变成一条盲跑 workflow”。workflow 是决策后的异步执行段；对象状态是策略主体。框架只提供任务身份、排重键、取消、poll、生命周期和观测机制。

## Suspend 与 Service Pause 不要混淆

- `SuspendRebuild`：业务命令。完成当前在途 BG，停止窗口补位，Rebuild 对象仍可接受 Resume/Cancel/Query；
- `control.pause()`：运维命令。Service 暂停从业务通道取新消息，但继续 poll 已在途 Future 和处理其完成消息；
- `control.drain()`：拒绝新请求，做完已经接收的消息和 Task，最终进入 Paused；
- `shutdown(Immediate)`：卸载。强制取消 TaskSet，然后进入 Stopped；
- Drop `PoolServices/ServiceEntry`：最后一道保险，宿主 Tokio task 被 abort，内部 FuturesUnordered 整体 Drop。

## Code review 检查表

- 是否只在一个 `install.rs` 声明了该 Payload 的映射？
- handler 是否包含长时间 `.await` 或阻塞 I/O？
- workflow 是否私自 `tokio::spawn`？
- Service 是否拿到了超出职责的 Pool/MDC 权限？
- 对象合并与取消是否以稳定 `TaskKey` 和 owner 集合表达？
- 完成、失败、取消是否都回到内部 Message 并更新 State？
- 发起者 ticket 是否只存在一个明确 owner？
- shutdown/Drop 后是否有 Future 泄露测试？

