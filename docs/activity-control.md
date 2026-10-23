# Activity 控制与工作流继续条件

Activity 仍只有一种执行语义：返回最终业务结果。暂停不返回 Suspend、不结束 Activity、不创建新 Run。
需要中间暂停的步骤可以在执行入口调用 `ActivityContext::register_pause(Arc<dyn ActivityPause>)`。

## 控制通知与可选暂停钩子

`ActivityContext::control()` 返回统一的 `ControlToken`：`cancelled()` / `is_cancelled()` 观察取消，
`request()` 查看带 generation 的用户/系统暂停请求，`changed()` 等待控制变化。它只是通知，不表示已停稳。

`ActivityPause` 只提供异步 `pause`、`resume`。领域适配器在钩子里发送领域指令并等待实际结果。
不再提供取消钩子，取消只通过 token 传递，领域适配器负责发送必要的 Cancel 指令并等待停稳。

- `pause` 成功必须表示领域操作已安全暂停，包括在途效果已稳定；收到指令不算成功。
- `resume` 成功表示同一领域任务按当前事实继续。
- 适配器必须串行化同一 job 的 Pause/Resume/Cancel 指令并等待结果，防止迟到的 Resume 覆盖 Cancel。
  示例用每 job 的异步控制锁串行化短控制指令；等待整个 job 完成的 Start 不持有该锁。

注册对象属于本次 Activity，应在长时间等待前注册且只能注册一次。若需要先取得领域 job 句柄，
先等待短暂 create-or-attach 的明确接受结果，再立即注册；接受阶段没有钩子，框架不会提前确认暂停。
接受请求不能因取消被丢弃，必须取得结果后再停稳 job。领域 job 身份由适配器持有，不交给 TaskCenter。
开始、控制、完成并发时的领域幂等性由领域服务保证，尤其应容许控制时任务已经完成。

没有注册钩子时，暂停请求进入 `Pausing`，等待当前步骤正常结束，再在安全边界进入 `Paused`。
注册钩子时，执行器等待 `pause()` 确认后才进入 `Paused`，当前 Activity 继续等待原业务结果。
`RunControl::wait_paused` 和 `wait_suspended` 等的是确认，不是请求送达。

取消时等待真实业务 Future 结算，不丢弃它。暂停钩子的已知拒绝会请求取消，清理和原操作均已停稳后返回已知错误；
只有结果不明确或 panic 才按 Uncertain 保留冲突 fence。领域适配器必须把可能已经生效的失联/超时报告为 Uncertain。

每个暂停确认绑定控制 generation 和被暂停 Activity 的取消作用域。
恢复后再次暂停不能复用旧确认；服务关闭取消其 Activity 时，旧确认也立即失效。
TaskCenter 用 `pause_acknowledged()` 放行 topo，不能只判断 RunState 是否为 Paused。

执行状态机位于 `crates/workflow-foundation/src/workflow/activity.rs`，明确经过
Running → Pausing → Paused → Checking → Resuming，以及 Settling。

## 取消何时生效

框架在派发前、资源等待、Signal/Timer 等待及安全边界检查 token，但不会在任意业务 Future 的每次 poll 中注入检查。
**领域能力已经运行后，只有领域实现或其适配器主动感知 token，才会开始取消处理。**
不感知 token 的 Future 即使继续被 poll、继续执行 await，也会正常运行到实际结果返回。

```rust,ignore
let operation = domain.start_and_wait(job_id);
tokio::pin!(operation);
tokio::select! {
    biased;
    result = &mut operation => result,
    _ = context.control().cancelled() => {
        // 先发送领域取消指令并等待实际停稳，随后仍要取得原操作的真实结果。
        let stopped = domain.cancel_and_wait(job_id).await;
        let settled = operation.await;
        combine_cancel_and_operation_results(stopped, settled)
    }
}
```

上面的 combine 必须保留任何 Uncertain，不能收到通知就返回 Cancelled。
尚未开始副作用时可以直接检查 `is_cancelled()` 并返回 Cancelled；CPU 循环应自行安排检查及让出执行机会。
已经发出的 IO 不会由 token 自动撤销；等待它完成或使用底层明确支持的取消协议。
不可中断的提交可以继续完成并返回真实成功，框架不会把成功伪装成“没有发生的取消”。

`control.cancellation_reason()` 区分 `Requested`（用户明确取消）和 `Interrupted`（宿主或服务停止）。
这是供适配器选择领域指令的原因，不改变协作检查规则。正式均衡适配器在前者发送 Cancel 并等待结算，
在后者确认 Pause 后结束当前执行，让新宿主按稳定 job ID 接管；不能把进程切换当成放弃业务任务。

暂停不会释放底层 Activity 资源。长迁移等待应申领 job/control 资源，不要一直占用 topo 要获取的磁盘写资源。
短提交步骤继续使用原来的资源保护，不支持中间暂停时就等待该步骤完成。

## 继续条件

Workflow 可通过 `continuation_guard()` 返回 `ContinuationGuard`。只读 `check()` 返回：

- `Ready`：当前允许继续。
- `Wait(reason)`：保持 `WaitingConditions(reason)`，任务不失败。
- 错误：检查本身失败，按正常工作流错误结算。

执行器在首次执行、暂停恢复和步骤边界调用检查；检查可能重复，不能产生业务副作用。
新的暂停/取消可作废正在等待的检查 Future。等待条件时不轮询数据库，业务事实变化后调用
`RunControl::recheck()`、`TaskCenter::recheck(id)` 或 `PoolHandle::recheck_task(id)`。
解除 topo 系统暂停会自动走检查，不需要由 topo 任务直接恢复均衡。

用户暂停与系统暂停分开保存，解除其中一个不能覆盖另一个。`pause_requested` 表示有效暂停请求，
`user_pause_requested` 和 `system_pause_requested` 可分别解释原因；存储的 `StoredTask.pause_requested`
保留原来的用户暂停含义，系统标记单独保存。观察暂停期间的实际迁移进度可使用 `ActivityContext::report_progress`。

## 缩容与 topo

TaskCenter 的 `submit_shrink` 要求提供继续条件；每 Pool 一个前序缩容，暂停/等待条件仍保留名额，后续缩容排队。
显式 topo、内置 Node/Disk/Tier 成员变更和 Pool Load/Unload/Delete，即使对象不相交也会触发缩容暂停。
topo 等真实暂停确认后执行。
所有已受理 topo 结束后才清除系统暂停；Uncertain fence 不视为释放。恢复指令在途再来 topo 时，
旧暂停确认已撤销，新 topo 必须等待新的暂停确认。

TaskSnapshot 的 `topology_blockers` 是当前受理任务的 WorkflowId，不包含已经终态但尚未对账的 fence。
此时仍可从系统暂停标记和 SchedulingSnapshot.fenced 看出存在未解阻塞。

运行 `cargo run --offline --locked -p storage-pool --example shrink_topology` 查看完整指令/应答示例。
真实磁盘流程与接入边界见 [缩容流程](shrink-workflow.md)。
