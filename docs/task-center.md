# 如何阅读与扩展 TaskCenter

## 先看三个入口

1. [conflicts.rs](../crates/storage-pool/src/task_center/conflicts.rs)：业务冲突判断。
2. [mod.rs](../crates/storage-pool/src/task_center/mod.rs)：队列、占用、停稳交接。
3. [tests.rs](../crates/storage-pool/src/task_center/tests.rs)：不需要运行真实领域服务的协调测试。

没有策略 trait、OperationSelector、RuleRelation 或规则优先级。规则在同一个具体
TaskCenter 的 conflict 方法中，是普通 Rust 业务代码。

## 四个明确结果

| 结果 | 意义 |
| --- | --- |
| Independent | 对象集合无交集，可并行；不再用 Queue 表示无冲突 |
| Wait | 有冲突，按接受顺序等待；后来的重叠任务不能插队 |
| CancelThenWait | 请求旧任务取消，但等其实际终态后才放行 |
| Reject(reason) | 拒绝本次提交，不取消任何已有任务 |

判断顺序是：对象关系 → 节点变更序号 → 重复请求 → 具名业务情况 → 默认等待。
同一节点的新序号替换旧序号；旧序号/相同序号的在途节点变更拒绝。
相同操作的重复任务拒绝，调用者应观察原 Run；本轮没有合并结果功能。

业务关系来自 Workflow 声明的完整对象集合：节点与磁盘通过共同 node 键建立冲突，
不是靠 namespace 字符串自动推断父子关系。新增操作必须声明其全部影响范围。

## 新增一条规则

假设新增一种流程，需要改变它与已有流程的交接方式：

1. 在 OperationKind 增加业务操作，Workflow 的 operation() 返回该操作与 objects()。
2. 明确所有受影响的 Disk/Node/Volume 对象。若涉及新增对象关系，先修正声明，不靠规则猜拓扑。
3. 在 conflict() 的 match 增加带方向的业务分支，返回四种结果之一和可读 reason。
4. 给正向、反向、不同对象、重复/陈旧请求补表格测试。
5. 对 CancelThenWait 补实际交接测试：旧 Activity 未停稳时新流程绝不能产生副作用。
   同时测试新任务被其他冲突拒绝、容量不足或对象被隔离时，不会误取消旧任务。

例如当前已有分支（不是配置语言）：

```rust,ignore
(OfflineDisk(a), OnlineDisk(b)) if a == b => "disk-down-cancels-disk-up",
```

该分支返回 CancelThenWait；反向 OnlineDisk/OfflineDisk 默认 Wait。
修改冲突处理通常只改这一处和测试，不需要新增 trait、选择器或修改执行驱动。

## 安全交接

TaskCenter 先看完整冲突集合，再通过嵌入的执行驱动同步登记一个未启动的执行。登记失败不发取消；
成功后才取消被替代者，并把新任务放入队列。开始许可由 TaskCenter 持有，调用者不能强行放行。

同一个循环轮询 Workflow Future，确认完成后才清理占用并推进队列。取消请求不等于完成。任何 Uncertain 会保留原对象隔离，
相交等待者得到 Rejected，不扩大隔离到其额外对象。被隔离的新任务不会取消其他活动任务。
正常排队任务可以按 ID 取消；互不相关的对象不受队头阻塞。

## 可观察性

`pool.task(id)`：执行状态、进度、对象、逐 Activity 历史及 conflicts（已有任务 ID、决定、原因）。
`pool.observe_task(id)`：状态/进度/Activity 更新订阅；不会保证保留每个中间事件。
`pool.scheduling()`：TaskCenter 排队、运行、逻辑隔离计数。
`pool.execution()`：Runtime 的底层资源/生命周期计数，不能代替业务队列计数。

conflicts 是准入时的解释，不是实时阻塞列表。普通执行历史有界，已有 observer 可在记录淘汰后
继续观察；不确定记录与隔离保留。本轮没有自动解除隔离或跨进程恢复。

`snapshot.activities.entries` 按调用序号列出每个步骤的名称、服务、资源、状态和时间戳。
例如 OnlineDisk 执行期间可以看到 `node.inspect`、`disk.add` 已完成，`disk.open` 正在执行；
执行完成后，同一 task ID 仍可查到这三步及后续 `disk.set-io` 的结果。
Activity 记录由框架自动产生，TaskCenter 直接透传，不依赖业务 `report_progress()`。
取消请求不等于 Activity 已结束；其终态由实际执行停稳后的结果决定。

`Config::activity_history` 限制每个 task 保留的步骤数（默认 1024），
`activities.total` / `activities.omitted` 表示累计调用与已淘汰数量。
这与 task 级历史上限独立；两者都是当前进程内的历史，不支持重启恢复。
未走到的动态分支不会被列为“未完成步骤”。具体语义见
[Workflow Foundation 逐 Activity 观测](../crates/workflow-foundation/README.md#逐-activity-观测)。
