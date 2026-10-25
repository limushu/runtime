# Task 队列与不可中断 Activity

## 业务约束与验收

同一节点的上线 A、离线 B、上线 C 不能合并掉 B：B 必须执行旧连接失效步骤，后续重建可以取消。

- 一个 current，后续按创建顺序排队，只有队首能影响 current；无关对象继续并行。
- 排队 B 不能被 C 策略取消。成为 current 后 B 必须进入业务体，不能被启动入口检查跳过。
- 不可中断 Activity 在取消先到、等待资源、已经派发时均等待真实结果；不暴露 Task 控制给领域。
- 暂停确认发生在结果返回后的安全边界，恢复不重做已经完成的必做步骤。
- 重建等普通步骤仍协作取消，后继等待真实停稳。Uncertain 继续隔离后继。
- 持久化接收失败不能影响旧任务；未确认接收的条目保留队列位置。
- 用户取消队列项、宿主关闭与切主保留原语义；不可中断不屏蔽失主。

## 实现

`scheduling.rs` 从现有任务集合计算队首及取消/暂停/等待/放行。`TaskCenter` 删除
replacements 名单，inspect 更名 check_submission；统一在 pump 应用调度结果。
`driver/execution.rs` 标记首次业务入口，`RunControl::has_started` 为策略取消提供边界。
`workflow.rs` 的 activity 返回可配置且仍实现 Future 的调用对象，原 `.await` 调用不变；
`.uninterruptible()` 使用独立的服务控制 token、仍继承宿主退出信号。

节点观察和离线失效/关 IO 前缀选用受保护调用，重建不选用。真正网络断链接口及物理断电尚未
实现，本轮没有将它们伪装为成功。非持久化任务不能跨崩溃保证必做步骤，持久化任务依赖显式
checkpoint 和领域幂等，不提供 exactly-once。已知业务失败仍返回原错误，Uncertain 才自动隔离。

## 验证

- `storage-pool/tests/task_queue.rs`：三次切换、普通重建抢占、不确定结果隔离。
- `storage-pool/tests/storage.rs`：真实领域关 IO 前缀不再被队列中的后继跳过。
- `storage-pool/tests/scheduling_policy.rs`：队首暂停原因交接、拒绝及持久化失败无副作用。
- `workflow-foundation/tests/scheduling.rs`：队首独占控制、多对象排队、持久化接收位置、独立并行。
- `workflow-foundation/tests/uninterruptible.rs`：取消先到、资源等待、暂停恢复、宿主关闭。

2026-10-25 验证通过：

- `./scripts/verify.ps1 -Toolchain stable-x86_64-pc-windows-gnu -Offline`：165 项测试、全部示例编译、文档测试、fmt、Clippy（-D warnings）。
- 外围工程 `npm run verify`：Harness、lint、TypeScript 与生产构建。
- `git -c core.safecrlf=false diff --check`：通过。

新增的真实节点三次切换测试确认：B 排队时未被取消；B 执行期间 observation=3、connected=false、
loaded=false，C 尚未进入业务体；关 IO 完成后 C 才以 observation=4 重新加载并上线。
