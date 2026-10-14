# Workflow Foundation — 基础模型与验收

## 范围

全新独立 crate，不依赖旧项目、service-sdk-next 或任何磁盘业务。
Pool 是实例隔离范围；Workflow 组织过程；Coordinator 统一管理执行和对象准入；
领域服务拥有自己的状态与运行单元，通过 Activity 提供能力。
这是 DDD 应用编排基础，不是完整 DDD 框架，也不把 Pool 定义成一个大聚合。

## 契约

- Workflow 有类型化输入（自身字段）与输出；一次执行有稳定 ID 和可观察状态。
- WorkflowContext 支持调用 Activity、等待 Signal、计时和取消检查。
- 每个 Coordinator 绑定一个 Pool 实例，领域 client 绑定同一实例；跨实例调用拒绝。
- 流程对象是逻辑操作的协调范围，整个流程持有；Activity 资源是一次领域动作的
  读写范围，只持有到步骤停稳。两种范围由一个协调者仲裁，但属于不同锁类别。
  它们不是重复加在同一业务字段上的两把锁：前者决定操作能否并行，后者保护实际修改。
- 流程对象一次性申请完整集合；一个 Activity 也一次性申请完整资源集合。
  不允许运行中扩展流程对象、不允许嵌套资源锁、不支持一个 Workflow 并发派发步骤。
  因而本版没有持有部分资源等待剩余资源的锁循环。领域不负责冲突排队。
- 同类冲突请求 FIFO，互不冲突的请求可继续；支持步骤资源共享读/独占写。
- Activity 的资源要求由领域能力声明，调用者不能覆盖。领域依然校验业务不变量。
- Activity 在独立服务运行单元执行，通过消息返回；Workflow 的等待 Future 被丢弃
  不会撤销已接收步骤。对象占用随实际执行存续，而不是随调用者等待存续。
- 取消先发信号，等待已发出步骤停稳；成功提交可以在同时取消时仍返回成功。
  结果不明不能被超时、取消、被忽略的错误或 panic 掩盖，相关占用保留恢复阻塞。
- shutdown 拒绝新流程，取消并等待旧流程，再停止领域服务。取消 shutdown 的等待
  不丢失所有权。强制 Drop/abort 无法保证远端副作用停止，不能宣称是安全恢复。
- 有界请求通道、Signal 通道和在途 Workflow 数量。完成记录由 Run handle 持有，
  不在协调者无限积累。恢复阻塞保留，不提供跳过对账的解锁接口。

## 首阶段验收

1. 两个无存储业务含义的服务，通过一个 Workflow 串联，并返回类型化输出。
2. Signal 可在等待前到达；Timer 和 Signal 等待都可取消。
3. 同流程对象串行、不同对象并行；不同 Pool 的同名对象独立。
4. Activity 的读写资源正确互斥、完整集合原子准入、冲突请求公平排队。
5. Workflow 等待事件时保留流程对象，但释放已经结束步骤的资源。
6. 同一服务可执行互不冲突的多个 Activity；领域没有自己的对象锁队列。
7. 取消、panic、丢弃 Activity 等待和结果不明时，不提前交接占用。
8. 拒绝跨 Pool client、无效配置；shutdown 等待步骤稳定结束；背压有测试。
9. 独立 crate tests、Clippy、格式检查、示例和根项目 npm run verify 通过。

## 非目标

不接入磁盘/VD/Node，不迁移旧代码，不实现通用 Actor 框架、PoolManager 业务、
动态扩锁、子 Workflow、优先级/抢占、自动重试、持久化历史、重放或切主恢复。
不要求一个 Tokio task 等于一个业务对象；运行单元数量只是宿主实现。
核心安全依赖领域正确声明资源和停止语义；这不是对不受信任业务代码的沙箱。
查询读取已提交视图，不提供跨服务事务或多次读取的一致快照。

## 参考与区别

借鉴 Temporal 的 [Workflow](https://docs.temporal.io/workflows) / [Activity](https://docs.temporal.io/activities)
分离，以及结果、消息、计时器推进流程的方式。本项目的对象准入是自行设计的本地语义。
第一版不声称具有 Temporal 的 durable execution 或 exactly-once 效果。

## 验证记录

2026-10-14 实测：

- 新 crate 独立编译，Cargo.toml 无旧 crate/path 依赖；未修改旧源码。
- `cargo test --all-targets --locked --offline`：26 项集成测试通过；含四线程、
  64 个并发请求的同资源互斥验证。全部集成测试额外连续重复 10 轮通过。
- `cargo clippy --all-targets --locked --offline -- -D warnings`：通过。
- `rustfmt --edition 2024 --check src/lib.rs tests/foundation.rs examples/two_services.rs`：通过。
- `cargo run --locked --offline --example two_services`：输出 `item-7`，最终 Workflow
  为 Completed，协调者和领域服务均正常 Stopped。
- 根项目 `npm run verify`：Harness、lint、TypeScript、Vite 生产构建通过。
- 提交前以暂存区独立副本复验，确保新目录不依赖本地实验文件或旧目录的未提交依赖。

本机默认 MSVC 链接环境不可用，Rust 检查使用已有 GNU 工具链：
`cargo +stable-x86_64-pc-windows-gnu`，并设置
`RUSTFLAGS=--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu`。
使用已有离线 registry 缓存，格式检查使用已安装的默认 rustfmt。

## 2026-10-15 扩展：应用策略与协作暂停

- 新增 AdmissionPolicy<D> 扩展点，应用自己定义描述和冲突原则，Runtime 只执行决定。
  所有准入决定串行、完整评估；容量或任一拒绝时不取消旧工作；策略 panic 拒绝本次输入。
  CancelExisting 只发取消，不早释对象、不越过旧 Activity 的停稳，也不解除不确定阻塞。
- 新增 pause / wait_paused / resume / safe_point。请求和确认分离；不抢停在途 IO，
  在下一执行边界确认暂停。暂停保留流程对象，完成步骤释放资源；资源等待可撤回。
  Signal 有界缓存，Timer 截止时间继续流逝；取消和 shutdown 优先唤醒暂停。
  最终成功已完成时不伪造暂停或取消结果；控制操作是进程内语义，不是崩溃恢复。
- 业务方的领域决策读取通过只读 Activity；外部查询仍可独立观察。执行结果尚无持久历史。
- 本轮原 26 项 + 新 7 项测试共 33 项通过，全部额外重复 10 轮通过；Clippy、rustfmt、
  根项目 npm run verify 通过。新 storage-pool 的 26 项业务集成测试也全部通过。
- 基础库仍然没有 Disk/VD/Node 业务依赖；Pool-specific policy 在相邻业务 crate 实现。
