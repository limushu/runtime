# 重建：Pool 内通用 Workflow 管理者

本决策取代同日 `pool-process-manager` 的职责划分。用户明确要求：
对象互斥集中在 Pool 内的通用 Workflow 管理者；每个领域是完整 Service，
有独立 Runtime，提供 Workflow 步骤，不再自行仲裁对象冲突。

## 验收标准

- ProcessManager 不导入 Disk、VD、Node 类型，不分发磁盘命令；任意 async Workflow 可提交。
- Workflow 声明对象集合；管理者统一原子准入，冲突排队，不冲突并发；Pool 之间隔离。
- 第一版锁作用域为整个 Workflow；支持命名空间、单对象/集合、共享/独占。
  不支持运行中扩锁，避免多对象死锁；空间分配保守声明整个 disk 集合独占。
- 每个领域拥有自己的消息入口、Future driver、启停和查询；不含对象锁或资源预约队列。
- 领域 Activity 通过管理者发放的执行上下文调用；边界校验 Pool 和已声明对象，
  校验不是第二套仲裁。不得直接公开绕过许可的写入口。
- Workflow 用顺序 async 代码编排 Activity 和计时等待；Activity 在领域 Runtime 内执行。
- 取消只发信号；已发出的 Activity 停稳后才释放对象。Workflow panic、丢弃 Activity
  等待 Future 或忽略 Activity 错误，也不能提前释放对象或清除不确定性。
- Pool 停止先停止 Workflow，再停止领域 Runtime；暂停管理者不等同于暂停领域服务。
- 不相关的实验文件、依赖改动保留，不纳入本次提交。

## Temporal 语义参考与范围

- [Workflow](https://docs.temporal.io/workflows)：流程代码组织步骤。
- [Activity](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/activities/workflow-activity.mdx)：步骤单独执行并返回结果。
- [Cancellation](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/workflow/cancellation-and-termination.mdx)：协作式取消。

对象锁是此项目的本地应用语义，不称为 Temporal 内置机制。
本次不是接入 Temporal，也不实现历史持久化、确定性重放、跨进程恢复、自动补偿。
不盲目重试写操作；领域保留已确认的安全重试和提交后发布规则。

## 实现与验证

- ProcessManager 只依赖通用任务、执行上下文和对象声明；旧 DiskWorkflow、
  ProcessCommand 磁盘枚举、领域 Admission 和第二套 runtime/workflow 注册器已移除。
- Pool 启动管理者、Disk、VD 三个独立 Runtime；ActivityHandler/ActivityService
  作为可复用领域宿主，具有独立消息、Future driver、取消、查询和观测。
- 通用对象准入采用完整集合、FIFO、共享/独占和命名空间；领域客户端验证许可覆盖，
  不竞争锁。Context 不可 Clone，Activity 请求持有停稳追踪票据。
- 具体磁盘流程移到 src/workflows.rs，保留提交后发布、部分结果、明确安全重试、
  Pool/版本绑定的排空凭据；错误不明统一在管理者保留恢复阻塞。
- 强制回收不是持久化恢复；完整 BG/Node 业务尚未实现。新增流程不需要修改管理者。

实测（2026-10-14）：

- 29 项正式库测试通过：通用非磁盘任务、同对象串行/异对象并行、多对象原子准入、
  FIFO、共享/集合锁、跨命名空间/Pool 隔离、缺失/跨 Pool 许可拒绝、独立 Runtime
  启停和观测、Activity 响应丢弃/未知错误/panic/worker abort 下的停稳与恢复阻塞、
  有界普通终态、预提交可见性、BLK 部分进度与重试、凭据版本和安全取消交接。
- cargo test --all-targets 通过；本地未跟踪实验的 8+10 项测试仍通过，不作为本次新增能力。
- cargo clippy --all-targets -- -D warnings 与 rustfmt --check 通过。
- cargo run --example pools、--example member_disk 通过；可见独立领域 Runtime 观测。
- 根项目 npm run verify 通过（Harness、lint、TypeScript、生产构建）。
- 推送前以暂存区独立副本复验，排除本地实验和未提交依赖的影响。

本机 GNU 工具链需要 ASCII sysroot：
`RUSTFLAGS=--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu`；
使用已有离线缓存，无新增依赖。GNU 工具链没有 rustfmt 组件，格式检查使用已有默认 rustfmt。
