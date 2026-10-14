# Pool / ProcessManager 正式接入

状态：已实现并完成本地验证（2026-10-14）。正式库已接入，不再停留在独立示例。

## 范围与验收

- 正式导出 PoolManager、Pool、ProcessManager、DiskService；不是示例或类型别名。
- 每个 Pool 一个 Service Runtime 根执行单元，独立流程目录与领域状态。
- ProcessManager 拥有生命周期流程排队、替换/取消策略、整体结果；Runtime 仅复用机械执行。
- DiskService 独立保护每盘修改与空间分配/释放互斥；直接调用领域能力也不能绕过保护。
- 空间操作不再因另一个盘的流程等待而被应用层全局锁阻塞；按领域实际动作仲裁。
- Workflow 调用类型化领域能力，Activity 不再拥有领域对象副本。
- PoolManager 按 Pool ID 路由，拒绝重复 ID、未知 Pool 和错属成员；同盘名在不同 Pool 内隔离。
- VD 排空结果绑定 Pool 实例、磁盘和领域版本；状态变化后拒绝过期凭据，其他 Pool 不可复用。
- 取消必须停稳，已提交成功优先；不明结果/panic/强制 drop 在领域侧留下恢复阻塞。
- Pool 停止等在途流程和领域动作停稳，不影响其他 Pool；Pause/Drain/Abort 保留现有含义。
- 既有契约测试迁移到 ProcessManager 正式入口，新增双 Pool、直接领域调用、资源竞争与凭据测试。
- Rust 全目标测试、Clippy、示例、npm run verify 通过。

## 不包含

持久化历史、切主恢复、完整 BG 调度/需求合并、NodeService 拓扑模型、全局共享缓存资源协议。
VdService 仅封装现有排空端口与凭据，PoolNodes 仍为显式节点能力端口。
provider 必须绑定目标 Pool；元数据/数据面实际隔离与引用屏障仍由生产适配保证。
独立 process_manager/temporal_process 示例及其未提交依赖继续保留，不纳入正式库能力。

## 实现与验证

实现摘要：

- 新增 src/pool.rs：Pool 装配、归属校验、PoolManager 路由/根任务所有权与稳定移除。
- 将完整操作协议、业务流程和既有 21 项契约测试移到 src/process_manager/。
- member_disk/service.rs 改为真正的 DiskService，不再实现应用流程宿主。
  effects.rs 私有拥有目录与提交，admission.rs 保护资源；强制 drop 也保留恢复阻塞。
- vd.rs 提供排空能力适配；线性 DrainTicket/DrainProof 绑定 Pool 实例、盘和提交版本。
  本地版本检查不冒充生产引用屏障，不持有跨域事务锁。
- Runtime 仍只驱动 Future、服务生命周期与通用执行记录；每 Pool 一个根 task。
  正常工作流中的领域 async 方法在该根 driver 上执行，无每盘 task。
- 新增 examples/pools.rs，迁移 examples/member_disk.rs 到正式 Pool/ProcessManager API。
- ProcessSnapshot 显式包含 Pool ID、流程结果、Disk/VD 恢复阻塞。
- 修复直接领域调用丢弃批量 close leader 时跟随者可能悬挂的路径，加入回归测试。

验证证据：

- cargo test --all-targets：正式库 32 项通过（原有 21 项 + 分层/隔离 11 项）。
  本地未提交实验另外 8 + 10 项回归通过，不计为正式库能力。
- cargo clippy --all-targets -- -D warnings：通过。
- cargo run --example pools：通过；Pool A 盘 Removed、Pool B 同名盘 UpActive；
  停止 A 后 B 仍成功分配 2 个 BLK。
- rustfmt --edition 2024 --check src/lib.rs examples/member_disk.rs examples/pools.rs：通过。
- JurisDesk 根目录 npm run verify：Harness、lint、TypeScript、构建通过。

使用已有 GNU 工具链和 ASCII sysroot；最终 Cargo 验证使用 --offline。
推送前另外以暂存区独立副本复验，避免依赖未提交实验或 Cargo dev-dependencies。

新增测试覆盖双 Pool 同名盘隔离、停止/重建后的旧句柄拒绝、Pool 身份边界、领域直调
分配互斥、磁盘操作等待空间提交、丢弃领域 Future 的阻塞、丢弃批量 leader 的结果、
过期/跨实例排空凭据、停服等待领域动作、长流程等待期间其他盘分配、直接领域 Pause，
以及取消 PoolManager.remove 不隐式释放根任务。
