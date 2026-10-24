# 文档导航

当前代码只有两个 workspace 成员，入口以本页和根 README 为准。

- [当前架构与目录边界](architecture.md)
- [PoolManager 当前实现与限制](pool-manager.md)
- [PoolManager 接口、多索引与创建/删除设计](pool-manager-design.md)（含后续持久化目标）
- [TaskCenter TUI：任务列表、Activity 时间线与历史](task-center-tui.md)（界面建议）
- [TaskCenter 五图导读：先总览，再展开执行机制](runtime-guide.md)
- [TaskCenter 动态演示：一次完整磁盘上线](diagrams/task-center-simulation/index.html)（离线教学模拟）
- [动态演示范围、使用与验证](changes/2026-10-22-task-center-simulation.md)
- [TaskCenter 关键组件协作图（含缩放版）](runtime-collaboration.md)
- [TaskCenter 冲突规则开发说明](task-center.md)
- [统一 Task 与暂停/恢复契约、验收记录](changes/2026-10-24-unified-task-controls.md)
- [TaskCenter 重构规格与证据](changes/2026-10-15-task-center.md)
- [构建与验证](verification.md)
- [本次目录迁移规格与证据](changes/2026-10-15-workspace-layout.md)
- [基础库接口与语义](../crates/workflow-foundation/README.md)
- [存储池业务与限制](../crates/storage-pool/README.md)
- [历史设计归档](archive/legacy/README.md)：用于追溯，不作为当前 API 或目录说明。

各 crate 的 DESIGN.md 保留分阶段设计及历史验证记录；不能把早期阶段的测试数量、
未实现范围或源码路径当作最新状态。当前验证命令见 verification.md。
