# Workspace 目录收敛

## 范围与验收

- 基于 codex/storage-workflow-development 的现有实现整理，不回退其他会话的分层与功能。
- 保留 workflow-foundation / storage-pool 两个 package，移至 crates/。
- 删除退役 service-sdk-next、根 src 和其专属根 tests；不留下无效根 package。
- 根 Cargo.toml 为虚拟 workspace，成员共用依赖版本和唯一 Cargo.lock。
- 当前文档与历史理论分开；示例命令、相对链接和验证入口指向新位置。
- 未提交源码及实验先备份到仓库外，不随旧目录删除而丢失，也不自动发布到 GitHub。
- workspace 测试、Clippy、格式、示例及外围工程验证通过；单一提交推送当前分支。

## 实现摘要

领域现有 app/domain/infrastructure 布局不变，公开 crate/module API 不变。
将历史 docs 整体迁到 docs/archive/legacy，配图相对结构保留。新建当前文档导航、
架构及验证说明，旧根 README 被当前 workspace 说明取代。
本次保留本地但未发布的历史草案位于归档 proposals 内，仍保持未跟踪状态。

## 恢复说明

已提交旧实现可从迁移前提交 `7653b7f1d811b513678cbccfd0f2ab27559a5f1b` 获取。
本机未提交源码、原始文档和旧测试已完整转存至仓库外 D:\Temp 下专用备份目录；
包括 driver.rs、ProcessManager 和 temporal_process 实验。备份不参与构建、不上传。

## 验证证据

2026-10-15 实测：

- 根入口 `scripts/verify.ps1 -Toolchain stable-x86_64-pc-windows-gnu -Offline` 通过。
  storage-pool 38 项，workflow-foundation 39 项，共 77 项集成测试；文档测试检查通过
  （当前无可执行 doctest），workspace 格式及 Clippy `-D warnings` 均通过。
- 四个示例：two_services、pool_lifecycle、shrink_commands、coordination_rules 全部运行成功。
  pool_lifecycle 仍明确报告重建占位，未将离线骨架伪装成完整成功。
- 外围 JurisDesk 的 npm run verify 通过（Harness、lint、TypeScript、Vite）。
- 所有已跟踪 Markdown 的本地链接有效，workspace 仅有两个成员和一份 Cargo.lock。
- 新根 Cargo.lock 与迁移前 storage-pool/Cargo.lock 内容一致，没有升级当前实现依赖。
- 迁移前后逐文件 Git 对象哈希比较：业务和基础库 Rust 内容不变；只有两个 example 的
  运行命令注释改为 workspace -p 形式。
- 备份目录为 `D:\Temp\runtime-layout-backup-bfd60c6db7904d50ab0987ac45a16199`，
  已核对 driver.rs 和未提交实验目录仍在，不上传此本地备份。
- 提交前将暂存区独立导出，在不含旧源码和未跟踪草案的副本中运行统一验证脚本，全部通过。
