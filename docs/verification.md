# 构建与验证

在仓库根目录执行，只有一份 Cargo.lock：

```sh
cargo fmt --all --check
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo run --locked -p workflow-foundation --example two_services
cargo run --locked -p storage-pool --example pool_lifecycle
cargo run --locked -p storage-pool --example shrink_commands
cargo run --locked -p storage-pool --example task_center
cargo run --locked -p storage-pool --example shrink_disk
```

可选 KV 适配器与持久任务续办还需验证：

```sh
cargo test --workspace --all-features --all-targets --locked
cargo clippy --workspace --all-features --all-targets --locked -- -D warnings
cargo run --locked -p storage-pool --features kv-redb --example task_persistence -- /tmp/tasks.redb
```

默认构建不启用 redb，可注入自己的 TaskStore。持久化示例执行模拟迁移并写入指定的本地 KV 文件。

Activity 控制与缩容/topo 协调可单独验证：

```sh
cargo test --offline --locked -p storage-pool --all-features --test topology_shrink
cargo test --offline --locked -p storage-pool --all-features --test shrink_disk
cargo run --offline --locked -p storage-pool --example shrink_topology
```

shrink_topology 只验证控制协议。shrink_disk 调用正式工作流，通过模拟均衡器提交实际元数据 remap、
核验引用并提交 Removed；不执行物理复制。测试还覆盖取消、容量等待、Pool 卸载/加载、切换续办和不确定提交隔离。

PowerShell 可执行 `./scripts/verify.ps1`，依次检查格式、测试、文档测试和 Clippy。
支持 `-Toolchain <名称>` 与 `-Offline`；格式检查使用当前安装的 rustfmt。
示例使用内存/模拟适配器，不会连接真实存储。离线重建等待仍是明确失败的占位能力。
缩容必须注入 Balancer 并核验真实引用、分配；不能将未实现能力改成成功以通过验证。

本机没有 MSVC 链接环境时，使用已安装的 GNU 环境：

```powershell
$env:CARGO_HOME = 'D:\Temp\workflow-cargo-5d1cb8373f47436d8471ae7f408614d2'
$env:RUSTFLAGS = '--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu'
./scripts/verify.ps1 -Toolchain stable-x86_64-pc-windows-gnu -Offline
```

这些路径仅为当前开发机的验证记录，不是项目依赖。其他机器安装普通 Rust 工具链即可。
外围 JurisDesk 工程仍单独执行 `npm run verify`，不并入本 Rust workspace。
