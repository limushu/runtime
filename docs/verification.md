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
cargo run --locked -p storage-pool --example managed_shrink
cargo run --locked -p storage-pool --example leader_handoff
```

PowerShell 可执行 `./scripts/verify.ps1`，依次检查格式、测试、文档测试和 Clippy。
支持 `-Toolchain <名称>` 与 `-Offline`；格式检查使用当前安装的 rustfmt。
示例使用内存/模拟适配器，不会连接真实存储。离线、缩容等尚未实现的重建/移除步骤
应返回示例明确处理的占位错误，不能将其改成成功以通过验证。

本机没有 MSVC 链接环境时，使用已安装的 GNU 环境：

```powershell
$env:RUSTFLAGS = '--sysroot C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu'
$env:Path = (Join-Path $env:TEMP 'workflow-binutils\mingw64\bin') + ';C:\rustup-ascii\toolchains\stable-x86_64-pc-windows-gnu\lib\rustlib\x86_64-pc-windows-gnu\bin\self-contained;' + $env:Path
./scripts/verify.ps1 -Toolchain stable-x86_64-pc-windows-gnu -Offline
```

这些路径仅为当前开发机的验证记录，不是项目依赖。其他机器安装普通 Rust 工具链即可。
外围 JurisDesk 工程仍单独执行 `npm run verify`，不并入本 Rust workspace。

## 最新运行图

```sh
python scripts/render-unified-task-guide.py
node scripts/verify-unified-task-guide.mjs --render
```

需要 Python、Node 22 和 Chrome（可用 CHROME_PATH 指定）。生成 SVG、离线 HTML 与 PNG，
并检查文字边界、导航、高亮、组件交互、移动端、打印与离线运行。
不传 --render 可仅执行检查。图示对应实现提交固定于导读页，改变执行机制时应同步更新。


动态运行图与 tick 分支检查：

```sh
python scripts/render-runtime-lab-graph.py
node scripts/verify-task-center-simulation.mjs --browser
```

支持可选 --screenshot、--screenshot-cycle、--screenshot-branches、--screenshot-waits 输出 PNG；
每个选项后传输出路径。覆盖事件状态、分支说明、三任务等待环和播放交互。
