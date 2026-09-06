# Pool Control Plane Prototype

这是 Monitor Pool 管控面的当前 Rust 原型。仓库只保留最新的 MemberDisk 纵切面和与其
一致的架构文档，不包含此前的通用 Runtime、Actor 或多 crate 实验实现。

## 入口

- 当前实现：[`src/member_disk/`](src/member_disk/)
- 架构与业务基线：[`docs/README.md`](docs/README.md)
- 对象协调决策：[`src/member_disk/service/reconcile.rs`](src/member_disk/service/reconcile.rs)
- 具体业务行为：[`src/member_disk/service/operations.rs`](src/member_disk/service/operations.rs)
- 单根 Future 驱动：[`src/member_disk/service/runtime.rs`](src/member_disk/service/runtime.rs)

## 验证

```bash
cargo fmt --check
cargo test --offline
cargo clippy --offline --all-targets -- -D warnings
```
