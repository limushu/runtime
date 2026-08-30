# control-runtime

业务无关的进程内管控面 Runtime：

- 每个 Service 一个根 Tokio task；
- 业务通道与控制通道分离；
- `FuturesUnordered` 在根 task 中统一 poll 工作流；
- ObjectActor 负责对象级准入、合并、排队和协作式替换；
- Router 提供类型化 oneshot 调用和结构化取消传播；
- TaskAttempt、调用链和 Service 生命周期统一观测。

本 crate 不得依赖 `pool-control-plane` 或任何具体领域实现。

源码结构：

```text
src
├── protocol.rs       # ServiceId、ObjectKey、ServiceRequest 等通用原语
├── context.rs        # 因果与结构化取消上下文
├── router.rs         # 类型化进程内调用
├── observation.rs    # Operation、Call、Task、Service 观测
├── state_cell.rs     # 不暴露锁 Guard 的私有状态容器
└── service
    ├── contract.rs   # Service、WorkflowMeta、ObjectDecision
    ├── container.rs  # Service 根 task、控制通道和统一 poll
    └── object_actor.rs
```

整个 crate 就是 Runtime，因此内部不再建立含义重复的 `runtime.rs` 模块。
