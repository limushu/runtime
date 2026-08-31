# control-runtime

业务无关的进程内 Service Runtime。它统一通信、执行和生命周期机制，不包含 Disk、Node、VD、BG 等业务知识。

## 领域开发者的表面

简单状态机只描述下一状态和需要收敛的工作流：

```rust
match (state, event) {
    (Ua, PhysicalDown) => Transition::to(Da).ensure(Offline),
    (Di, PhysicalUp) => Transition::to(Ui).ensure(Online),
    (Ua, PhysicalUp) => Transition::to(Ua),
    (Removed, PhysicalUp) => Transition::to(Removed).reject("explicit rejoin required"),
}
```

工作流是普通异步方法：

```rust
async fn offline_workflow(
    &self,
    disk: MemberDiskId,
    context: WorkflowContext,
) -> RuntimeResult<MemberDiskResponse> {
    self.pool_nodes.publish_member_disk(&context, disk.clone(), Down).await?;
    self.virtual_disks.evacuate_member_disk(&context, disk.clone()).await?;
    self.finish_drain(&context, disk).await
}
```

领域不创建 `TaskSpec`，不包装 `move |task| async move`，不检查运行中的 Actor，也不手工选择 Start/Join/Replace。

## `ensure` 的固定含义

状态机只输出“这个对象现在需要哪种 Workflow”。隐藏的 `ActorCell` 原子推导：

- 对象空闲：启动 Workflow；
- 已在运行同类 Workflow：合并为结果订阅者；
- 正在运行另一类 Workflow：请求旧 Workflow 协作取消，等待稳定退出，再启动新 Workflow。

因此，`(ObjectKey, WorkflowKind)` 必须完整表示一个可共享的收敛目标。参数不同且不能共享结果的写操作必须使用不同 Kind/Key，或选择 `Enqueue`。

排队型的普通对象写操作使用 `RequestPlan::Enqueue`；只读查询使用 `Inline`。这些是 Service 接入 Runtime 的小型适配层，不进入领域状态机。

## 运行模型

- 每个 Service 一个根 Tokio task；
- 业务通道与控制通道分离；
- `FuturesUnordered` 在根 task 中统一 poll 所有 Future；
- 私有 `ActorCell` 管理同一对象的 active、replacement、queue 和订阅者；
- 最后一个订阅者离开时，根据 `OrphanPolicy` 取消或继续；
- `ServiceClient::call` 等待下游稳定返回，并在父 Workflow 继续前传播取消；
- `StateCell` 不暴露锁 Guard，业务无法跨 `.await` 持锁；
- 业务 panic 只失败当前请求，不会杀死 Service 根 task；
- Workflow、普通 Future 和等待队列都有显式上限。

## 源码结构

```text
src
├── protocol.rs       # ServiceId、ObjectKey、ServiceRequest
├── context.rs        # 因果与结构化取消
├── client.rs         # 指向明确 Service 实例的类型化调用
├── observation.rs    # Operation、Call、Task、Service 观测
├── state_cell.rs     # 不暴露锁 Guard 的私有状态容器
├── state_machine.rs  # Transition::to(...).ensure(...)
└── service
    ├── contract.rs   # RequestPlan、Service
    ├── container.rs  # RuntimeConfig、控制句柄和所有权
    ├── service_loop.rs # 根 task、业务通道和统一 poll
    └── actor_cell.rs # 完全私有的对象执行状态
```

`ActorCell` 不拥有业务元数据，也不出现在领域 API 中。领域对象仍是状态的唯一拥有者。
