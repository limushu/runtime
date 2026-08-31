# control-runtime

业务无关的进程内 Service Runtime。它只统一执行机制，不包含 Disk、Node、VD、BG 等业务知识。

## 开发者只需要理解三个入口

```rust
#[async_trait]
impl Service for ExampleService {
    type Request = ExampleRequest;
    type WorkflowKind = ExampleWorkflow;

    fn route(&self, request: &ExampleRequest) -> RequestRoute<ExampleWorkflow> {
        // Query 使用 Untracked；长流程返回对象键和 Workflow 类型。
    }

    fn admit(
        &self,
        context: &WorkflowContext,
        request: &ExampleRequest,
        activity: &ObjectActivity<ExampleWorkflow>,
    ) -> RuntimeResult<Admission<ExampleReply>> {
        // 只回答 Start / Join / Queue / Replace / Complete / Reject。
    }

    async fn handle(
        &self,
        request: ExampleRequest,
        context: WorkflowContext,
    ) -> RuntimeResult<ExampleReply> {
        // 普通的 async 业务代码，不构造 TaskSpec 或 Future 闭包。
    }
}
```

`route` 决定请求是否进入对象准入；`admit` 声明业务冲突关系；`handle` 执行自然工作流。Task、Future 集合、channel、oneshot 和取消传播全部由 Runtime 管理。`ServiceRequest` 不包含目标服务，调用者必须持有目标实例的显式 `ServiceClient<R>`；领域代码通常再用一个只有命名方法的 facade 隐藏命令枚举。

## 运行模型

- 每个 Service 一个根 Tokio task；
- 业务通道与控制通道分离；
- `FuturesUnordered` 在根 task 中统一 poll 所有 Future；
- `ObjectSlot` 原子维护同一对象的 active、replacement 和 queue；
- 相同意图的多个调用者只是共享 Workflow 的订阅者；
- 最后一个订阅者离开时，根据 `OrphanPolicy` 取消或继续；
- `ServiceClient::call` 等待下游进入稳定结果，并在返回父 Workflow 前统一传播取消；普通 Workflow 不逐步轮询取消；
- 领域执行器只在真正的原子工作单元边界解释取消，例如完成当前 BG 后停止继续调度；
- `StateCell` 不暴露锁 Guard，业务无法跨 `.await` 持锁；
- 业务 panic 只失败当前请求，不会杀死整个 Service 根 task；
- 活动 Workflow、Untracked Future 和内部等待队列都有显式上限。

## 源码结构

```text
src
├── protocol.rs       # ServiceId、ObjectKey、ServiceRequest 等通用原语
├── context.rs        # 因果与结构化取消
├── client.rs         # 指向一个明确 Service 实例的类型化调用
├── observation.rs    # Operation、Call、Task、Service 观测
├── state_cell.rs     # 不暴露锁 Guard 的私有状态容器
└── service
    ├── contract.rs   # RequestRoute、Admission、Service
    ├── container.rs  # RuntimeConfig、控制句柄和 Service 所有权
    ├── service_loop.rs # 根 task、业务通道和统一 poll
    └── object_slot.rs  # 虚拟对象意图槽位
```

`ObjectSlot` 只拥有执行意图的 `Idle/Pending/Running/Cancelling`、订阅者、替代请求和排队，不拥有领域状态。逻辑领域 Actor（例如 MemberDisk Actor）仍由业务模块实现，二者不可合并为第二套业务状态机。
