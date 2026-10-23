# TaskCenter 任务历史与可选续办

本文保留已对齐需求的设计；第一版已落地，当前接口和限制见 [实现说明](task-persistence.md)。核心对象统一为 TaskCenter 中的 Task：
所有 Task 可以持久查询历史，只有显式声明可续办的 Task 保存恢复所需参数和检查点。
缩容也是同一种 Task，不引入独立 OperationRecord、OperationStore 或业务操作管理器。
本轮范围不包含全局事件中心或任意 Future 的透明恢复。

## 结构

```text
PoolManager：句柄登记与事件分发
  → Pool
      → TaskCenter
          ├─ 当前执行、准入、冲突控制（现有）
          ├─ TaskStore：任务、执行记录、Activity、可选检查点
          ├─ Recorder：生命周期记录的后台写入与 flush
          └─ 续办入口：发现可续办 Task，调用对应业务恢复函数
```

TaskStore 可以共用系统 KV，每个 TaskCenter 使用本池 Pool UUID 的数据范围。
任务查询和续办接口归 TaskCenter，PoolHandle 转发；Manager 不接管任务状态和业务恢复。
事件 ID 只是任务可选来源字段，第一版不另外建设全局事件记录体系。

拟新增文件如下，保持现有两个 crate：

```text
workflow-foundation/src/audit.rs     可选生命周期输出协议，不依赖 KV 或存储业务类型
storage-pool/src/task_center/
  persistence/model.rs              TaskRecord、ExecutionRecord、ActivityRecord
  persistence/store.rs              TaskStore 协议与分页查询
  persistence/recorder.rs            写入队列、批量提交、flush
  persistence/kv.rs                  KV 编码与索引
  recovery.rs                       可续办 Task 的发现、状态更新和业务恢复调用
storage-pool/src/workflows/shrink_disk.rs  缩容流程及其恢复参数/检查点解释
```

## 一个 Task，多次执行

新增持久 TaskId，Task 的整个生命周期保持不变。
现有 WorkflowId / TaskRef 继续标识某个实例内的执行，作为执行记录关联到 TaskId。
切换后不复用旧 RunControl；同一 TaskId 下新增 execution_attempt 和新的 TaskRef。
Activity 按 TaskId、执行代次、步骤序号归属，避免切换前后重复步骤号混淆。

```text
TaskRecord
  task_id / pool_uuid
  name / objects / source_event_id?
  status / revision / created_at / updated_at
  current_execution?
  resume?                         普通任务为空
    kind / schema_version
    input                         可重新构造流程的业务参数
    checkpoint                    最近确认的业务阶段与必要进度
    user_pause_requested

Task #T1：缩容 disk-7
  ├─ execution 1 → 旧 TaskRef → Activity…
  └─ execution 2 → 新 TaskRef → Activity…
```

业务类型决定 checkpoint 的内容，TaskCenter 只保存和调度恢复。
不把任意泛型返回值、运行时客户端或 Future 强制序列化。
恢复类型和版本必须明确；找不到匹配恢复函数时显示“无法续办”，不猜测执行。

## 提交与写入

以下接口仅表达意图，不代表已存在 API：

```text
submit(workflow)                         默认记录历史
submit_resumable(workflow, resume_spec)  额外持久保存续办描述
list_tasks(filter, cursor)               查询本池当前/历史任务
task_detail(task_id)                     查看全部执行和步骤
list_recoverable()                       列举可续办任务
continue_task(task_id)                   续办同一个 Task
```

TaskStore 是统一存储协议，支持创建任务、追加执行/步骤记录、查询历史、
比较 revision 保存检查点、列举待续办任务和 flush；检查点与相关索引采用事务提交。

普通历史从现有生命周期事实处接入：TaskCenter 接受提交，Workflow 开始/结束，
ActivityObservation 创建/开始/结束。由池内适配器附加 TaskId、Pool UUID、执行代次。
不依赖 TUI、observer 或 gather 才留记录；watch 合并更新不能作为完整历史的唯一来源。

普通历史可有界排队、异步批量写入；失败/缺口必须可见，正常退出前 flush。
本轮不要求突然断电时零历史尾部丢失，旧实例中未确认结束的执行标记为中断/结果未确认。
记录版本确保迟到更新不覆盖更新的状态；内存历史淘汰不等于删除 KV 中的历史。

可续办任务先持久保存参数和初始检查点，再允许执行并确认持久接受。
可续办任务在已确认的业务边界调用可等待的 save_checkpoint 接口；保存失败时不进入依赖它的下一阶段。
真实硬盘缩容按阶段推进，一次提交完整排空 job 给均衡模块；不由 Workflow 循环分批迁移 BG，
迁移步骤的暂停/恢复委托同一个均衡 job，见 [缩容与 topo 协调](shrink-workflow.md)。
该接口是新的持久能力，与现有仅检查取消/执行边界的 WorkflowContext::checkpoint() 区分。
检查点落盘通过受控上下文或专用 Activity 接入，Workflow 不直接操作具体 KV。
如持久保存后运行准入失败，保留待执行/受阻状态及原因，不展示虚假的 Running。

## 缩容与切换接入流程

```text
首次提交：
  PoolHandle → TaskCenter.submit_resumable
    → 保存 TaskId、缩容参数、初始检查点
    → 按原有冲突规则准入执行
    → 阶段推进并保存检查点，同时记录 Activity 历史

接管：
  Pool 完成现有加载/接管准备
    → TaskCenter 查询本池未完成且声明可续办的 Task
    → 恢复为待继续，保留用户暂停及待核对状态
    → 调用缩容恢复函数，核对真实进度和旧工作是否已停稳
    → 在原 TaskId 下提交新一次执行，重新经过准入和冲突判断
```

缩容阶段示意：准备 → 停止新增分配 → 迁移数据 → 校验清空 → 移除目标 → 完成。
检查点保存可解释的阶段、已确认游标或下游 job_id，不能仅凭百分比决定继续位置。
历史记录不驱动普通 Task 自动重跑；仅可续办 Task 进入恢复流程。
恢复出的冲突占用须在开放相关新业务前建立，避免续办与新请求同时修改目标。

计划切换先在安全边界停稳并确认检查点已保存。切换导致一次执行结束，不等于用户取消整个 Task。
同一 Task 只允许一次当前执行，复用平台已有接管约束并用 Task revision 防止重复续办。
无法确认某步业务是否完成时，该 Task 显示“待核对”；恢复函数先查询事实，不盲目重复迁移或删除。
用户主动暂停的 Task 保持暂停。最终完成状态由业务结果决定，不将中间执行结束一律视为任务完成。
切换前后需读到同一份 KV，Pool 和领域状态沿用系统已有加载/接管流程。

## 现有 TUI

Pool → 任务列表 → Task → 执行记录 / Activity，保留 Running / History 分区。
同一 Task 的切换前后执行在详情里展示，不显示成两件无关的缩容事项。
可续办任务额外显示“待继续/恢复检查中/暂停/待核对”，控制仍通过 TaskCenter。
历史页分页读取 TaskStore，普通历史仅提供查询，终态任务不能通过“继续”重新发起。

## 最小落地顺序

1. 为 TaskCenter 接入 TaskStore 与生命周期留痕，支持历史分页和详情查询。
2. 增加可选 resume 描述、检查点保存及 TaskId 与执行代次关联。
3. 用缩容模拟任务验证切换后发现原 Task、续办新执行、关联前后 Activity、维持暂停状态。

本文保留设计目标；当前实现范围见 [任务持久化](task-persistence.md) 与 [完整缩容工作流](shrink-workflow.md)。
shrink_commands 仍只是进程内控制示例；正式 shrink_disk 已支持指定任务续办，真实复制由注入的均衡模块实现。
