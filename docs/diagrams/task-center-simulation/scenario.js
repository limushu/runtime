/* Offline teaching scenario. No storage calls, timers, or network requests. */
(function (root) {
  "use strict";
  const clone = value => JSON.parse(JSON.stringify(value));
  const steps = [
    { name: "检查节点", command: "NodeCommand::Inspect", service: "node", key: "node:N1", access: "Read", effect: "读取节点的已确认视图：节点已加载且连接，允许上线磁盘。" },
    { name: "加入磁盘", command: "DiskCommand::Add", service: "disk", key: "disk:D1", access: "Write", effect: "提交 AddDisk 元数据并登记成员磁盘；初始 IO 仍为 Down。" },
    { name: "打开磁盘", command: "DiskCommand::Open", service: "disk", key: "disk:D1", access: "Write", effect: "模拟 data_plane.open_disk() 成功。Open 不修改 IO 状态，也不是模型中的独立 open 字段。" },
    { name: "启用 IO", command: "DiskCommand::SetIo(Up)", service: "disk", key: "disk:D1", access: "Write", effect: "模拟数据面启用 IO，确认元数据提交，再更新成员磁盘 IO 为 Up。" }
  ];
  let state = {
    workflow: "尚未提交", future: "未创建", owners: [], queue: [], waiting: [], held: [], permit: "无",
    pending: 0, step: -1, done: 0, services: { node: "空闲", disk: "空闲", vd: "未参与" },
    disk: { member: false, openConfirmed: false, io: "—" }, ready: false, finalized: false
  };
  const frames = [];
  function add(id, title, explain, nodes, routes, change, options = {}) {
    state = clone(state);
    if (change) change(state);
    frames.push({ id, title, explain, nodes, routes, duration: 1550, ...options, state: clone(state) });
  }
  add("intro", "从一个上线请求开始", "示例：Pool P1 / 磁盘 D1 / 节点 N1。假设节点已加载且连接、无竞争流程、所有步骤成功。点击播放；首次 Acquire 后会自动停下讲解。", [], [], null);
  add("submit", "请求进入 TaskCenter", "PoolHandle 提交 OnlineDiskWorkflow。一次完整上线是一个 Workflow，Inspect、Add、Open、SetIo 是它的步骤。", ["caller", "state"], ["submit"], s => { s.workflow = "待准入"; });
  add("admit", "检查冲突，登记 Future", "State 检查对象集合 node:N1、disk:D1 和容量。成功后 driver.defer() 同步登记 Future；当前仍等待启动许可。", ["state", "futures"], ["defer"], s => { s.workflow = "WaitingObjects"; s.future = "等待 StartPermit"; });
  add("start", "对象获准占用，发出 StartPermit", "无冲突，State.pump() 放行。对象占用属于完整 Workflow，在各 Activity 之间也不会释放。", ["state", "futures"], ["defer"], s => { s.owners = ["node:N1", "disk:D1"]; s.future = "Ready · 待轮询"; s.ready = true; });
  add("poll", "Driver 第一次轮询 Workflow", "TaskCenter 的同一循环调用 driver.tick()。running.next() 轮询就绪的 Future，开始 workflow.run(ctx)，没有为 Workflow 另起 task。", ["loop", "tick", "futures", "context"], ["tick", "poll", "run"], s => { s.workflow = "Running"; s.future = "Polling"; s.ready = false; }, { duration: 2200 });
  steps.forEach((step, i) => {
    const prefix = "s" + i + "-";
    const claim = { key: step.key, access: step.access };
    step.startFrame = frames.length;
    add(prefix + "enter", "步骤 " + (i + 1) + " / 4 · " + step.name, step.command + " 经 ctx.activity() 执行。通过安全边界检查后，使用该服务声明的资源访问集合。", ["context"], [], s => { s.step = i; s.future = "Polling"; s.workflow = "Running"; s.permit = "无"; });
    add(prefix + "acquire", "Acquire 入队，Workflow 等待许可", "Broker 发出 " + step.access + "(" + step.key + ")。acquire().await 等待 oneshot：当前 Future 为 Pending，但整个 TaskCenter 没有被阻塞。", ["context", "broker", "queue"], ["acquire", "enqueue"], s => { s.queue = ["Acquire " + step.key]; s.workflow = "WaitingResources"; s.future = "Pending · 等许可"; }, { checkpoint: i === 0, duration: 2300 });
    add(prefix + "resource", "切换到资源事件分支", "资源队列已就绪。同一个 tick() 的 events.recv() 分支取得 Acquire，调用 LockTable.event()，加入 waiting 并尝试 pump。", ["loop", "tick", "queue", "locks"], ["events", "arbitrate"], s => { s.queue = []; s.waiting = [claim]; }, { duration: 2100 });
    add(prefix + "grant", "LockTable 判定可以放行", "没有冲突或隔离：从 waiting 移到 held，准备 Permit。为便于观察，这里把一次 pump 内的微操作拆帧；不是额外的调度边界。", ["locks"], [], s => { s.waiting = []; s.held = [claim]; s.permit = "准备发放"; });
    add(prefix + "wake", "Permit 返回，Waker 发出唤醒", "oneshot 发送 Permit，唤醒等待链。FuturesUnordered 标记该 Future 可再次轮询，并向外传播唤醒。此时业务步骤还没有继续执行。", ["locks", "context", "futures"], ["permit", "wake"], s => { s.future = "Ready · 待轮询"; s.ready = true; s.permit = "已发送，待接收"; }, { duration: 2300 });
    add(prefix + "repoll", "再次 poll，Workflow 才能继续", "Tokio 调度 TaskCenter，Driver 再次轮询就绪的 Workflow，取得 Permit。Waker 不直接执行 Workflow；running.next() 也还没有返回完成项。", ["loop", "tick", "futures", "context"], ["tick", "poll", "run"], s => { s.future = "Polling"; s.ready = false; s.permit = "Workflow 已取得"; }, { duration: 2100 });
    add(prefix + "dispatch", "Guard 随请求进入 " + (step.service === "node" ? "Node" : "Disk") + " 服务", "Tracker 注册 ActivityGuard（pending +1）。ActivityClient 发送请求、取消上下文与 Guard；资源许可跟随实际执行，而不是调用者等待的寿命。", ["context", "client", step.service], ["dispatch", "request-" + step.service], s => { s.pending = 1; s.permit = "由 ActivityGuard 持有"; s.services[step.service] = "邮箱中 1"; s.workflow = "Activity"; s.future = "Pending · 等结果"; }, { duration: 2200 });
    add(prefix + "execute", "领域服务独立驱动这一步", step.effect + " 服务宿主轮询自己的 Activity Future；TaskCenter 此时仍可以接收其他请求。", [step.service], [], s => { s.services[step.service] = "执行中"; }, { duration: 2000 });
    add(prefix + "settle", "步骤停稳，释放 Guard", "服务已确认步骤结果。Guard settled/drop：Tracker pending -1，并投递 Release。此时资源表尚未处理 Release，因此 held 暂时仍为 1。", [step.service, "tracker", "queue"], ["notify-" + step.service, "release-" + step.service], s => {
      s.pending = 0; s.queue = ["Release " + step.key]; s.services[step.service] = "空闲"; s.permit = "已 Drop，待资源表回收";
      if (i === 1) { s.disk.member = true; s.disk.io = "Down"; }
      if (i === 2) s.disk.openConfirmed = true;
      if (i === 3) s.disk.io = "Up";
    }, { duration: 2200 });
    add(prefix + "reply", "结果返回，再次唤醒 Workflow", "服务通过独立 oneshot 返回结果。Workflow 现在可被轮询；本演示随后先处理已排队的 Release，再消费步骤结果。", [step.service, "client", "futures"], ["reply-" + step.service, "result-wake"], s => { s.future = "Ready · 待轮询"; s.ready = true; }, { duration: 2000 });
    add(prefix + "release", "Driver 回收步骤资源", "Driver 的资源分支处理 Release，移除 held。本场景没有其他等待者。State 中的 Workflow 对象占用仍保留，不能与步骤锁混淆。", ["tick", "queue", "locks"], ["events", "arbitrate"], s => { s.queue = []; s.held = []; s.permit = "无"; });
    add(prefix + "return", "Workflow 取得结果，推进编排", "Driver 再次 poll，ctx.activity() 返回成功结果。" + (i === 0 ? "检查 node.serving() 为真。" : "") + (i < 3 ? "接下来进入下一个 Activity；这不是一个新的 Workflow。" : "四个步骤全部结束，准备统一收尾。"), ["tick", "futures", "context"], ["poll", "run"], s => { s.future = "Polling"; s.ready = false; s.workflow = "Running"; s.done = i + 1; });
  });
  add("settling", "流程收尾：等待全部步骤停稳", "Workflow 包装层进入 Settling，Tracker 确认 pending=0。本次没有不确定结果，可以发布成功终态。", ["context", "tracker"], ["settle"], s => { s.workflow = "Settling"; });
  add("complete", "Workflow 完成，返回最终结果", "Completion 发布 Completed 和 Run.result()。直到此时，running.next() 才返回 Ready(Some(id))，Driver 记录完成任务。", ["tracker", "futures"], ["complete"], s => { s.workflow = "Completed"; s.future = "已完成"; s.finalized = true; });
  add("reap", "对象交接结束 · 磁盘已上线", "下一轮 State.reap()/pump() 回收旧流程对象占用，并可放行后继。D1 已是成员磁盘，Open 调用成功，IO 为 Up。整个过程为离线教学模拟，未执行真实存储操作。", ["state", "caller"], ["reap"], s => { s.owners = []; }, { duration: 2100 });
  function freeze(value) {
    if (value && typeof value === "object") { Object.values(value).forEach(freeze); Object.freeze(value); }
    return value;
  }
  root.TaskCenterScenario = freeze({ steps, frames });
})(globalThis);
