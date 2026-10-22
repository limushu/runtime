/* Presentation only: immutable teaching snapshots, no application runtime. */
(function () {
  "use strict";
  const { frames, steps } = globalThis.TaskCenterScenario;
  const byId = id => document.getElementById(id);
  const put = (id, value) => { byId(id).textContent = value; };
  const reduced = matchMedia("(prefers-reduced-motion: reduce)");
  const nodes = [...document.querySelectorAll("[data-node]")];
  const paths = [...document.querySelectorAll("[data-route]")];
  const details = {
    caller: ["PoolHandle", "请求入口。把完整业务操作提交给所属 Pool 的 TaskCenter；不直接执行领域步骤。"],
    state: ["State · 流程协调", "维护业务冲突规则、排队、容量与 Workflow 对象占用。先检查冲突，再给出 StartPermit。对象占用贯穿完整流程，最终停稳后才交接；不同于每一步的资源锁。本演示没有竞争流程，冲突判断走允许分支。"],
    loop: ["TaskCenter 主循环", "由 Tokio 驱动的一个 task。循环 reap 已完成流程、pump 准入队列，再 select 请求、停止信号和 driver.tick()。Driver 由它持有，不是另一个 task。"],
    tick: ["WorkflowDriver.tick()", "在同一个 task 中选择资源事件分支或 Workflow Future 分支。Acquire 到来时处理 LockTable；就绪 Future 由 running.next() 轮询。一次 tick 处理资源事件后返回外层循环。"],
    futures: ["Workflow Futures", "FuturesUnordered 保存多个 Workflow Future。Waker 只标记可以再次轮询，不执行 Workflow。running.next() 会推进就绪 Future，只有整个 Workflow 完成时才返回 Some(id)。"],
    context: ["WorkflowContext", "Workflow 使用 ctx.activity() 编排步骤：经过安全边界、请求资源许可、注册 Guard、调用服务并等待结果。这里不是独立 task，也不直接修改 LockTable。"],
    broker: ["Broker", "Workflow 与资源驱动器之间的协议端点。发送 Acquire 到事件队列，通过 oneshot 等待 Permit；等待导致当前 Future Pending。图中许可画到 Context，实际接收封装在 Broker.acquire() 内。"],
    queue: ["资源事件队列", "mpsc 管道，传送 Acquire / Release。Broker 申请许可、Permit Drop 投递释放；只有 Driver 消费事件后才更新锁表。入队不等于资源已分配或释放。"],
    locks: ["LockTable", "由 Driver 独占维护 waiting、held 与隔离信息。判断 Activity 资源读写是否兼容，再发 Permit。本演示没有竞争者或隔离，均立即获准。它不负责完整 Workflow 的业务准入。"],
    client: ["ActivityClient", "把命令、取消上下文和 ActivityGuard 放入对应服务邮箱，等待独立 oneshot 返回结果。不是另一个执行 task；执行者是右侧服务宿主。"],
    tracker: ["Tracker / Completion", "追踪真实在途步骤。Guard 结束后 pending 减一，Permit Drop 发出 Release。Workflow 收尾等待步骤停稳，然后发布终态。终态与对象占用回收是两个动作。"],
    node: ["Node 服务", "独立服务宿主 task 驱动自己的 Activity Future。本流程通过 Inspect Activity 读取已确认节点状态，再由 Workflow 检查 serving()；不是绕过步骤的观察 Query。"],
    disk: ["Disk 服务", "独立宿主接收 Add、Open、SetIo。服务通过端口访问元数据和数据面。Open 只确认打开成功，不代表 IO Up；资源许可随 Guard 覆盖真实执行寿命。"],
    vd: ["VD 服务", "拥有独立宿主和领域能力。本次磁盘上线不调用它，保留在图中用于说明服务运行边界。"]
  };
  let cursor = 0, motion = null, playing = false, raf = 0, lastTime = null;
  let checkpointVisited = false, selected = null;
  const chapterButtons = steps.map((step, i) => {
    const button = document.createElement("button");
    button.type = "button"; button.className = "chapter";
    const number = document.createElement("span"); number.className = "number"; number.textContent = i + 1;
    const label = document.createElement("span"); label.textContent = step.name;
    const command = document.createElement("small"); command.textContent = step.command;
    label.append(command); button.append(number, label);
    button.addEventListener("click", () => seek(step.startFrame));
    byId("chapters").append(button); return button;
  });
  byId("timeline").max = frames.length - 1;
  byId("event-title").setAttribute("aria-live", "polite");

  function tags(id, values, className = "tag") {
    const parent = byId(id); parent.replaceChildren();
    for (const value of values.length ? values : ["无"]) {
      const span = document.createElement("span");
      span.className = values.length ? className : "empty"; span.textContent = value; parent.append(span);
    }
  }
  function snapshot() {
    const s = frames[cursor].state;
    put("workflow-state", s.workflow); put("future-state", s.future); put("pending-state", s.pending);
    tags("ownership", s.owners);
    put("lock-count", `waiting ${s.waiting.length} / held ${s.held.length}`);
    tags("resource-state", [
      ...s.waiting.map(r => `等待 · ${r.access}(${r.key})`),
      ...s.held.map(r => `持有 · ${r.access}(${r.key})`)
    ], "resource-entry");
    put("queue-state", s.queue.join("，") || "空");
    put("member-state", s.disk.member ? "已加入" : "未加入");
    put("open-state", s.disk.openConfirmed ? "已成功" : "未执行");
    put("io-state", s.disk.io);
    const labels = {
      caller: cursor ? "OnlineDisk · D1" : "准备提交", state: `对象占用 ${s.owners.length}`,
      loop: "reap / pump / select", tick: "资源事件 | Future", futures: s.future,
      context: s.step < 0 ? "尚未开始" : steps[s.step].command.replace(/\w+Command::/, ""),
      broker: s.workflow === "WaitingResources" ? "等待 Permit" : "协议端点",
      queue: s.queue.length ? s.queue[0].split(" ")[0] + " ×1" : "空",
      locks: `waiting ${s.waiting.length} · held ${s.held.length}`,
      client: s.pending ? "等待服务结果" : "无在途步骤",
      tracker: `pending = ${s.pending} · ${s.finalized ? "Completed" : "尚未发布终态"}`,
      node: s.services.node, disk: s.services.disk, vd: s.services.vd
    };
    Object.entries(labels).forEach(([key, value]) => put("state-" + key, value));
    chapterButtons.forEach((b, i) => {
      b.classList.toggle("current", s.step === i); b.classList.toggle("done", s.done > i);
      if (s.step === i) b.setAttribute("aria-current", "step"); else b.removeAttribute("aria-current");
    });
    byId("timeline").value = cursor;
    byId("timeline").setAttribute("aria-valuetext", frames[cursor].title);
    put("position", `${cursor} / ${frames.length - 1}`);
    put("log-count", `(${cursor})`);
    const log = byId("event-log"); log.replaceChildren();
    frames.slice(1, cursor + 1).forEach(frame => {
      const li = document.createElement("li"); li.textContent = frame.title; log.append(li);
    });
    log.scrollTop = log.scrollHeight;
  }
  function presentation() {
    const frame = frames[motion ? motion.target : cursor], s = frames[cursor].state;
    const color = /-(acquire|resource|grant|release)$/.test(frame.id) ? "resource" :
      /-(wake|reply|settle)$|^(complete|reap)$/.test(frame.id) ? "result" : "execution";
    byId("architecture").setAttribute("data-tone", color);
    nodes.forEach(n => {
      n.classList.toggle("is-active", frame.nodes.includes(n.dataset.node));
      n.classList.toggle("is-selected", selected === n.dataset.node);
      n.classList.toggle("is-pending", n.dataset.node === "futures" && s.future.startsWith("Pending"));
      n.classList.toggle("is-ready", n.dataset.node === "futures" && s.ready);
    });
    paths.forEach(p => p.classList.toggle("active", frame.routes.includes(p.dataset.route)));
    put("frame-count", `事件 ${motion ? motion.target : cursor} / ${frames.length - 1}`);
    put("event-title", frame.title); put("event-explain", frame.explain);
    put("motion-status", motion ? (playing ? "正在演示此事件" : "演示已暂停") : "事件已完成");
    put("live-caption", motion ? `正在演示：${frame.title}` : frame.title);
    put("clock-status", motion ? "状态面板：上一已完成事件" : "状态面板：当前事件之后");
    put("play", playing ? "Ⅱ 暂停演示" : cursor === frames.length - 1 ? "↺ 重新播放" : cursor || motion ? "▶ 继续演示" : "▶ 开始演示");
    byId("play").setAttribute("aria-pressed", String(playing));
    byId("back-step").disabled = cursor === 0 && !motion;
    byId("next-step").disabled = cursor === frames.length - 1;
    byId("checkpoint").hidden = !!motion || !frames[cursor].checkpoint;
    document.body.dataset.playing = String(playing);
    drawPacket();
  }
  function drawPacket() {
    const routes = motion ? frames[motion.target].routes : [];
    const visible = !!motion && routes.length > 0 && !reduced.matches;
    ["packet", "packet-halo"].forEach(id => byId(id).toggleAttribute("hidden", !visible));
    if (!visible) return;
    const fraction = Math.min(.999999, motion.elapsed / frames[motion.target].duration);
    const position = fraction * routes.length;
    const path = byId("route-" + routes[Math.floor(position)]);
    const point = path.getPointAtLength(path.getTotalLength() * (position % 1));
    ["packet", "packet-halo"].forEach(id => {
      byId(id).setAttribute("cx", point.x); byId(id).setAttribute("cy", point.y);
    });
  }
  function pause() {
    playing = false; cancelAnimationFrame(raf); lastTime = null; presentation();
  }
  function seek(index) {
    pause(); motion = null; cursor = Math.max(0, Math.min(frames.length - 1, index));
    if (frames[cursor].checkpoint) byId("resource-detail").open = true;
    snapshot(); presentation();
  }
  function restart() {
    checkpointVisited = false; byId("resource-detail").open = false; seek(0);
  }
  function play() {
    if (playing) return;
    if (cursor === frames.length - 1) restart();
    if (frames[cursor].checkpoint) checkpointVisited = true;
    playing = true; lastTime = null;
    if (!motion) motion = { target: cursor + 1, elapsed: 0 };
    presentation(); raf = requestAnimationFrame(tick);
  }
  function tick(time) {
    if (!playing) return;
    // Do not catch up a backgrounded tab or a long renderer stall.
    const delta = lastTime === null ? 0 : Math.min(100, time - lastTime);
    lastTime = time; motion.elapsed += delta * Number(byId("speed").value);
    if (motion.elapsed >= frames[motion.target].duration) {
      cursor = motion.target; motion = null; snapshot();
      if (frames[cursor].checkpoint && !checkpointVisited && byId("auto-pause").checked) {
        checkpointVisited = true; byId("resource-detail").open = true; pause(); return;
      }
      if (cursor === frames.length - 1) { pause(); return; }
      motion = { target: cursor + 1, elapsed: 0 }; presentation();
    }
    drawPacket(); raf = requestAnimationFrame(tick);
  }
  byId("play").addEventListener("click", () => playing ? pause() : play());
  byId("continue").addEventListener("click", play);
  byId("restart").addEventListener("click", restart);
  byId("next-step").addEventListener("click", () => seek(cursor + 1));
  byId("back-step").addEventListener("click", () => seek(motion ? cursor : cursor - 1));
  byId("timeline").addEventListener("input", e => seek(Number(e.target.value)));
  byId("fit-diagram").addEventListener("click", () => {
    const fit = byId("canvas-scroll").classList.toggle("fitted");
    put("fit-diagram", fit ? "放大阅读" : "适应全图");
  });
  byId("canvas-scroll").classList.add("fitted"); put("fit-diagram", "放大阅读");
  nodes.forEach(node => {
    function select() {
      selected = node.dataset.node;
      put("component-name", details[selected][0]); put("component-explain", details[selected][1]);
      byId("component-detail").open = true; presentation();
    }
    node.addEventListener("click", select);
    node.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); e.stopPropagation(); select(); } });
  });
  document.addEventListener("keydown", e => {
    if (e.ctrlKey || e.metaKey || e.altKey || (e.target instanceof Element && e.target.closest("button,input,select,summary,a,[data-node]"))) return;
    if (e.key === " ") { e.preventDefault(); if (playing) pause(); else play(); }
    if (e.key === "ArrowRight") { e.preventDefault(); seek(cursor + 1); }
    if (e.key === "ArrowLeft") { e.preventDefault(); seek(motion ? cursor : cursor - 1); }
  });
  document.addEventListener("visibilitychange", () => { if (document.hidden && playing) pause(); });
  reduced.addEventListener("change", drawPacket);
  // Read-only inspection for reproducible browser verification, never a runtime API.
  globalThis.TaskCenterDemo = Object.freeze({ inspect: () => ({
    cursor, playing, motion: motion ? { ...motion } : null,
    frame: frames[cursor].id, state: frames[cursor].state
  }) });
  snapshot(); presentation();
})();
