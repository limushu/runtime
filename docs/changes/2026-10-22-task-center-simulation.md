# TaskCenter 动态运行演示（2026-10-22 历史记录）

原地址已升级为 [Runtime Lab：多任务冲突与交接动画](2026-10-24-runtime-lab.md)。以下记录描述旧版，不作为当前行为说明。

[打开交互动画](../diagrams/task-center-simulation/index.html) · [返回静态导读](../runtime-guide.md)

## 目的与验收标准

面向第一次接触系统的开发者，在一张固定架构图上观察消息、许可、唤醒和业务状态的变化，
说明资源事件分支怎样推进同一个 Workflow，而不把组件误画成各自独立运行的 task。

- 一次完整磁盘上线：Inspect → Add → Open → SetIo(Up)，四个 Activity，不是四个 Workflow。
- 同时展示 TaskCenter、State、Driver、Workflow Futures、Context、Broker、资源队列、
  LockTable、ActivityClient、Tracker/Completion，以及独立运行的 Node/Disk/VD 服务。
- 用沿管道移动的消息点、高亮和事件说明表示协作；区分 Pending、Ready 与再次 poll。
- 首次 Acquire 入队后自动暂停讲解；可展开解释，再继续观察 Permit 和 Waker。
- 支持播放、暂停、前后单步、重新播放、0.5/1/2/4 倍速、进度拖动、业务步骤跳转、组件说明。
- 显示流程对象占用、步骤资源等待/持有、资源事件队列、在途 Activity 与模拟磁盘结果。
- 无第三方依赖，直接通过 file:// 打开；不连接网络，不执行真实业务。

## 使用

下载仓库后打开 `docs/diagrams/task-center-simulation/index.html`。保留同目录的 JS/CSS 文件。
GitHub 文件浏览页只显示源码，不会执行动画；不需要安装依赖或启动服务。
静态五图导读顶部也有“观看运行演示”入口。

点击“开始演示”。默认首次资源申请后停下，点击“继续”观察资源分支与 Workflow 分支的衔接。
空格控制播放；左右方向键单步（输入框、按钮等保留自身键盘行为）。点击组件查看职责。
“放大阅读”可在画布内滚动；“适应全图”恢复全貌。支持系统减少动态效果设置。
页面切到后台会自动暂停，回来后手动继续。

暂停的是教学播放器，不是向业务发送 suspend/cancel。播放中的面板显示上一已完成事件状态，
事件完成后才更新；暂停保留消息位置，单步和跳转则回到确定的事件快照。

## 范围与真实性边界

本版是确定性的离线教学模型，不是 Rust 执行日志回放、监控页面或真实调度器。
假设 P1 中 N1 已加载且连接、D1 未加入，无竞争流程，所有步骤成功。
State 的冲突准入走允许分支；尚不模拟第二个冲突流程、失败、隔离、业务取消或暂停恢复。
VD 本次不参与。未来可以追加独立场景，但不能把未实现能力画成现有能力。

为了讲解，一次 poll 或 pump 内的同步微操作被拆成多个动画事件，时长不代表性能；
服务回复和 Release 的交错只展示一种合法顺序。移动的点可以表示调用或唤醒，不都代表邮箱消息。
流程对象占用由 State 保留到整个流程停稳；步骤 Permit Drop 只把 Release 入队，
Driver 消费后才清除 LockTable 的 held。
Open 只记录“调用成功”的教学视图，不杜撰领域模型 open 字段，也不自动把 IO 变成 Up。

实现对照当前 Rust 基线 `7b3b408`：

- `crates/storage-pool/src/workflows/online_disk.rs`：四个 Activity 的真实编排。
- `crates/storage-pool/src/task_center/mod.rs`：reap/pump/select、对象占用和 defer。
- `crates/workflow-foundation/src/driver.rs`：资源事件和 Future 分支共用 tick。
- `crates/workflow-foundation/src/locks.rs`：Acquire、Permit、Release 协议。
- `crates/workflow-foundation/src/service.rs`：Guard 停稳、Drop，再发送结果。
- `crates/storage-pool/src/domains/disk/service.rs`：Add/Open/SetIo 的实际业务差异。

## 实现结构

- `scenario.js`：56 个深度冻结的教学快照（起点 + 55 个事件）。
- `app.js`：requestAnimationFrame 播放器、管道消息、快照呈现与交互；无后端调用。
- `index.html` / `styles.css`：原生 SVG 架构图、响应式面板、键盘与减少动态效果支持。
- `scripts/verify-task-center-simulation.mjs`：无依赖的语义检查及可选 Chrome 实机交互验证。

## 验证

```sh
node scripts/verify-task-center-simulation.mjs
node scripts/verify-task-center-simulation.mjs --browser
```

需要 Node.js 22+。浏览器检查默认寻找本机 Chrome，可通过环境变量 CHROME_PATH 指定。
测试启动独立临时浏览器配置，不使用用户浏览器数据；临时配置保留在系统临时目录用于诊断。
可用 `--screenshot <path.png>` 输出暂停讲解及 Ready 状态截图。

验证记录见本页末尾；未修改 Rust 或执行语义，不需要为动画重新构建存储程序。

### 2026-10-22 验证结果

- 语义检查通过：56 个不可变快照、四个 Activity、唤醒与 poll 分离、Guard/资源释放顺序、
  对象占用贯穿步骤、Open/IO 差异、最终资源与对象全部回收。
- Chrome 实机检查通过：file:// 加载、全部单步快照、自动暂停、暂停位置冻结与续播、
  完整自动播放、重播、章节/进度跳转、方向键、组件说明、减少动态效果。
- 1600×1050 桌面截图人工检查；SVG 文本边界检查通过；390px 移动端与放大画布无页面横向溢出。
- 页面无脚本/CSP 错误、无外部网络请求。
- 外围工程 `npm run verify` 通过：Harness、lint（无警告）、TypeScript、生产构建。
- Rust 未修改，未重跑 Rust 测试。演示验证不等于真实存储运行验证。
