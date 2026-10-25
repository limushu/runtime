# 最新架构运行图：规格、实现与验证

> 历史实现记录。当前规则和暂停机制已简化，见 [本轮变更](2026-10-24-simple-task-rules.md)；本文中的 relation/priority 与保留调用 hook 不再是当前接口。

初版记录如下；当前图已更新至 53fae88，见 [tick 分支与死锁回归图示](2026-10-24-driver-tick-views.md)。

## 范围与验收

按已推送实现 `53b5ea6` 重绘运行图，帮助评审者识别组件边界并跟踪实际运行。
本次仅修改图示、生成/校验脚本和导航文档，不改变 Rust 行为。

| 验收标准 | 对应交付 |
| --- | --- |
| 有清晰的逻辑分层，区分业务决策与执行机制 | 01-layers |
| 区分 TaskCenter Tokio task、被嵌入 Driver、各服务宿主 | 02-runtime |
| 展示 Future 的 poll / Pending / 回复唤醒闭环 | 02-runtime |
| 策略请求与真实停稳 ACK 分开，许可发给 topo Workflow | 03-scheduling |
| 并列解释保留调用与部分结果返回，恢复先检查条件 | 04-pause |
| 持久化由 Workflow 声明，恢复保留身份并有外部前提 | 05-recovery |
| 观察、控制与存储都是同一个 Task 的访问或表示 | 06-observation |
| 可独立阅读、离线交互、导出与打印，无外部请求 | HTML、SVG、PNG |

## 实现

- 6 份可编辑 SVG，统一 1600 × 1040 画布；配套 6 份 Chrome 渲染的 PNG。
- HTML 内嵌相同 SVG，支持章节导航、步骤高亮、组件点击/键盘查询、缩放、移动端阅读和打印全部页面。
- Python 生成脚本作为可维护源文件；manifest 记录组件说明与阅读路径。
- runtime-guide.md 更新为当前入口；旧五图及单图网页标记为历史版本并链接新入口。
- 明确说明：Migration 是普通服务接入示例；Task checkpoint 不等于确定性重放或生产存储池容灾。

## 验证命令

```sh
python scripts/render-unified-task-guide.py
node scripts/verify-unified-task-guide.mjs --render
```

校验脚本使用 Node 22 内置 WebSocket 与本机 Chrome，不安装依赖。CHROME_PATH 可指定浏览器。
不传 --render 时只校验，不重新导出 PNG。临时浏览器使用独立 profile，退出后关闭自己的进程。

## 验证证据

- 浏览器检查：6 份 SVG 与 HTML 内嵌内容一致；文字在画布与卡片内；所有阅读步骤、组件键盘交互、
  章节/方向键导航、缩放、移动端无页面横向溢出、六页打印、无 JS 错误和无外部请求，全部通过。
- 实际查看全部六份 PNG；修正分层标签、过长标题、topo 许可目标与运行回复连线后复查。
- SVG XML、Markdown 相对链接和生成结果一致性检查通过。
- 外围 `npm run verify` 通过：harness、lint、TypeScript 与 Vite 构建。
- Rust 在第一次代码提交前已完成 125 项测试、格式和 Clippy；图示提交不修改 Rust，不重复执行。

入口：[最新运行导读](../runtime-guide.md)、[离线交互页面](../diagrams/unified-task-guide/index.html)。
