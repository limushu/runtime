import { duration, millis, objectName, progress, stamp, status, text } from './presentation.mjs';

const names = {
  'storage.shrink': '存储池缩容', 'storage.online-disk': '硬盘上线',
  'storage.offline-disk': '硬盘离线', 'storage.create-tier': '创建存储层',
  'storage.attach-node': '加入节点', 'storage.load-node': '加载节点',
  'storage.detach-node': '移除节点', 'storage.create-volume': '创建卷', 'storage.delete-volume': '删除卷',
};
export const workflowName = name => names[name] ?? name.replace(/^storage\./, '');
const stateLabels = { DONE:'成功', FAILED:'失败', CANCELLED:'已取消', UNCERTAIN:'待核对',
  PAUSED:'已暂停', 'WAIT COND':'等待条件', 'WAIT RES':'等待资源', 'WAIT OBJ':'排队中',
  WAITING:'等待中', SETTLING:'等待停稳', RUNNING:'执行中', RECOVERABLE:'中断待恢复', UNKNOWN:'未确认' };
export function resultState(value) {
  const s = status(value);
  return { ...s, label: stateLabels[s.label] ?? '未确认', code: s.label };
}
export function roots(execution) {
  if (!execution) return [];
  return [...execution.activities.values()].filter(a => !a.data.parent_id || !execution.activities.has(a.data.parent_id.sequence));
}
export const children = (execution, activity) => [...execution.activities.values()].filter(a => a.data.parent_id?.sequence === activity.id);
export function stepResult(activity, execution) {
  const s = resultState(activity.data.state);
  if (s.code === 'DONE' && activity.data.kind === 'Call') {
    return { ...s, code:'RETURNED', label:'调用返回', tone:'accent', note:'调用正常返回，不等于整个业务目标已经完成。' };
  }
  if (execution?.finished && !['DONE','FAILED','CANCELLED','UNCERTAIN'].includes(s.code)) {
    return { ...s, code:'UNCONFIRMED', label:'末态未确认', tone:'warn', note:'该批执行已结束，但此项没有记录终态；不推断成功或失败。' };
  }
  return { ...s, note: s.code === 'DONE' ? '逻辑步骤已成功结束。' : '' };
}
export function counts(execution) {
  const rows = roots(execution), states = rows.map(a => stepResult(a, execution));
  return { total:rows.length, steps:rows.filter(a => a.data.kind === 'Step').length,
    success:states.filter(s => s.code === 'DONE').length, returned:states.filter(s => s.code === 'RETURNED').length,
    failed:states.filter(s => s.code === 'FAILED').length,
    cancelled:states.filter(s => s.code === 'CANCELLED').length,
    pending:states.filter(s => !['DONE','RETURNED','FAILED','CANCELLED'].includes(s.code)).length };
}
export function countText(execution) {
  const c = counts(execution), parts = [`已记录 ${c.total} 项`];
  if (c.steps) parts.push(`步骤成功 ${c.success}`);
  if (c.returned) parts.push(`调用返回 ${c.returned}`);
  if (c.failed) parts.push(`失败 ${c.failed}`);
  if (c.cancelled) parts.push(`取消 ${c.cancelled}`);
  if (c.pending) parts.push(`未确认/未结束 ${c.pending}`);
  return parts.join(' · ');
}
export function consistency(execution) {
  if (!execution || execution.rawState !== 'Completed') return '';
  const open = [...execution.activities.values()].filter(a => !['DONE','FAILED','CANCELLED','UNCERTAIN'].includes(resultState(a.data.state).code));
  return open.length ? `工作流上报成功，但本次还有 ${open.length} 项末态未闭合；请核对记录。` : '';
}
export function currentAction(task) {
  if (task.terminal) return task.state === 'Completed' ? '工作流成功结束' : text(task.rawState?.Failed ?? task.state);
  const ex = task.executions.get(task.execution);
  const active = roots(ex).findLast(a => !['DONE','RETURNED','FAILED','CANCELLED','UNCERTAIN'].includes(stepResult(a, ex).code));
  if (active) return active.data.name;
  const last = roots(ex).at(-1);
  return last ? `${last.data.name}（最近调用）` : '等待开始';
}
export function stepSections(task, execution, activity) {
  const d = activity.data, s = stepResult(activity, execution);
  const sections = [ ['结果', `${s.label}${s.note ? ' · ' + s.note : ''}`],
    ['服务 / 类型', `${d.service ?? '-'} / ${d.kind === 'Step' ? '逻辑步骤' : '服务调用'}`] ];
  if (progress(d) !== '-') sections.push(['最后上报进度', progress(d)]);
  if (d.kind === 'Call' && s.code === 'RETURNED' && d.progress?.total > d.progress?.completed) {
    sections.push(['为什么没有全部完成', '该记录确认的是一次调用返回。上报进度尚未达到总量；返回值可能要求暂停或继续，不能据此标记业务步骤成功。']);
  }
  if (d.last_error) sections.push(['错误', text(d.last_error)]);
  if (d.retry_reason) sections.push(['重试原因', d.retry_reason]);
  if (d.description) sections.push(['说明', d.description]);
  sections.push(['调用次数 / 重试', `${d.attempts ?? 0} / ${d.retries ?? 0}`],
    ['资源', (d.resources ?? []).map(r => `${objectName(r.object)} [${r.access}]`).join(', ') || '无'],
    ['开始 / 结束 (UTC)', `${stamp(millis(d.started_at))} / ${stamp(millis(d.finished_at))}`],
    ['执行耗时', duration(d.started_at && d.finished_at ? millis(d.finished_at) - millis(d.started_at) : null)]);
  const calls = children(execution, activity);
  if (calls.length) sections.push(['内部调用（不重复计入步骤数）', calls.map(a => `#${a.id}  ${a.data.name}  ${stepResult(a, execution).label}`).join('\n')]);
  sections.push(['结果数据', '审计未记录任意业务返回 payload；不补造返回内容。'],
    ['执行批次', `${[...task.executions.keys()].indexOf(execution.id) + 1} / ${task.executions.size}，epoch ${execution.epoch}`]);
  return sections;
}
