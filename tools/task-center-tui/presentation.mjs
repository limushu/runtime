import { stateName } from './model.mjs';

export const shortTask = id => id ? `${id.split('/')[0]}:${id.split('/').at(-1)}` : '-';
export const objectName = o => o ? `${o.namespace ?? '?'}/${o.id ?? '?'}` : '-';
export const text = value => typeof value === 'string' ? value : JSON.stringify(value ?? '-');
export const time = ms => Number.isFinite(ms) && Math.abs(ms) <= 8.64e15
  ? new Date(ms).toISOString().slice(11, 23) : '-';
export const stamp = ms => Number.isFinite(ms) && Math.abs(ms) <= 8.64e15
  ? new Date(ms).toISOString() : '-';
export const millis = value => value ? value.secs_since_epoch * 1000 + value.nanos_since_epoch / 1e6 : null;
export function duration(ms) {
  if (!Number.isFinite(ms) || ms < 0) return '-';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m${Math.floor(ms / 1000) % 60}s`;
}

export function status(value) {
  if (typeof value === 'string' && value.startsWith('{')) {
    try { value = JSON.parse(value); } catch { /* Unknown text is not a state transition. */ }
  }
  const tag = typeof value === 'string' ? value : Object.keys(value ?? {})[0];
  if (tag === 'Interrupted / recoverable') return { label: 'RECOVERABLE', tone: 'warn', rank: 1 };
  if (tag === 'Failed') {
    const failure = typeof value.Failed === 'string' ? value.Failed : Object.keys(value.Failed ?? {})[0];
    if (failure === 'Uncertain') return { label: 'UNCERTAIN', tone: 'bad', rank: 0 };
    if (failure === 'Cancelled') return { label: 'CANCELLED', tone: 'muted', rank: 5 };
    return { label: 'FAILED', tone: 'bad', rank: 0 };
  }
  if (tag === 'Suspended') return { label: 'PAUSED', tone: 'warn', rank: 2 };
  if (tag === 'WaitingConditions') return { label: 'WAIT COND', tone: 'warn', rank: 1 };
  if (tag === 'WaitingResources') return { label: 'WAIT RES', tone: 'warn', rank: 1 };
  if (tag === 'WaitingObjects') return { label: 'WAIT OBJ', tone: 'warn', rank: 1 };
  if (tag?.startsWith('Waiting') || tag === 'Queued' || tag === 'Backoff') return { label: 'WAITING', tone: 'warn', rank: 1 };
  if (tag === 'Completed') return { label: 'DONE', tone: 'good', rank: 6 };
  if (tag === 'Running' || tag === 'Activity') return { label: 'RUNNING', tone: 'accent', rank: 3 };
  return { label: 'UNKNOWN', tone: 'muted', rank: 4 };
}

export function currentActivities(task) {
  return [...(task.executions.get(task.execution)?.activities.values() ?? [])];
}
export function activitySummary(task) {
  const activities = currentActivities(task);
  const active = activities.filter(a => !['DONE', 'FAILED', 'CANCELLED', 'UNCERTAIN'].includes(status(a.data.state).label));
  return active.at(-1)?.data.name ?? activities.at(-1)?.data.name ?? '-';
}
export function progress(data) {
  const p = data?.progress;
  if (!p || (p.total === null && !p.completed && !p.stage)) return '-';
  if (p.total > 0) return `${p.completed}/${p.total} (${Math.round(p.completed / p.total * 100)}%)`;
  return p.stage ? `${p.stage}: ${p.completed}` : `${p.completed ?? 0}`;
}
export function taskProgress(task) {
  const activities = currentActivities(task);
  const a = [...activities].reverse().find(a => progress(a.data) !== '-');
  return a ? progress(a.data) : '-';
}
export function stats(tasks) {
  return {
    current: tasks.filter(t => !t.terminal).length,
    history: tasks.filter(t => t.terminal).length,
    waiting: tasks.filter(t => !t.terminal && status(t.state).rank === 1).length,
    paused: tasks.filter(t => !t.terminal && status(t.state).label === 'PAUSED').length,
    issues: tasks.filter(t => t.incomplete || status(t.state).tone === 'bad').length,
  };
}
export function eventSummary(record) {
  const { kind, data, activity } = record.event;
  if (kind === 'Activity') return `#${activity} ${data?.name ?? '?'}  ${status(data?.state).label}  ${progress(data)}`;
  if (kind === 'TaskState' || kind === 'Finished') return stateName(data);
  if (kind === 'Registered' || kind === 'Restored') return data?.workflow ?? kind;
  if (kind === 'Objects') return (Array.isArray(data) ? data : []).map(objectName).join(', ');
  if (kind === 'ControlRequested') return `${data?.action ?? '?'} requested (not acknowledgement)`;
  if (kind === 'CheckpointSaved') return 'Recovery checkpoint persisted; payload excluded';
  if (kind === 'RecoveryPending') return 'Host interrupted; durable workflow can be restored';
  return data === null ? kind : text(data);
}

// Human-readable sections first; raw event JSON remains an explicit secondary view.
export function inspect(row) {
  if (!row) return [['Selection', 'No records in this scope.']];
  if (row.type === 'pool') {
    const p = row.value, s = stats(p.tasks);
    return [['Pool', p.name], ['Identity', p.id], ['Owner epoch', String(p.epoch ?? '-')],
      ['Reported tasks', `${s.current} non-terminal / ${s.history} historical`],
      ['Needs attention', `${s.waiting} waiting / ${s.paused} paused / ${s.issues} issues`],
      ['Last evidence (UTC)', stamp(p.last)], ['Next', 'Enter opens workflows in this pool.'],
      ['Meaning', 'Counts are last reported states, not process liveness.']];
  }
  if (row.type === 'task') {
    const t = row.value, ex = t.executions.get(t.execution);
    return [['Workflow', t.name], ['State', t.state], ['Current / last step', activitySummary(t)],
      ['Progress', taskProgress(t)], ['Objects', t.objects.map(objectName).join(', ') || 'None recorded'],
      ['Current blockers', text(t.blockers)], ['Admission decisions', text(t.conflicts)],
      ['Policy suspensions', text(t.suspensions)], ['Last control request', t.control ? text(t.control) : 'None recorded'],
      ['Execution', `${t.executions.size} batch(es); latest epoch ${ex?.epoch ?? '-'}`],
      ['First / last evidence (UTC)', `${stamp(t.first)} / ${stamp(t.last)}`],
      ['Task ID', t.id], ['Next', 'Enter opens steps; e selects an execution batch.']];
  }
  if (row.type === 'activity') {
    const { activity: a, task: t, execution: ex } = row.value, d = a.data;
    return [['Activity', d.name ?? '-'], ['State', stateName(d.state)], ['Service / kind', `${d.service ?? '-'} / ${d.kind ?? '-'}`],
      ['Progress', progress(d)], ['Attempts / retries', `${d.attempts ?? 0} / ${d.retries ?? 0}`],
      ['Resources', (d.resources ?? []).map(r => `${objectName(r.object)} [${r.access}]`).join(', ') || 'None recorded'],
      ['Parent step', d.parent_id ? `#${d.parent_id.sequence}` : 'Top-level'], ['Description', d.description ?? '-'],
      ['Last error', text(d.last_error)], ['Retry reason', text(d.retry_reason)],
      ['Created (UTC)', stamp(millis(d.created_at))], ['Started (UTC)', stamp(millis(d.started_at))],
      ['Finished (UTC)', stamp(millis(d.finished_at))], ['Cancellation', d.cancellable ? 'Cooperative' : 'Protected / deferred'],
      ['Execution', `epoch ${ex.epoch} / ${ex.id}`], ['Task ID', t.id], ['Next', 'Enter opens the exact event history of this step.']];
  }
  const r = row.value, e = r.event;
  return [['Event', `${r.cursor} / ${e.kind}`], ['Recorded at (UTC)', stamp(e.at_ms)],
    ['Summary', eventSummary(r)], ['Pool', `${e.pool_name} / ${e.pool_uuid}`],
    ['Task ID', e.task ?? 'Pool event'], ['Execution', `epoch ${e.owner_epoch} / ${e.execution}`],
    ['Activity', e.activity === null ? '-' : `#${e.activity}`], ['Payload', text(e.data)],
    ['Audit', 'r opens the original versioned JSON record.']];
}
