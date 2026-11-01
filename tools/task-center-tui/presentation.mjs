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
  if (tag === 'Suspending' || tag === 'Settling') return { label: 'SETTLING', tone: 'warn', rank: 1 };
  if (tag === 'WaitingConditions') return { label: 'WAIT COND', tone: 'warn', rank: 1 };
  if (tag === 'WaitingResources') return { label: 'WAIT RES', tone: 'warn', rank: 1 };
  if (tag === 'WaitingObjects') return { label: 'WAIT OBJ', tone: 'warn', rank: 1 };
  if (tag?.startsWith('Waiting') || tag === 'Queued' || tag === 'Backoff') return { label: 'WAITING', tone: 'warn', rank: 1 };
  if (tag === 'Completed') return { label: 'DONE', tone: 'good', rank: 6 };
  if (tag === 'Running' || tag === 'Activity') return { label: 'RUNNING', tone: 'accent', rank: 3 };
  return { label: 'UNKNOWN', tone: 'muted', rank: 4 };
}

export function progress(data) {
  const p = data?.progress;
  if (!p || (p.total === null && !p.completed && !p.stage)) return '-';
  if (p.total > 0) return `${p.completed}/${p.total} (${Math.round(p.completed / p.total * 100)}%)`;
  return p.stage ? `${p.stage}: ${p.completed}` : `${p.completed ?? 0}`;
}
export function eventSummary(record) {
  const { kind, data, activity } = record.event;
  if (kind === 'Activity') return `#${activity} ${data?.name ?? '?'}  ${data?.kind === 'Call' && data?.state === 'Completed' ? '调用正常返回' : stateName(data?.state)}  ${progress(data)}`;
  if (kind === 'TaskState' || kind === 'Finished') return stateName(data);
  if (kind === 'Registered' || kind === 'Restored') return data?.workflow ?? kind;
  if (kind === 'Objects') return (Array.isArray(data) ? data : []).map(objectName).join(', ');
  if (kind === 'ControlRequested') return `${data?.action ?? '?'} requested (not acknowledgement)`;
  if (kind === 'CheckpointSaved') return 'Recovery checkpoint persisted; payload excluded';
  if (kind === 'RecoveryPending') return 'Host interrupted; durable workflow can be restored';
  return data === null ? kind : text(data);
}
