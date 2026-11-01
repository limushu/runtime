// Projection only: never infers missing transitions, retries, or outcomes.
export const safe = value => [...String(value)].map(c=>{
  const n=c.codePointAt(0);return n<32||(n>=127&&n<=159)||(n>=0x202a&&n<=0x202e)||(n>=0x2066&&n<=0x2069)?'?':c;
}).join('');
export const stateName = value => typeof value === 'string' ? value : JSON.stringify(value ?? 'Unknown');
export const cellWidth = c => {
  if (/\p{Mark}/u.test(c)) return 0;
  const n=c.codePointAt(0);
  return n>=0x1100 && (n<=0x115f || n>=0x2e80&&n<=0xa4cf || n>=0xac00&&n<=0xd7a3 || n>=0xf900&&n<=0xfaff || n>=0xfe10&&n<=0xfe6f || n>=0xff01&&n<=0xff60 || n>=0x1f300) ? 2 : 1;
};
export function wrapCells(value,width) {
  const lines=[];let line='',used=0;
  for(const c of safe(value)){const size=cellWidth(c);if(used+size>width){lines.push(line);line='';used=0;}line+=c;used+=size;}
  lines.push(line);return lines;
}
export class Model {
  tasks = new Map();
  catalog = new Map();
  records = [];
  cursor = 0;
  incomplete = false;
  apply(record) {
    const e = record?.event;
    if (!Number.isSafeInteger(record?.cursor) || record.cursor !== this.cursor + 1 || e?.schema !== 1 ||
        (e.task !== null && (typeof e.task !== 'string' || !/^\d+\/[0-9a-f]{32}\/[\w.-]+\/\d+$/.test(e.task))) ||
        typeof e.pool_uuid !== 'string' || !/^[0-9a-f]{32}$/.test(e.pool_uuid) ||
        typeof e.execution !== 'string' || typeof e.pool_name !== 'string' || typeof e.kind !== 'string' ||
        !Number.isSafeInteger(e.at_ms) || (e.activity !== null && !Number.isSafeInteger(e.activity))) {
      throw Error('Invalid audit record, schema or cursor gap; refusing a misleading history');
    }
    if(e.task !== null && e.task.split('/')[1] !== e.pool_uuid) throw Error('Task and pool scope mismatch');
    if (!Number.isSafeInteger(e.owner_epoch) || e.owner_epoch < 0) throw Error('Invalid owner epoch');
    if (e.kind === 'Activity' && (!e.data || typeof e.data !== 'object' || Array.isArray(e.data) ||
        (e.data.resources !== undefined && (!Array.isArray(e.data.resources) || e.data.resources.some(r => !r || typeof r !== 'object'))))) {
      throw Error('Invalid activity evidence');
    }
    this.cursor = record.cursor;
    this.records.push(record);
    if(e.kind === 'EvidenceIncomplete') this.incomplete = true;
    const catalog = this.catalog.get(e.pool_uuid);
    this.catalog.set(e.pool_uuid, {id:e.pool_uuid,name:e.pool_name,epoch:Math.max(catalog?.epoch ?? 0,e.owner_epoch),last:e.at_ms});
    if (e.task === null) return;
    let task = this.tasks.get(e.task);
    if (!task) {
      task = { id: e.task, pool: e.task.split('/')[1], poolName: e.pool_name, name: e.task.split('/')[2],
        state: 'Unknown', rawState: 'Unknown', terminal: false, executions: new Map(), events: [],
        first: e.at_ms, last: e.at_ms, objects: [], blockers: [], conflicts: [], suspensions: [] };
      this.tasks.set(e.task, task);
    }
    let execution = task.executions.get(e.execution);
    if (!execution) {
      execution = { id: e.execution, epoch: e.owner_epoch, activities: new Map(), events: [], first: e.at_ms,
        firstCursor: record.cursor, state: 'Unknown', rawState: 'Unknown', terminal: false };
      task.executions.set(e.execution, execution);
    }
    task.last = e.at_ms; task.events.push(record); execution.events.push(record);
    execution.last = e.at_ms;
    const current = task.executions.get(task.execution);
    if (!current || execution.epoch > current.epoch || (execution.epoch === current.epoch && execution.firstCursor > current.firstCursor)) task.execution = e.execution;
    const isCurrent = task.execution === e.execution;
    if (isCurrent && e.kind === 'Objects' && Array.isArray(e.data)) task.objects = e.data;
    if (isCurrent && e.kind === 'AdmissionDecision') { task.conflicts = e.data?.conflicts ?? []; task.blockers = e.data?.blockers ?? []; }
    if (isCurrent && e.kind === 'Blockers') task.blockers = e.data?.blockers ?? [];
    if (isCurrent && e.kind === 'PolicySuspensions') task.suspensions = e.data;
    if (isCurrent && e.kind === 'ControlRequested') task.control = { ...e.data, at: e.at_ms };
    if (e.kind === 'Started') execution.started = e.at_ms;
    if (e.kind === 'Finished') execution.finished = e.at_ms;
    if (e.kind === 'RecoveryPending') execution.recoverable = true;
    if (e.kind === 'Registered' || e.kind === 'Restored') {
      execution.state = 'WaitingObjects'; execution.rawState = execution.state; execution.terminal = false;
      if (isCurrent) { task.blockers = []; task.conflicts = []; task.suspensions = []; task.control = null; }
    }
    if (e.kind === 'TaskState' || e.kind === 'Finished') {
      execution.state = stateName(e.data);
      execution.rawState = e.data;
      execution.terminal = e.data === 'Completed' || !!e.data?.Failed;
      if (execution.recoverable && e.data?.Failed === 'Cancelled') { execution.state = 'Interrupted / recoverable'; execution.terminal = false; }
    }
    if (isCurrent) { task.state = execution.state; task.rawState = execution.rawState; task.terminal = execution.terminal; }
    if (e.kind === 'Activity' && e.activity !== null) {
      let activity = execution.activities.get(e.activity);
      if (!activity) { activity = { id: e.activity, events: [], data: {} }; execution.activities.set(e.activity, activity); }
      activity.data = e.data; activity.events.push(record);
    }
    if (e.kind === 'EvidenceIncomplete') { task.incomplete = true; }
  }
  pools() {
    const pools = new Map([...this.catalog].map(([id,pool])=>[id,{...pool,tasks:[]}]));
    for (const t of this.tasks.values()) {
      if (!pools.has(t.pool)) pools.set(t.pool, { id: t.pool, name: t.poolName, tasks: [] });
      pools.get(t.pool).tasks.push(t);
    }
    return [...pools.values()].sort((a,b) => a.id.localeCompare(b.id));
  }
  selectTasks(pool, filter = 'all', search = '') {
    return [...this.tasks.values()].filter(t => t.pool === pool &&
      (filter === 'all' || (filter === 'history' ? t.terminal : !t.terminal)) &&
      `${t.name} ${t.id} ${t.state}`.toLowerCase().includes(search.toLowerCase())).sort((a,b) => b.last-a.last || a.id.localeCompare(b.id));
  }
}
