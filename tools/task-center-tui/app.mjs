import { Model, safe } from './model.mjs';
import { activitySummary, duration, eventSummary, inspect, millis, objectName, progress,
  shortTask, stats, status, time } from './presentation.mjs';

export const tabs = ['Pools', 'Workflows', 'Activities', 'Events'];
export const columns = {
  Pools: [ ['name','POOL',23], ['epoch','EPOCH',7], ['current','CURRENT',9], ['waiting','WAIT',7],
    ['paused','PAUSED',8], ['issues','ISSUES',8], ['history','HISTORY',9], ['last','LAST EVENT UTC',14] ],
  Workflows: [ ['id','TASK',8], ['name','WORKFLOW',24], ['state','STATE',13], ['step','CURRENT / LAST STEP',25],
    ['pool','POOL',16], ['progress','PROGRESS',18], ['epoch','EPOCH',7], ['events','EVENTS',8], ['last','LAST EVENT UTC',14] ],
  Activities: [ ['id','STEP',7], ['name','ACTIVITY / CALL',27], ['state','STATE',13], ['epoch','EPOCH',7],
    ['service','SERVICE',12], ['attempts','TRY',5], ['retries','RETRY',7], ['duration','EXEC TIME',11],
    ['progress','PROGRESS',18], ['task','TASK',8] ],
  Events: [ ['id','CURSOR',8], ['time','TIME UTC',14], ['kind','EVENT',21], ['summary','EVIDENCE',45],
    ['epoch','EPOCH',7], ['task','TASK',8] ],
};
const sortChoices = {
  Pools: ['attention', 'name', 'last'], Workflows: ['attention', 'last', 'name', 'pool'],
  Activities: ['sequence', 'attention', 'name', 'epoch'], Events: ['newest', 'oldest', 'kind'],
};

export class App {
  tab = 'Pools';
  pool = null;
  task = null;
  execution = null;
  activity = null;
  filter = 'all';
  search = '';
  selected = new Map();
  history = [];
  sorts = { Pools: 'attention', Workflows: 'attention', Activities: 'sequence', Events: 'newest' };
  hidden = Object.fromEntries(tabs.map(t => [t, new Set()]));
  modal = null;
  frozen = null;
  searchBefore = '';
  searching = false;
  pageSize = 10;
  constructor(model) { this.live = model; }
  get model() { return this.frozen ?? this.live; }
  get selectionKey() { return `${this.tab}/${this.pool}/${this.task}/${this.execution}/${this.activity}`; }

  rows() {
    const model = this.model;
    let result;
    const tasks = [...model.tasks.values()].filter(t => (!this.pool || t.pool === this.pool) && (!this.task || t.id === this.task));
    if (this.tab === 'Pools') {
      result = model.pools().map(p => {
        const s = stats(p.tasks);
        return { id: p.id, type: 'pool', value: p, tone: s.issues ? 'bad' : s.waiting || s.paused ? 'warn' : 'accent',
          rank: s.issues ? 0 : s.current ? 1 : 2, last: p.last, name: p.name,
          cells: { name: p.name, epoch: p.epoch ?? '-', ...s, last: time(p.last) } };
      });
    } else if (this.tab === 'Workflows') {
      result = tasks.filter(t => this.filter === 'all' || (this.filter === 'current' ? !t.terminal :
        this.filter === 'history' ? t.terminal : t.incomplete || status(t.state).tone === 'bad')).map(t => {
        const st = status(t.state), ex = t.executions.get(t.execution);
        return { id: t.id, type: 'task', value: t, tone: t.incomplete ? 'bad' : st.tone,
          rank: t.incomplete ? 0 : st.rank, last: t.last, name: t.name, pool: t.poolName,
          cells: { id: shortTask(t.id), name: t.name.replace(/^storage\./, ''), state: st.label,
            step: activitySummary(t), pool: t.poolName, progress: this.taskProgress(t), epoch: ex?.epoch ?? '-',
            events: t.events.length, last: time(t.last) } };
      });
    } else if (this.tab === 'Activities') {
      result = tasks.flatMap(t => [...t.executions.values()].filter(ex => !this.execution || ex.id === this.execution)
        .flatMap(ex => [...ex.activities.values()].map(a => {
          const d = a.data, st = status(d.state);
          const elapsed = d.started_at && d.finished_at ? millis(d.finished_at) - millis(d.started_at) : null;
          return { id: `${t.id}/${ex.id}/${a.id}`, type: 'activity', value: { task: t, execution: ex, activity: a },
            rank: st.rank, name: d.name, epoch: ex.epoch, sequence: a.id, first: ex.first, tone: st.tone,
            cells: { id: `${d.parent_id ? '  >' : ''}${a.id}`, name: d.name, state: st.label, service: d.service ?? '-',
              attempts: d.attempts ?? 0, retries: d.retries ?? 0, duration: duration(elapsed), progress: progress(d),
              epoch: ex.epoch, task: shortTask(t.id) } };
        })));
    } else {
      result = model.records.filter(r => (!this.pool || r.event.pool_uuid === this.pool) &&
        (!this.task || r.event.task === this.task) && (!this.execution || r.event.execution === this.execution) &&
        (this.activity === null || r.event.activity === this.activity)).map(r => ({
          id: r.cursor, type: 'event', value: r, name: r.event.kind, last: r.cursor,
          tone: r.event.kind === 'EvidenceIncomplete' ? 'bad' : r.event.kind === 'Restored' ? 'accent' : 'normal',
          cells: { id: r.cursor, time: time(r.event.at_ms), kind: r.event.kind, summary: eventSummary(r),
            epoch: r.event.owner_epoch, task: shortTask(r.event.task) },
        }));
    }
    if (this.search) {
      const query = this.search.toLowerCase();
      result = result.filter(r => `${r.id} ${Object.values(r.cells).join(' ')} ${r.type === 'task' ? r.value.objects.map(objectName).join(' ') : ''}`.toLowerCase().includes(query));
    }
    const sort = this.sorts[this.tab];
    return result.sort((a, b) => {
      let order = 0;
      if (sort === 'attention') order = a.rank - b.rank || (b.last ?? 0) - (a.last ?? 0);
      else if (sort === 'last' || sort === 'newest') order = b.last - a.last;
      else if (sort === 'oldest') order = a.last - b.last;
      else if (sort === 'sequence') order = a.first - b.first || a.sequence - b.sequence;
      else if (sort === 'epoch') order = b.epoch - a.epoch || a.sequence - b.sequence;
      else order = String(a[sort === 'kind' ? 'name' : sort] ?? '').localeCompare(String(b[sort === 'kind' ? 'name' : sort] ?? ''));
      return order || String(a.id).localeCompare(String(b.id));
    });
  }

  taskProgress(task) {
    const ex = task.executions.get(task.execution);
    const step = [...(ex?.activities.values() ?? [])].reverse().find(a => progress(a.data) !== '-');
    return step ? progress(step.data) : '-';
  }
  selection(rows = this.rows()) {
    let index = rows.findIndex(r => r.id === this.selected.get(this.selectionKey));
    if (index < 0) index = 0;
    const row = rows[index];
    if (row) this.selected.set(this.selectionKey, row.id);
    return { row, index, rows };
  }
  setTab(tab) {
    this.tab = tab; this.search = ''; this.modal = null;
    if (tab === 'Pools') this.pool = null;
    if (tab === 'Pools' || tab === 'Workflows') { this.task = null; this.execution = null; this.activity = null; }
    if (tab === 'Activities') this.activity = null;
  }
  open(row) {
    if (!row) return;
    if (row.type !== 'event') this.history.push({ tab: this.tab, pool: this.pool, task: this.task,
      execution: this.execution, activity: this.activity, search: this.search, filter: this.filter });
    this.search = '';
    if (row.type === 'pool') {
      this.pool = row.value.id; this.task = null; this.execution = null; this.activity = null;
      this.setTab('Workflows');
    } else if (row.type === 'task') {
      this.task = row.id; this.pool = row.value.pool; this.execution = null; this.activity = null;
      this.setTab('Activities');
    } else if (row.type === 'activity') {
      this.task = row.value.task.id; this.pool = row.value.task.pool;
      this.execution = row.value.execution.id; this.activity = row.value.activity.id;
      this.setTab('Events');
    } else this.details(row);
  }
  details(row, raw = false) {
    if (!row) return;
    // Snapshot details so incoming events cannot move content underneath the reader.
    const last = row.type === 'event' ? row.value : row.type === 'task' ? row.value.events.at(-1) :
      row.type === 'activity' ? row.value.activity.events.at(-1) : this.model.records.findLast(r => r.event.pool_uuid === row.id);
    this.modal = { kind: 'details', title: raw ? 'Original audit record' : 'Record details',
      row: structuredClone(row), sections: inspect(structuredClone(row)), raw, record: last, scroll: 0 };
  }
  menu(kind) {
    let items = [];
    if (kind === 'sort') items = sortChoices[this.tab].map(id => ({ id, label: `${id === this.sorts[this.tab] ? '[x]' : '[ ]'} ${id}` }));
    if (kind === 'columns') items = columns[this.tab].map(([id, label], i) => ({ id, label: `${this.hidden[this.tab].has(id) ? '[ ]' : '[x]'} ${label}${i === 0 ? ' (required)' : ''}` }));
    if (kind === 'pool') items = [{ id: null, label: 'All pools' }, ...this.model.pools().map(p => ({ id: p.id, label: `${p.name}  ${p.id.slice(0, 8)}` }))];
    if (kind === 'execution') {
      const task = this.model.tasks.get(this.task) ?? (this.selection().row?.type === 'task' ? this.selection().row.value : null);
      if (!task) { this.modal = { kind: 'help', title: 'Select a workflow first', lines: ['Select a workflow and press Enter or e to inspect its execution batches.'], scroll: 0 }; return; }
      items = [{ id: null, label: 'All executions (preserve old and restored history)' }, ...[...task.executions.values()].map(ex => ({
        id: ex.id, label: `epoch ${ex.epoch}  ${ex.id.slice(0, 8)}  ${ex.activities.size} steps/calls  ${ex.recoverable ? 'interrupted / recoverable' : ex.finished ? 'finished' : 'non-terminal'}`,
      }))];
    }
    this.modal = { kind, title: { sort: 'Sort by', columns: 'Visible columns', pool: 'Pool scope', execution: 'Execution batches' }[kind], items, index: 0, scroll: 0,
      task: kind === 'execution' ? this.task ?? this.selection().row?.id : null };
  }
  choose() {
    const m = this.modal, item = m.items?.[m.index];
    if (!item) return;
    if (m.kind === 'sort') this.sorts[this.tab] = item.id;
    if (m.kind === 'pool') { this.pool = item.id; this.task = null; this.execution = null; this.activity = null; this.search = ''; this.history = []; }
    if (m.kind === 'execution') {
      this.task = m.task; this.pool = this.model.tasks.get(m.task).pool;
      this.execution = item.id; this.activity = null; this.setTab('Activities');
    }
    if (m.kind === 'columns') {
      if (item.id !== columns[this.tab][0][0]) {
        const hidden = this.hidden[this.tab];
        if (hidden.has(item.id)) hidden.delete(item.id); else hidden.add(item.id);
      }
      const index = m.index;
      this.menu('columns'); this.modal.index = index; return;
    }
    this.modal = null;
  }
  back() {
    if (this.history.length) { Object.assign(this, this.history.pop()); return; }
    if (this.tab === 'Events') { this.activity = null; this.setTab(this.task ? 'Activities' : 'Workflows'); }
    else if (this.tab === 'Activities') { this.task = null; this.execution = null; this.activity = null; this.setTab('Workflows'); }
    else { this.pool = null; this.task = null; this.execution = null; this.activity = null; this.setTab('Pools'); }
  }
  key(str, key = {}) {
    if (key.ctrl && key.name === 'c') return 'quit';
    if (this.searching) {
      if (key.name === 'return') this.searching = false;
      else if (key.name === 'escape') { this.searching = false; this.search = this.searchBefore; }
      else if (key.name === 'backspace') this.search = [...this.search].slice(0, -1).join('');
      else if (str && !key.ctrl && !key.meta && !key.sequence?.startsWith('\x1b')) this.search += safe(str);
      return;
    }
    if (key.name === 'q') return 'quit';
    if (this.modal) {
      const m = this.modal;
      if (key.name === 'escape') { this.modal = null; return; }
      if (m.items) {
        if (key.name === 'up' || key.name === 'k') m.index = Math.max(0, m.index - 1);
        if (key.name === 'down' || key.name === 'j') m.index = Math.min(m.items.length - 1, m.index + 1);
        if (key.name === 'return' || key.name === 'space') this.choose();
      } else {
        const move = key.name === 'pagedown' ? this.pageSize : key.name === 'pageup' ? -this.pageSize :
          ['down','j'].includes(key.name) ? 1 : ['up','k'].includes(key.name) ? -1 : 0;
        m.scroll = Math.max(0, m.scroll + move);
        if (key.name === 'r' && m.kind === 'details') { m.raw = !m.raw; m.scroll = 0; }
      }
      return;
    }
    if (tabs[Number(str) - 1]) { this.history = []; this.setTab(tabs[Number(str) - 1]); return; }
    if (key.name === 'tab') { this.history = []; this.setTab(tabs[(tabs.indexOf(this.tab) + (key.shift ? 3 : 1)) % 4]); return; }
    if (key.name === 'space') { this.frozen = this.frozen ? null : Object.assign(new Model(), structuredClone(this.live)); return; }
    if (key.name === 'escape' || key.name === 'b') { this.back(); return; }
    if (str === '/') { this.searchBefore = this.search; this.searching = true; return; }
    if (key.name === 'f' && this.tab === 'Workflows') {
      const filters = ['all','current','history','issues']; this.filter = filters[(filters.indexOf(this.filter) + 1) % filters.length]; return;
    }
    if (key.name === 's') { this.menu('sort'); return; }
    if (key.name === 'c') { this.menu('columns'); return; }
    if (key.name === 'p') { this.menu('pool'); return; }
    if (key.name === 'e') { this.menu('execution'); return; }
    if (key.name === 'a') { this.pool = null; this.task = null; this.execution = null; this.activity = null; this.search = ''; this.filter = 'all'; this.history = []; return; }
    if (key.name === 'h' || str === '?') {
      this.modal = { kind: 'help', title: 'TaskCenter TOP / keyboard', scroll: 0, lines: [
        '1..4 / Tab       Pools / Workflows / Activities / Events',
        'Up Down / j k    Select row (identity stays stable on refresh)',
        'PgUp PgDn        Previous / next table page',
        'Home End         First / last row',
        'Enter            Pool -> Workflow -> Activity -> event details',
        'd                Readable details of the selected row',
        'r                Original JSON of the latest associated event',
        'Esc / b          Close popup / return one level',
        'p / e            Pool scope / execution batch selector',
        'a                Clear all scopes, filters and search',
        'f                Workflows: all / current / history / issues',
        '/                Search this table (Esc restores previous query)',
        's / c            Sorting / visible columns',
        'Space            Freeze viewed data; journal ingestion continues',
        'q / Ctrl+C       Exit and restore the terminal',
        '', 'Read-only. No cancel, retry, pause or resume commands are sent.',
        'Timestamps use UTC. Non-terminal states are NOT host liveness.',
        'EXEC TIME is recorded start-to-finish only; incomplete times show -.',
        'Raw payloads are audit metadata, not arbitrary business replies.',
      ] }; return;
    }
    const { row, index, rows } = this.selection();
    if (key.name === 'return') { this.open(row); return; }
    if (key.name === 'd' || key.name === 'r') { this.details(row, key.name === 'r'); return; }
    let next = index;
    if (['up','k'].includes(key.name)) next--;
    if (['down','j'].includes(key.name)) next++;
    if (key.name === 'pageup') next -= this.pageSize;
    if (key.name === 'pagedown') next += this.pageSize;
    if (key.name === 'home') next = 0;
    if (key.name === 'end') next = rows.length - 1;
    if (rows.length) this.selected.set(this.selectionKey, rows[Math.max(0, Math.min(rows.length - 1, next))].id);
  }
}
