import { Model, safe } from './model.mjs';
import { eventSummary, objectName, shortTask, time } from './presentation.mjs';
import { counts, currentAction, resultState, roots, stepResult, workflowName } from './semantics.mjs';

// Navigation follows the user's domain hierarchy, never a cross-pool event browser.
export class App {
  page = 'pool';
  pool = null;
  task = null;
  execution = null; // null follows the latest execution; explicit IDs inspect earlier batches.
  activity = null;
  section = 'current';
  selected = new Map();
  history = [];
  search = '';
  searching = false;
  searchBefore = '';
  modal = null;
  frozen = null;
  scroll = 0;
  sizes = { current: 4, history: 8, workflow: 12, step: 12 };
  constructor(model) { this.live = model; }
  get model() { return this.frozen ?? this.live; }
  get currentPool() {
    const pools = this.model.pools();
    if (!this.pool) {
      this.pool = pools.sort((a,b) => b.tasks.filter(t => !t.terminal).length - a.tasks.filter(t => !t.terminal).length || b.tasks.length-a.tasks.length)[0]?.id ?? null;
      if (this.pool && !this.model.selectTasks(this.pool, 'current').length) this.section = 'history';
    }
    return pools.find(p => p.id === this.pool);
  }
  get currentTask() { return this.model.tasks.get(this.task); }
  get currentExecution() { return this.currentTask?.executions.get(this.execution ?? this.currentTask.execution); }
  get currentActivity() { return this.currentExecution?.activities.get(this.activity); }
  keyFor(section = this.section) { return `${this.page}/${this.pool}/${this.task}/${this.currentExecution?.id}/${section}`; }
  rows(section = this.section) {
    let rows = [];
    if (this.page === 'pool') {
      const pool = this.currentPool;
      rows = (pool?.tasks ?? []).filter(t => section === 'history' ? t.terminal : !t.terminal)
        .sort((a,b) => section === 'current' ? resultState(a.state).rank - resultState(b.state).rank || b.last-a.last : b.last-a.last)
        .map(t => ({ id: t.id, type: 'task', value: t, result: resultState(t.state),
          name: workflowName(t.name), ref: shortTask(t.id), action: section==='current'?currentAction(t):t.objects.map(objectName).join(', ')||'-',
          count: counts(t.executions.get(t.execution)).total, time: time(t.last) }));
    } else if (this.page === 'workflow') {
      rows = roots(this.currentExecution).sort((a,b) => a.id-b.id).map((a,i) => ({ id:a.id, type:'activity', value:a,
        number:i+1, name:a.data.name, result:stepResult(a, this.currentExecution), kind:a.data.kind === 'Step' ? '步骤' : '调用' }));
    }
    return this.search ? rows.filter(r => `${r.name} ${r.ref ?? ''} ${r.result.label} ${r.action ?? ''}`.toLowerCase().includes(this.search.toLowerCase())) : rows;
  }
  selection(section = this.section) {
    const rows = this.rows(section), key = this.keyFor(section);
    let index = rows.findIndex(r => r.id === this.selected.get(key));
    if (index < 0) index = 0;
    const row = rows[index];
    if (row) this.selected.set(key, row.id);
    return { rows, row, index };
  }
  open(row = this.selection().row) {
    if (!row) return;
    this.history.push({ page:this.page, pool:this.pool, task:this.task, execution:this.execution,
      activity:this.activity, section:this.section, search:this.search, scroll:this.scroll });
    this.search = ''; this.scroll = 0;
    if (row.type === 'task') { this.task = row.id; this.execution = null; this.activity = null; this.page = 'workflow'; }
    else { this.activity = row.id; this.page = 'step'; }
  }
  back() {
    if (this.history.length) Object.assign(this, this.history.pop());
    else if (this.page === 'step') { this.page = 'workflow'; this.activity = null; this.scroll = 0; }
    else { this.page = 'pool'; this.task = null; this.execution = null; this.activity = null; this.scroll = 0; }
  }
  choosePool(id) {
    this.pool = id; this.page = 'pool'; this.task = null; this.execution = null; this.activity = null;
    this.search = ''; this.history = []; this.scroll = 0;
    this.section = this.model.selectTasks(id, 'current').length ? 'current' : 'history';
  }
  menu(kind) {
    if (kind === 'pool') this.modal = { kind, title:'切换存储池', index:0, scroll:0,
      items:this.model.pools().map(p => ({ id:p.id, label:`${p.name}    当前 ${p.tasks.filter(t=>!t.terminal).length} · 历史 ${p.tasks.filter(t=>t.terminal).length}` })) };
    if (kind === 'execution' && this.currentTask) {
      const executions = [...this.currentTask.executions.values()];
      this.modal = { kind, title:'执行批次（彼此独立，不混合步骤）', index:0, scroll:0,
        items:[{ id:null, label:'最新执行（默认）' }, ...executions.map((ex,i) => ({ id:ex.id,
          label:`第 ${i+1} 次 · epoch ${ex.epoch} · ${resultState(ex.state).label} · ${counts(ex).total} 项记录` }))] };
    }
  }
  audit(raw = false) {
    let records = this.page === 'step' ? this.currentActivity?.events : this.page === 'workflow' ? this.currentExecution?.events : this.selection().row?.value.events;
    records ??= [];
    this.modal = { kind:'audit', title: raw ? '原始审计记录' : '审计时间线（记录时状态，不是当前状态）', raw,
      records:structuredClone(records), index:Math.max(0,records.length-1), scroll:0 };
  }
  choose() {
    const m = this.modal, item = m.items?.[m.index];
    if (!item) return;
    if (m.kind === 'pool') this.choosePool(item.id);
    if (m.kind === 'execution') {
      this.execution = item.id; this.page = 'workflow'; this.activity = null; this.scroll = 0; this.search = '';
      while(this.history.length && this.history.at(-1).page !== 'pool')this.history.pop();
    }
    this.modal = null;
  }
  click(hit) {
    if (!hit) return;
    if (hit.action === 'pool') { this.menu('pool'); return; }
    if (hit.action === 'back') { this.back(); return; }
    if (hit.action === 'close') { this.modal = null; return; }
    if (hit.action === 'menu') { this.modal.index = hit.index; this.choose(); return; }
    if (hit.action === 'section') { this.section = hit.section; return; }
    if (hit.action === 'row') {
      this.section = hit.section;
      const row = this.rows(hit.section).find(r => r.id === hit.id);
      if (row) { this.selected.set(this.keyFor(hit.section), row.id); this.open(row); }
    }
  }
  key(str, key = {}) {
    if (key.ctrl && key.name === 'c') return 'quit';
    if (this.searching) {
      if (key.name === 'return') this.searching = false;
      else if (key.name === 'escape') { this.search = this.searchBefore; this.searching = false; }
      else if (key.name === 'backspace') this.search = [...this.search].slice(0,-1).join('');
      else if (str && !key.ctrl && !key.meta && !key.sequence?.startsWith('\x1b')) this.search += safe(str);
      return;
    }
    if (key.name === 'q') return 'quit';
    const delta = ['down','j'].includes(key.name) ? 1 : ['up','k'].includes(key.name) ? -1 :
      key.name === 'pagedown' ? this.sizes[this.page === 'pool' ? this.section : this.page] : key.name === 'pageup' ? -this.sizes[this.page === 'pool' ? this.section : this.page] : 0;
    if (this.modal) {
      const m = this.modal;
      if (key.name === 'escape') { this.modal = null; return; }
      if (m.items) { m.index = Math.max(0,Math.min(m.items.length-1,m.index+delta)); if(key.name === 'return') this.choose(); }
      else if (m.kind === 'audit' && !m.raw) {
        m.index = Math.max(0,Math.min(m.records.length-1,m.index+delta));
        if (key.name === 'return' || key.name === 'r') { m.raw = true; m.scroll = 0; }
      } else {
        m.scroll = Math.max(0,m.scroll+delta);
        if (m.kind === 'audit' && key.name === 'r') { m.raw = !m.raw; m.scroll = 0; }
      }
      return;
    }
    if (key.name === 'space') { this.frozen = this.frozen ? null : Object.assign(new Model(), structuredClone(this.live)); return; }
    if (key.name === 'p') { this.menu('pool'); return; }
    if (key.name === 'e') { this.menu('execution'); return; }
    if (key.name === 'a' || key.name === 'r') { this.audit(key.name === 'r'); return; }
    if (key.name === 'escape' || key.name === 'b') { this.back(); return; }
    if (key.name === 'tab' && this.page === 'pool') { this.section = this.section === 'current' ? 'history' : 'current'; return; }
    if (str === '/' && this.page !== 'step') { this.searchBefore = this.search; this.searching = true; return; }
    if (key.name === 'h' || str === '?') {
      this.modal = { kind:'help', title:'操作帮助', scroll:0, lines:[
        'Enter / 鼠标点击    存储池 → Workflow → 步骤详情',
        'Esc / b             返回；保留之前的选择与搜索',
        'Tab                 在当前 / 历史工作流之间切换',
        '↑ ↓ / j k           选择；PgUp PgDn 翻页；Home End 首尾',
        'p                   切换存储池', 'e                   查看之前的执行批次（仅 Workflow 内）',
        '/                   搜索；Esc 撤销当前编辑', 'a / r               审计时间线 / 原始记录',
        'Space               冻结视图（仍然读取日志，不暂停业务）', 'q / Ctrl+C          退出', '',
        '成功 = Workflow 或逻辑步骤已成功结束。',
        '调用返回 = 一次服务调用正常返回，不保证业务目标已完成。',
        '只统计已经记录的逻辑步骤；子调用不重复计数。',
        '没有逻辑步骤的旧 Workflow 按服务调用展示，不伪造步骤计划。',
        '状态来自最后记录，不表示进程仍在线；全部时间为 UTC。',
      ] }; return;
    }
    if (this.page === 'step') { this.scroll = Math.max(0,this.scroll+delta); if(key.name === 'home')this.scroll=0; return; }
    const { rows, row, index } = this.selection();
    if (key.name === 'return' || key.name === 'd') { this.open(row); return; }
    let next = index+delta;
    if (key.name === 'home') next = 0;
    if (key.name === 'end') next = rows.length-1;
    if (rows.length) this.selected.set(this.keyFor(),rows[Math.max(0,Math.min(rows.length-1,next))].id);
  }
  auditLines() {
    return this.modal.records.map(r => `${time(r.event.at_ms)}  ${eventSummary(r)}`);
  }
}
