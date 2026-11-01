import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Model } from './model.mjs';
import { App, tabs } from './app.mjs';
import { Frame, cells, fit, render, wrap } from './screen.mjs';
import { JournalReader } from './reader.mjs';
import { duration, inspect, millis, status } from './presentation.mjs';

const pool = '00000000000000000000000000000001';
const other = '00000000000000000000000000000002';
const task = `1/${pool}/storage.shrink/1`;
const source = { exists: true, lastRead: 1000, file: 'test.jsonl' };
function event(model, kind, data = null, options = {}) {
  const e = { schema: 1, task, pool_uuid: pool, pool_name: 'pool-one', execution: 'old', owner_epoch: 1,
    at_ms: 1000 + model.cursor, kind, activity: null, data, ...options };
  const record = { cursor: model.cursor + 1, event: e };
  model.apply(record);
  return record;
}
function fixture() {
  const model = new Model();
  event(model, 'PoolRegistered', null, { task: null });
  event(model, 'PoolRegistered', null, { task: null, pool_uuid: other, pool_name: '空池' });
  event(model, 'Registered', { workflow: 'storage.shrink' });
  event(model, 'Objects', [{ namespace: 'disk', id: 'a' }]);
  event(model, 'Started');
  event(model, 'Activity', { name: 'migrate', kind: 'Step', service: 'vd', state: 'Suspended', attempts: 1, retries: 0,
    progress: { completed: 2, total: 8 }, resources: [] }, { activity: 1 });
  event(model, 'Activity', { name: 'copy', kind: 'Call', service: 'vd', state: 'Completed', parent_id: { sequence: 1 },
    attempts: 1, retries: 0, resources: [{ object: { namespace: 'disk', id: 'a' }, access: 'Write' }] }, { activity: 2 });
  event(model, 'TaskState', 'Suspended');
  for (let i = 2; i <= 35; i++) {
    const id = `1/${pool}/storage.online-disk/${i}`;
    event(model, 'Registered', {}, { task: id, execution: `task-${i}` });
    event(model, 'Finished', 'Completed', { task: id, execution: `task-${i}` });
  }
  return model;
}
const press = (app, name, str = '') => app.key(str, { name });

test('four tables, drilldown and back preserve pool / task / execution identity', () => {
  const app = new App(fixture());
  press(app, 'return');
  assert.equal(app.tab, 'Workflows'); assert.equal(app.pool, pool);
  assert.equal(app.selection().row.id, task);
  press(app, 'return');
  assert.equal(app.tab, 'Activities'); assert.equal(app.task, task);
  press(app, 'down'); press(app, 'return');
  assert.equal(app.tab, 'Events'); assert.equal(app.activity, 2);
  assert.ok(app.rows().every(r => r.value.event.activity === 2));
  press(app, 'return'); assert.equal(app.modal.kind, 'details');
  press(app, 'escape'); press(app, 'escape');
  assert.equal(app.tab, 'Activities'); assert.equal(app.selection().row.value.activity.id, 2);
  press(app, 'escape'); assert.equal(app.tab, 'Workflows'); assert.equal(app.task, null);
  press(app, 'escape'); assert.equal(app.tab, 'Pools'); assert.equal(app.pool, null);
});

test('attention ordering, filters, search and stable selection under new events', () => {
  const model = fixture(), app = new App(model);
  app.setTab('Workflows');
  assert.equal(app.selection().row.id, task);
  press(app, 'down'); const selected = app.selection().row.id;
  event(model, 'TaskState', { Failed: { Uncertain: 'inspect effect' } });
  assert.equal(app.selection().row.id, selected);
  press(app, 'f'); assert.equal(app.rows().length, 0);
  press(app, 'f'); assert.equal(app.rows().length, 35);
  press(app, 'f'); assert.equal(app.rows().length, 1);
  press(app, '/', '/'); app.key('shrink', {}); press(app, 'return');
  assert.equal(app.rows().length, 1);
  press(app, '/', '/'); app.key('missing', {}); press(app, 'escape');
  assert.equal(app.search, 'shrink');
  press(app, 'a'); assert.equal(app.filter, 'all'); assert.equal(app.search, '');
});

test('freeze retains a coherent projection while live ingestion continues', () => {
  const model = fixture(), app = new App(model);
  app.setTab('Workflows'); press(app, 'space');
  const cursor = app.model.cursor;
  event(model, 'Finished', 'Completed');
  assert.equal(app.model.cursor, cursor);
  assert.equal(app.model.tasks.get(task).state, 'Suspended');
  assert.equal(app.live.tasks.get(task).state, 'Completed');
  press(app, 'space'); assert.equal(app.model.tasks.get(task).state, 'Completed');
});

test('execution chooser keeps old/new steps separate and cancelling has no scope effects', () => {
  const model = fixture();
  event(model, 'RecoveryPending'); event(model, 'Finished', { Failed: 'Cancelled' });
  event(model, 'Restored', {}, { execution: 'new', owner_epoch: 2 });
  event(model, 'Activity', { name: 'migrate', state: 'Running', kind: 'Step' }, { execution: 'new', owner_epoch: 2, activity: 1 });
  const app = new App(model); app.setTab('Workflows');
  press(app, 'e'); press(app, 'escape'); assert.equal(app.task, null);
  press(app, 'e'); press(app, 'down'); press(app, 'down'); press(app, 'return');
  assert.equal(app.execution, 'new'); assert.equal(app.tab, 'Activities'); assert.equal(app.rows().length, 1);
  assert.equal(app.rows()[0].value.execution.epoch, 2);
  press(app, 'e'); press(app, 'return'); assert.equal(app.rows().length, 3);
});

test('sort/columns/pool menus and pagination are real operations', () => {
  const app = new App(fixture()); app.setTab('Workflows');
  render(app, source, { width: 80, height: 24 });
  assert.ok(app.pageSize >= 8);
  press(app, 'pagedown'); assert.equal(app.selection().index, app.pageSize);
  press(app, 'end'); assert.equal(app.selection().index, 34);
  press(app, 'home'); assert.equal(app.selection().index, 0);
  press(app, 's'); press(app, 'down'); press(app, 'return'); assert.equal(app.sorts.Workflows, 'last');
  press(app, 'c'); press(app, 'return'); assert.equal(app.hidden.Workflows.size, 0);
  press(app, 'down'); press(app, 'space'); assert.ok(app.hidden.Workflows.has('name'));
  press(app, 'escape');
  press(app, 'p'); press(app, 'down'); press(app, 'down'); press(app, 'return');
  assert.equal(app.pool, other); assert.equal(app.rows().length, 0);
  press(app, 'a'); assert.equal(app.pool, null);
});

test('details are readable, immutable snapshots; raw JSON is opt-in', () => {
  const model = fixture(), app = new App(model);
  app.setTab('Workflows'); press(app, 'd');
  assert.equal(app.modal.raw, false);
  assert.ok(app.modal.sections.some(([label]) => label === 'Current blockers'));
  event(model, 'TaskState', 'Completed');
  assert.equal(app.modal.row.value.state, 'Suspended');
  press(app, 'r'); assert.equal(app.modal.raw, true);
  assert.equal(press(app, 'q'), 'quit');
});

test('all tabs and popups fit 64/80/120/160-column terminals without control injection', () => {
  const model = fixture();
  event(model, 'Activity', { name: '\x1b[2J磁盘上线👨‍💻', state: 'Running', kind: 'Step' }, { activity: 3 });
  const app = new App(model);
  for (const [width, height] of [[64,20],[80,24],[120,30],[160,44],[30,8]]) {
    for (const tab of tabs) {
      app.setTab(tab);
      for (const modal of [null, 'help', 'details', 'raw', 'columns', 'sort', 'pool']) {
        app.modal = null;
        if (modal === 'help') press(app, 'h');
        else if (modal === 'details' || modal === 'raw') app.details(app.selection().row, modal === 'raw');
        else if (modal) app.menu(modal);
        const screen = render(app, source, { width, height }).plain();
        assert.equal(screen.length, height);
        for (const line of screen) { assert.equal(cells(line), width, `${tab}/${modal} ${width}: ${line}`); assert.ok(!line.includes('\x1b')); }
      }
    }
  }
});

test('CJK, combining marks and emoji fit cells; overlay cannot split a wide glyph', () => {
  assert.equal(cells('磁盘é👨‍💻'), 7);
  assert.equal(cells(fit('磁盘abc', 6)), 6);
  assert.deepEqual(wrap('磁盘上线', 4), ['磁盘','上线']);
  const frame = new Frame(10, 2);
  frame.put(0, 0, '磁盘上线'); frame.put(1, 0, '|');
  frame.put(4, 0, 'X');
  assert.equal(cells(frame.plain()[0]), 10);
});

test('error, incomplete evidence, partial tail and tiny-screen messages stay visible', () => {
  const app = new App(fixture());
  assert.match(render(app, { ...source, error: 'corrupt frame' }).plain().join('\n'), /READ ERROR.*corrupt frame/);
  app.live.incomplete = true;
  assert.match(render(app, source).plain().join('\n'), /INCOMPLETE EVIDENCE/);
  app.live.incomplete = false;
  assert.match(render(app, { ...source, partial: true }).plain().join('\n'), /PARTIAL TAIL/);
  assert.match(render(app, source, { width: 40, height: 10 }).plain().join('\n'), /minimum 64x20/);
});

test('duration never invents end times and states distinguish uncertainty/recovery', () => {
  assert.equal(duration(null), '-'); assert.equal(duration(-1), '-');
  assert.equal(millis({ secs_since_epoch: 1, nanos_since_epoch: 500000000 }), 1500);
  assert.equal(status({ Failed: { Uncertain: 'timeout' } }).label, 'UNCERTAIN');
  assert.equal(status('Interrupted / recoverable').label, 'RECOVERABLE');
  assert.equal(status({ WaitingConditions: 'Failed disk needs recovery' }).label, 'WAIT COND');
  const app = new App(fixture()); app.setTab('Activities');
  assert.ok(app.rows().every(r => r.cells.duration === '-'));
  assert.ok(inspect(app.rows()[1]).some(([label]) => label === 'Resources'));
});

test('reader waits for complete UTF8 frames, tails append, rejects corruption/truncation', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'taskcenter-reader-'));
  t.after(() => fs.rmSync(directory, { recursive: true }));
  const file = path.join(directory, 'audit.jsonl');
  const reader = new JournalReader(file);
  reader.refresh(); assert.equal(reader.exists, false); assert.equal(reader.error, '');
  const model = new Model(), record = event(model, 'PoolRegistered', null, { task: null, pool_name: '存储池' });
  const bytes = Buffer.from(JSON.stringify(record) + '\n');
  const split = bytes.indexOf(Buffer.from('存')) + 1;
  fs.writeFileSync(file, bytes.subarray(0, split)); reader.refresh();
  assert.equal(reader.model.cursor, 0); assert.equal(reader.snapshot().partial, true);
  fs.appendFileSync(file, bytes.subarray(split)); reader.refresh();
  assert.equal(reader.model.catalog.get(pool).name, '存储池');
  const second = event(model, 'Registered'); fs.appendFileSync(file, JSON.stringify(second) + '\n'); reader.refresh();
  assert.equal(reader.model.cursor, 2);
  fs.appendFileSync(file, 'broken\n'); reader.refresh(); assert.ok(reader.error);
  fs.writeFileSync(file, '');
  const truncated = new JournalReader(file);
  fs.writeFileSync(file, bytes); truncated.refresh(); fs.truncateSync(file, 0); truncated.refresh();
  assert.match(truncated.error, /truncated/);
});

test('malformed Activity payload is rejected before advancing the projection', () => {
  const model = new Model();
  assert.throws(() => event(model, 'Activity', null, { activity: 1 }), /Invalid activity/);
  assert.equal(model.cursor, 0);
});
