import { cellWidth, safe } from './model.mjs';
import { columns, tabs } from './app.mjs';
import { inspect, shortTask, stats, text, time } from './presentation.mjs';

const segmenter = new Intl.Segmenter('en', { granularity: 'grapheme' });
const glyphs = value => [...segmenter.segment(safe(value))].map(s => s.segment);
const widthOf = glyph => /\p{Extended_Pictographic}/u.test(glyph) || /\p{Regional_Indicator}/u.test(glyph) ? 2 : cellWidth([...glyph][0] ?? ' ');
export const cells = value => glyphs(value).reduce((sum, g) => sum + widthOf(g), 0);
export function fit(value, width, right = false) {
  if (width < 1) return '';
  const all = glyphs(value ?? '-');
  let output = '', used = 0;
  const truncated = all.reduce((sum, g) => sum + widthOf(g), 0) > width;
  for (const g of all) {
    if (used + widthOf(g) > width - (truncated ? 1 : 0)) break;
    output += g; used += widthOf(g);
  }
  if (truncated) { output += '~'; used++; }
  return right ? ' '.repeat(width - used) + output : output + ' '.repeat(width - used);
}
export function wrap(value, width) {
  const lines = [];
  for (const source of String(value).split('\n')) {
    let line = '', used = 0;
    for (const g of glyphs(source)) {
      const size = widthOf(g);
      if (used + size > width && line) { lines.push(line); line = ''; used = 0; }
      if (size > width) continue;
      line += g; used += size;
    }
    lines.push(line);
  }
  return lines;
}

const palette = {
  normal: '38;5;252;48;5;234', muted: '38;5;245;48;5;234', border: '38;5;239;48;5;234',
  title: '1;38;5;51;48;5;235', header: '1;38;5;252;48;5;237',
  accent: '38;5;81;48;5;234', good: '38;5;114;48;5;234', warn: '38;5;221;48;5;234', bad: '1;38;5;203;48;5;234',
  selected: '1;38;5;16;48;5;80', tab: '1;38;5;16;48;5;81', footer: '38;5;252;48;5;237',
  dim: '38;5;242;48;5;233', overlay: '38;5;252;48;5;235', label: '1;38;5;81;48;5;235',
};
export class Frame {
  constructor(width, height) {
    this.width = width; this.height = height;
    this.rows = Array.from({ length: height }, () => Array.from({ length: width }, () => ({ text: ' ', style: 'normal' })));
  }
  put(x, y, value, style = 'normal', max = this.width - x) {
    if (y < 0 || y >= this.height) return;
    let used = 0;
    for (const glyph of glyphs(value)) {
      const size = widthOf(glyph);
      if (used + size > max || x + size > this.width) break;
      if (size === 0) continue;
      if (x >= 0) {
        // Overlay borders may bisect a double-width glyph; clear the whole old cell.
        for (let c = x; c < x + size; c++) {
          if (this.rows[y][c].text === '' && c > 0) this.rows[y][c - 1] = { text: ' ', style };
          else if (widthOf(this.rows[y][c].text) === 2 && c + 1 < this.width) this.rows[y][c + 1] = { text: ' ', style };
        }
        this.rows[y][x] = { text: glyph, style };
        if (size === 2) this.rows[y][x + 1] = { text: '', style };
      }
      x += size; used += size;
    }
  }
  fill(x, y, width, height, style = 'normal') {
    for (let row = y; row < y + height; row++) this.put(x, row, ' '.repeat(Math.max(0, width)), style);
  }
  box(x, y, width, height, title, style = 'border') {
    if (width < 3 || height < 3) return;
    this.put(x, y, '+' + '-'.repeat(width - 2) + '+', style);
    this.put(x, y + height - 1, '+' + '-'.repeat(width - 2) + '+', style);
    for (let row = y + 1; row < y + height - 1; row++) { this.put(x, row, '|', style); this.put(x + width - 1, row, '|', style); }
    this.put(x + 2, y, fit(` ${title} `, width - 5).trimEnd(), 'accent');
  }
  plain() { return this.rows.map(row => row.map(c => c.text).join('')); }
  ansi() {
    return this.rows.map(row => {
      let output = '', previous = null;
      for (const cell of row) {
        if (cell.style !== previous) { output += `\x1b[0;${palette[cell.style] ?? palette.normal}m`; previous = cell.style; }
        output += cell.text;
      }
      return output + '\x1b[0m';
    });
  }
}

function tableColumns(app, width) {
  const available = columns[app.tab].filter(([id]) => !app.hidden[app.tab].has(id));
  if (app.tab === 'Workflows' && !app.pool) {
    const pool = available.findIndex(([id]) => id === 'pool');
    if (pool >= 0) available.splice(3, 0, ...available.splice(pool, 1));
  }
  if (app.tab === 'Activities' && !app.task) {
    const task = available.findIndex(([id]) => id === 'task');
    if (task >= 0) available.unshift(...available.splice(task, 1));
  }
  const result = [];
  let remaining = width;
  for (const column of available) {
    if (remaining < 6) break;
    const size = Math.min(column[2], remaining);
    result.push({ id: column[0], label: column[1], width: size });
    remaining -= size;
  }
  if (remaining && result.length) result[result.length - 1].width += remaining;
  return result;
}
function sectionLines(sections, width) {
  return sections.flatMap(([label, value]) => [ { text: label.toUpperCase(), style: 'accent' },
    ...wrap(value, width).map(text => ({ text, style: 'normal' })), { text: '', style: 'normal' } ]);
}

export function render(app, source, { width = 120, height = 36, now = Date.now() } = {}) {
  const frame = new Frame(Math.max(1, width), Math.max(1, height));
  if (width < 64 || height < 20) {
    frame.put(1, 1, 'TASKCENTER TOP', 'title');
    frame.put(1, 3, `Terminal ${width}x${height}; minimum 64x20.`, 'warn');
    frame.put(1, 5, 'Resize terminal. Ingestion continues. q exits.', 'muted');
    return frame;
  }
  const compact = height < 30;
  const tableTop = compact ? 6 : 8;
  const tabsY = compact ? 4 : 5;
  const model = app.model, tasks = [...model.tasks.values()], count = stats(tasks);
  frame.fill(0, 0, width, 1, 'title');
  frame.put(2, 0, 'TASKCENTER TOP', 'title');
  const feed = app.frozen ? `FROZEN +${app.live.cursor - model.cursor}` : 'FOLLOW';
  frame.put(width - 30, 0, fit(`${feed}  |  READ ONLY`, 28, true), 'title');
  const last = model.records.at(-1)?.event.at_ms;
  frame.put(2, 1, fit(`#${model.cursor}  Last event ${time(last)} UTC | States != host liveness`, width - 4), 'muted');
  const metrics = [['POOLS', model.catalog.size, 'accent'], ['CURRENT', count.current, 'accent'], ['WAITING', count.waiting, 'warn'],
    ['PAUSED', count.paused, 'warn'], ['ISSUES', count.issues, count.issues ? 'bad' : 'muted'], ['HISTORY', count.history, 'good']];
  const metricWidth = Math.floor((width - 4) / metrics.length);
  metrics.forEach(([label, value, tone], i) => {
    const x = 2 + i * metricWidth;
    if (compact) frame.put(x, 2, fit(`${label} ${value}`, metricWidth - 1), tone);
    else {
      frame.put(x, 2, label, 'muted'); frame.put(x, 3, String(value), tone);
      if (i > 0) { frame.put(x - 2, 2, '|', 'border'); frame.put(x - 2, 3, '|', 'border'); }
    }
  });
  frame.put(0, tabsY, ' '.repeat(width), 'header');
  let tabX = 1;
  for (let i = 0; i < tabs.length; i++) {
    const label = ` ${i + 1} ${tabs[i].toUpperCase()} `;
    frame.put(tabX, tabsY, label, tabs[i] === app.tab ? 'tab' : 'header'); tabX += label.length + 1;
  }
  const pool = app.pool ? model.catalog.get(app.pool)?.name ?? app.pool.slice(0, 8) : 'ALL';
  const scope = `Pool: ${pool}${app.task ? ` / WF ${shortTask(app.task)}` : ''}${app.execution ? ` / run ${app.execution.slice(0, 8)}` : ''}${app.activity !== null ? ` / step ${app.activity}` : ''}`;
  frame.put(2, tabsY + 1, fit(`${scope}  |  ${app.tab === 'Workflows' ? `filter:${app.filter}  ` : ''}sort:${app.sorts[app.tab]}${app.search ? `  find:${app.search}` : ''}`, width - 4), 'muted');
  const wide = width >= 124;
  const paneWidth = wide ? Math.min(46, Math.floor(width * 0.30)) : 0;
  const tableWidth = wide ? width - paneWidth - 2 : width;
  const bottom = height - (wide ? 4 : 6);
  frame.box(0, tableTop, tableWidth, bottom - tableTop + 1, app.tab.toUpperCase());
  const cols = tableColumns(app, tableWidth - 4);
  let cx = 2;
  for (const c of cols) { frame.put(cx, tableTop + 1, fit(c.label, c.width), 'header'); cx += c.width; }
  const { row: selected, rows, index } = app.selection();
  app.pageSize = Math.max(1, bottom - tableTop - 3);
  const page = Math.floor(index / app.pageSize), start = page * app.pageSize;
  for (let i = 0; i < app.pageSize; i++) {
    const row = rows[start + i];
    if (!row) continue;
    const chosen = row.id === selected?.id;
    frame.put(1, tableTop + 2 + i, chosen ? '>' : ' ', chosen ? 'selected' : 'normal');
    let x = 2;
    for (const c of cols) {
      const style = chosen ? 'selected' : c.id === 'state' || c.id === 'issues' ? row.tone : i % 2 ? 'muted' : 'normal';
      frame.put(x, tableTop + 2 + i, fit(row.cells[c.id], c.width - 1) + ' ', style); x += c.width;
    }
  }
  if (!rows.length) {
    frame.put(3, tableTop + 2, 'No matching records.', 'muted');
    if (app.pageSize > 2) frame.put(3, tableTop + 4, fit('a: clear filters  p: choose pool  /: search', tableWidth - 6), 'muted');
  }
  const pageLabel = `${rows.length ? start + 1 : 0}-${Math.min(start + app.pageSize, rows.length)} / ${rows.length}  Page ${page + 1}/${Math.max(1, Math.ceil(rows.length / app.pageSize))}`;
  frame.put(2, bottom - 1, fit(pageLabel, tableWidth - 4), 'muted');
  if (wide) {
    const x = tableWidth + 1;
    frame.box(x, tableTop, paneWidth + 1, bottom - tableTop + 1, 'INSPECTOR / d: expand');
    const lines = sectionLines(inspect(selected), paneWidth - 4);
    lines.slice(0, bottom - tableTop - 3).forEach((line, i) => frame.put(x + 2, tableTop + 2 + i, line.text, line.style, paneWidth - 4));
    if (lines.length > bottom - tableTop - 3) frame.put(x + 2, bottom - 1, 'd: full details / r: raw record', 'muted', paneWidth - 4);
  } else {
    const sections = inspect(selected);
    frame.put(2, bottom + 1, fit(`${sections[0][0]}: ${sections[0][1]}`, width - 4), 'accent');
    frame.put(2, bottom + 2, fit(`${sections[1]?.[0] ?? ''}: ${sections[1]?.[1] ?? ''}`, width - 4), selected?.tone ?? 'muted');
  }
  let sourceState = !source.exists ? 'WAITING FOR FILE' : source.pending ? 'READING BACKLOG' : source.partial ? 'PARTIAL TAIL / awaiting writer' :
    `READ OK / ${source.lastRead ? `${Math.max(0, Math.floor((now - source.lastRead) / 1000))}s since ingestion` : 'no events'}`;
  if (source.error) sourceState = `READ ERROR: ${source.error}`;
  else if (model.incomplete) sourceState = 'INCOMPLETE EVIDENCE / audit writer reported lost records';
  frame.put(1, height - 3, fit(sourceState, width - 2), source.error || model.incomplete ? 'bad' : 'muted');
  frame.fill(0, height - 2, width, 2, 'footer');
  frame.put(1, height - 2, fit('Enter Open  d Details  s Sort  c Columns  / Search  f Filter  h Help', width - 2), 'footer');
  frame.put(1, height - 1, fit(app.searching ? `Find: ${app.search}_  [Enter accept / Esc undo]` :
    '1-4 Tabs  p Pool  e Execution  PgUp/Dn Page  Space Freeze  Esc Back  q Quit', width - 2), 'footer');
  if (app.modal) drawModal(frame, app.modal);
  return frame;
}

function drawModal(frame, modal) {
  for (const row of frame.rows) for (const cell of row) cell.style = 'dim';
  const width = Math.min(frame.width - 6, 100);
  const height = Math.min(frame.height - 4, modal.items ? modal.items.length + 4 : 32);
  const x = Math.floor((frame.width - width) / 2), y = Math.floor((frame.height - height) / 2);
  frame.fill(x, y, width, height, 'overlay');
  frame.box(x, y, width, height, modal.raw ? 'ORIGINAL AUDIT RECORD' : modal.title, 'accent');
  const capacity = height - 4;
  let lines;
  if (modal.items) {
    lines = modal.items.map((item, i) => ({ text: `${i === modal.index ? '> ' : '  '}${item.label}`, style: i === modal.index ? 'selected' : 'overlay' }));
    modal.scroll = Math.max(0, modal.index - capacity + 1);
  } else if (modal.kind === 'details') {
    lines = modal.raw ? wrap(JSON.stringify(modal.record, null, 2) ?? 'No audit event recorded', width - 6).map(text => ({ text, style: 'overlay' })) :
      sectionLines(modal.sections, width - 6);
  } else lines = modal.lines.flatMap(line => wrap(text(line), width - 6)).map(text => ({ text, style: 'overlay' }));
  modal.scroll = Math.min(modal.scroll ?? 0, Math.max(0, lines.length - capacity));
  lines.slice(modal.scroll, modal.scroll + capacity).forEach((line, i) => frame.put(x + 3, y + 2 + i, fit(line.text, width - 6), line.style));
  frame.put(x + 2, y + height - 2, fit(`Esc Close  ${modal.items ? 'Up/Down Select  Enter Apply' : 'Up/Down/PgUp/PgDn Scroll  r Raw'}  ${modal.scroll + 1}/${Math.max(1, lines.length)}`, width - 4), 'label');
}
