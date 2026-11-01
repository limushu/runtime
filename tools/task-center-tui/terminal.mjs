import { cellWidth, safe } from './model.mjs';

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
    this.hits = [];
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
