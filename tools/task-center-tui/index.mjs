// Node 22+; local, read-only, no third-party packages or business control endpoints.
import readline from 'node:readline';
import { JournalReader } from './reader.mjs';
import { App } from './app.mjs';
import { render } from './screen.mjs';
import { MouseInput, hitTest } from './mouse.mjs';

const args = process.argv.slice(2);
if (!args.length || args.includes('--help')) {
  console.log('TaskCenter\nnode tools/task-center-tui/index.mjs <audit.jsonl> [--pool name-or-uuid] [--once] [--width 120] [--height 32]\nPool -> current/history workflows -> steps -> details. Enter or click opens a row. h shows help. Read only.');
  process.exit(0);
}
const option = name => args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
const dimension = (name, fallback) => {
  const value = option(name);
  if (value === undefined) return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1 || n > 500) throw Error(`${name} must be an integer between 1 and 500`);
  return n;
};
const reader = new JournalReader(args[0]);
reader.refresh();
const app = new App(reader.model);
// Keep the previous pool/workflow launch command usable; there are no global tabs now.
if (option('--tab') && !['pools','workflows'].includes(option('--tab').toLowerCase())) throw Error('Global activity/event tabs were removed. Open a workflow from its pool.');
if (option('--pool')) {
  while (!reader.error && reader.pending) reader.refresh();
  const pool = reader.model.pools().find(p => p.id === option('--pool') || p.name === option('--pool'));
  if (!pool) throw Error('Requested pool was not found in the journal');
  app.choosePool(pool.id);
}
const frame = () => render(app, reader.snapshot(), {
  // Reserve the rightmost terminal cell to prevent autowrap/scroll on the last row.
  width: dimension('--width', Math.max(1, (process.stdout.columns || 141) - 1)),
  height: dimension('--height', process.stdout.rows || 36),
});
if (args.includes('--once')) {
  while (!reader.error && reader.pending) reader.refresh();
  console.log(frame().plain().join('\n'));
  process.exitCode = reader.error ? 1 : 0;
} else if (!process.stdin.isTTY || !process.stdout.isTTY) {
  console.error('Interactive mode needs a terminal. Use --once for a snapshot.');
  process.exitCode = 1;
} else {
  readline.emitKeypressEvents(process.stdin);
  process.stdin.setRawMode(true);
  process.stdout.write('\x1b[?1049h\x1b[?25l\x1b[?1000h\x1b[?1006h\x1b[2J');
  let previous = [], closed = false, size = '', currentFrame;
  function draw() {
    const screen = frame(), nextSize = `${screen.width}:${screen.height}`, lines = screen.ansi();
    currentFrame = screen;
    let output = '';
    if (size !== nextSize) { output = '\x1b[2J'; previous = []; size = nextSize; }
    // Only replace changed lines. No periodic full-screen clear or newline scrolling.
    for (let i = 0; i < lines.length; i++) if (lines[i] !== previous[i]) output += `\x1b[${i + 1};1H${lines[i]}\x1b[K`;
    if (output) process.stdout.write(output);
    previous = lines;
  }
  const timer = setInterval(() => { reader.refresh(); draw(); }, 250);
  function close() {
    if (closed) return;
    closed = true;
    clearInterval(timer);
    process.stdin.setRawMode(false);
    process.stdin.pause();
    process.stdout.write('\x1b[?1000l\x1b[?1006l\x1b[0m\x1b[?25h\x1b[?1049l');
  }
  process.on('exit', close);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { close(); process.exit(0); });
  process.stdout.on('resize', draw);
  process.stdin.on('keypress', (str, key) => {
    if (key?.sequence?.startsWith('\x1b[<')) return;
    if (app.key(str, key) === 'quit') { close(); process.exit(0); }
    draw();
  });
  const mouse = new MouseInput();
  process.stdin.on('data', data => {
    for (const event of mouse.feed(data)) {
      if (event.wheel) app.key('', { name:event.wheel > 0 ? 'down' : 'up' });
      else app.click(hitTest(currentFrame, event.x, event.y));
      draw();
    }
  });
  draw();
}
