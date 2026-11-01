// Node 22+; local, read-only, no third-party packages or business control endpoints.
import readline from 'node:readline';
import { JournalReader } from './reader.mjs';
import { App, tabs } from './app.mjs';
import { render } from './screen.mjs';

const args = process.argv.slice(2);
if (!args.length || args.includes('--help')) {
  console.log('TaskCenter TOP\nnode tools/task-center-tui/index.mjs <audit.jsonl> [--once] [--tab pools|workflows|activities|events] [--width 140] [--height 40]\nRead-only terminal console. Press h for keys. --once is a plain-text snapshot.');
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
if (option('--tab')) {
  const tab = tabs.find(t => t.toLowerCase() === option('--tab').toLowerCase());
  if (!tab) throw Error('Unknown tab');
  app.setTab(tab);
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
  process.stdout.write('\x1b[?1049h\x1b[?25l\x1b[2J');
  let previous = [], closed = false, size = '';
  function draw() {
    const screen = frame(), nextSize = `${screen.width}:${screen.height}`, lines = screen.ansi();
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
    process.stdout.write('\x1b[0m\x1b[?25h\x1b[?1049l');
  }
  process.on('exit', close);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { close(); process.exit(0); });
  process.stdout.on('resize', draw);
  process.stdin.on('keypress', (str, key) => {
    if (app.key(str, key) === 'quit') { close(); process.exit(0); }
    draw();
  });
  draw();
}
