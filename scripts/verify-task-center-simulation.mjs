// Node >= 22. No packages. Optional real-browser tests: --browser [--screenshot path.png].
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

const base = fileURLToPath(new URL('../docs/diagrams/task-center-simulation/', import.meta.url));
const context = vm.createContext({});
vm.runInContext(fs.readFileSync(path.join(base, 'scenario.js'), 'utf8'), context);
const { frames, steps } = context.TaskCenterScenario;
const frame = id => { const f = frames.find(f => f.id === id); assert.ok(f, id); return f.state; };
assert.equal(frames.length, 56);
assert.equal(steps.length, 4);
assert.ok(Object.isFrozen(frames[6].state.disk));
assert.equal(new Set(frames.map(f => f.id)).size, frames.length);
assert.equal(frames.filter(f => f.checkpoint).length, 1);
const html = fs.readFileSync(path.join(base, 'index.html'), 'utf8');
for (const f of frames) {
  for (const id of f.nodes) assert.ok(html.includes(`id="node-${id}"`), `${f.id}: node ${id}`);
  for (const id of f.routes) assert.ok(html.includes(`id="route-${id}"`), `${f.id}: route ${id}`);
  assert.ok(f.state.pending === 0 || f.state.pending === 1);
  if (f.state.pending) assert.equal(f.state.held.length, 1, 'guard retains resource');
}
for (let i = 0; i < 4; i++) {
  const state = phase => frame(`s${i}-${phase}`);
  assert.equal(state('acquire').queue.length, 1);
  assert.equal(state('acquire').future, 'Pending · 等许可');
  assert.equal(state('wake').future, 'Ready · 待轮询');
  assert.equal(state('wake').pending, 0, 'wake does not execute Activity');
  assert.equal(state('repoll').future, 'Polling');
  assert.equal(state('dispatch').pending, 1);
  assert.equal(state('settle').pending, 0);
  assert.equal(state('settle').held.length, 1, 'Release is queued, not applied');
  assert.equal(state('release').held.length, 0);
  assert.equal(state('release').owners.length, 2, 'logical ownership spans steps');
  assert.equal(state('return').done, i + 1);
}
assert.equal(frame('s1-settle').disk.io, 'Down');
assert.equal(frame('s2-settle').disk.io, 'Down', 'Open does not enable IO');
assert.equal(frame('s2-settle').disk.openConfirmed, true);
assert.equal(frame('s3-settle').disk.io, 'Up');
assert.equal(frame('complete').owners.length, 2);
const final = frame('reap');
assert.equal(final.workflow, 'Completed');
for (const key of ['held', 'waiting', 'owners', 'queue']) assert.equal(final[key].length, 0);
console.log('PASS: 56 immutable frames, four Activities, wake/poll separation, guard lifetime, disk effects and cleanup.');

if (process.argv.includes('--browser')) await browserCheck();

async function browserCheck() {
  const executable = process.env.CHROME_PATH || (process.platform === 'win32'
    ? 'C:/Program Files/Google/Chrome/Application/chrome.exe'
    : process.platform === 'darwin' ? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' : 'google-chrome');
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'task-center-browser-'));
  const chrome = spawn(executable, ['--headless', '--disable-gpu', '--no-first-run', '--remote-debugging-port=0', '--user-data-dir=' + profile, 'about:blank'], { windowsHide: true, stdio: 'ignore' });
  let socket, launchError;
  chrome.on('error', e => { launchError = e; });
  const errors = [], external = [];
  try {
    let port;
    for (let i = 0; i < 100; i++) {
      if (launchError) throw launchError;
      try { port = fs.readFileSync(path.join(profile, 'DevToolsActivePort'), 'utf8').split('\n')[0]; break; } catch { await delay(100); }
    }
    assert.ok(port, 'Chrome startup timeout; set CHROME_PATH');
    const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
    socket = new WebSocket(targets.find(t => t.type === 'page').webSocketDebuggerUrl);
    await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, { once: true }); socket.addEventListener('error', reject, { once: true }); });
    let sequence = 0;
    const pending = new Map();
    socket.addEventListener('message', event => {
      const m = JSON.parse(event.data), p = pending.get(m.id);
      if (p) { clearTimeout(p.timer); pending.delete(m.id); if (m.error) p.reject(Error(JSON.stringify(m.error))); else p.resolve(m.result); }
      if (m.method === 'Runtime.exceptionThrown') errors.push(m.params.exceptionDetails);
      if (m.method === 'Log.entryAdded' && m.params.entry.level === 'error') errors.push(m.params.entry);
      if (m.method === 'Network.requestWillBeSent' && /^https?:/.test(m.params.request.url)) external.push(m.params.request.url);
    });
    const call = (method, params = {}) => new Promise((resolve, reject) => {
      const id = ++sequence;
      const timer = setTimeout(() => { pending.delete(id); reject(Error('CDP timeout: ' + method)); }, 10000);
      pending.set(id, { resolve, reject, timer }); socket.send(JSON.stringify({ id, method, params }));
    });
    const evaluate = async expression => {
      const r = await call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
      if (r.exceptionDetails) throw Error(JSON.stringify(r.exceptionDetails));
      return r.result.value;
    };
    const until = async (expression, timeout = 10000) => {
      const start = Date.now();
      while (Date.now() - start < timeout) { if (await evaluate(expression)) return; await delay(50); }
      throw Error('Wait timed out: ' + expression);
    };
    const click = id => evaluate(`document.getElementById(${JSON.stringify(id)}).click()`);
    const inspect = () => evaluate('TaskCenterDemo.inspect()');
    await call('Runtime.enable'); await call('Log.enable'); await call('Network.enable');
    await call('Emulation.setDeviceMetricsOverride', { width: 1600, height: 1050, deviceScaleFactor: 1, mobile: false });
    await call('Page.navigate', { url: pathToFileURL(path.join(base, 'index.html')).href });
    await until("document.readyState === 'complete' && !!window.TaskCenterDemo");
    assert.equal((await inspect()).cursor, 0);
    const ids = await evaluate('[...document.querySelectorAll("[id]")].map(n=>n.id)');
    assert.equal(ids.length, new Set(ids).size, 'unique DOM ids');
    // Single-step every event and verify UI follows the committed snapshot.
    for (let i = 1; i < frames.length; i++) {
      await click('next-step');
      const actual = await inspect();
      assert.equal(actual.cursor, i);
      assert.equal(actual.playing, false);
      assert.equal(JSON.stringify(actual.state), JSON.stringify(frames[i].state));
      assert.equal(await evaluate('document.getElementById("io-state").textContent'), frames[i].state.disk.io);
    }
    await click('restart');
    await evaluate('document.getElementById("speed").value="4"');
    await click('play');
    await until('TaskCenterDemo.inspect().frame === "s0-acquire" && !TaskCenterDemo.inspect().playing');
    assert.equal(await evaluate('document.getElementById("checkpoint").hidden'), false);
    assert.equal(await evaluate('document.getElementById("resource-detail").open'), true);
    const shotIndex = process.argv.indexOf('--screenshot');
    if (shotIndex !== -1) {
      const screenshot = await call('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(process.argv[shotIndex + 1], Buffer.from(screenshot.data, 'base64'));
    }
    await click('continue'); await delay(120); await click('play');
    const paused = await inspect(); await delay(180);
    assert.deepEqual(await inspect(), paused, 'pause freezes packet and snapshot');
    assert.equal(paused.playing, false); assert.ok(paused.motion);
    await click('play');
    await until('TaskCenterDemo.inspect().frame === "reap" && !TaskCenterDemo.inspect().playing', 45000);
    assert.equal((await inspect()).state.disk.io, 'Up');
    await click('play'); assert.equal((await inspect()).cursor, 0, 'replay resets');
    await click('restart');
    await evaluate('document.querySelectorAll(".chapter")[2].click()');
    assert.equal((await inspect()).frame, 's2-enter');
    await click('back-step'); assert.equal((await inspect()).frame, 's1-return');
    await evaluate('document.dispatchEvent(new KeyboardEvent("keydown", {key:"ArrowRight",bubbles:true}))');
    assert.equal((await inspect()).frame, 's2-enter');
    await evaluate('const slider=document.getElementById("timeline");slider.value=9;slider.dispatchEvent(new Event("input"))');
    assert.equal((await inspect()).frame, 's0-wake');
    assert.equal(await evaluate('document.getElementById("node-futures").classList.contains("is-ready")'), true);
    await evaluate('document.getElementById("node-locks").dispatchEvent(new MouseEvent("click",{bubbles:true}))');
    assert.equal(await evaluate('document.getElementById("component-detail").open'), true);
    assert.equal(await evaluate('document.getElementById("component-name").textContent'), 'LockTable');
    assert.equal(await evaluate(`[...document.querySelectorAll('#architecture text')].every(t => {
      const b=t.getBBox(); return b.x>=0 && b.y>=0 && b.x+b.width<=1160 && b.y+b.height<=850;
    })`), true, 'SVG text inside canvas');
    if (shotIndex !== -1) {
      const shot = await call('Page.captureScreenshot', {format:'png'});
      fs.writeFileSync(process.argv[shotIndex+1].replace(/\.png$/, '-wake.png'), Buffer.from(shot.data,'base64'));
    }
    await call('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-reduced-motion', value: 'reduce' }] });
    await click('play'); await delay(60);
    assert.equal(await evaluate('getComputedStyle(document.getElementById("packet")).display'), 'none');
    await click('play');
    await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, 'no mobile page overflow');
    await click('fit-diagram');
    assert.equal(await evaluate('document.getElementById("canvas-scroll").classList.contains("fitted")'), false);
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, 'zoom contained in canvas');
    assert.deepEqual(errors, [], 'no browser/CSP errors');
    assert.deepEqual(external, [], 'no external network requests');
    console.log('PASS: file:// browser, all single steps, autoplay checkpoint, pause/resume, full playback, replay, seek, chapters, keyboard, component details, reduced motion, mobile, CSP and offline operation.');
    await call('Browser.close').catch(() => {});
  } finally {
    if (socket) socket.close(); chrome.kill();
    // Retain the isolated temporary profile for diagnostics; never touch a user profile.
  }
}
