// Node 22+ terminal viewer. No server, external packages, telemetry, or write/control API.
import fs from 'node:fs';
import readline from 'node:readline';
import { StringDecoder } from 'node:string_decoder';
import { Model, safe, stateName, wrapCells } from './model.mjs';

const args = process.argv.slice(2);
if (!args.length || args.includes('--help')) {
  console.log('node tools/task-center-tui/index.mjs <audit.jsonl> [--once]\nRead-only: arrows/j/k select, Enter drill down, Esc back, Tab current/history/all, / search, space freeze, q quit.');
  process.exit(0);
}
const file = args[0], once = args.includes('--once');
let model = new Model(), offset = 0, tail = '', decoder = new StringDecoder('utf8'), error = '', frozen = false;
let stack = [{ page: 'pools', selected: null }], search = '', searching = false, filter = 'all', scroll = 0;
let identity = null;
function refresh() {
  if (error) return;
  let fd;
  try {
    fd = fs.openSync(file, 'r'); const stat = fs.fstatSync(fd);
    const currentIdentity = `${stat.dev}:${stat.ino}`;
    if ((identity && identity !== currentIdentity) || stat.size < offset) {
      throw Error('Audit file replaced/truncated. Restart viewer to load the new stream.');
    }
    identity = currentIdentity;
    const buffer = Buffer.alloc(64 * 1024);
    let budget = 4 * 1024 * 1024;
    while (budget > 0) {
      const n = fs.readSync(fd, buffer, 0, Math.min(buffer.length, budget), offset);
      if (!n) break;
      offset += n; budget -= n; tail += decoder.write(buffer.subarray(0,n));
      let end;
      while ((end = tail.indexOf('\n')) >= 0) {
        const line = tail.slice(0,end); tail = tail.slice(end+1);
        model.apply(JSON.parse(line));
      }
      if (tail.length > 16 * 1024 * 1024) throw Error('Audit frame exceeds 16 MiB');
    }
  } catch (e) { if (e.code !== 'ENOENT') error = e.message; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
}
function rows() {
  const p = stack.at(-1);
  if (p.page === 'pools') return model.pools().map(pool => ({ id: pool.id,
    label: `${pool.name}  [${pool.id.slice(0,8)}]   ${pool.tasks.filter(t=>!t.terminal).length} non-terminal / ${pool.tasks.filter(t=>t.terminal).length} history`, next: {page:'tasks',pool:pool.id} }));
  if (p.page === 'tasks') return model.selectTasks(p.pool,filter,search).map(t => ({ id:t.id,
    label:`${t.incomplete?'! ':''}${t.name}  ${t.state}  [${t.id}]`, next:{page:'steps',task:t.id,pool:p.pool} }));
  const task = model.tasks.get(p.task);
  if (p.page === 'steps') {
    const result = [{id:'task-events',label:'[Workflow event timeline / controls / checkpoints]',next:{...p,page:'events',execution:null,activity:null}}];
    for (const execution of task.executions.values()) {
      result.push({id:execution.id,label:`[Execution epoch=${execution.epoch} ${execution.id.slice(0,8)} — event timeline]`, next:{...p,page:'events',execution:execution.id,activity:null}});
      for (const activity of execution.activities.values()) {
        const d=activity.data;
        result.push({id:`${execution.id}:${activity.id}`,label:`  ${d.parent_id?'  ↳ ':''}#${activity.id} ${d.kind} ${d.name} · ${stateName(d.state)} · attempts=${d.attempts} retries=${d.retries}`,next:{...p,page:'events',execution:execution.id,activity:activity.id}});
      }
    }
    return result;
  }
  if (p.page === 'events') {
    const events = p.activity !== null ? task.executions.get(p.execution).activities.get(p.activity).events : p.execution ? task.executions.get(p.execution).events : task.events;
    return events.map(r=>({id:r.cursor,label:`${r.cursor} ${new Date(r.event.at_ms).toISOString()} ${r.event.kind} ${stateName(r.event.data?.state ?? r.event.data).slice(0,160)}`,next:{...p,page:'detail',record:r}}));
  }
  return [];
}
function clip(value,width) { return wrapCells(value,Math.max(2,width))[0]; }
function render() {
  const p=stack.at(-1), width=Math.max(25,process.stdout.columns||120), height=Math.max(8,process.stdout.rows||35);
  const lines=[`TASKCENTER  |  READ ONLY  |  cursor ${model.cursor}${frozen?'  [DISPLAY FROZEN]':''}`, stack.map(s=>s.page).join(' > '),
    `Filter: ${filter}  Search: ${search}  | states are last reported, NOT host liveness`,
    error ? `ERROR: ${error} — displayed evidence may be incomplete` : model.incomplete ? 'WARNING: journal reports incomplete evidence; do not treat this as a complete audit' : `Journal: ${file}${tail?'  [partial tail, awaiting writer]':''}`];
  if (p.page === 'detail') {
    const detail=JSON.stringify(p.record,null,2).split('\n');
    const wrapped=detail.flatMap(line=>wrapCells(line,width-2));
    scroll=Math.min(scroll,Math.max(0,wrapped.length-(height-6)));
    lines.push(...wrapped.slice(scroll,scroll+height-6));
  } else {
    const list=rows(); let index=list.findIndex(r=>r.id===p.selected);
    if(index<0) {index=0;p.selected=list[0]?.id??null;}
    const start=Math.max(0,index-Math.floor((height-6)/2));
    lines.push(...list.slice(start,start+height-6).map(r=>`${r.id===p.selected?'>':' '} ${r.label}`));
    if(!list.length) lines.push('(No records in this scope. Waiting for audit-enabled pools.)');
  }
  while(lines.length<height-1) lines.push('');
  lines.push(searching?`Search: ${search}_  Enter accept / Esc clear`:'q:quit Esc:back Enter:open ↑↓/jk /:find Tab:filter Space:freeze');
  return lines.slice(0,height).map(line=>clip(line,width-1)).join('\n');
}
refresh();
if (once) {
  // Drain all currently present records, without altering the file.
  while(!error && fs.existsSync(file) && offset<fs.statSync(file).size) refresh();
  console.log(render()); process.exitCode=error?1:0;
} else if (!process.stdin.isTTY || !process.stdout.isTTY) {
  console.error('Interactive TUI requires a terminal. Use --once for a read-only snapshot.'); process.exitCode=1;
} else {
  readline.emitKeypressEvents(process.stdin); process.stdin.setRawMode(true);
  process.stdout.write('\x1b[?1049h\x1b[?25l');
  let lastScreen='';
  const draw=()=>{const screen=render();if(screen!==lastScreen){lastScreen=screen;process.stdout.write('\x1b[H\x1b[2J'+screen);}};
  const timer=setInterval(()=>{refresh();if(!frozen)draw();},250);
  let closed=false;
  function close(){if(closed)return;closed=true;clearInterval(timer);process.stdin.setRawMode(false);process.stdin.pause();process.stdout.write('\x1b[?25h\x1b[?1049l');}
  process.on('exit',close);process.on('SIGINT',()=>{close();process.exit(0);});
  process.stdout.on('resize',draw);
  process.stdin.on('keypress',(str,key={})=>{
    if(key.ctrl&&key.name==='c'){close();process.exit(0);}
    if(searching){
      if(key.name==='return')searching=false;
      else if(key.name==='escape'){searching=false;search='';}
      else if(key.name==='backspace')search=[...search].slice(0,-1).join('');
      else if(str&&!key.ctrl&&!key.meta)search+=safe(str);
      draw();return;
    }
    if(key.name==='q'){close();process.exit(0);}
    if(key.name==='space'){frozen=!frozen;draw();return;}
    if(key.name==='escape'){if(stack.length>1)stack.pop();scroll=0;}
    if(key.name==='tab'){filter=filter==='all'?'current':filter==='current'?'history':'all';}
    if(str==='/'){searching=true;}
    const p=stack.at(-1),list=rows(),i=Math.max(0,list.findIndex(r=>r.id===p.selected));
    if(key.name==='down'||key.name==='j'){if(p.page==='detail')scroll++;else p.selected=list[Math.min(list.length-1,i+1)]?.id;}
    if(key.name==='up'||key.name==='k'){if(p.page==='detail')scroll=Math.max(0,scroll-1);else p.selected=list[Math.max(0,i-1)]?.id;}
    if(key.name==='return'&&list[i]?.next){stack.push({...list[i].next,selected:null});scroll=0;}
    draw();
  });
  draw();
}
