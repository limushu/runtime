// Node >= 22. Deterministic trace checks + optional real Chrome interaction checks.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawn} from 'node:child_process';
import {setTimeout as delay} from 'node:timers/promises';
const base=fileURLToPath(new URL('../docs/diagrams/task-center-simulation/',import.meta.url));
const context=vm.createContext({});vm.runInContext(fs.readFileSync(path.join(base,'scenario.js'),'utf8'),context);
const scenarios=context.RuntimeScenarios;
const scene=id=>scenarios.find(s=>s.id===id);
const frame=(id,name)=>{const f=scene(id).frames.find(f=>f.id===name);assert.ok(f,id+': '+name);return f.state};
const html=fs.readFileSync(path.join(base,'index.html'),'utf8');
assert.equal(scenarios.length,7);
let total=0;
for(const s of scenarios){
  assert.equal(new Set(s.frames.map(f=>f.id)).size,s.frames.length,'unique frame IDs: '+s.id);
  const identities=new Map();
  for(const f of s.frames){total++;assert.ok(Object.isFrozen(f.state));for(const n of f.nodes)assert.ok(html.includes('id="node-'+n+'"'),n);for(const r of f.routes)assert.ok(html.includes('id="route-'+r+'"'),r);
    for(const [id,t]of Object.entries(f.state.tasks)){
      if(t.id){if(identities.has(id))assert.equal(t.id,identities.get(id),'same Task identity');else identities.set(id,t.id)}
      if(t.ack)assert.ok(t.reasons.length>0,'ACK requires an active pause');
      if(!t.registered)assert.equal(t.activity,0,'no effects before registration');
    }
    for(const id of f.state.queue){assert.ok(!f.state.owners.some(o=>o.task===id),'queued task cannot hold logical ownership');assert.equal(f.state.tasks[id].activity,0,'queued task has no dispatched activity')}
    for(const r of f.state.resources){assert.ok(f.state.tasks[r.task].registered);if(r.state==='Fenced')assert.ok(!f.state.owners.some(o=>o.task!==r.task&&o.objects.includes(r.key)),'fenced resource cannot be handed off')}
  }
}
assert.equal(scene('normal').frames.at(-1).state.tasks.A.activity,5);
assert.equal(scene('normal').frames.at(-1).state.disk,'Up');
assert.equal(scene('normal').frames.at(-1).state.resources.length,0);
assert.equal(frame('normal','A-1-wake').pending.A,undefined,'permit wake does not dispatch');
assert.equal(frame('normal','A-1-settle').resources.length,1,'Release is only enqueued');
assert.equal(frame('normal','A-1-release').resources.length,0);
assert.equal(frame('fifo','queued').tasks.B.status,'Queued');
assert.equal(frame('fifo','poll-C').tasks.A.status,'Activity','disjoint C proceeds while A active');
assert.equal(frame('fifo','start-B').tasks.A.status,'Completed');
for(const id of ['yield','yield-return']){
  assert.equal(frame(id,'pause-request').tasks.A.ack,false);
  assert.equal(frame(id,'pause-request').tasks.B.activity,0);
  assert.equal(frame(id,'start-B').tasks.A.ack,true);
  assert.equal(frame(id,'policy-clear').tasks.A.reasons.join(','),'user');
  assert.equal(frame(id,'resume-user').tasks.A.status,'WaitingConditions');
  assert.equal(frame(id,'resume-user').guard,'Wait · placement 不满足');
  assert.equal(frame(id,'fresh-ack').tasks.A.reasons.join(','),'policy:B,user');
  assert.equal(scene(id).frames.at(-1).state.tasks.A.activity,id==='yield'?1:2);
}
assert.equal(frame('yield','ack').pending.A,1,'retained await remains active');
assert.equal(frame('yield-return','ack').pending.A,0,'partial result settles first');
assert.equal(frame('yield-return','ack').tasks.A.checkpoint,7);
assert.equal(frame('cancel','cancel-request').resources.length,1);
assert.equal(frame('cancel','start-B').tasks.A.status,'Cancelled');
assert.equal(frame('uncertain','reject-fenced').tasks.B.activity,0);
assert.equal(frame('uncertain','reject-fenced').fences.length,1);
assert.equal(frame('recovery','restore').tasks.A.id,frame('recovery','old-checkpoint').tasks.A.id);
assert.equal(frame('recovery','restore').epoch,43);
assert.equal(frame('recovery','restore').owners.length,0);
console.log('PASS: '+scenarios.length+' trace variants / '+total+' immutable events; task identity, FIFO, independent progress, ACK before handoff, both pause modes, user pause, readiness, cancellation, fencing, checkpoint recovery.');
if(process.argv.includes('--browser'))await browserCheck();
async function browserCheck(){
 const executable=process.env.CHROME_PATH||'C:/Program Files/Google/Chrome/Application/chrome.exe';
 const profile=fs.mkdtempSync(path.join(os.tmpdir(),'runtime-lab-'));
 const chrome=spawn(executable,['--headless','--disable-gpu','--no-first-run','--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'],{windowsHide:true,stdio:'ignore'});
 let socket,launchError;chrome.on('error',e=>launchError=e);
 try{
  let port;for(let i=0;i<100;i++){if(launchError)throw launchError;try{port=fs.readFileSync(path.join(profile,'DevToolsActivePort'),'utf8').split('\n')[0];break}catch{await delay(100)}}assert.ok(port);
  const targets=await(await fetch('http://127.0.0.1:'+port+'/json')).json();socket=new WebSocket(targets.find(t=>t.type==='page').webSocketDebuggerUrl);
  await new Promise((resolve,reject)=>{socket.addEventListener('open',resolve,{once:true});socket.addEventListener('error',reject,{once:true})});
  let sequence=0;const pending=new Map(),errors=[],external=[];
  socket.addEventListener('message',event=>{const m=JSON.parse(event.data),p=pending.get(m.id);if(p){clearTimeout(p.timer);pending.delete(m.id);if(m.error)p.reject(Error(JSON.stringify(m.error)));else p.resolve(m.result)}if(m.method==='Runtime.exceptionThrown')errors.push(m.params.exceptionDetails);if(m.method==='Log.entryAdded'&&m.params.entry.level==='error')errors.push(m.params.entry);if(m.method==='Network.requestWillBeSent'&&/^https?:/.test(m.params.request.url))external.push(m.params.request.url)});
  const call=(method,params={})=>new Promise((resolve,reject)=>{const id=++sequence;const timer=setTimeout(()=>{pending.delete(id);reject(Error('CDP timeout: '+method))},10000);pending.set(id,{resolve,reject,timer});socket.send(JSON.stringify({id,method,params}))});
  const evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value};
  const until=async (expr,timeout=12000)=>{const end=Date.now()+timeout;while(Date.now()<end){if(await evaluate(expr))return;await delay(50)}throw Error('Timeout: '+expr)};
  const click=id=>evaluate('document.getElementById('+JSON.stringify(id)+').click()');
  const inspect=()=>evaluate('RuntimeLab.inspect()');
  await call('Runtime.enable');await call('Log.enable');await call('Network.enable');await call('Emulation.setDeviceMetricsOverride',{width:1600,height:1200,deviceScaleFactor:1,mobile:false});
  await call('Page.navigate',{url:pathToFileURL(path.join(base,'index.html')).href});await until('document.readyState === "complete" && !!window.RuntimeLab');
  const ids=await evaluate('[...document.querySelectorAll("[id]")].map(n=>n.id)');assert.equal(new Set(ids).size,ids.length);
  for(const s of scenarios){await evaluate('RuntimeLab.choose('+JSON.stringify(s.id)+')');for(let i=1;i<s.frames.length;i++){await click('next-step');const actual=await inspect();assert.equal(actual.cursor,i);assert.equal(JSON.stringify(actual.state),JSON.stringify(s.frames[i].state));assert.equal(actual.playing,false)}}
  await evaluate('RuntimeLab.choose("yield");document.getElementById("pause-mode").value="return";document.getElementById("pause-mode").dispatchEvent(new Event("change"))');assert.equal((await inspect()).scenario,'yield-return');
  await evaluate('RuntimeLab.choose("yield");document.getElementById("speed").value="4"');await click('play');
  await until('RuntimeLab.inspect().frame === "inject" && !RuntimeLab.inspect().playing',30000);
  assert.equal(await evaluate('document.getElementById("gate").hidden'),false);
  await click('continue');await delay(120);await click('play');const paused=await inspect();assert.ok(paused.motion);await delay(120);assert.deepEqual(await inspect(),paused,'pause preserves packet and state');
  await click('play');await until('RuntimeLab.inspect().frame === "pause-request" && !RuntimeLab.inspect().playing',20000);
  assert.equal(await evaluate('document.getElementById("business-queue").textContent.includes("B")'),true);
  assert.equal(await evaluate('document.getElementById("tasks").textContent.includes("尚未确认停稳")'),true);
  const shotIndex=process.argv.indexOf('--screenshot');if(shotIndex!==-1){const shot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});fs.writeFileSync(process.argv[shotIndex+1],Buffer.from(shot.data,'base64'))}
  await evaluate('document.getElementById("node-locks").dispatchEvent(new KeyboardEvent("keydown",{key:"Enter",bubbles:true}))');assert.equal(await evaluate('document.getElementById("component-detail").open'),true);
  assert.equal(await evaluate('document.getElementById("component-name").textContent'),'LockTable');
  await click('next-step');await click('back-step');assert.equal((await inspect()).frame,'pause-request');
  await evaluate('document.dispatchEvent(new KeyboardEvent("keydown",{key:"ArrowRight"}))');assert.equal((await inspect()).frame,'suspend-command');
  await evaluate('const slider=document.getElementById("timeline");slider.value=2;slider.dispatchEvent(new Event("input"))');assert.equal((await inspect()).cursor,2);
  await evaluate('document.querySelector("#chapters button:last-child").click()');assert.equal((await inspect()).frame,'resume-user');
  await evaluate('RuntimeLab.choose("uncertain");document.getElementById("auto-explain").checked=false');await click('play');await until('RuntimeLab.inspect().frame === "reject-fenced" && !RuntimeLab.inspect().playing',35000);await click('play');assert.equal((await inspect()).cursor,0);await click('play');
  const overflow=await evaluate(`(()=>{const bad=[];document.querySelectorAll('#architecture text').forEach(t=>{if(t.closest('#packet-label'))return;const b=t.getBBox();if(b.x<0||b.y<0||b.x+b.width>1280||b.y+b.height>880)bad.push(t.textContent);const r=t.closest('.component')?.querySelector('.node-card');if(r&&b.x+b.width>+r.getAttribute('x')+ +r.getAttribute('width')-8)bad.push(t.textContent+' [card]')});return bad})()`);assert.deepEqual(overflow,[],'SVG text fits');
  await call('Emulation.setEmulatedMedia',{features:[{name:'prefers-reduced-motion',value:'reduce'}]});await click('play');await delay(70);assert.equal(await evaluate('getComputedStyle(document.getElementById("packet")).display'),'none');await click('play');
  await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'mobile page contains graph scroll');await click('fit-diagram');assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'zoom remains inside canvas');
  assert.deepEqual(errors,[],'no script/CSP errors');assert.deepEqual(external,[],'no external requests');
  console.log('PASS: Chrome file:// all-event playback states; animation, pause/resume, gates, complete autoplay, replay, pause mode, seek, keyboard, component inspection, SVG bounds, mobile, reduced motion, offline.');await call('Browser.close').catch(()=>{});
 }finally{socket?.close();chrome.kill()}
}
