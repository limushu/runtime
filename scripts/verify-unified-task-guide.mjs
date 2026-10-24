// Node >= 22, Chrome; no npm dependencies. --render refreshes the six PNG exports.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawn} from 'node:child_process';
import {setTimeout as delay} from 'node:timers/promises';

const base=fileURLToPath(new URL('../docs/diagrams/unified-task-guide/',import.meta.url));
const pages=JSON.parse(fs.readFileSync(path.join(base,'manifest.json'),'utf8'));
assert.equal(pages.length,6);
const html=fs.readFileSync(path.join(base,'index.html'),'utf8');
for(const p of pages){
  const svg=fs.readFileSync(path.join(base,p.slug+'.svg'),'utf8');
  assert.ok(html.includes(svg),'HTML embeds the exact SVG');
  for(const s of p.steps)for(const key of s.keys)assert.ok(p.components[key],key);
}
const executable=process.env.CHROME_PATH||'C:/Program Files/Google/Chrome/Application/chrome.exe';
const profile=fs.mkdtempSync(path.join(os.tmpdir(),'unified-task-guide-'));
const chrome=spawn(executable,['--headless','--disable-gpu','--no-first-run','--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'],{windowsHide:true,stdio:'ignore'});
let socket,launchError;chrome.on('error',e=>launchError=e);
try{
  let port;
  for(let i=0;i<100;i++){
    if(launchError)throw launchError;
    try{port=fs.readFileSync(path.join(profile,'DevToolsActivePort'),'utf8').split('\n')[0];break}catch{await delay(100)}
  }
  assert.ok(port,'Chrome did not start');
  const targets=await(await fetch(`http://127.0.0.1:${port}/json`)).json();
  socket=new WebSocket(targets.find(t=>t.type==='page').webSocketDebuggerUrl);
  await new Promise((resolve,reject)=>{socket.addEventListener('open',resolve,{once:true});socket.addEventListener('error',reject,{once:true})});
  let seq=0;const pending=new Map(),errors=[],external=[];
  socket.addEventListener('message',event=>{
    const m=JSON.parse(event.data),p=pending.get(m.id);
    if(p){clearTimeout(p.timer);pending.delete(m.id);if(m.error)p.reject(Error(JSON.stringify(m.error)));else p.resolve(m.result)}
    if(m.method==='Runtime.exceptionThrown')errors.push(m.params.exceptionDetails);
    if(m.method==='Network.requestWillBeSent'&&/^https?:/.test(m.params.request.url))external.push(m.params.request.url);
  });
  const call=(method,params={})=>new Promise((resolve,reject)=>{
    const id=++seq,timer=setTimeout(()=>{pending.delete(id);reject(Error('CDP timeout: '+method))},10000);
    pending.set(id,{resolve,reject,timer});socket.send(JSON.stringify({id,method,params}));
  });
  const evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value};
  const until=async expr=>{for(let i=0;i<100;i++){if(await evaluate(expr))return;await delay(50)}throw Error('Timed out: '+expr)};
  await call('Runtime.enable');await call('Network.enable');
  await call('Emulation.setDeviceMetricsOverride',{width:1600,height:1040,deviceScaleFactor:1,mobile:false});
  if(process.argv.includes('--render'))for(const p of pages){
    await call('Page.navigate',{url:pathToFileURL(path.join(base,p.slug+'.svg')).href});
    await until("document.readyState==='complete' && document.documentElement.tagName==='svg'");
    await evaluate('document.fonts.ready.then(()=>true)');
    const image=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});
    fs.writeFileSync(path.join(base,p.slug+'.png'),Buffer.from(image.data,'base64'));
  }
  await call('Page.navigate',{url:pathToFileURL(path.join(base,'index.html')).href});
  await until('document.readyState === "complete" && !!window.Guide');
  const screenshotIndex=process.argv.indexOf('--screenshot');
  if(screenshotIndex!==-1){
    const image=await call('Page.captureScreenshot',{format:'png'});
    fs.writeFileSync(process.argv[screenshotIndex+1],Buffer.from(image.data,'base64'));
  }
  const ids=await evaluate('[...document.querySelectorAll("[id]")].map(n=>n.id)');
  assert.equal(ids.length,new Set(ids).size,'DOM IDs unique');
  for(let i=0;i<pages.length;i++){
    await evaluate(`Guide.show(${i})`);
    assert.equal((await evaluate('Guide.inspect()')).current,i);
    const overflow=await evaluate(`(()=>{const bad=[];document.querySelectorAll('section:not([hidden]) svg text').forEach(t=>{const b=t.getBBox();if(b.x<0||b.y<0||b.x+b.width>1600||b.y+b.height>1040)bad.push(t.textContent+' [canvas]');const r=t.closest('.component')?.querySelector('rect.card');if(r){const x=+r.getAttribute('x'),y=+r.getAttribute('y'),w=+r.getAttribute('width'),h=+r.getAttribute('height');if(b.x<x+8||b.x+b.width>x+w-8||b.y<y||b.y+b.height>y+h-5)bad.push(t.textContent+' [card]')}});return bad})()`);
    assert.deepEqual(overflow,[],pages[i].slug+' text fits');
    for(let j=0;j<pages[i].steps.length;j++){
      await evaluate(`Guide.selectStep(${j})`);
      const active=await evaluate('[...document.querySelectorAll("section:not([hidden]) .active")].map(g=>g.dataset.key)');
      assert.deepEqual(active.sort(),[...pages[i].steps[j].keys].sort());
    }
    const key=Object.keys(pages[i].components)[0];
    await evaluate(`document.querySelector('section:not([hidden]) [data-key="${key}"]').dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}))`);
    assert.equal(await evaluate('document.getElementById("component-title").textContent'),pages[i].components[key].title);
    await evaluate('document.getElementById("reset").click()');
    assert.equal(await evaluate('document.querySelectorAll("section:not([hidden]) .dim").length'),0);
  }
  await evaluate('Guide.show(0);document.getElementById("next").click();document.getElementById("plus").click()');
  assert.deepEqual(await evaluate('Guide.inspect()'),{current:1,scale:1.25,step:-1,pages:6});
  await evaluate('document.dispatchEvent(new KeyboardEvent("keydown",{key:"ArrowLeft"}));document.getElementById("fit").click()');
  assert.equal((await evaluate('Guide.inspect()')).current,0);
  await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'mobile overflow');
  await evaluate('document.getElementById("plus").click()');
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'zoom contained');
  await call('Emulation.setEmulatedMedia',{media:'print'});
  assert.equal(await evaluate('[...document.querySelectorAll("section")].every(s=>getComputedStyle(s).display!=="none")'),true,'all six print');
  assert.deepEqual(errors,[],'no JS errors');assert.deepEqual(external,[],'offline');
  console.log('PASS: six exact SVG embeds; all text fits; all reading steps; component keyboard access; navigation; zoom; mobile; print; offline.');
  await call('Browser.close').catch(()=>{});
}finally{socket?.close();chrome.kill()}
