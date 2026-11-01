import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Model } from './model.mjs';
import { App } from './app.mjs';
import { Frame, cells, fit, render, wrap } from './screen.mjs';
import { JournalReader } from './reader.mjs';
import { MouseInput, hitTest } from './mouse.mjs';
import { counts, countText, consistency, stepResult, stepSections, resultState } from './semantics.mjs';

const pool='00000000000000000000000000000001',other='00000000000000000000000000000002';
const task=`1/${pool}/storage.shrink/1`;
const source={ exists:true,lastRead:1000,file:'audit.jsonl' };
function add(model,kind,data=null,options={}) {
  const r={cursor:model.cursor+1,event:{schema:1,task,pool_uuid:pool,pool_name:'pool-one',execution:'old',owner_epoch:1,
    at_ms:1000+model.cursor,kind,activity:null,data,...options}};
  model.apply(r);return r;
}
function fixture() {
  const m=new Model();
  add(m,'PoolRegistered',null,{task:null});add(m,'PoolRegistered',null,{task:null,pool_uuid:other,pool_name:'空池'});
  add(m,'Registered');add(m,'Started');
  add(m,'Activity',{name:'prepare',kind:'Step',state:'Completed'},{activity:1});
  add(m,'Activity',{name:'disk.prepare',kind:'Call',state:'Completed',parent_id:{sequence:1}},{activity:2});
  add(m,'Activity',{name:'migrate',kind:'Step',state:'Suspended',progress:{completed:2,total:20}},{activity:3});
  add(m,'Activity',{name:'balance.evacuate',kind:'Call',state:'Completed',parent_id:{sequence:3},progress:{completed:2,total:20}},{activity:4});
  add(m,'TaskState','Suspended');
  for(let i=2;i<=20;i++) {
    const id=`1/${pool}/storage.online-disk/${i}`;
    add(m,'Registered',null,{task:id,execution:`task-${i}`});add(m,'Finished','Completed',{task:id,execution:`task-${i}`});
  }
  return m;
}
const press=(app,name,str='')=>app.key(str,{name});
const plain=(app,w=100,h=30)=>render(app,source,{width:w,height:h}).plain().join('\n');

test('pool landing shows current AND historical workflows without global tabs/inspector',()=>{
  const app=new App(fixture()),screen=plain(app);
  assert.equal(app.currentPool.id,pool);
  assert.match(screen,/当前 Workflow.*1/);assert.match(screen,/历史 Workflow.*19/);
  assert.match(screen,/存储池缩容/);assert.match(screen,/硬盘上线/);
  assert.doesNotMatch(screen,/INSPECTOR|ACTIVITIES|EVENTS|EPOCH|CURRENT BLOCKERS/);
});
test('pool -> workflow -> step details; back restores selection and scope',()=>{
  const app=new App(fixture());press(app,'return');
  assert.equal(app.page,'workflow');assert.equal(app.task,task);
  assert.equal(app.rows().length,2); // two logical steps, not four including calls
  press(app,'down');press(app,'return');assert.equal(app.page,'step');assert.equal(app.activity,3);
  assert.match(plain(app),/migrate.*已暂停/);
  press(app,'escape');assert.equal(app.selection().row.id,3);
  press(app,'escape');assert.equal(app.page,'pool');assert.equal(app.selection().row.id,task);
});
test('step counts separate success/failure/pending and exclude nested calls',()=>{
  const m=fixture(),t=m.tasks.get(task),ex=t.executions.get('old');
  assert.deepEqual(counts(ex),{total:2,steps:2,success:1,returned:0,failed:0,cancelled:0,pending:1});
  add(m,'Activity',{name:'migrate',kind:'Step',state:{Failed:{Failed:'disk failed'}}},{activity:3});
  assert.match(countText(ex),/步骤成功 1.*失败 1/);
  assert.equal(stepResult(ex.activities.get(3),ex).label,'失败');
});
test('successful service return at 2/20 is never presented as successful business step',()=>{
  const m=fixture(),t=m.tasks.get(task),ex=t.executions.get('old'),call=ex.activities.get(4);
  assert.equal(stepResult(call,ex).label,'调用返回');
  assert.match(stepSections(t,ex,call).map(x=>x.join(':')).join('\n'),/尚未达到总量/);
  assert.equal(stepResult(ex.activities.get(3),ex).label,'已暂停');
  assert.equal(resultState(t.state).label,'已暂停');
});
test('restoration defaults to latest execution; older interrupted batch stays explicit',()=>{
  const m=fixture();add(m,'RecoveryPending');add(m,'Finished',{Failed:'Cancelled'});
  add(m,'Restored',{}, {execution:'new',owner_epoch:2});
  add(m,'Activity',{name:'finish',kind:'Step',state:'Completed'},{execution:'new',owner_epoch:2,activity:1});
  add(m,'Finished','Completed',{execution:'new',owner_epoch:2});
  const app=new App(m);assert.equal(app.currentPool.id,pool);app.section='history';app.selected.set(app.keyFor(),task);press(app,'return');
  assert.equal(app.currentExecution.id,'new');assert.equal(app.rows().length,1);assert.equal(app.rows()[0].name,'finish');
  assert.match(plain(app),/第 2\/2 次执行（最新）/);
  press(app,'e');press(app,'down');press(app,'return');
  assert.equal(app.currentExecution.id,'old');assert.equal(app.rows().length,2);
  assert.match(plain(app),/历史.*中断待恢复/);
});
test('late old-owner events cannot replace latest state or reset current execution',()=>{
  const m=fixture();add(m,'Restored',{}, {execution:'new',owner_epoch:2});add(m,'Finished','Completed',{execution:'new',owner_epoch:2});
  add(m,'TaskState','Running');add(m,'ControlRequested',{action:'cancel'});
  assert.equal(m.tasks.get(task).execution,'new');assert.equal(m.tasks.get(task).state,'Completed');
  assert.equal(m.tasks.get(task).control,null);assert.equal(m.catalog.get(pool).epoch,2);
});
test('successful workflow with unclosed evidence is flagged, never paints steps successful',()=>{
  const m=fixture();add(m,'Finished','Completed');const t=m.tasks.get(task),ex=t.executions.get('old');
  assert.match(consistency(ex),/末态未闭合/);
  assert.equal(stepResult(ex.activities.get(3),ex).label,'末态未确认');
});
test('legacy call-only workflow counts calls, not inferred business plan',()=>{
  const m=new Model();add(m,'Registered');
  add(m,'Activity',{name:'balance.evacuate',kind:'Call',state:'Completed',progress:{completed:2,total:20}},{activity:1});
  add(m,'TaskState','Suspended');const app=new App(m);press(app,'return');
  assert.match(plain(app),/只记录了服务调用/);assert.match(plain(app),/调用返回/);
  assert.equal(counts(app.currentExecution).success,0);assert.equal(counts(app.currentExecution).returned,1);
});
test('pool switching prevents cross-pool mixing, including empty pools',()=>{
  const app=new App(fixture());press(app,'p');press(app,'down');press(app,'return');
  assert.equal(app.pool,other);assert.equal(app.rows('current').length,0);assert.equal(app.rows('history').length,0);
  assert.match(plain(app),/暂无历史 Workflow/);
});
test('both sections paginate independently, search and selection survive refresh',()=>{
  const m=fixture(),app=new App(m);plain(app,80,24);press(app,'tab');
  const before=app.selection().row.id;press(app,'pagedown');assert.notEqual(app.selection().row.id,before);
  const selected=app.selection().row.id;add(m,'TaskState','Running');assert.equal(app.selection().row.id,selected);
  press(app,'/','/');app.key('硬盘',{});press(app,'return');assert.equal(app.rows().length,19);
  press(app,'/','/');app.key('none',{});press(app,'escape');assert.equal(app.search,'硬盘');
});
test('freeze preserves complete data projection; errors stay visible while frozen',()=>{
  const m=fixture(),app=new App(m);assert.equal(app.currentPool.id,pool);press(app,'space');const cursor=app.model.cursor;
  add(m,'Finished','Completed');assert.equal(app.model.cursor,cursor);assert.equal(app.model.tasks.get(task).state,'Suspended');
  assert.match(render(app,{...source,error:'bad frame'}).plain().join('\n'),/读取失败/);
  press(app,'space');assert.equal(app.model.tasks.get(task).state,'Completed');
});
test('audit history is secondary and explicitly historical; raw record is opt-in',()=>{
  const app=new App(fixture());press(app,'return');press(app,'return');press(app,'a');
  assert.match(app.modal.title,/记录时状态/);assert.equal(app.modal.raw,false);
  press(app,'r');assert.equal(app.modal.raw,true);press(app,'escape');assert.equal(app.page,'step');
  assert.equal(press(app,'q'),'quit');
});
test('all hierarchy pages and dialogs fit narrow/wide terminals with CJK',()=>{
  const m=fixture();add(m,'Activity',{name:'\x1b[2J磁盘é👨‍💻',kind:'Step',state:'Running'},{activity:5});
  for(const [width,height] of [[64,20],[80,24],[120,32],[160,44],[30,8]]) {
    const app=new App(m);
    for(let page=0;page<3;page++) {
      for(const popup of [null,'pool','execution','help','audit']) {
        app.modal=null;
        if(popup==='help')press(app,'h');else if(popup==='audit')press(app,'a');else if(popup)app.menu(popup);
        const frame=render(app,source,{width,height});
        assert.equal(frame.plain().length,height);
        for(const l of frame.plain()){assert.equal(cells(l),width,`${app.page}/${popup}: ${l}`);assert.ok(!l.includes('\x1b'));}
      }
      app.modal=null;press(app,'return');
    }
  }
});
test('CJK/combining/emoji widths and modal overlays preserve exact cell widths',()=>{
  assert.equal(cells('磁盘é👨‍💻'),7);assert.equal(cells(fit('磁盘abc',6)),6);
  assert.deepEqual(wrap('磁盘上线',4),['磁盘','上线']);
  const f=new Frame(10,2);f.put(0,0,'磁盘上线');f.put(1,0,'|');assert.equal(cells(f.plain()[0]),10);
});
test('mouse packets support chunking, row click drilldown, wheel and modal isolation',()=>{
  const parser=new MouseInput();assert.deepEqual(parser.feed('\x1b[<0;4;'),[]);
  assert.deepEqual(parser.feed('8M'),[{x:3,y:7,wheel:0}]);assert.deepEqual(parser.feed('\x1b[<0;4;8m'),[]);
  assert.equal(parser.feed('\x1b[<65;1;1M')[0].wheel,1);
  const app=new App(fixture()),frame=render(app,source),row=frame.hits.find(h=>h.action==='row'&&h.section==='current');
  app.click(hitTest(frame,row.x,row.y));assert.equal(app.page,'workflow');
  press(app,'p');const modal=render(app,source);assert.ok(modal.hits.every(h=>h.action==='menu'||h.action==='close'));
});
test('reader handles split UTF8 append, corrupt frames and truncation',t=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'taskcenter-reader-'));t.after(()=>fs.rmSync(dir,{recursive:true}));
  const file=path.join(dir,'audit.jsonl'),reader=new JournalReader(file),m=new Model();reader.refresh();assert.equal(reader.error,'');
  const bytes=Buffer.from(JSON.stringify(add(m,'PoolRegistered',null,{task:null,pool_name:'存储池'}))+'\n');
  const split=bytes.indexOf(Buffer.from('存'))+1;fs.writeFileSync(file,bytes.subarray(0,split));reader.refresh();assert.equal(reader.model.cursor,0);
  fs.appendFileSync(file,bytes.subarray(split));reader.refresh();assert.equal(reader.model.catalog.get(pool).name,'存储池');
  fs.appendFileSync(file,'broken\n');reader.refresh();assert.ok(reader.error);
  fs.writeFileSync(file,bytes);const second=new JournalReader(file);second.refresh();fs.truncateSync(file,0);second.refresh();assert.match(second.error,/truncated/);
});
test('wait reasons mentioning failure do not change the actual state',()=>{
  assert.equal(resultState({WaitingConditions:'Failed disk'}).label,'等待条件');
  assert.equal(resultState('Settling').label,'等待停稳');
});
