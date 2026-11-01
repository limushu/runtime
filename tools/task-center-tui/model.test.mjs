import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Model,safe,wrapCells} from './model.mjs';
const task='1/00000000000000000000000000000001/test.flow/1';
function add(m,kind,data={},execution='old',activity=null,id=task) {
  m.apply({cursor:m.cursor+1,event:{schema:1,task:id,pool_uuid:id.split('/')[1],pool_name:'pool',execution,owner_epoch:execution==='old'?1:2,at_ms:1000,kind,activity,data}});
}
test('pool filtering, durable events and Activity identity across handoff',()=>{
  const m=new Model();
  add(m,'Registered'); add(m,'Activity',{kind:'Step',name:'work',state:'Completed'},'old',1);add(m,'Finished','Completed');
  assert.equal(m.selectTasks(task.split('/')[1],'history').length,1);
  add(m,'Restored',{},'new'); add(m,'Activity',{kind:'Step',name:'work',state:'Running'},'new',1);
  assert.equal(m.selectTasks(task.split('/')[1],'current').length,1);
  assert.equal(m.tasks.get(task).executions.size,2);
  assert.equal(m.tasks.get(task).executions.get('old').activities.get(1).data.state,'Completed');
  const other=task.replace('00000000000000000000000000000001','00000000000000000000000000000002');
  add(m,'Registered',{},'other',null,other);
  assert.equal(m.pools().length,2); assert.equal(m.selectTasks(task.split('/')[1]).length,1);
});
test('interrupted durable task is not presented as user-cancelled history',()=>{
  const m=new Model();add(m,'Registered');add(m,'RecoveryPending');add(m,'Finished',{Failed:'Cancelled'});
  assert.equal(m.tasks.get(task).terminal,false);assert.match(m.tasks.get(task).state,/recoverable/);
});
test('gaps, unknown schemas and terminal escape injection',()=>{
  const m=new Model();assert.throws(()=>m.apply({cursor:2,event:{}}));
  assert.equal(m.cursor,0);assert.ok(!safe('\x1b[2Jevil\u009b').includes('\x1b'));
});
test('empty pools, incomplete-evidence warning and CJK terminal widths',()=>{
  const m=new Model();
  m.apply({cursor:1,event:{schema:1,task:null,pool_uuid:task.split('/')[1],pool_name:'空池',execution:'pool',owner_epoch:1,at_ms:1000,kind:'PoolRegistered',activity:null,data:null}});
  assert.equal(m.pools().length,1);assert.equal(m.pools()[0].tasks.length,0);
  add(m,'EvidenceIncomplete');assert.equal(m.incomplete,true);
  assert.deepEqual(wrapCells('磁盘上线ABC',4),['磁盘','上线','ABC']);
});
