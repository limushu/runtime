import { Frame, fit, wrap } from './terminal.mjs';
import { shortTask, progress, text } from './presentation.mjs';
import { consistency, countText, counts, resultState, stepResult, stepSections, workflowName, children } from './semantics.mjs';
export { Frame, cells, fit, wrap } from './terminal.mjs';

function line(frame, y, value, tone = 'normal', x = 2) {
  frame.put(x, y, fit(value, frame.width-x-2), tone);
}
function hit(frame, y, action, extra = {}, x = 1, width = frame.width-2) {
  frame.hits.push({ x, y, width, height:1, action, ...extra });
}
function poolTable(frame, app, section, y, height) {
  const { rows, row:selected, index } = app.selection(section);
  const active = section === app.section;
  frame.put(1, y, '-'.repeat(frame.width-2), 'border');
  const total = app.currentPool?.tasks.filter(t => section === 'current' ? !t.terminal : t.terminal).length ?? 0;
  frame.put(2, y, ` ${section === 'current' ? '当前 Workflow' : '历史 Workflow'}  ${total} ${app.search ? `(匹配 ${rows.length})` : ''} `, active ? 'accent' : 'muted');
  hit(frame, y, 'section', { section });
  const size = Math.max(1,height-3);
  app.sizes[section] = size;
  if (!rows.length) { line(frame, y+2, section === 'current' ? '暂无进行中的 Workflow' : '暂无历史 Workflow', 'muted'); return; }
  const wide = frame.width >= 110;
  const widths = wide ? [9,25,12,frame.width-66,16] : [8,20,12,frame.width-45];
  const headers = ['编号','Workflow','状态',section==='current'?'当前动作':'对象'];
  if(wide)headers.push('最新记录 UTC');
  let x=2;
  headers.forEach((header,i) => { frame.put(x,y+1,fit(header,widths[i]),'muted'); x+=widths[i]; });
  const start=Math.floor(index/size)*size;
  rows.slice(start,start+size).forEach((r,i) => {
    const chosen=active && r.id===selected?.id, yy=y+2+i;
    frame.put(1,yy,chosen?'>':' ',chosen?'selected':'normal');
    const values=[r.ref,r.name,r.result.label,r.action,r.time];
    let xx=2;
    widths.forEach((w,j) => { frame.put(xx,yy,fit(values[j],w-1)+' ',chosen?'selected':j===2?r.result.tone:'normal');xx+=w; });
    hit(frame,yy,'row',{section,id:r.id});
  });
  if(rows.length>size) line(frame,y+height-1,`${start+1}–${Math.min(start+size,rows.length)} / ${rows.length}  PgUp / PgDn`,'muted');
}

export function render(app, source, { width=120, height=36 }={}) {
  const f=new Frame(Math.max(1,width),Math.max(1,height));
  if(width<64 || height<20) { line(f,1,'TASKCENTER','title');line(f,3,'请将终端调整为至少 64×20','warn');return f; }
  const pool=app.currentPool, task=app.currentTask, ex=app.currentExecution;
  f.fill(0,0,width,1,'title');
  f.put(2,0,fit(`TASKCENTER  /  ${pool?.name ?? '等待存储池'}`,width-30),'title');
  f.put(width-25,0,app.frozen?'视图已冻结 · 只读':'实时记录 · 只读','muted');
  hit(f,0,'pool');
  const path=app.page==='pool'?'p 切换存储池':`${pool?.name}  >  ${task ? workflowName(task.name)+' #'+shortTask(task.id) : '-'}${app.page==='step'?'  >  '+app.currentActivity?.data.name:''}`;
  line(f,1,path,'muted');
  if(app.page!=='pool')hit(f,1,'back');
  if(app.page==='pool') {
    const tasks=pool?.tasks??[];
    line(f,3,`当前 ${tasks.filter(t=>!t.terminal).length} 个  ·  历史 ${tasks.filter(t=>t.terminal).length} 个${app.search?'  ·  搜索: '+app.search:''}`,'normal');
    const current=app.rows('current');
    const currentHeight=Math.max(4,Math.min(current.length+3,Math.floor((height-8)/2)));
    poolTable(f,app,'current',5,currentHeight);
    const historyY=5+currentHeight+1;
    poolTable(f,app,'history',historyY,height-3-historyY);
  } else if(app.page==='workflow' && task && ex) {
    const outcome=resultState(task.state), batches=[...task.executions.keys()], old=ex.id!==task.execution;
    line(f,3,`Workflow 结果：${outcome.label}  ·  ${workflowName(task.name)}`,outcome.tone);
    line(f,4,`第 ${batches.indexOf(ex.id)+1}/${batches.length} 次执行${old?'（历史）':'（最新）'}  ·  本次${resultState(ex.state).label}${batches.length>1?'  ·  e 查看其他批次':''}`,old?'warn':'muted');
    line(f,6,countText(ex),'accent');
    const c=counts(ex), warning=consistency(ex);
    line(f,7,warning || (ex.rawState?.Failed ? `本次结束原因：${text(ex.rawState.Failed)}` : c.steps ? '子调用在步骤详情中，不重复计数。未执行的计划步骤不在记录中。' : '此 Workflow 只记录了服务调用；“调用返回”不代表业务目标全部完成。'),warning?'warn':'muted');
    f.put(1,9,'-'.repeat(width-2),'border');
    const widths=[5,Math.max(23,Math.floor(width*0.36)),8,14];
    widths.push(width-5-widths.reduce((a,b)=>a+b,0));
    let x=2;
    ['序号','步骤 / 调用','类型','结果','说明'].forEach((v,i)=>{f.put(x,10,fit(v,widths[i]),'muted');x+=widths[i];});
    const {rows,row:selected,index}=app.selection();
    app.sizes.workflow=Math.max(1,height-15);
    const start=Math.floor(index/app.sizes.workflow)*app.sizes.workflow;
    rows.slice(start,start+app.sizes.workflow).forEach((r,i)=>{
      const calls=children(ex,r.value).length;
      const note=progress(r.value.data)!=='-'?progress(r.value.data):calls?`${calls} 个内部调用`:r.result.code==='RETURNED'?'调用已结束':'';
      const values=[r.number,r.name,r.kind,r.result.label,note], chosen=r.id===selected?.id;
      f.put(1,11+i,chosen?'>':' ',chosen?'selected':'normal');
      let xx=2;
      widths.forEach((w,j)=>{f.put(xx,11+i,fit(values[j],w-1)+' ',chosen?'selected':j===3?r.result.tone:'normal');xx+=w;});
      hit(f,11+i,'row',{section:app.section,id:r.id});
    });
    if(!rows.length)line(f,12,'尚未记录步骤或调用。','muted');
    line(f,height-4,`${rows.length?start+1:0}–${Math.min(start+app.sizes.workflow,rows.length)} / ${rows.length}${app.search?' · 搜索: '+app.search:''}`,'muted');
  } else if(app.page==='step' && task && ex && app.currentActivity) {
    const a=app.currentActivity,s=stepResult(a,ex);
    line(f,3,`${a.data.name}  ·  ${s.label}`,s.tone);
    const labelWidth=width>=100?24:18;
    const lines=stepSections(task,ex,a).flatMap(([label,value])=>wrap(value,width-labelWidth-5).map((text,i)=>({label:i?'':label,text})));
    app.sizes.step=height-9;
    app.scroll=Math.min(app.scroll,Math.max(0,lines.length-app.sizes.step));
    lines.slice(app.scroll,app.scroll+app.sizes.step).forEach((l,i)=>{
      f.put(3,5+i,fit(l.label,labelWidth),'muted');f.put(3+labelWidth,5+i,fit(l.text,width-labelWidth-5),'normal');
    });
    line(f,height-4,`↑↓ 滚动详情  ${app.scroll+1}–${Math.min(app.scroll+app.sizes.step,lines.length)} / ${lines.length}`,'muted');
  }
  const incomplete=app.model.incomplete||app.live.incomplete;
  let health=source.error?`读取失败：${source.error}`:incomplete?'审计记录不完整，请核对执行结果':!source.exists?'等待审计文件':source.partial?'等待完整日志记录':'状态来自最后记录，不代表宿主在线。时间 UTC。';
  if(app.frozen&&!source.error&&!incomplete)health=`视图已冻结；后台新增 ${app.live.cursor-app.model.cursor} 条记录。Space 解冻。`;
  line(f,height-2,health,source.error||incomplete?'warn':'muted',1);
  f.fill(0,height-1,width,1,'footer');
  const keys=width<90 ? (app.page==='pool'?'Enter 打开  Tab 分区  p 池  /搜索  h 帮助  q 退出':'Enter 详情  Esc 返回  e 批次  a 审计  h 帮助  q 退出') : app.page==='pool'?'Enter/点击 打开  Tab 当前/历史  p 切换池  / 搜索  h 帮助  q 退出':app.page==='workflow'?'Enter/点击 步骤详情  Esc 返回  e 执行批次  a 审计  h 帮助  q 退出':'↑↓ 滚动  Esc 返回步骤  a 审计  r 原文  h 帮助  q 退出';
  line(f,height-1,app.searching?`搜索：${app.search}_  Enter 确认 / Esc 撤销`:keys,'footer',1);
  if(app.modal)modal(f,app);
  return f;
}

function modal(frame,app) {
  const m=app.modal;
  for(const row of frame.rows)for(const c of row)c.style='dim';
  frame.hits=[];
  const width=Math.min(frame.width-4,100),height=Math.min(frame.height-4,m.items?m.items.length+4:30);
  const x=Math.floor((frame.width-width)/2),y=Math.floor((frame.height-height)/2),capacity=height-4;
  frame.fill(x,y,width,height,'overlay');frame.box(x,y,width,height,m.kind==='audit'&&m.raw?'原始审计记录（历史）':m.title,'accent');
  frame.put(x+width-7,y,' 关闭 ','accent');hit(frame,y,'close',{},x+width-7,6);
  let lines;
  if(m.items) {
    lines=m.items.map((item,i)=>({text:`${i===m.index?'>':' '} ${item.label}`,tone:i===m.index?'selected':'normal'}));
    m.scroll=Math.max(0,m.index-capacity+1);
  } else if(m.kind==='audit'&&!m.raw) {
    lines=app.auditLines().map((text,i)=>({text,tone:i===m.index?'selected':'normal'}));
    m.scroll=Math.max(0,m.index-capacity+1);
  } else if(m.kind==='audit')lines=wrap(JSON.stringify(m.records[m.index]??{},null,2),width-6).map(text=>({text,tone:'normal'}));
  else lines=m.lines.flatMap(l=>wrap(l,width-6)).map(text=>({text,tone:'normal'}));
  m.scroll=Math.min(m.scroll??0,Math.max(0,lines.length-capacity));
  lines.slice(m.scroll,m.scroll+capacity).forEach((l,i)=>{
    frame.put(x+3,y+2+i,fit(l.text,width-6),l.tone);
    if(m.items)hit(frame,y+2+i,'menu',{index:m.scroll+i},x+2,width-4);
  });
  frame.put(x+2,y+height-2,fit(m.items?'↑↓ 选择  Enter 确认  Esc 关闭':'↑↓ 滚动/选择  r 原文/时间线  Esc 关闭',width-4),'muted');
}
