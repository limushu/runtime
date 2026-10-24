/* Deterministic teaching traces for implementation 53b5ea6; never executes Rust or IO. */
(function(root){
  "use strict";
  const clone=x=>JSON.parse(JSON.stringify(x));
  function freeze(x){if(x&&typeof x==='object'){Object.values(x).forEach(freeze);Object.freeze(x)}return x}
  const initial=()=>({tasks:{},queue:[],owners:[],resources:[],events:[],fences:[],service:'空闲',mailbox:[],pending:{},future:'未创建',futureTask:'—',decision:'尚未准入',store:'未写入',epoch:42,guard:'未检查',disk:'Down'});
  class Trace{
    constructor(id,name,subtitle){this.id=id;this.name=name;this.subtitle=subtitle;this.state=initial();this.frames=[];this.add('intro','准备发送请求',subtitle,[],[],()=>{}, {chapter:'开始'})}
    add(id,title,why,nodes,routes,change=()=>{},opts={}){const previousFuture=this.state.future;this.state=clone(this.state);change(this.state);if(this.state.future!==previousFuture)this.state.futureTask=opts.actor||'A';this.frames.push({id,title,why,nodes,routes,actor:'A',duration:1400,chapter:null,gate:false,...opts,state:clone(this.state)});return this}
    request(id,label,kind,objects=['disk:D1'],durable=false){this.add('request-'+id,'请求 '+id+' 进入 TaskCenter',label+' 是完整 Workflow 请求。调用方不执行领域动作，也不持有启动许可。',['caller','state'],['submit'],s=>{s.tasks[id]={label,kind,objects,durable,registered:false,status:'待准入',id:null,blockers:[],reasons:[],ack:false,activity:0,checkpoint:null};s.decision='正在检查 '+id},{actor:id,chapter:'提交 '+id});return this}
    admit(id,decision='Independent',blockers=[]){this.add('policy-'+id,'Policy 判断：'+decision,'策略提供业务判断；TaskCenter 先检查完整冲突集合与容量，拒绝时不影响旧任务。',['state','policy'],['policy'],s=>{s.decision=decision;s.tasks[id].blockers=blockers},{actor:id});this.add('register-'+id,'defer 登记同一个 Task','Driver 登记 Future，State 保存 StartPermit。任务有身份，但业务还没有开始。',['state','driver','futures'],['defer','poll'],s=>{let t=s.tasks[id];t.registered=true;t.id='42 / P1 / '+t.kind+' / '+id;t.status=t.durable?'等待持久化确认':'Queued';s.queue.push(id);s.future='Pending · 等启动许可'},{actor:id});if(this.state.tasks[id].durable){this.add('persist-'+id,'初始 Task 记录写入 TaskStore','Workflow 自声明 Durable；写入由 spawn_blocking 执行。此时不得启动业务或抢占旧任务。',['driver','store'],['persist'],s=>{s.store='A revision=0 · Pending';},{actor:id});this.add('accepted-'+id,'写入确认，激活调度','初始存储失败时不会执行业务，也不会取消或暂停旧任务。本场景写入成功。',['store','state'],['accepted'],s=>{s.tasks[id].status='Queued'},{actor:id})}return this}
    start(id){this.add('start-'+id,'给 '+id+' 发放 StartPermit','State.pump() 确认没有未解决的运行阻塞，启动许可交给 Workflow，不能由调用方绕过。',['state','futures'],['start'],s=>{let t=s.tasks[id];t.status='Ready';t.blockers=[];s.queue=s.queue.filter(x=>x!==id);s.owners.push({task:id,objects:t.objects});s.future='Ready · 等待再次 poll'},{actor:id});this.add('poll-'+id,'Driver poll '+id+'，执行流程体','同一个 TaskCenter Tokio task 推进 driver.tick() 和 Workflow Future；没有为每个 Workflow 再 spawn 一个宿主。',['driver','futures','context'],['poll','run'],s=>{s.tasks[id].status='Running';s.future='Polling'},{actor:id});return this}
    activity(id,command,key='disk:D1'){
      const n=this.state.tasks[id].activity+1,p=id+'-'+n;
      this.add(p+'-acquire',command+'：申请资源','Context 经过安全边界，Broker 发 Acquire。当前 Future 进入 Pending，不阻塞 TaskCenter 接收新请求。',['context','broker'],['acquire'],s=>{s.events=['Acquire '+key];s.tasks[id].status='WaitingResources';s.future='Pending · 等 Permit'},{actor:id,chapter:command});
      this.add(p+'-lock','Driver 消费资源事件','资源队列与 Workflow Futures 共享 driver.tick()。LockTable 判断资源读写，不承担业务准入。',['broker','driver','locks'],['events','lock'],s=>{s.events=[];s.resources.push({task:id,key,state:'Held'});},{actor:id});
      this.add(p+'-wake','Permit 返回，只标记 Future 就绪','Permit 的 oneshot 唤醒等待链。Waker 不直接执行 Workflow；还要由 Driver 再次 poll。',['locks','context','futures'],['permit','wake'],s=>{s.future='Ready · 等待 poll';s.tasks[id].status='Ready'},{actor:id});
      this.add(p+'-dispatch','再次 poll，派发 '+command,'Context 取得 Permit，Tracker 登记 Guard，ActivityClient 把请求送入独立服务邮箱。',['driver','futures','context','client','mail'],['poll','run','dispatch','send'],s=>{s.tasks[id].activity=n;s.tasks[id].status='Activity';s.pending[id]=1;s.mailbox=[command];s.future='Pending · 等结果'},{actor:id});
      this.add(p+'-execute','服务宿主执行 '+command,'服务自己的 Tokio task 轮询 Activity Future。领域校验和 IO 在此发生，TaskCenter 同时仍可处理其他请求。',['mail','service'],['execute'],s=>{s.mailbox=[];s.service=command+' · 执行中'},{actor:id});return this
    }
    settle(id,uncertain=false){const n=this.state.tasks[id].activity;this.add(id+'-'+n+'-settle',uncertain?'结果不确定：保留资源隔离':'Activity 真正停稳，Guard 结算','Guard 跟随真实执行寿命。它通知 Tracker 并投递 Release；发送 Release 还不等于 LockTable 已经回收资源。',['service','tracker','broker'],['settle','release'],s=>{s.pending[id]=0;s.events=['Release '+id];s.service=uncertain?'Uncertain · 效果需核对':'空闲'},{actor:id});this.add(id+'-'+n+'-release',uncertain?'Driver 将步骤资源标为 Fenced':'Driver 处理 Release','资源表变化在 Driver 消费事件时发生，不能把调用方停止等待当作资源已释放。',['driver','locks'],['lock'],s=>{s.events=[];if(uncertain)s.resources.filter(r=>r.task===id).forEach(r=>r.state='Fenced');else s.resources=s.resources.filter(r=>r.task!==id)},{actor:id});this.add(id+'-'+n+'-reply','oneshot 结果唤醒原 Workflow','服务结果返回等待链；下一次 poll 才由 Workflow 消费结果。调用次序和真实耗时在此作教学简化。',['service','futures','context'],['reply','run'],s=>{s.future='Ready · 等待 poll';s.tasks[id].status=uncertain?'Settling · Uncertain':'Running'},{actor:id});return this}
    finish(id,kind='Completed'){
      this.add('finish-'+id,kind==='Uncertain'?'Task 结果不确定，逻辑占用隔离':'Task '+id+' 发布 '+kind,'Tracker 已停稳，才发布终态。Durable 任务还要保存终态；取消请求与最终取消结果是两个时刻。',['tracker','observe'],['observe'],s=>{s.tasks[id].status=kind;s.future='已完成';if(s.tasks[id].durable)s.store=id+' · '+(kind==='Uncertain'?'NeedsCheck':kind)},{actor:id});
      this.add('reap-'+id,kind==='Uncertain'?'reap 保留隔离，后继不能穿过':'reap 回收逻辑占用','TaskCenter 下一轮清理终态并重新规划；Activity 的资源锁和完整任务的逻辑占用是两种生命周期。',['observe','state'],['reap'],s=>{s.owners=s.owners.filter(x=>x.task!==id);s.queue=s.queue.filter(x=>x!==id);if(kind==='Uncertain')s.fences.push({task:id,objects:s.tasks[id].objects})},{actor:id});return this
    }
    export(){return {id:this.id,name:this.name,subtitle:this.subtitle,frames:this.frames}}
  }
  const all=[];
  const normal=new Trace('normal','一次完整请求','磁盘上线：Tier Inspect → Node Inspect → Add → Open → SetIo。一个 Memory Task，五次 Activity。');
  normal.request('A','磁盘 D1 上线','online-disk',['disk:D1','node:N1','tier:T1']).admit('A').start('A');
  for(const [cmd,key] of [['tier.inspect','tier:T1'],['node.inspect','node:N1'],['disk.add','disk:D1'],['disk.open','disk:D1'],['disk.set-io(Up)','disk:D1']]){
    normal.activity('A',cmd,key).settle('A');
    if(cmd==='disk.set-io(Up)')normal.add('io-up','领域提交确认后，IO 才是 Up','Open 成功不代表磁盘已上线；只有数据面与元数据确认后才发布 Up。',['service','context'],[],s=>s.disk='Up');
  }
  normal.finish('A');all.push(normal.export());
  const fifo=new Trace('fifo','冲突 → FIFO 排队','两个自定义维护流程争用 disk:D1：策略 Exclusive；另一个 disk:D2 流程不受队头阻塞。');
  fifo.request('A','维护 D1','maintenance-a').admit('A').start('A').activity('A','maintenance(D1)');
  fifo.request('B','同对象维护','maintenance-b').admit('B','Wait / Exclusive',['A']);
  fifo.add('queued','B 进入业务队列，尚未申请步骤资源','阻塞者 A 仍拥有 D1。B 的 StartPermit 在 TaskCenter 手中，不能进入 Activity，也没有服务端副作用。',['state','policy'],[],s=>s.decision='B 等待 A：Exclusive',{actor:'B',gate:true,chapter:'冲突排队'});
  fifo.request('C','独立维护 D2','maintenance-c',['disk:D2']).admit('C','Independent').start('C');
  fifo.add('independent','C 可运行，不被 B 阻塞','此处 C 只执行无 Activity 的安全步骤，说明业务队列不因一个冲突请求阻塞所有无关任务。',['futures','context'],['run'],()=>{},{actor:'C'}).finish('C');
  fifo.settle('A').finish('A').start('B').activity('B','maintenance(D1)').settle('B').finish('B');all.push(fifo.export());
  function yieldTrace(returning){
    const t=new Trace(returning?'yield-return':'yield','topo → 暂停缩容',returning?'停稳返回模式：先返回 cursor，保存 checkpoint 后暂停；恢复需新 Activity。':'保留调用模式：迁移实际停稳后 topo 才启动；原 Activity.await 不返回。');
    t.request('A','缩容迁移 D1','shrink',['disk:D1'],true).admit('A').start('A').activity('A','migration.migrate','migration:job-A');
    t.add('inject','迁移正在执行，现在注入 topo','接下来 B 将到达 TaskCenter。此时 A 的 Activity 在途，发暂停通知不能直接让 B 开始。',['caller','service'],[],()=>{},{gate:true,chapter:'注入 topo'});
    t.request('B','topo 变更 D2','topology',['disk:D2']).admit('B','Wait / YieldTo',['A']);
    t.add('pause-request','Policy 要求 A 让出给 B','通用规划器计算 pause_reasons / blockers；A 进入 Pausing，B 仍排队。策略属于应用，不硬编码在循环中。',['policy','state','context'],['policy','control'],s=>{s.tasks.A.reasons=['policy:B'];s.tasks.A.blockers=['B'];s.tasks.A.status='Pausing';s.future='Pending · 等真实停稳';s.guard='拓扑改变，待重检';s.decision='A YieldTo B；B 等待 A 停稳'},{gate:true,chapter:'等待真实 ACK'});
    if(returning){
      t.add('stop-return','迁移服务看到 token，自行停稳','服务采用停稳后返回的契约，不注册保留调用适配器。部分结果是业务类型，不是新的 Activity 类型。',['context','service'],['stop'],s=>s.service='停止迁移，准备 cursor=7');
      t.settle('A');
      t.add('save-cursor','原 await 返回 cursor，Workflow 保存 checkpoint','此时尚未确认整个 Task Paused。Workflow 先消费结果并保存续跑点。',['context','store'],['checkpoint'],s=>{s.tasks.A.checkpoint=7;s.store='A checkpoint=7 · revision=1'});
    }else t.add('suspend-command','所选 ActivityControl 发送 suspend','适配器必须等待真实停稳，才能返回成功。框架不知道迁移命令内容；不同 Workflow 可以选择不同命令。',['context','service'],['stop'],s=>s.service='suspend 在途 · 仍未确认');
    t.add('ack','A 实际停稳，当前代次 ACK 有效',returning?'Workflow 进入 safe_point 确认 Task Paused；旧 Activity 已结束。':'ActivityControl::suspend 返回确认。原 Activity 保留，pending=1，步骤资源锁仍保留。',['service','context','state'],returning?['control-ack']:['stop-ack','control-ack'],s=>{s.tasks.A.status='Paused';s.tasks.A.ack=true;s.service=returning?'空闲':'迁移已停稳 · 原调用保留';s.future='Pending · 等恢复原因清除'},{gate:true,chapter:'停稳后交接'});
    t.start('B');
    t.add('user-pause','用户也暂停 A：两个原因分别维护','用户暂停增加新控制代次，旧 ACK 失效；A 未重新开始迁移，在安全边界重新确认当前代次。',['caller','context'],['user-control'],s=>{s.tasks.A.reasons=['policy:B','user'];s.tasks.A.ack=false});
    t.add('fresh-ack','仍在停稳点，重新确认当前代次','用户原因和 policy 原因不能相互覆盖。此处没有新的 resume 命令。',['context','state'],['control-ack'],s=>{s.tasks.A.ack=true;s.store='A · user_paused=true'});
    t.finish('B');
    t.add('policy-clear','topo 结束，只移除 policy:B','用户原因仍在，A 不会自动继续迁移。TaskCenter 不得用一次 resume 清掉所有暂停来源。',['state','context'],['control'],s=>{s.tasks.A.reasons=['user'];s.tasks.A.blockers=[];s.tasks.A.ack=false;s.future='Pending · 用户暂停';s.decision='用户暂停仍有效'},{gate:true,chapter:'用户原因仍在'});
    t.add('user-ack','A 对剩余用户原因确认停稳','控制代次已变化；保持停稳并更新 ACK。',['context','state'],['control-ack'],s=>s.tasks.A.ack=true);
    t.add('resume-user','用户请求 resume，进入 Workflow guard','只移除 user 原因；恢复条件必须重新检查，不能直接 resume 迁移。',['caller','context'],['user-control'],s=>{s.tasks.A.reasons=[];s.tasks.A.ack=false;s.tasks.A.status='WaitingConditions';s.guard='Wait · placement 不满足';s.future='Pending · 等 recheck';s.store='A · user_paused=false'},{gate:true,chapter:'恢复前再检查'});
    t.add('recheck','条件变化，业务触发 recheck','ContinuationGuard 返回 Ready 后才允许继续。guard 是可取消的只读检查，领域提交仍须校验当前状态。',['caller','context'],['user-control'],s=>{s.guard='Ready';s.tasks.A.status='Running'});
    if(returning)t.activity('A','migration.migrate(cursor=7)','migration:job-A');
    else t.add('resume-command','调用所选 resume，继续原迁移','还是第一次 Activity，调用次数不变；绝不将暂停当成业务成功返回。',['context','service'],['stop'],s=>{s.tasks.A.status='Activity';s.service='原迁移恢复执行';s.future='Pending · 原 await'});
    t.settle('A').finish('A');all.push(t.export());
  }
  yieldTrace(false);yieldTrace(true);
  const cancel=new Trace('cancel','取消 → 停稳后交接','D1 上线正在 Open，D1 下线到达。默认规则 CancelThenWait；演示止于后继获得执行权。');
  cancel.request('A','D1 上线（Open 在途）','online-disk').admit('A').start('A').activity('A','disk.open');
  cancel.request('B','D1 下线','offline-disk').admit('B','CancelThenWait',['A']);
  cancel.add('cancel-request','请求取消 A，但不释放任何占用','控制令牌送达不代表 IO 已停止。B 继续等待，不能与 A 同时对 D1 发动作。',['state','context','service'],['control','stop'],s=>{s.tasks.A.status='取消请求中';s.decision='CancelThenWait · 等待 A 实际终态';s.service='Open 在途，正在停稳'},{gate:true,chapter:'取消不是完成'});
  cancel.settle('A').finish('A','Cancelled').start('B');
  cancel.add('handoff','B 获得执行权，交接完成','演示在此结束。真实 OfflineDisk 的重建/移除仍有占位能力，本演示不把下线业务伪报为完成。',['state','context'],[],s=>s.decision='A 停稳后 B 才能执行',{gate:true});all.push(cancel.export());
  const uncertain=new Trace('uncertain','失败 → 对象隔离','A 已产生外部效果，但提交结果不确定。隔离保留；B 不得将旧任务消失视为可重试。');
  uncertain.request('A','维护 D1','maintenance-a').admit('A').start('A').activity('A','mutate(D1)').settle('A',true).finish('A','Uncertain');
  uncertain.request('B','D1 后继维护','maintenance-b').admit('B','Wait / Exclusive',['A']);
  uncertain.add('reject-fenced','B 被隔离规则拒绝，业务从未启动','B 可有登记记录，但 StartPermit 被 reject，不能产生 Activity。拒绝不扩大隔离到新对象。只有对账才能解除不确定性。',['policy','state','observe'],['policy','observe'],s=>{s.tasks.B.status='Rejected';s.queue=[];s.decision='Rejected · object requires reconciliation'},{gate:true,chapter:'隔离拒绝'});all.push(uncertain.export());
  const recover=new Trace('recovery','切主 → checkpoint 续跑','假设旧执行已停稳、checkpoint=7 已保存；新主先完成数据面 fencing 与领域恢复，再重建原 Task。');
  recover.request('A','可恢复迁移','shrink',['disk:D1'],true).admit('A');
  recover.add('old-checkpoint','旧主已保存 checkpoint=7，并停稳退出','此处从已有恢复记录开始，不模拟崩溃瞬间的远端 IO。若结果不确定则必须对账，不能直接走此路径。',['store'],[],s=>{s.tasks.A.checkpoint=7;s.tasks.A.status='Interrupted';s.queue=[];s.store='A Pending · checkpoint=7 · owner=42';s.future='旧执行已停稳'},{gate:true,chapter:'切主前提'});
  recover.add('epoch','新主 epoch=43，外部 fencing 完成','选主轮次来自外部持久化权威。TaskIds.revoke 阻止旧主本地继续；数据面 fencing 另由业务保证。',['caller','state'],['submit'],s=>{s.epoch=43;s.decision='准入关闭 · 恢复中'});
  recover.add('claim','define + claim：绑定类型并声明新 owner','新宿主先恢复领域状态与客户端。TaskStore 拒绝旧轮次写入，普通新任务准入保持关闭。',['driver','store'],['persist'],s=>s.store='claim(P1,43) · 拒绝旧主写入');
  recover.add('restore','全量预检并 CAS 接管','校验类型、input/version、objects；未知类型或 NeedsCheck 会阻止恢复。本场景全部通过。',['store','driver','futures'],['restore','poll'],s=>{s.store='A owner=43 · checkpoint=7';s.tasks.A.status='Queued';s.queue=['A'];s.future='新 Future · 原 TaskId'});
  recover.add('claims','一次安装全部恢复任务占用','TaskCenter::from_restored 安装全部调度描述，之后才发启动许可。用户暂停从记录恢复，policy 原因重新计算。',['futures','state'],['reap'],s=>s.decision='恢复预检通过',{gate:true,chapter:'恢复同一身份'});
  recover.start('A');recover.add('recovery-guard','Workflow 读取 checkpoint，再检查条件','不是恢复 Rust 栈或事件重放。业务通过 checkpoint、稳定操作 ID 和幂等/对账定义续跑。',['context'],[],s=>s.guard='Ready');recover.activity('A','migration.migrate(cursor=7)','migration:job-A').settle('A').finish('A');all.push(recover.export());
  root.RuntimeScenarios=freeze(all);
})(globalThis);
