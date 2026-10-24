"""Build editable SVGs and an offline, interactive guide. PNGs: verify script --render."""
from pathlib import Path
from html import escape
import json

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/diagrams/unified-task-guide'
OUT.mkdir(parents=True, exist_ok=True)
W, H = 1600, 1040
C = {'blue': '#2459aa', 'green': '#237562', 'amber': '#99611c', 'purple': '#7055a2', 'ink': '#172b45'}
BG = {'blue': '#edf3fd', 'green': '#edf7f3', 'amber': '#fcf5e9', 'purple': '#f3effa', 'ink': '#f0f3f7'}
pages = []

class Diagram:
    def __init__(self, slug, title, subtitle):
        self.slug, self.title, self.subtitle = slug, title, subtitle
        self.parts, self.components, self.steps = [], {}, []
        markers = ''.join(f'<marker id="{slug}-{k}" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0 0L8 4L0 8Z" fill="{v}"/></marker>' for k,v in C.items())
        self.parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" aria-labelledby="{slug}-title"><title id="{slug}-title">{escape(title)}</title><defs>{markers}</defs><style>text{{font-family:"Microsoft YaHei","Segoe UI",sans-serif}}.component{{cursor:pointer}}.component:focus{{outline:none}}.component:focus .card{{stroke-width:4}}.active .card{{stroke:#e08426;stroke-width:4}}.dim{{opacity:.25}}</style><rect width="1600" height="1040" fill="#fafbfd"/>')
        self.text(48, 42, f'RUNTIME / 架构运行导读     {len(pages)+1:02d} / 06', 17, '#63758c')
        self.text(48, 93, title, 36, C['ink'], bold=True)
        self.text(48, 133, subtitle, 21, '#52657c')
        self.parts.append('<path d="M48 970H1552" stroke="#d8e0eb"/>')
        self.text(48, 1008, '实现基线 53b5ea6  ·  Task = 业务管理身份；Tokio task = 执行宿主  ·  箭头表示调用 / 消息 / 唤醒，见标注', 17, '#63758c')

    def text(self,x,y,t,size=21,color='#263b55',bold=False,anchor='start'):
        self.parts.append(f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" font-weight="{650 if bold else 400}" text-anchor="{anchor}">{escape(t)}</text>')

    def panel(self,x,y,w,h,label,color='blue'):
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="18" fill="{BG[color]}" stroke="{C[color]}" stroke-opacity=".3"/>')
        self.text(x+22,y+35,label,21,C[color],True)

    def card(self,key,x,y,w,h,title,lines,color='blue',detail=None):
        self.components[key] = {'title':title,'detail':detail or '；'.join(lines)}
        self.parts.append(f'<g class="component" data-key="{key}" tabindex="0" role="button" aria-label="{escape(title)}"><rect class="card" x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="white" stroke="{C[color]}" stroke-width="1.5"/>')
        self.parts.append(f'<rect x="{x}" y="{y+15}" width="4" height="{h-30}" rx="2" fill="{C[color]}"/>')
        self.text(x+20,y+32,title,24,C[color],True)
        for i,line in enumerate(lines): self.text(x+20,y+62+i*28,line,20)
        self.parts.append('</g>')

    def arrow(self,points,label='',color='blue',dash=False,lx=None,ly=None):
        d='M'+' L'.join(f'{x} {y}' for x,y in points)
        self.parts.append(f'<path d="{d}" fill="none" stroke="{C[color]}" stroke-width="2.2" stroke-dasharray="{6 if dash else 0}" marker-end="url(#{self.slug}-{color})"/>')
        if label:
            x,y=points[len(points)//2]
            self.text(lx if lx is not None else x, ly if ly is not None else y-12,label,18,C[color])

    def note(self,x,y,t,color='ink'): self.text(x,y,t,20,C[color])
    def step(self,title,body,*keys): self.steps.append({'title':title,'body':body,'keys':keys})
    def done(self):
        svg=''.join(self.parts)+'</svg>'
        (OUT/(self.slug+'.svg')).write_text(svg,encoding='utf-8')
        pages.append({'slug':self.slug,'title':self.title,'subtitle':self.subtitle,'components':self.components,'steps':self.steps,'svg':svg})

d=Diagram('01-layers','逻辑分层：谁决定业务，谁负责推进？','先认清职责。层是代码职责边界，不代表进程、线程或独立服务部署。')
rows=[
 ('01  应用入口 / 装配','ink', [('root','Pool / 组合根',['装配 TaskCenter、客户端、依赖','新主轮次由外部选主系统提供']),('entry','PoolHandle / 业务调用方',['提交 Workflow；按 TaskId 管理','一个主的各池共享 TaskIds']),('defs','Workflow 类型定义',['声明 input、details、persistence','需要恢复的类型绑定 dependencies'])]),
 ('02  应用编排 / 决策','blue',[('wf','业务 Workflow',['组织步骤与 checkpoint','定义恢复前业务条件 guard']),('policy','SchedulingPolicy',['准入决策 + 有方向的运行关系','默认 topo → 缩容；可替换策略']),('center','TaskCenter / State',['队列、占用、隔离、启动许可','计算暂停原因；等待真实 ACK'])]),
 ('03  通用执行机制','purple',[('driver','WorkflowDriver',['由 TaskCenter 轮询 Future 集合','不是额外的 Tokio task']),('context','WorkflowContext',['Activity / wait_until_runnable / 控制','资源申请、停稳跟踪、续跑记录']),('index','TaskRegistry / ExecutionView',['同一个 Task 的观察与控制入口','Snapshot / Observer / RunControl'])]),
 ('04  领域能力 / 适配','green',[('host','ServiceHost / ServiceHandler',['邮箱 + Activity Futures','每个服务有独立 Tokio 宿主']),('domain','Disk / Node / VD / Migration',['命令校验、状态和实际业务动作','迁移示例也通过普通 client 接入']),('ports','Ports / Adapters',['元数据、数据面等外部接口','领域停稳确认来自真实执行'])]),
 ('05  持久化 / 外部效果','amber',[('store','TaskStore',['同一个 Task 的存储表示','epoch fencing + revision CAS']),('io','数据面 / 元数据后端',['执行真实 IO 与领域状态提交','TaskStore 不能替代数据面 fencing']),('scope','当前实现范围',['领域侧仍为模拟；无确定性重放','完整跨主接入需领域恢复与对账'])])]
for i,(label,color,cards) in enumerate(rows):
    y=165+i*157
    d.panel(48,y,1504,143,'',color)
    first,second=label.split('  ',1)
    d.text(70,y+36,first,25,C[color],True)
    for n,part in enumerate(second.split(' / ')): d.text(70,y+72+n*29,part,21,C[color],True)
    for j,(key,title,lines) in enumerate(cards): d.card(key,265+j*424,y+13,402,118,title,lines,color)
d.step('从业务定义开始','Workflow 决定步骤、持久化和续跑条件；策略决定不同任务如何共存。','wf','policy','defs')
d.step('TaskCenter 负责协调','维护队列与占用，按策略发控制请求；拿到真实停稳 ACK 后才能交接。','center')
d.step('Driver 负责推进','Driver 只是可轮询的执行集合，嵌在 TaskCenter 循环内；Context 连接 Workflow 与领域客户端。','driver','context')
d.step('服务执行实际动作','各服务宿主独立接收命令，领域校验与真实停稳属于服务。','host','domain','ports')
d.step('观察和存储属于同一任务','所有 Workflow 都有 Task；持久化只改变该 Task 是否保存恢复记录。','index','store','scope')
d.done()

d=Diagram('02-runtime','运行闭环：谁在 poll，谁在等待，谁唤醒谁？','每池一个 TaskCenter Tokio task；每个领域服务另有自己的宿主。其余框是对象或 Future。')
d.panel(48,170,990,690,'执行宿主 A   ·   TaskCenter 的事件循环','blue')
d.panel(1090,170,462,690,'执行宿主 B…N   ·   每个领域服务','green')
d.card('state',80,230,926,110,'State.reap / pump + select',['处理提交、关闭、driver.tick() 和提交确认回执','准入已确认后才激活调度；持有 StartPermit，按策略放行'])
d.panel(80,380,926,445,'WorkflowDriver.tick()   ·   同一条执行链','purple')
d.card('future',110,450,405,112,'running.next()',['FuturesUnordered<Workflow Future>','轮询就绪流程；整体完成才返回 ID'],'purple')
d.card('resource',550,450,420,136,'Broker / 队列 / LockTable',['Broker 发送 Acquire / Release','LockTable 决定等待或返回 Permit','状态仅在 Driver 循环中修改'],'amber')
d.card('context',110,650,405,135,'WorkflowContext',['wait_until_runnable → ctx.activity()','等待 Permit / 业务结果时 Pending','Tracker 等待真实 Activity 停稳'],'purple')
d.card('client',550,650,420,108,'ActivityClient',['发送 Execute + Context + Guard','通过 oneshot 等待类型化结果'],'purple')
d.card('domain',1122,240,398,125,'Handler → Service → Ports',['领域校验、命令、外部效果','停稳结果决定资源释放 / 隔离'],'green')
d.card('host',1122,440,398,125,'ServiceHost 循环',['轮询 Activity Futures','接收命令和 Query；处理停止'],'green')
d.card('mail',1122,650,398,108,'有界服务邮箱',['收到 Execute 后登记执行 Future','服务结果通过 oneshot 返回'],'green')
d.arrow([(520,340),(520,380)],'poll tick()',lx=540,ly=368)
d.arrow([(312,562),(312,650)],'poll / await',color='purple',lx=330,ly=611)
d.arrow([(515,688),(550,688)],color='purple')
d.arrow([(515,715),(530,715),(530,535),(550,535)],color='amber')
d.text(545,615,'Acquire → Permit；Pending → 唤醒 → 再次 poll',18,C['amber'])
d.arrow([(970,690),(1122,690)],'Execute',color='green',lx=994,ly=673)
d.arrow([(1320,650),(1320,565)],'接收',color='green',lx=1336,ly=610)
d.arrow([(1320,440),(1320,365)],'poll',color='green',lx=1336,ly=405)
d.arrow([(1520,330),(1535,330),(1535,810),(312,810),(312,785)],'oneshot 结果 → 唤醒原等待链',color='green',dash=True,lx=1100,ly=798)
d.note(82,904,'① 许可 / 服务回复唤醒等待链，由 Tokio 再次调度 TaskCenter；唤醒方不会直接执行 Workflow。')
d.note(82,940,'② 结束先等 Tracker 停稳，再保存终态；State 随后 reap，释放逻辑占用或保留 Uncertain 隔离。')
d.step('准入与放行','State 依据策略持有或发放 StartPermit，TaskCenter 一直同时处理提交与执行推进。','state')
d.step('轮询 Workflow','Driver 的 running.next() poll 就绪 Future；await 等待不阻塞整个循环。','future','context')
d.step('取得资源许可','Context 通过 Broker 发 Acquire；LockTable 处理事件，Permit 回复唤醒原等待链。','resource','context')
d.step('派发并执行 Activity','普通客户端发送 Execute；服务宿主单独轮询执行，调用领域能力。','client','mail','host','domain')
d.step('结果返回与收尾','服务真实停稳后 Guard 通知 Tracker 并发送 Release；oneshot 返回结果，Workflow 再次被 poll。','context','future','resource','state')
d.done()

d=Diagram('03-scheduling','调度交接：topo 到来，缩容何时真的让出？','以保留 Activity 调用的暂停方式展开。默认业务规则在 Policy，通用交接机制在 TaskCenter。')
xs=[180,490,800,1110,1420]
for x,title,color in zip(xs,['调用方','TaskCenter','topo Workflow','缩容 Workflow','迁移服务'],['ink','blue','blue','purple','green']):
    d.card('lane'+str(x),x-125,177,250,82,title,[],color)
    d.parts.append(f'<path d="M{x} 264V865" stroke="#cbd6e4" stroke-dasharray="4 7"/>')
events=[(0,1,300,'1 提交 topo；等待准入确认','blue'),(1,3,370,'2 Policy → YieldTo：加入 policy 暂停原因','blue'),(3,4,440,'3 suspend：等待实际停稳','purple'),(4,3,510,'4 ACK；原 await 保留','green'),(3,1,580,'5 当前控制代次的停稳 ACK 有效','purple'),(1,2,650,'6 发放 StartPermit','blue'),(2,1,720,'7 topo 实际完成 / reap','blue'),(1,3,790,'8 移除该 topo 的暂停原因','blue')]
for a,b,y,label,color in events:
    d.arrow([(xs[a],y),(xs[b],y)],label,color,lx=min(xs[a],xs[b])+18,ly=y-15)
d.note(70,903,'topo 尚未确认写入：不能抢占旧任务。暂停请求送达：不能当成缩容已停稳。','amber')
d.note(70,941,'最后一个 topo 结束 → 无用户暂停 → guard 重新检查 → 条件满足后才恢复迁移。','purple')
d.step('topo 准入','检查全部冲突与容量；Durable 的初始记录成功后，任务才激活调度。','lane180','lane490')
d.step('策略产生暂停原因','默认策略将缩容对 topo 的关系定义为 YieldTo。多个 topo 各自形成原因。','lane490','lane1110')
d.step('领域确认停稳','Driver 调用所选 suspend 适配器。仅发送命令不构成 ACK；此时 topo 仍不能启动。','lane1110','lane1420')
d.step('确认后交接','TaskCenter 使用当前控制代次的有效 ACK 重新规划，才发放 topo 的启动许可。','lane490','lane1110')
d.step('恢复仍要检查','topo 完成只清除自身原因。用户暂停还在就继续等；条件未满足则 WaitingConditions。','lane1110','lane1420')
d.done()

d=Diagram('04-pause','同一个 Activity 接口，两种暂停契约','上游 Workflow 选择语义；迁移服务仍通过普通 client / command 接口提供能力。')
d.panel(48,175,730,682,'A   保留调用   ·   await 暂不返回','purple')
d.panel(822,175,730,682,'B   停稳返回   ·   await 返回部分业务结果','green')
left=[('a1','1  普通 ctx.activity(...).await',['服务注册默认 ActivityControl','Workflow 也可覆盖下一次调用']),('a2','2  收到暂停 → suspend 命令',['适配器等待真实停稳 ACK','原 Activity Future 继续存在']),('a3','3  Task = Suspended',['原调用与资源锁保留','TaskCenter 此时才可安全交接']),('a4','4  暂停原因清除 → guard 检查',['不满足：WaitingConditions','满足：调用所选 resume，原 await 继续'])]
right=[('b1','1  普通 ctx.activity(...).await',['服务读取控制 token','暂停时自行停止并结算已完成进度']),('b2','2  返回类型化部分结果',['原 await 返回 cursor / 业务结果','没有新增统一的 Suspended Activity']),('b3','3  保存 checkpoint → wait_until_runnable',['Workflow 保存进度，再确认 Task 暂停','已结束 Activity 释放其资源']),('b4','4  暂停原因清除 → guard 检查',['满足后 Workflow 发起下一次 Activity','仍然使用同一个 TaskId'])]
for x,items,color in [(78,left,'purple'),(852,right,'green')]:
    for i,(key,title,lines) in enumerate(items):
        y=230+i*151
        d.card(key,x,y,670,125,title,lines,color)
        if i<3:d.arrow([(x+335,y+125),(x+335,y+151)],color=color)
d.note(70,902,'没有暂停能力的服务：等动作自然完成。控制失败 / 结果不确定：保留隔离，不伪造停稳。','amber')
d.note(70,940,'A 保留资源锁；若 topo 需要同一资源，需选择 B 或重新定义符合实际的资源范围。','amber')
d.step('仍然只有普通 Activity','两种方式都用 ctx.activity。上游可以选择不同 suspend/resume 命令，不新增 Activity 类型。','a1','b1')
d.step('await 是否返回','A 等最终业务结果；B 停稳后返回领域定义的部分结果，Workflow 有机会保存续跑数据。','a2','b2')
d.step('何时确认 Task 暂停','A 是停稳适配器 ACK；B 是 Workflow 消费结果、保存 checkpoint 后进入 wait_until_runnable。','a3','b3')
d.step('恢复前重新准入','guard 属于 Workflow。条件变化用 recheck 唤醒；检查通过后才继续迁移。','a4','b4')
d.done()

d=Diagram('05-recovery','持久化与切主：重建同一个 Task，继续业务','可恢复的是显式业务 checkpoint；不是序列化 Rust Future，也不是 Temporal 确定性重放。')
d.panel(48,175,710,670,'正常运行   ·   Workflow 自声明 Memory / Durable','blue')
d.panel(800,175,752,670,'新主恢复   ·   先 fencing，再恢复，再开放准入','amber')
left=[('persist','Workflow::persistence()',['默认 Memory：仍有完整 Task','Durable：声明 version + input']),('journal','Journal → TaskStore',['初始保存确认后才执行 / 激活调度','checkpoint、details、控制意图、终态']),('cas','TaskStore 写入约束',['owner_epoch 校验旧主；revision CAS','存储写入通过 spawn_blocking 等待']),('outcome','中断 / 完成记录',['停稳中断 Pending；用户取消 Cancelled','不确定效果 NeedsCheck，需对账'])]
right=[('fence','1  外部选主 / 领域恢复',['新轮次严格递增；阻止旧主数据面写入','恢复领域状态，再绑定当前服务客户端']),('define','2  define → enable_persistence',['绑定 Workflow::restore 与 dependencies','claim(pool, epoch)；普通准入保持关闭']),('restore','3  restore 全量预检与 CAS 接管',['校验类型 / input / version / objects','未知类型或需对账记录：停止恢复']),('install','4  from_restored → guard → 执行',['一次安装全部任务占用，再放行许可','用户暂停恢复；policy 原因重新计算'])]
for x,w,items,color in [(78,650,left,'blue'),(830,692,right,'amber')]:
    for i,(key,title,lines) in enumerate(items):
        y=230+i*145
        d.card(key,x,y,w,120,title,lines,color)
        if i<3:d.arrow([(x+w/2,y+120),(x+w/2,y+145)],color=color)
d.note(70,891,'TaskId 不变：创建轮次 / 池 UUID / 业务 / 流水；接管后只改变 owner_epoch 和 revision。','purple')
d.note(70,932,'动作已完成但 checkpoint 未写入：由稳定操作 ID、幂等命令或领域对账处理；框架不自动重放。')
d.step('持久性由 Workflow 决定','Memory 和 Durable 是同一 Task 管理模型。Durable 需要输入版本和 restore 构造。','persist')
d.step('保存续跑状态','report_details 更新观察；save_checkpoint 保存进度及 details。取消意图写盘后才交付取消令牌。','journal','cas','outcome')
d.step('外部选主是前提','所有池共享新轮次 TaskIds；TaskStore fencing 不能替代数据面 fencing 和领域状态恢复。','fence')
d.step('全量预检与接管','restore 在打开任何流程前校验全部待恢复记录，更新 owner_epoch/revision，保留原 TaskId。','define','restore')
d.step('恢复原任务','TaskCenter 安装所有占用再启动。Workflow 从 checkpoint 继续；用户暂停和业务 guard 仍有效。','install')
d.done()

d=Diagram('06-observation','观察与控制：所有入口指向同一个 Task','Run 是类型化结果句柄；Snapshot 是观察快照；SavedTask 是存储表示。它们不会生成新的 TaskId。')
d.card('caller',48,200,400,155,'用户 / 调度策略 / 业务事件',['用户：pause / resume / cancel','策略：维护各自的暂停原因','业务：条件变化后 recheck'],'blue')
d.card('control',545,200,510,155,'RunControl / ControlToken',['请求 ≠ 确认；控制代次使旧 ACK 失效','用户原因与 policy 原因独立','wait_suspended 等实际停稳'],'purple')
d.card('actor',1150,200,402,155,'Workflow / Activity',['Driver 处理控制与条件检查','领域执行者确认停稳 / 返回结果','取消也必须等待实际动作结束'],'green')
d.arrow([(448,270),(545,270)],'请求',lx=468,ly=253)
d.arrow([(1055,270),(1150,270)],'交付',color='purple',lx=1076,ly=253)
d.panel(48,425,1504,355,'同一个 Task   ·   上排：TaskSnapshot 内容；下排：访问句柄与存储表示','purple')
d.card('identity',78,490,440,110,'身份与结果状态',['TaskId / name / objects / RunState','progress / cancellation_requested'],'purple')
d.card('pauses',580,490,440,110,'调度与暂停',['conflicts：准入时解释','blockers / suspend_reasons / ACK'],'blue')
d.card('details',1080,490,442,110,'业务与步骤观察',['details：类型化业务附加信息','activities：当前宿主内调用历史'],'green')
d.card('view',78,645,440,104,'ExecutionView / TaskObserver',['同一快照；watch 可合并更新','不是无损事件日志'],'purple')
d.card('saved',580,645,440,104,'SavedTask / TaskStore',['同一 ID 的可选恢复记录','不持久化 Activity 历史 / Rust 栈'],'amber')
d.card('run',1080,645,442,104,'Run<T>',['accepted()：提交确认','result()：本次执行的类型化结果'],'ink')
d.arrow([(800,355),(800,425)],'控制、进度、调度解释更新同一个观察模型',color='purple',lx=820,ly=398)
d.note(70,837,'新任务身份：epoch / pool UUID / business / sequence；同一主共享序号分配器。','blue')
d.note(70,880,'PoolManager 的 TaskRef 包含池实例路由信息；PoolTaskSnapshot 包装同一个快照，不是另一套任务。')
d.note(70,928,'终态：Tracker 停稳 → Durable 保存结果 → 发布结果 → State 回收；Uncertain 继续隔离。','amber')
d.step('请求进入同一控制对象','用户控制和策略控制是不同原因。resume 只去掉用户原因，不越过仍存在的 topo 阻塞。','caller','control')
d.step('实际执行决定确认','命令送达不代表完成。Workflow/Activity 的停稳状态决定 pause ACK 和最终结果。','actor')
d.step('统一观察','任务状态、业务 details、调度解释、Activity 历史汇集到一个 TaskSnapshot。','identity','pauses','details')
d.step('不同入口，没有新任务','Observer 观察、Run 等结果、SavedTask 用于恢复；全部指向同一 TaskId。','view','saved','run')
d.done()

data=[{k:v for k,v in p.items() if k!='svg'} for p in pages]
nav=''.join(f'<button data-page="{i}" aria-current="{str(i==0).lower()}">{i+1:02d} {escape(p["title"].split("：")[0])}</button>' for i,p in enumerate(pages))
sections=''.join(f'<section data-page="{i}" {"hidden" if i else ""}>{p["svg"]}</section>' for i,p in enumerate(pages))
html='''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>统一 Task · 最新架构运行导读</title><style>
*{box-sizing:border-box}body{margin:0;color:#172b45;background:#edf1f6;font-family:"Microsoft YaHei","Segoe UI",sans-serif}header{padding:18px 28px;background:white;border-bottom:1px solid #dae2ee;display:flex;align-items:center;gap:20px}h1{font-size:22px;margin:0}header span{font-size:13px;color:#61728a}nav{padding:10px 28px;display:flex;gap:8px;flex-wrap:wrap;background:white}button,a{font:inherit}button{cursor:pointer;border:1px solid #cbd6e4;border-radius:7px;background:white;color:#23466f;padding:8px 12px}button:hover{background:#eaf1fc}button:focus-visible,a:focus-visible{outline:3px solid #dc8a31;outline-offset:2px}button[aria-current=true]{background:#2459aa;color:white;border-color:#2459aa}button:disabled{opacity:.4;cursor:default}.toolbar{padding:10px 28px;display:flex;align-items:center;gap:8px;font-size:14px}.toolbar a{margin-left:auto;color:#2459aa}.layout{display:grid;grid-template-columns:minmax(0,1fr) 310px;gap:16px;padding:0 20px 20px}#viewport{overflow:auto;height:calc(100vh - 190px);min-height:400px;border:1px solid #dae2ee;border-radius:12px;background:#fafbfd}section{width:100%;margin:0 auto}section[hidden]{display:none}svg{width:100%;height:auto;display:block}aside{background:white;border:1px solid #dae2ee;border-radius:12px;padding:20px;overflow:auto;max-height:calc(100vh - 190px)}aside h2{font-size:18px;line-height:1.5;margin:0 0 12px}aside p{font-size:14px;line-height:1.9;color:#465d77}.eyebrow{color:#61728a;font-size:12px;letter-spacing:1px}#steps{display:flex;gap:6px;flex-wrap:wrap;margin:15px 0}#detail{border-top:1px solid #e1e7ef;margin-top:24px;padding-top:18px}#detail h3{font-size:16px}footer{font-size:12px;color:#61728a;margin:20px 0}#zoom{min-width:40px}.hint{color:#61728a;margin-left:8px}@media(max-width:900px){header{padding:15px;display:block}header span{display:block;margin-top:8px}nav{padding:8px}.layout{grid-template-columns:minmax(0,1fr);padding:0 8px 12px}#viewport{height:60vh;min-height:320px}aside{max-height:none}.toolbar{padding:8px;flex-wrap:wrap}.hint{display:none}}@media print{header,nav,.toolbar,aside{display:none}.layout{display:block;padding:0}#viewport{height:auto;overflow:visible;border:0}section,section[hidden]{display:block!important;width:100%!important;break-after:page}svg .dim{opacity:1}svg .active .card{stroke-width:1.5}section:last-child{break-after:auto}@page{size:A3 landscape;margin:8mm}}
</style></head><body><header><h1>统一 Task · 架构运行导读</h1><span>基于 53b5ea6 · 六张图逐层展开 · 无外部依赖 · 可离线打开</span></header><nav aria-label="图示章节">NAV</nav><div class="toolbar"><button id="prev">← 上一图</button><button id="next">下一图 →</button><button id="minus">−</button><span id="zoom">100%</span><button id="plus">+</button><button id="fit">适应宽度</button><button id="print">打印全部</button><a href="../task-center-simulation/index.html">▶ 打开请求运行动画</a><span class="hint">点击组件查看职责 · 点击右侧步骤高亮运行路径</span><a id="source" href="01-layers.svg">下载 SVG</a></div><div class="layout"><main id="viewport">SECTIONS</main><aside><div class="eyebrow">READING PATH / 阅读路径</div><h2 id="heading"></h2><div id="steps" aria-label="阅读步骤"></div><p id="description" aria-live="polite"></p><button id="reset">显示全部组件</button><div id="detail"><div class="eyebrow">COMPONENT / 组件职责</div><h3 id="component-title">点击图中组件</h3><p id="component-body">选中一个组件，查看它的职责以及与执行机制的关系。</p></div><footer>教学图示，不执行真实任务。迁移仍为模拟接入；Task checkpoint 恢复不等于完整生产容灾。</footer></aside></div><script>
const pages=DATA;let current=0,scale=1,step=-1;
const $=id=>document.getElementById(id);
function highlight(keys){const groups=document.querySelectorAll('section:not([hidden]) .component');groups.forEach(g=>{g.classList.toggle('active',keys.includes(g.dataset.key));g.classList.toggle('dim',keys.length>0&&!keys.includes(g.dataset.key))})}
function selectStep(i){step=i;const s=pages[current].steps[i];$('description').textContent=s.title+'：'+s.body;highlight(s.keys);document.querySelectorAll('#steps button').forEach((b,n)=>b.setAttribute('aria-current',String(i===n)))}
function show(i){current=Math.max(0,Math.min(pages.length-1,i));step=-1;document.querySelectorAll('section').forEach((s,n)=>s.hidden=n!==current);document.querySelectorAll('nav button').forEach((b,n)=>b.setAttribute('aria-current',String(n===current)));$('heading').textContent=pages[current].title;$('description').textContent=pages[current].subtitle;$('steps').replaceChildren(...pages[current].steps.map((s,n)=>{const b=document.createElement('button');b.textContent=n+1;b.title=s.title;b.addEventListener('click',()=>selectStep(n));return b}));$('source').href=pages[current].slug+'.svg';$('prev').disabled=current===0;$('next').disabled=current===pages.length-1;$('component-title').textContent='点击图中组件';$('component-body').textContent='或按右侧步骤高亮运行路径。';highlight([]);zoom(1);$('viewport').scrollTo(0,0)}
function zoom(n){scale=Math.max(.5,Math.min(2.5,n));document.querySelectorAll('section').forEach(s=>s.style.width=(scale*100)+'%');$('zoom').textContent=Math.round(scale*100)+'%'}
document.querySelectorAll('nav button').forEach((b,i)=>b.addEventListener('click',()=>show(i)));
document.querySelectorAll('.component').forEach(g=>{const select=()=>{const c=pages[current].components[g.dataset.key];$('component-title').textContent=c.title;$('component-body').textContent=c.detail;highlight([g.dataset.key])};g.addEventListener('click',select);g.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();select()}})});
$('prev').onclick=()=>show(current-1);$('next').onclick=()=>show(current+1);$('plus').onclick=()=>zoom(scale+.25);$('minus').onclick=()=>zoom(scale-.25);$('fit').onclick=()=>zoom(1);$('reset').onclick=()=>{highlight([]);step=-1;document.querySelectorAll('#steps button').forEach(b=>b.setAttribute('aria-current','false'));$('description').textContent=pages[current].subtitle};$('print').onclick=()=>window.print();
document.addEventListener('keydown',e=>{if(e.key==='ArrowRight')show(current+1);if(e.key==='ArrowLeft')show(current-1)});
window.Guide={show,selectStep,inspect:()=>({current,scale,step,pages:pages.length})};show(0);
</script></body></html>'''
html=html.replace('NAV',nav).replace('SECTIONS',sections).replace('DATA',json.dumps(data,ensure_ascii=False).replace('</','<\\/'))
(OUT/'index.html').write_text(html,encoding='utf-8')
(OUT/'manifest.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print('Generated six SVG diagrams, manifest and offline guide')
