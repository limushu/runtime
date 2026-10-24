"""Generate hosts, stacked select branches, pipes, event dispatch and a state table."""
from pathlib import Path
from html import escape
import re

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / 'docs/diagrams/task-center-simulation/index.html'
parts = ['''<svg id="architecture" viewBox="0 0 1500 940" role="img" aria-labelledby="graph-title graph-desc">
<title id="graph-title">异步执行现场：宿主、等待点与资源事件</title>
<desc id="graph-desc">TaskCenter 宿主内，running.next 在上，events.recv 在下。Broker 和 mpsc 是管道；LockTable 是状态表。各领域服务分别拥有独立 Tokio task。琥珀色虚线持续标出 Pending Future，蓝色表示本帧推进。</desc>
<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M1 1L7 4L1 7" fill="none" stroke="context-stroke" stroke-width="1.6"/></marker></defs>
<rect width="1500" height="940" fill="#ffffff"/>
<rect class="host-boundary" data-host="taskcenter" x="205" y="65" width="1000" height="810" rx="16"/>
<text class="boundary-label" x="225" y="45">TaskCenter · 一个 Tokio task</text>
<text class="host-status" id="host-taskcenter" x="1185" y="45" text-anchor="end">等待提交</text>
<rect class="driver-boundary" x="230" y="230" width="950" height="620" rx="10"/>
<text class="boundary-label small" x="250" y="220">WorkflowDriver · 嵌入 TaskCenter，无独立宿主</text>
<text class="boundary-label small" x="1250" y="45">领域服务 · 各自独立 Tokio task</text>
''']
routes = {
    'submit':'M180 155H245', 'policy':'M645 155H690',
    'defer':'M445 205V220H530V260', 'select-events':'M430 540H465',
    'poll':'M430 305H465', 'persist':'M755 280H780V80H1240V105',
    'accepted':'M1240 120H1220V60H445V105', 'start':'M615 205V260',
    'run':'M755 310H800', 'acquire':'M1130 325H1160V520H1130',
    'events':'M800 520H755', 'lock':'M610 590V630', 'arbitrate':'M755 665H800',
    'permit':'M800 730H775V365H970V350', 'wake':'M800 285H755',
    'dispatch':'M970 350V390',
    'observe':'M1240 825H1185V905H680',
    'reap':'M245 310H220V220H350V205',
    'control':'M645 185H790V330H800',
    'checkpoint':'M1130 280H1210V120H1240',
    'control-ack':'M800 330H785V815H430',
    'user-control':'M100 205V915H1190V330H1130',
    'restore':'M1240 145H1215V245H725V260',
    'notify':'M335 780V755',
    'resource-exit':'M465 565H450V725H430',
    'workflow-exit':'M465 330H430',
    'wait':'M800 760H755',
    'reject':'M800 805H775V815H755',
    'withdraw':'M1130 335H1160V520H1130',
}
services = [('tier','Tier',190),('node','Node',305),('disk','Disk',420),('vd','VD',535),('balance','Balance',650)]
for key, _, y in services:
    routes['send-'+key]=f'M1130 425H1220V{y+35}H1260'
    routes['execute-'+key]=f'M1370 {y+35}V{y+73}'
    routes['reply-'+key]=f'M1260 {y+78}H1215V250H1090V260'
    routes['release-'+key]=f'M1260 {y+86}H1195V540H1130'
    routes['settle-'+key]=f'M1480 {y+75}H1490V825H1480'
    routes['stop-'+key]=f'M1130 330H1205V{y+60}H1260'
    routes['stop-ack-'+key]=f'M1260 {y+68}H1200V340H1130'
for base in ['send','execute','reply','release','settle','stop','stop-ack']:
    routes[base]=routes[base+'-disk']
parts.append('<g id="routes">')
for key, route in routes.items():
    parts.append(f'<path id="route-{key}" class="route" data-route="{key}" d="{route}" marker-end="url(#arrow)"/>')
parts.append('</g>')

def node(key, x, y, w, h, title, lines, kind='object'):
    parts.append(f'<g id="node-{key}" class="component kind-{kind}" data-node="{key}" data-kind="{kind}" role="button" tabindex="0" aria-label="{escape(title)}">')
    if kind == 'future':
        parts.append(f'<rect class="stack-back" x="{x+8}" y="{y-8}" width="{w}" height="{h}" rx="10"/>')
    radius = h/2 if kind == 'pipe' else 10
    parts.append(f'<rect class="node-card" x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}"/>')
    if kind == 'pipe':
        for offset in (12, w-12):
            parts.append(f'<path class="pipe-cap" d="M{x+offset} {y+12}V{y+h-12}"/>')
    if kind == 'table':
        for offset in (47,82,117):
            if offset < h:
                parts.append(f'<path class="table-rule" d="M{x} {y+offset}H{x+w}"/>')
    parts.append(f'<text class="node-title" x="{x+20}" y="{y+28}">{escape(title)}</text>')
    for i, line in enumerate(lines):
        extra = (' id="receiver-event"' if i == 0 else ' id="receiver-payload"') if key == 'event-branch' else ''
        parts.append(f'<text class="node-line"{extra} x="{x+20}" y="{y+53+i*22}">{escape(line)}</text>')
    parts.append(f'<rect class="await-ring" x="{x-4}" y="{y-4}" width="{w+8}" height="{h+8}" rx="13"/>')
    if key in ['futures','context'] or key.startswith('service-'):
        parts.append(f'<text class="await-label" id="await-{key}" x="{x+10}" y="{y+h+19}"></text>')
    parts.append('</g>')

node('caller',20,105,160,100,'PoolHandle',['业务请求','提交 / 用户控制'])
node('state',245,105,400,100,'State · 应用协调',['reap / pump / 外层 select','业务准入、排队与逻辑占用'])
node('policy',690,105,310,100,'SchedulingPolicy',['admission / relation / priority','由应用定义业务关系'])
node('store',1240,75,240,90,'TaskStore',['epoch / revision CAS'],'table')
node('driver',245,260,185,495,'tick() / select!',['同一个宿主内','biased 检查：','通知 → 事件','→ Workflow','','一支就绪后','publish 并返回','外层再 reap/pump'],'selector')
node('futures',465,260,290,100,'running.next()',['FuturesUnordered · poll','整体结束才返回 Some(id)'],'future')
node('context',800,260,330,90,'WorkflowContext',['Activity Future / await'],'future')
node('client',800,390,330,70,'ActivityClient',['Execute → 服务；oneshot ← 结果'],'pipe')
node('broker',800,490,330,70,'Broker / mpsc<Event>',['发送资源事件；acquire 等 Permit'],'pipe')
node('event-branch',465,490,290,100,'events.recv()',['取出一条 Event → table.event','处理后返回 None'],'receiver')
node('event-types',465,630,290,78,'Event 分派',[],'router')
for index, name in enumerate(['Acquire','Release','Withdraw']):
    parts.append(f'<text class="event-choice" data-event-type="{name}" x="{480+index*92}" y="690">{name}</text>')
node('locks',800,630,330,175,'LockTable · 状态表',[],'table')
for i, name in enumerate(['held','waiting','fences']):
    parts.append(f'<text class="table-value" id="table-{name}" x="820" y="{700+i*35}">{name}: —</text>')
node('outcomes',465,745,290,90,'pump() 仲裁结果',[],'router')
for index, (name, label) in enumerate([('permit','Permit'),('wait','Waiting'),('reject','Rejected')]):
    parts.append(f'<text class="outcome-choice" data-outcome="{name}" x="{480+index*92}" y="806">{label}</text>')
node('notify',245,780,185,65,'wake.notified()',['通知外层重算'],'pipe')
node('service',1240,885,240,45,'各服务均独立运行',[],'legend')
for key, label, y in services:
    node('service-'+key,1260,y,220,90,label+' · Tokio task',['Mailbox → Activity Futures'],'host')
node('tracker',1240,790,240,70,'Tracker / Completion',['Guard 结算 → 真实停稳'])
node('observe',440,880,240,45,'统一 Task 观察',[],'legend')
parts.append('''<g id="packet" aria-hidden="true"><circle r="12" class="packet-halo"/><circle r="5" class="packet-dot"/></g><g id="packet-label" aria-hidden="true"><rect width="180" height="28" rx="5"/><text x="8" y="19" id="packet-text"></text></g></svg>''')
html = PAGE.read_text(encoding='utf-8')
html, count = re.subn(r'<svg id="architecture".*?</svg>', lambda _: '\n'.join(parts), html, count=1, flags=re.S)
assert count == 1
PAGE.write_text(html, encoding='utf-8')
print('Generated hosts / stacked tick branches / Broker pipe / event dispatch / LockTable')
