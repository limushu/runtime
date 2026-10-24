"""Update the animation's SVG. Explicit select branches, not sequential workers."""
from pathlib import Path
from html import escape
import re

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / 'docs/diagrams/task-center-simulation/index.html'
parts = ['''<svg id="architecture" viewBox="0 0 1280 980" role="img" aria-labelledby="graph-title graph-desc">
<title id="graph-title">Driver tick 的资源事件与工作流轮询分支</title>
<desc id="graph-desc">同一个 TaskCenter 宿主，tick 使用 biased select。资源事件分支返回 None；工作流分支在 Pending 时继续等待，只有整体结束返回 Some(TaskId)。通知分支另行让外层重算。</desc>
<defs><pattern id="grid" width="24" height="24" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r=".8" fill="#313447"/></pattern><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M1 1L7 4L1 7" fill="none" stroke="context-stroke" stroke-width="1.6"/></marker></defs>
<rect width="1280" height="980" fill="url(#grid)"/>
<rect class="boundary" x="200" y="82" width="735" height="835" rx="18"/>
<text class="boundary-label" x="220" y="44">TASKCENTER / 每池一个 Tokio task</text>
<rect class="driver-boundary" x="230" y="235" width="673" height="545" rx="12"/>
<text class="boundary-label small" x="245" y="225">WORKFLOW DRIVER / 同一宿主内的两个工作分支</text>
<rect class="service-boundary" x="975" y="350" width="280" height="365" rx="12"/>
<text class="boundary-label small" x="990" y="335">领域服务 / 独立 Tokio task</text>
<text class="boundary-label small" x="995" y="698">Disk / Node / Tier / VD / Balance</text>
''']

routes = {
    'submit':'M180 155H240', 'policy':'M520 155H590',
    'defer':'M380 205V255', 'select-events':'M375 330V365',
    'poll':'M740 330V365', 'persist':'M880 400H950V70H1120V105',
    'accepted':'M985 145H940V60H380V105', 'start':'M520 180H920V345H820V365',
    'run':'M715 455V505', 'acquire':'M600 550H505',
    'events':'M375 505V455', 'lock':'M245 410H220V695H245',
    'permit':'M505 695H560V570H600', 'wake':'M795 505V455',
    'dispatch':'M740 595V650', 'send':'M880 695H950V445H1000',
    'execute':'M1118 490V550', 'settle':'M1235 615H1260V935H375V885',
    'release':'M1000 630H960V760H525V570H505',
    'reply':'M1000 585H940V350H860V365', 'observe':'M505 842H600',
    'reap':'M245 290H205V225H300V205',
    'control':'M520 185H575V525H600', 'stop':'M880 545H1000',
    'checkpoint':'M880 525H920V225H1120V205',
    'control-ack':'M600 575H580V350H100V345',
    'stop-ack':'M1000 615H910V580H880',
    'user-control':'M100 205V950H910V570H880',
    'restore':'M1120 205V240H890V410H880',
    'notify':'M180 290H245', 'resource-exit':'M505 385H540V330',
    'workflow-exit':'M600 385H570V330',
}
parts.append('<g id="routes">')
for key, route in routes.items():
    parts.append(f'<path id="route-{key}" class="route" data-route="{key}" d="{route}" marker-end="url(#arrow)"/>')
parts.append('</g>')


def node(key, x, y, w, h, title, lines):
    parts.append(f'<g id="node-{key}" class="component" data-node="{key}" role="button" tabindex="0" aria-label="{escape(title)}">')
    parts.append(f'<rect class="node-card" x="{x}" y="{y}" width="{w}" height="{h}" rx="9"/><rect class="node-accent" x="{x}" y="{y+15}" width="3" height="{h-30}" rx="1"/>')
    parts.append(f'<text class="node-title" x="{x+15}" y="{y+27}">{escape(title)}</text>')
    for i, line in enumerate(lines):
        parts.append(f'<text class="node-line" x="{x+15}" y="{y+50+i*20}">{escape(line)}</text>')
    parts.append('</g>')


node('caller',20,105,160,100,'业务调用方',['PoolHandle','提交 / 用户控制'])
node('state',240,105,280,100,'TaskCenter / State',['reap → pump → 外层 select','队列 / 逻辑占用 / StartPermit'])
node('policy',590,105,290,100,'SchedulingPolicy',['admission / relation / priority','topo > 其他 > shrink；同级 FIFO'])
node('store',985,105,270,100,'TaskStore',['Journal · blocking IO','epoch / revision 校验'])
node('driver',245,255,635,75,'WorkflowDriver.tick() · tokio::select! { biased; … }',['检查顺序：通知 → 资源 → 工作流；一支就绪才返回外层'])
node('notify',20,255,160,90,'通知分支',['wake.notified()','控制 / ACK → None'])
node('event-branch',245,365,260,90,'① events.recv()',['table.event → publish','就绪：返回 None'])
node('futures',600,365,280,90,'② running.next()',['poll 就绪 Workflow Futures','Pending ≠ 返回；完成才 Some(id)'])
node('broker',245,505,260,90,'Broker / 资源队列',['Acquire / Release / Withdraw'])
node('context',600,505,280,90,'WorkflowContext',['Activity / RunCondition / 控制'])
node('locks',245,650,260,90,'LockTable',['waiting / held / fences'])
node('client',600,650,280,90,'ActivityClient',['类型化命令 + Guard'])
node('mail',1000,400,235,90,'服务邮箱',['Execute / Query'])
node('service',1000,550,235,110,'领域动作执行',['Host → Service → Ports','独立 Activity Futures'])
node('tracker',245,800,260,85,'Tracker / Completion',['实际停稳 → 发布终态'])
node('observe',600,800,280,85,'统一 Task 观察',['Snapshot / Observer'])
parts.append('''<g id="packet" aria-hidden="true"><circle r="13" class="packet-halo"/><circle r="5" class="packet-dot"/></g><g id="packet-label" aria-hidden="true"><rect width="125" height="26" rx="5"/><text x="8" y="18" id="packet-text"></text></g></svg>''')
html = PAGE.read_text(encoding='utf-8')
html, count = re.subn(r'<svg id="architecture".*?</svg>', lambda _: '\n'.join(parts), html, count=1, flags=re.S)
assert count == 1
PAGE.write_text(html, encoding='utf-8')
print('Generated runtime SVG with two work branches and a separate notification branch')
