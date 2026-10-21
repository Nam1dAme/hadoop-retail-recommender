"""Small pure helpers shared by rendering and session-state tests."""
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

PAGES = {
    'overview': ('总览', '经营总览', '从销售走势到商品表现，查看这一版本的经营全貌。'),
    'sales': ('销售分析', '销售分析', '按来源日期、国家和商品，拆解销售与退货。'),
    'customers': ('客户分层', '客户分层', '用最近购买、订单频次与消费金额，理解不同客户。'),
    'associations': ('商品关联', '商品关联', '查看真实订单中的共购关系，寻找商品之间的联系。'),
    'recommendations': ('客户推荐', '客户推荐', '比较四种推荐方法，查看商品顺序与推荐依据。'),
    'experiments': ('模型实验', '模型实验', '在同一评价协议下，比较推荐质量与训练表现。'),
    'quality': ('数据质量与任务', '数据质量与任务', '追溯来源、质量统计和每一步处理记录。'),
}
ALGORITHMS = ('POP', 'ITEMCF', 'ALS', 'NCF')
SEGMENTS = {'CORE': '核心客户', 'AT_RISK': '需关注客户', 'NEW': '新近客户', 'POTENTIAL': '潜力客户', 'OTHER': '其他客户'}
STATUS = {'REGISTERED': '已登记', 'INGESTING': '导入中', 'PROCESSING': '处理中', 'PENDING': '等待中',
          'RUNNING': '运行中', 'SUCCEEDED': '已完成', 'FAILED': '失败', 'CANCELLED': '已取消',
          'CREATED': '已创建', 'READY': '可用'}
REASONS = {'POPULAR': '训练/服务历史热门商品', 'CO_PURCHASE': '与你购买的商品存在同单关联',
           'LATENT_FACTOR': '根据历史购买偏好推荐', 'NEURAL_CF': '根据神经协同过滤模型推荐',
           'FALLBACK_POPULAR': '当前模型信息不足，使用热门商品补足'}


def pounds(value):
    if value is None:
        return '—'
    amount = Decimal(str(value))
    if not amount.is_finite():
        raise ValueError('Non-finite money')
    return '£'+format(amount.quantize(Decimal('.01'), rounding=ROUND_HALF_UP), ',.2f')


def count(value):
    return '—' if value is None else format(int(value), ',')


def percent(numerator, denominator):
    if numerator is None or denominator is None or Decimal(str(denominator)) == 0:
        return '—'
    return format(Decimal(numerator)/Decimal(denominator)*100, '.2f')+'%'


def clear_page(state, page):
    prefixes = ('view_'+page+'_', 'pager_'+page+'_', 'selection_'+page+'_')
    for key in list(state):
        if key.startswith(prefixes) or key in ('applied_'+page, 'validation_'+page):
            del state[key]


def activate_release(state, release):
    for page in PAGES:
        clear_page(state, page)
    for key in list(state):
        if key.startswith('lookup_'):del state[key]
    state['fixed_release'] = dict(release)
    state['release_initialized'] = True
    state.pop('release_fault', None)


def sales_defaults(release):
    return dict(start_date=date.fromisoformat(release['source_start']),
                end_date=date.fromisoformat(release['source_end']), country=None,
                granularity='DAY', sort='GROSS_DESC', country_sort='NET_DESC')


def duration(run):
    if not run.get('started_at'):
        return '—'
    if not run.get('finished_at') and run.get('status')!='RUNNING':
        return '—'
    start = datetime.fromisoformat(run['started_at'].replace('Z', '+00:00'))
    end = datetime.fromisoformat(run['finished_at'].replace('Z', '+00:00')) if run.get('finished_at') else datetime.now(timezone.utc)
    seconds = max(0, int((end-start).total_seconds()))
    if seconds >= 3600:
        return f'{seconds//3600} 小时 {(seconds%3600)//60} 分'
    if seconds >= 60:
        return f'{seconds//60} 分 {seconds%60} 秒'
    return f'{seconds} 秒'


def task_lanes(runs):
    latest = {}
    for run in runs:
        old = latest.get(run['task_name'])
        if old is None or run['attempt'] > old['attempt']:
            latest[run['task_name']] = run
    groups = [[], [], [], [], []]
    for name, run in latest.items():
        if name == 'ingest': lane = 0
        elif name.startswith(('J01','J02','J03','J04','J05')): lane = 1
        elif name.startswith(('J06','J07','J08')): lane = 2
        elif name.startswith(('recommend_', 'publish_')): lane = 4
        elif name != 'pipeline': lane = 3
        else: continue
        groups[lane].append(run)
    result = []
    for name, rows in zip(('来源导入', '销售与客户', '共购与特征', '实验与模型', '推荐与发布'), groups):
        statuses = {r['status'] for r in rows}
        state = next((s for s in ('RUNNING','FAILED','PENDING','CANCELLED') if s in statuses), 'SUCCEEDED' if rows else None)
        result.append(dict(name=name, status=state, tasks=len(rows)))
    return result
