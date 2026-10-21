"""Bound-parameter queries against published results; never run model code."""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import math
import re
from statistics import median

from service.errors import APIError, missing, pending

ALGORITHMS = ('POP', 'ITEMCF', 'ALS', 'NCF')
SEGMENTS = ('CORE', 'AT_RISK', 'NEW', 'POTENTIAL', 'OTHER')
REASONS = {'POPULAR': '按全历史热门商品推荐。', 'CO_PURCHASE': '基于历史已购商品的共购关联推荐。',
           'LATENT_FACTOR': '基于 ALS 隐式偏好模型推荐。', 'NEURAL_CF': '基于 NCF 神经协同过滤模型推荐。',
           'FALLBACK_POPULAR': '个性化候选不足，使用未购热门商品补足。'}
RELEASE_FIELDS = '''r.release_id,r.batch_id,r.experiment_id,r.champion_algorithm,
    b.min_source_date AS source_start,b.max_source_date AS source_end,r.published_at,r.baseline_version'''
RUN_FIELDS = '''run_id,batch_id,task_name,attempt,status,started_at,finished_at,
    application_id,error_code,error_message'''
EXPERIMENT_FIELDS = '''experiment_id,batch_id,status,split_policy_version,champion_algorithm,created_at,finished_at'''


def money(value):
    return format(Decimal(value or 0).quantize(Decimal('.01'), rounding=ROUND_HALF_UP), '.2f')


def amounts(row):
    row = dict(row)
    for field in ('gross_amount', 'return_amount', 'net_amount', 'monetary'):
        if field in row:
            row[field] = money(row[field])
    for field in ('order_count', 'customer_count', 'sale_quantity', 'return_quantity'):
        if field in row:
            row[field] = int(row[field] or 0)
    if 'is_merchandise' in row:
        row['is_merchandise'] = bool(row['is_merchandise'])
    row['currency'] = 'GBP'
    return row


def safe_task(row):
    # Legacy worker messages are untrusted free text and may contain SQL/passwords.
    # Keep the stable machine code; do not pass raw exception text into HTTP responses.
    if row['error_message'] is not None:
        row['error_message'] = '任务执行异常，请凭 run_id 查看本机任务日志。'
    if row['error_code'] and not re.fullmatch('[A-Z0-9_]{1,64}', row['error_code']):
        row['error_code'] = 'TASK_ERROR'
    if 'log_available' in row:
        row['log_available'] = bool(row['log_available'])
    return row


class Repository:
    def __init__(self, session):
        self.db = session
        self.release = None
        self.batch_id = None

    def resolve(self, release_id=None):
        if release_id is None:
            pointer = self.db.one('SELECT release_id FROM current_release WHERE singleton_id=1')
            if not pointer or not pointer['release_id']:
                raise APIError(404, 'NO_ACTIVE_RELEASE', '尚无当前发布版本。')
            release_id = pointer['release_id']
        row = self.db.one('SELECT '+RELEASE_FIELDS+''',r.status FROM data_release r
            JOIN data_batch b ON b.batch_id=r.batch_id WHERE r.release_id=%s''', (release_id,))
        if not row:
            raise missing()
        if row['status'] == 'RETIRED':
            raise APIError(410, 'RELEASE_RETIRED', '该版本已退休，请切换版本。')
        if row['status'] != 'READY':
            raise APIError(409, 'RELEASE_NOT_READY', '该版本尚不可查询。')
        row.pop('status')
        self.release = row
        self.batch_id = row['batch_id']
        return row

    @property
    def rid(self):
        return self.release['release_id']

    def page(self, sql, args, query, order, count_sql=None, count_args=None):
        total = int(self.db.one(count_sql or 'SELECT COUNT(*) AS total FROM ('+sql+') counted',
                                args if count_args is None else count_args)['total'])
        offset = (query.page-1)*query.page_size
        # Avoid sending arbitrary-precision offsets to MariaDB for far-out pages.
        items = [] if offset >= total else self.db.rows(sql+' ORDER BY '+order+' LIMIT %s OFFSET %s',
                                                        (*args, query.page_size, offset))
        return dict(items=items, page=query.page, page_size=query.page_size, total=total,
                    has_next=offset+len(items) < total)

    def releases(self, query):
        return self.page('SELECT '+RELEASE_FIELDS+''',r.status FROM data_release r
            JOIN data_batch b ON b.batch_id=r.batch_id WHERE r.status='READY' ''', (), query,
            'r.published_at DESC,r.release_id ASC')

    def dates(self, query):
        start = max(query.start_date or date.min, self.release['source_start'] or date.max)
        end = min(query.end_date or date.max, self.release['source_end'] or date.min)
        return start, end

    def filters(self, query, alias):
        start, end = self.dates(query)
        sql = f'{alias}.release_id=%s AND {alias}.source_date BETWEEN %s AND %s'
        args = [self.rid, start, end]
        if query.country is not None:
            # utf8mb4_bin still has PAD SPACE semantics. Cast the *parameter*
            # to binary so an unknown value such as "UK " stays an empty result.
            sql += f' AND {alias}.country=BINARY %s'
            args.append(query.country)
        return sql, args

    def summary(self, query):
        cube, args = self.filters(query, 's')
        result = self.db.one('''SELECT SUM(gross_amount) gross_amount,SUM(return_amount) return_amount,
            SUM(gross_amount)-SUM(return_amount) net_amount,SUM(sale_quantity) sale_quantity,
            SUM(return_quantity) return_quantity FROM sales_cube s WHERE '''+cube, args)
        orders, args = self.filters(query, 'o')
        result.update(self.db.one('''SELECT COUNT(*) order_count,COUNT(DISTINCT customer_id) customer_count
            FROM sale_order o WHERE '''+orders, args))
        result['average_order_value'] = money(Decimal(result['gross_amount'] or 0)/result['order_count']) if result['order_count'] else None
        return amounts(result)

    def trend(self, query):
        start, end = self.dates(query)
        if start > end:
            return dict(currency='GBP', points=[])
        if query.granularity == 'DAY' and (end-start).days+1 > 400:
            raise APIError(422, 'INVALID_PARAMETER', 'DAY 查询最多允许 400 个日期点。', {'granularity': '日期交集超过 400 天'})
        cube, ca = self.filters(query, 's')
        orders, oa = self.filters(query, 'o')
        cube_period = 's.source_date' if query.granularity == 'DAY' else "CAST(DATE_FORMAT(s.source_date,'%%Y-%%m-01') AS DATE)"
        order_period = 'o.source_date' if query.granularity == 'DAY' else "CAST(DATE_FORMAT(o.source_date,'%%Y-%%m-01') AS DATE)"
        sales = {r['period_start']: r for r in self.db.rows(f'''SELECT {cube_period} period_start,
            SUM(gross_amount) gross_amount,SUM(return_amount) return_amount,
            SUM(gross_amount)-SUM(return_amount) net_amount FROM sales_cube s WHERE {cube} GROUP BY period_start''', ca)}
        counts = {r['period_start']: r['order_count'] for r in self.db.rows(f'''SELECT {order_period} period_start,
            COUNT(*) order_count FROM sale_order o WHERE {orders} GROUP BY period_start''', oa)}
        day = start if query.granularity == 'DAY' else start.replace(day=1)
        points = []
        while day <= end:
            row = sales.get(day, dict(period_start=day, gross_amount=0, return_amount=0, net_amount=0))
            row['order_count'] = counts.get(day, 0)
            point = amounts(row)
            point.pop('currency')
            points.append(point)
            if query.granularity == 'DAY':
                if day == date.max:
                    break
                day += timedelta(days=1)
            else:
                if day.year == 9999 and day.month == 12:
                    break
                day = date(day.year+(day.month == 12), day.month % 12+1, 1)
        return dict(currency='GBP', points=points)

    def countries(self, query):
        cube, ca = self.filters(query, 's')
        orders, oa = self.filters(query, 'o')
        sql = f'''SELECT a.*,COALESCE(o.order_count,0) order_count FROM
            (SELECT country,SUM(gross_amount) gross_amount,SUM(return_amount) return_amount,
             SUM(gross_amount)-SUM(return_amount) net_amount FROM sales_cube s WHERE {cube} GROUP BY country) a
            LEFT JOIN (SELECT country,COUNT(*) order_count FROM sale_order o WHERE {orders} GROUP BY country) o
            ON o.country=a.country'''
        order = {'NET_DESC': 'net_amount DESC,a.country ASC', 'GROSS_DESC': 'gross_amount DESC,a.country ASC'}[query.sort]
        result = self.page(sql, (*ca, *oa), query, order,
            'SELECT COUNT(DISTINCT s.country) total FROM sales_cube s WHERE '+cube, ca)
        result['items'] = [amounts(r) for r in result['items']]
        return result

    def products(self, query):
        cube, ca = self.filters(query, 's')
        orders, oa = self.filters(query, 'o')
        if query.stock_code is not None:
            cube += ' AND s.stock_code=BINARY %s'
            ca.append(query.stock_code)
            orders += ' AND i.stock_code=BINARY %s'
            oa.append(query.stock_code)
        sql = f'''SELECT a.*,p.description,p.is_merchandise FROM
            (SELECT s.stock_code,SUM(s.gross_amount) gross_amount,SUM(s.return_amount) return_amount,
             SUM(s.gross_amount)-SUM(s.return_amount) net_amount,SUM(s.sale_quantity) sale_quantity,
             SUM(s.return_quantity) return_quantity FROM sales_cube s WHERE {cube} GROUP BY s.stock_code) a
            JOIN product_dim p ON p.release_id=%s AND p.stock_code=a.stock_code'''
        order = {'GROSS_DESC': 'gross_amount DESC,a.stock_code ASC', 'QUANTITY_DESC': 'sale_quantity DESC,a.stock_code ASC'}[query.sort]
        result = self.page(sql, (*ca, self.rid), query, order,
            'SELECT COUNT(DISTINCT s.stock_code) total FROM sales_cube s WHERE '+cube, ca)
        # Count orders only for this page's products, using (release,stock,invoice).
        # Aggregating all 519k order-item rows before LIMIT exceeded the SQL budget.
        codes = [row['stock_code'] for row in result['items']]
        counts = {}
        if codes:
            marks = ','.join(['%s']*len(codes))
            rows = self.db.rows(f'''SELECT i.stock_code,COUNT(DISTINCT i.invoice_no) order_count
                FROM sale_order_item i JOIN sale_order o ON o.release_id=i.release_id AND o.invoice_no=i.invoice_no
                WHERE i.release_id=%s AND i.stock_code IN ({marks}) AND {orders} GROUP BY i.stock_code''',
                (self.rid, *codes, *oa))
            counts = {row['stock_code']: row['order_count'] for row in rows}
        for row in result['items']:
            row['order_count'] = counts.get(row['stock_code'], 0)
        result['items'] = [amounts(r) for r in result['items']]
        return result

    def customers(self, query):
        sql = '''SELECT customer_id,last_purchase,as_of_date,recency_days,frequency,monetary,
            r_score,f_score,m_score,segment FROM customer_rfm WHERE release_id=%s'''
        args = [self.rid]
        for key in ('segment', 'customer_id'):
            if getattr(query, key) is not None:
                sql += f' AND {key}=BINARY %s'
                args.append(getattr(query, key))
        result = self.page(sql, args, query, 'customer_id ASC')
        result['items'] = [amounts(r) for r in result['items']]
        return result

    def segments(self):
        rows = self.db.rows('''SELECT segment,COUNT(*) customer_count,MAX(as_of_date) as_of_date
            FROM customer_rfm WHERE release_id=%s GROUP BY segment''', (self.rid,))
        counts = {r['segment']: r['customer_count'] for r in rows}
        return dict(as_of_date=max((r['as_of_date'] for r in rows), default=None),
                    items=[dict(segment=s, customer_count=counts.get(s, 0)) for s in SEGMENTS])

    def similar(self, stock_code, query):
        product = self.db.one('SELECT stock_code,description FROM product_dim WHERE release_id=%s AND stock_code=BINARY %s', (self.rid, stock_code))
        if not product:
            raise missing()
        product['items'] = self.db.rows('''SELECT s.related_stock_code,p.description,s.`rank`,s.similarity,
            s.co_count,s.confidence,s.lift FROM item_similarity s JOIN product_dim p
            ON p.release_id=s.release_id AND p.stock_code=s.related_stock_code
            WHERE s.release_id=%s AND s.stock_code=BINARY %s ORDER BY s.`rank` LIMIT %s''', (self.rid, stock_code, query.k))
        return product

    def recommendations(self, customer_id, query):
        algorithm = query.algorithm or self.release['champion_algorithm']
        model = self.db.one('''SELECT rm.model_run_id FROM release_model rm JOIN model_run m
            ON m.model_run_id=rm.model_run_id WHERE rm.release_id=%s AND rm.algorithm=%s
            AND m.algorithm=rm.algorithm AND m.batch_id=%s AND m.experiment_id=%s
            AND m.purpose='FULLFIT' AND m.status='SUCCEEDED' ''',
            (self.rid, algorithm, self.batch_id, self.release['experiment_id']))
        if not model:
            raise RuntimeError('Published model binding is invalid')
        known = self.db.one('SELECT customer_id FROM customer_rfm WHERE release_id=%s AND customer_id=BINARY %s', (self.rid, customer_id)) is not None
        eligible = self.db.one('''SELECT COUNT(*) n FROM product_dim p WHERE p.release_id=%s AND p.is_candidate=1
            AND NOT EXISTS (SELECT 1 FROM customer_seen_item s WHERE s.release_id=p.release_id
                            AND s.customer_id=BINARY %s AND s.stock_code=p.stock_code)''', (self.rid, customer_id))['n']
        if known:
            items = self.db.rows('''SELECT r.stock_code,p.description,r.`rank`,r.score,r.reason_code,r.is_fallback
                FROM customer_recommendation r JOIN product_dim p ON p.release_id=r.release_id AND p.stock_code=r.stock_code
                WHERE r.release_id=%s AND r.algorithm=%s AND r.customer_id=BINARY %s ORDER BY r.`rank` LIMIT %s''',
                (self.rid, algorithm, customer_id, query.k))
        else:
            items = self.db.rows('''SELECT p.stock_code,d.description,p.order_count score FROM product_popularity p
                JOIN product_dim d ON d.release_id=p.release_id AND d.stock_code=p.stock_code AND d.is_candidate=1
                WHERE p.release_id=%s ORDER BY p.`rank` LIMIT %s''', (self.rid, query.k))
            for rank, item in enumerate(items, 1):
                item.update(rank=rank, reason_code='POPULAR' if algorithm == 'POP' else 'FALLBACK_POPULAR',
                            is_fallback=algorithm != 'POP')
        if known and len(items) != min(query.k, eligible):
            raise RuntimeError('Published recommendation coverage is invalid')
        for item in items:
            item['score'] = float(item['score'])
            item['is_fallback'] = bool(item['is_fallback'])
            item['reason_text'] = REASONS[item['reason_code']]
        return dict(customer_id=customer_id, known_customer=known, algorithm=algorithm, **model,
                    eligible_candidates=eligible, returned_count=len(items),
                    fallback_used=any(i['is_fallback'] for i in items), items=items)

    def batches(self, query):
        return self.page('''SELECT batch_id,status,source_name,source_sha256,policy_version,raw_rows,
            created_at,updated_at FROM data_batch''', (), query, 'created_at DESC,batch_id ASC')

    def quality(self, batch_id):
        row = self.db.one('''SELECT batch_id,status,(quality_report_path IS NOT NULL) report_available
            FROM data_batch WHERE batch_id=%s''', (batch_id,))
        if not row:
            raise missing()
        self.batch_id = batch_id
        row['report_available'] = bool(row['report_available'])
        row['metrics'] = self.db.rows('''SELECT metric_name,metric_value,description FROM data_quality
            WHERE batch_id=%s ORDER BY metric_name''', (batch_id,))
        return row

    def runs(self, query):
        sql, args = 'SELECT '+RUN_FIELDS+' FROM task_run WHERE 1=1', []
        for key in ('batch_id', 'status'):
            if getattr(query, key) is not None:
                sql += f' AND {key}=%s'
                args.append(getattr(query, key))
        result = self.page(sql, args, query, 'created_at DESC,run_id ASC')
        result['items'] = [safe_task(r) for r in result['items']]
        return result

    def run(self, run_id):
        row = self.db.one('SELECT '+RUN_FIELDS+''',parent_run_id,heartbeat_at,output_manifest_path,
            (log_path IS NOT NULL) log_available FROM task_run WHERE run_id=%s''', (run_id,))
        if not row:
            raise missing()
        self.batch_id = row['batch_id']
        return safe_task(row)

    def experiments(self, query):
        sql, args = 'SELECT '+EXPERIMENT_FIELDS+' FROM experiment', []
        if query.batch_id is not None:
            sql += ' WHERE batch_id=%s'
            args.append(query.batch_id)
        return self.page(sql, args, query, 'created_at DESC,experiment_id ASC')

    def experiment(self, experiment_id):
        row = self.db.one('SELECT batch_id,status,test_sealed_at FROM experiment WHERE experiment_id=%s', (experiment_id,))
        if not row:
            raise missing()
        self.batch_id = row['batch_id']
        return row

    def metrics(self, experiment_id, query):
        experiment = self.experiment(experiment_id)
        if query.split == 'TEST' and not experiment['test_sealed_at']:
            raise pending()
        rows = self.db.rows('''SELECT s.algorithm,s.model_run_id,m.user_count,m.catalog_count,m.window_user_count,
            m.precision_at_k,m.recall_at_k,m.ndcg_at_k,m.coverage,m.cold_user_count,m.cold_item_count,
            m.cold_interaction_count,m.excluded_user_count,m.candidate_policy_version FROM experiment_model s
            JOIN model_run r ON r.model_run_id=s.model_run_id AND r.experiment_id=s.experiment_id
                AND r.algorithm=s.algorithm AND r.purpose='EVAL' AND r.status='SUCCEEDED'
            JOIN model_metric m ON m.model_run_id=s.model_run_id
            WHERE s.experiment_id=%s AND m.split_name=%s AND m.cohort=%s AND m.k=10''',
            (experiment_id, query.split, query.cohort))
        if {r['algorithm'] for r in rows} != set(ALGORITHMS) or len(rows) != 4:
            raise pending()
        for row in rows:
            if row.pop('candidate_policy_version') != 'full-catalog-v1':
                raise RuntimeError('Unsupported candidate policy')
        return dict(experiment_id=experiment_id, split=query.split, cohort=query.cohort,
                    items=sorted(rows, key=lambda r: ALGORITHMS.index(r['algorithm'])))

    def epochs(self, model_run_id):
        row = self.db.one('SELECT model_run_id,batch_id,purpose,algorithm FROM model_run WHERE model_run_id=%s', (model_run_id,))
        if not row:
            raise missing()
        self.batch_id = row.pop('batch_id')
        row['items'] = self.db.rows('''SELECT epoch,train_loss,validation_ndcg,elapsed_seconds,peak_gpu_mib
            FROM model_epoch WHERE model_run_id=%s ORDER BY epoch''', (model_run_id,)) if row['algorithm'] == 'NCF' else []
        return row

    def benchmarks(self, experiment_id):
        self.experiment(experiment_id)
        rows = self.db.rows('''SELECT b.* FROM experiment_model s JOIN model_run m ON m.model_run_id=s.model_run_id
            AND m.experiment_id=s.experiment_id AND m.algorithm='NCF' AND m.purpose='EVAL' AND m.status='SUCCEEDED'
            JOIN gpu_benchmark b ON b.model_run_id=s.model_run_id WHERE s.experiment_id=%s AND s.algorithm='NCF'
            ORDER BY b.device,b.repeat_index,b.benchmark_id''', (experiment_id,))
        if not rows:
            raise pending()
        complete = len(rows) == 6 and all(sorted(r['repeat_index'] for r in rows if r['device'] == d) == [1, 2, 3] for d in ('CPU', 'CUDA'))
        config_keys = ('dataset_sha256', 'config_sha256', 'seed', 'batch_size', 'epochs', 'warmup_steps', 'torch_version')
        valid = complete and len({tuple(r[k] for k in config_keys) for r in rows}) == 1
        valid = valid and all(math.isfinite(r['train_seconds']) and r['train_seconds'] > 0 for r in rows)
        cpu = median(r['train_seconds'] for r in rows if r['device'] == 'CPU') if valid else None
        gpu = median(r['train_seconds'] for r in rows if r['device'] == 'CUDA') if valid else None
        public = ('benchmark_id', 'model_run_id', 'device', 'repeat_index', 'batch_size', 'epochs', 'warmup_steps',
                  'train_seconds', 'samples_per_second', 'peak_memory_mib', 'hardware', 'torch_version', 'cuda_runtime', 'driver_version')
        return dict(experiment_id=experiment_id, items=[{k: r[k] for k in public} for r in rows],
            summary=dict(cpu_median_seconds=cpu, gpu_median_seconds=gpu, speedup=cpu/gpu if valid else None,
                         complete_repeats=complete, comparison_valid=bool(valid)))
