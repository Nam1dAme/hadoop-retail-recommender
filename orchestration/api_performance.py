"""Real, fixed-release HTTP measurements; never substitute fixtures for NF-02."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import subprocess
import time

import httpx
import pymysql.cursors

from orchestration.database import named_lock
from orchestration.provision import READ_COLUMNS


ROOT = Path(__file__).resolve().parents[1]


def percentile(values, fraction):
    """Nearest-rank percentile, including failed requests in the latency sample."""
    if not values or not 0 < fraction <= 1:
        raise ValueError('Invalid percentile sample')
    return sorted(values)[math.ceil(len(values)*fraction)-1]


def summarize(samples):
    seconds = [s['seconds'] for s in samples]
    errors = sum(not s['ok'] for s in samples)
    return dict(requests=len(samples), errors=errors, error_rate=errors/len(samples),
                p50_seconds=percentile(seconds, .5), p95_seconds=percentile(seconds, .95),
                max_seconds=max(seconds),
                status='PASS' if errors/len(samples) < .01 and percentile(seconds, .95) <= 2 else 'FAIL')


def snapshot(connection):
    connection.rollback()
    with connection.cursor() as q:
        q.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
        q.execute('SHOW TABLES')
        tables = [next(iter(r.values())) for r in q.fetchall()]
    result = {}
    for table in sorted(tables):
        # Names originate from the database, but still validate SQL identifiers.
        if not table.replace('_', '').isalpha():
            raise ValueError('Unexpected table identifier')
        with connection.cursor() as q:
            q.execute('SHOW KEYS FROM `'+table+'`')
            keys = sorted((r for r in q.fetchall() if r['Key_name'] == 'PRIMARY'), key=lambda r:r['Seq_in_index'])
        order = ','.join('`'+r['Column_name']+'`' for r in keys)
        fields = READ_COLUMNS.get(table, '*')
        digest, count = hashlib.sha256(), 0
        with connection.cursor(pymysql.cursors.SSCursor) as q:
            q.execute('SELECT '+fields+' FROM `'+table+'` ORDER BY '+order)
            for row in q:
                digest.update((json.dumps(row, default=str, ensure_ascii=False, separators=(',', ':'))+'\n').encode())
                count += 1
        result[table] = dict(rows=count, sha256=digest.hexdigest(), columns=fields)
    connection.rollback()
    return result


def measure(connection, config, directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    started = datetime.now(timezone.utc).isoformat()
    sources = [ROOT/'orchestration/api_performance.py',ROOT/'scripts/benchmark-api.py']
    source_hashes = {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    with named_lock(connection):
        with connection.cursor() as q:
            q.execute("SELECT run_id FROM task_run WHERE status IN ('PENDING','RUNNING')")
            if q.fetchone():
                raise RuntimeError('Resolve active offline tasks before the performance test')
        before = snapshot(connection)
        with connection.cursor() as q:
            q.execute('''SELECT r.*,b.source_sha256 FROM current_release c JOIN data_release r USING(release_id)
                JOIN data_batch b ON b.batch_id=r.batch_id WHERE c.singleton_id=1''')
            release = q.fetchone()
            if not release or release['status'] != 'READY':
                raise RuntimeError('A real READY release is required')
            rid, bid = release['release_id'], release['batch_id']
            q.execute('SELECT customer_id FROM customer_rfm WHERE release_id=%s ORDER BY customer_id LIMIT 1', (rid,))
            customer = q.fetchone()['customer_id']
            q.execute('SELECT stock_code FROM item_similarity WHERE release_id=%s ORDER BY stock_code LIMIT 1', (rid,))
            item = q.fetchone()['stock_code']
            q.execute('SELECT VERSION() version')
            db_version = q.fetchone()['version']
        connection.rollback()
        cases = [
            ('summary', '/summary', {}),
            ('trend_month', '/sales/trend', {'granularity': 'MONTH'}),
            ('countries', '/sales/countries', {'page_size': 20}),
            ('products', '/products', {'page_size': 20}),
            ('products_country', '/products', {'page_size': 20, 'country': 'France'}),
            ('customers', '/customers', {'page_size': 20}),
            ('customers_page2', '/customers', {'page_size': 20, 'page': 2}),
            ('segments', '/customers/segments', {}),
            ('similar', '/products/'+item+'/similar', {}),
        ] + [('recommendations_'+a, '/customers/'+customer+'/recommendations', {'algorithm': a})
             for a in ('POP', 'ITEMCF', 'ALS', 'NCF')]
        results = []
        base = config['api']['url']
        with httpx.Client(timeout=10, trust_env=False, limits=httpx.Limits(max_connections=5)) as client:
            def request(path, params):
                begin = time.perf_counter()
                code, ok, error = None, False, None
                try:
                    response = client.get(base+path, params=params | {'release_id': rid})
                    code = response.status_code
                    body = response.json()
                    ok = code == 200 and body['meta']['release_id'] == rid and body['meta']['batch_id'] == bid
                    if not ok:
                        error = 'HTTP_OR_VERSION_MISMATCH'
                except Exception as exc:
                    error = type(exc).__name__
                return dict(seconds=time.perf_counter()-begin, http_status=code, ok=ok, error=error)
            for name, path, params in cases:
                warmup = [request(path, params) for _ in range(10)]
                if not all(s['ok'] for s in warmup):
                    raise RuntimeError('Warmup failed for '+name)
                begin = time.perf_counter()
                with ThreadPoolExecutor(max_workers=5) as pool:
                    samples = list(pool.map(lambda _: request(path, params), range(100)))
                result = dict(name=name, path=path, params=params, warmup_requests=10, concurrency=5,
                              wall_seconds=time.perf_counter()-begin, samples=samples, **summarize(samples))
                results.append(result)
                (directory/(name+'.json')).write_text(json.dumps(result, indent=2)+'\n')
                print(json.dumps({k:v for k,v in result.items() if k not in ('samples','params')}), flush=True)
        # Representative predicates and ordering used by the API; full HTTP SQL
        # correctness remains covered separately by verify-w09.py.
        statements = {
            'customer_list': ('SELECT * FROM customer_rfm WHERE release_id=%s ORDER BY customer_id LIMIT 20', (rid,)),
            'recommendation': ('SELECT * FROM customer_recommendation WHERE release_id=%s AND algorithm=%s AND customer_id=%s ORDER BY `rank`', (rid, 'NCF', customer)),
            'similar': ('SELECT * FROM item_similarity WHERE release_id=%s AND stock_code=%s ORDER BY `rank`', (rid, item)),
            'country_sales': ('SELECT stock_code,SUM(gross_amount) FROM sales_cube WHERE release_id=%s AND country=%s GROUP BY stock_code', (rid, 'France')),
        }
        plans = {}
        for name, (sql, params) in statements.items():
            with connection.cursor() as q:
                q.execute('EXPLAIN FORMAT=JSON '+sql, params)
                plans[name] = dict(sql=sql, plan=json.loads(next(iter(q.fetchone().values()))))
        after = snapshot(connection)
    record = dict(schema_version='1.0', test_id='T-18', baseline='HL-DOC-1.0.0',
                  status='PASS' if before == after and all(r['status']=='PASS' for r in results) else 'FAIL',
                  started_at=started, finished_at=datetime.now(timezone.utc).isoformat(),
                  code_commit=subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip(),
                  executed_source_sha256=source_hashes,
                  environment_sha256=hashlib.sha256((ROOT/'uv.lock').read_bytes()).hexdigest(),
                  config_sha256=hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
                  release_id=rid, batch_id=bid, experiment_id=release['experiment_id'], data_sha256=release['source_sha256'],
                  database_version=db_version, cpu=Path('/proc/cpuinfo').read_text().split('model name')[1].split('\n')[0].split(':',1)[1].strip(),
                  platform=platform.platform(), meminfo=Path('/proc/meminfo').read_text(),
                  thresholds=dict(p95_seconds=2, error_rate_exclusive=.01),
                  results=results, query_plans=plans, before=before, after=after, database_unchanged=before==after,
                  scope='Real HTTP, one class at a time, 10 warmups + 100 requests at concurrency 5; startup excluded; advisory lock excludes project offline jobs')
    (directory/'api-performance.json').write_text(json.dumps(record, indent=2, default=str)+'\n')
    return record
