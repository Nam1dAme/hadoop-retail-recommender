#!/usr/bin/env python3
"""Independent real HTTP/reader audit of W09; never imports API query code.

The digest covers every reader-visible row/column in all 23 tables, before/after
HTTP requests. Restricted control columns outside the reader grant are excluded.
Synthetic lifecycle/error tests belong to tests/test_api.py, never this database.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path
import platform
import re
from statistics import median
import subprocess
import time
from uuid import UUID

import httpx
import pymysql.cursors

from orchestration.database import connect
from orchestration.provision import READ_COLUMNS
from orchestration.settings import load_config

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def money(value):
    return format(Decimal(value or 0).quantize(Decimal('.01'), rounding=ROUND_HALF_UP), '.2f')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--db-defaults-file', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    connection = connect(config, args.db_defaults_file)
    connection.begin()

    def sql(statement, params=()):
        with connection.cursor() as cursor:
            cursor.execute(statement, params)
            return list(cursor.fetchall())

    def snapshot():
        connection.rollback()
        with connection.cursor() as q:
            q.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
        tables = [next(iter(r.values())) for r in sql('SHOW TABLES')]
        assert len(tables) == 23
        result = {}
        for table in sorted(tables):
            assert re.fullmatch('[a-z_]+', table)
            keys = sorted((r for r in sql('SHOW KEYS FROM `'+table+'`') if r['Key_name'] == 'PRIMARY'), key=lambda r:r['Seq_in_index'])
            order = ','.join('`'+r['Column_name']+'`' for r in keys)
            fields = READ_COLUMNS.get(table, '*')
            h, count = hashlib.sha256(), 0
            with connection.cursor(pymysql.cursors.SSCursor) as q:
                q.execute('SELECT '+fields+' FROM `'+table+'` ORDER BY '+order)
                for row in q:
                    h.update((json.dumps(row, default=str, ensure_ascii=False, separators=(',',':'))+'\n').encode())
                    count += 1
            result[table] = dict(rows=count, sha256=h.hexdigest(), columns=fields)
        connection.rollback()
        return result

    before = snapshot()
    release = sql('''SELECT r.*,b.min_source_date,b.max_source_date,b.source_sha256 FROM current_release c
        JOIN data_release r ON r.release_id=c.release_id JOIN data_batch b ON b.batch_id=r.batch_id WHERE c.singleton_id=1''')[0]
    rid, bid, eid = (release[k] for k in ('release_id','batch_id','experiment_id'))
    assert release['status'] == 'READY'
    checks, requests = [], []
    base = config['api']['url']

    def check(name, condition):
        assert condition, name
        checks.append(name)

    def get(path, params=None, expected=200):
        start = time.monotonic()
        response = client.get(base+path, params=params or {})
        elapsed = time.monotonic()-start
        check('HTTP '+path, response.status_code == expected)
        body = response.json()
        check('request_id '+path, str(UUID(response.headers['x-request-id'])) == body['meta']['request_id'])
        check('JSON '+path, response.headers['content-type'] == 'application/json')
        if params and 'release_id' in params and expected == 200:
            check('fixed version '+path, body['meta']['release_id'] == rid and body['meta']['batch_id'] == bid)
        requests.append(dict(path=path, status=response.status_code, seconds=round(elapsed,6)))
        return body

    def summary(params=None):
        params = params or {}
        payload = get('/summary', {'release_id':rid, **params})['data']
        filt, values = 'release_id=%s', [rid]
        for field in ('country','start_date','end_date'):
            if field in params:
                filt += ' AND '+({'country':'country=%s','start_date':'source_date>=%s','end_date':'source_date<=%s'}[field])
                values.append(params[field])
        sales = sql('''SELECT SUM(gross_amount) gross,SUM(return_amount) ret,SUM(sale_quantity) sq,
            SUM(return_quantity) rq FROM sales_cube WHERE '''+filt, values)[0]
        orders = sql('SELECT COUNT(*) n,COUNT(DISTINCT customer_id) users FROM sale_order WHERE '+filt, values)[0]
        expected = dict(currency='GBP',gross_amount=money(sales['gross']),return_amount=money(sales['ret']),
            net_amount=money((sales['gross'] or 0)-(sales['ret'] or 0)),sale_quantity=int(sales['sq'] or 0),
            return_quantity=int(sales['rq'] or 0),order_count=orders['n'],customer_count=orders['users'],
            average_order_value=money(sales['gross']/orders['n']) if orders['n'] else None)
        check('summary equals independent SQL '+str(params), payload == expected)
        return expected

    with httpx.Client(timeout=7, trust_env=False) as client:
        spec = client.get(base.removesuffix('/api/v1')+'/openapi.json').json()
        check('20 documented GET endpoints', len(spec['paths']) == 20 and all(set(m)=={'get'} for m in spec['paths'].values()))
        check('live', get('/health/live')['data']['status'] == 'UP')
        check('ready', get('/health/ready')['data']['status'] == 'READY')
        current = get('/releases/current')['data']
        check('current release identity', current['release_id'] == rid and current['batch_id'] == bid and current['experiment_id'] == eid)
        check('source dates', current['source_start'] == release['min_source_date'].isoformat() and current['source_end'] == release['max_source_date'].isoformat())
        versions = get('/releases')['data']
        check('READY versions only', versions['total'] == sql("SELECT COUNT(*) n FROM data_release WHERE status='READY'")[0]['n'] and all(r['status']=='READY' for r in versions['items']))
        totals = summary()
        summary(dict(country='United Kingdom',start_date='2011-11-01',end_date='2011-11-30'))
        summary(dict(country='__UNKNOWN_COUNTRY__'))
        summary(dict(start_date='2026-01-01',end_date='2026-12-31'))
        for granularity in ('DAY','MONTH'):
            points = get('/sales/trend',dict(release_id=rid,granularity=granularity))['data']['points']
            check('trend exact totals '+granularity, sum(Decimal(p['gross_amount']) for p in points) == Decimal(totals['gross_amount']) and sum(p['order_count'] for p in points) == totals['order_count'])
            check('trend sorted unique '+granularity, [p['period_start'] for p in points] == sorted({p['period_start'] for p in points}))
            check('trend finite money '+granularity, all(re.fullmatch(r'-?\d+\.\d{2}',p['net_amount']) for p in points))
        countries = get('/sales/countries',dict(release_id=rid,page_size=100))['data']
        check('all countries counted',countries['total'] == len(countries['items']) == sql('SELECT COUNT(DISTINCT country) n FROM sales_cube WHERE release_id=%s',(rid,))[0]['n'])
        check('countries exact amounts',sum(Decimal(p['net_amount']) for p in countries['items']) == Decimal(totals['net_amount']))
        check('countries order counts',sum(p['order_count'] for p in countries['items']) == totals['order_count'])
        check('countries stable ordering',countries['items'] == sorted(countries['items'],key=lambda p:(-Decimal(p['net_amount']),p['country'].encode())))
        for sorting, field in (('GROSS_DESC','gross_amount'),('QUANTITY_DESC','sale_quantity')):
            products = get('/products',dict(release_id=rid,sort=sorting,page_size=7))['data']
            expected_codes = sql('SELECT stock_code,SUM('+('gross_amount' if sorting=='GROSS_DESC' else 'sale_quantity')+') v FROM sales_cube WHERE release_id=%s GROUP BY stock_code ORDER BY v DESC,stock_code LIMIT 7',(rid,))
            check('product ranking '+sorting,[p['stock_code'] for p in products['items']] == [p['stock_code'] for p in expected_codes])
            for product in products['items']:
                count = sql('SELECT COUNT(DISTINCT invoice_no) n FROM sale_order_item WHERE release_id=%s AND stock_code=%s',(rid,product['stock_code']))[0]['n']
                check('distinct product orders '+product['stock_code'],product['order_count'] == count)
        customers = get('/customers',dict(release_id=rid,page_size=3))['data']
        check('customer count',customers['total'] == sql('SELECT COUNT(*) n FROM customer_rfm WHERE release_id=%s',(rid,))[0]['n'])
        customer_id = customers['items'][0]['customer_id']
        check('customer monetary',customers['items'][0]['monetary'] == money(sql('SELECT monetary FROM customer_rfm WHERE release_id=%s AND customer_id=%s',(rid,customer_id))[0]['monetary']))
        segments = get('/customers/segments',dict(release_id=rid))['data']
        check('segments order',[p['segment'] for p in segments['items']] == ['CORE','AT_RISK','NEW','POTENTIAL','OTHER'])
        check('segments customer total',sum(p['customer_count'] for p in segments['items']) == customers['total'])
        stock_code = sql('SELECT stock_code FROM item_similarity WHERE release_id=%s ORDER BY stock_code LIMIT 1',(rid,))[0]['stock_code']
        similar = get('/products/'+stock_code+'/similar',dict(release_id=rid))['data']['items']
        expected = sql('SELECT related_stock_code,`rank`,similarity,co_count,confidence,lift FROM item_similarity WHERE release_id=%s AND stock_code=%s ORDER BY `rank`',(rid,stock_code))
        check('similarity exact',[{k:p[k] for k in expected[0]} for p in similar] == expected)
        bindings = {p['algorithm']:p['model_run_id'] for p in sql('SELECT * FROM release_model WHERE release_id=%s',(rid,))}
        catalog = sql('SELECT COUNT(*) n FROM product_dim WHERE release_id=%s AND is_candidate=1',(rid,))[0]['n']
        seen = {p['stock_code'] for p in sql('SELECT stock_code FROM customer_seen_item WHERE release_id=%s AND customer_id=%s',(rid,customer_id))}
        unknown = '__W09_UNKNOWN_CUSTOMER__'
        check('unknown customer absent',not sql('SELECT customer_id FROM customer_rfm WHERE release_id=%s AND customer_id=%s',(rid,unknown)))
        for algorithm in ('POP','ITEMCF','ALS','NCF'):
            known = get('/customers/'+customer_id+'/recommendations',dict(release_id=rid,algorithm=algorithm))['data']
            expected = sql('SELECT stock_code,`rank`,score,reason_code,is_fallback FROM customer_recommendation WHERE release_id=%s AND algorithm=%s AND customer_id=%s ORDER BY `rank`',(rid,algorithm,customer_id))
            check('FULLFIT model '+algorithm,known['model_run_id'] == bindings[algorithm] and known['algorithm'] == algorithm)
            check('stored recommendation exact '+algorithm,[{k:p[k] for k in expected[0]} for p in known['items']] == expected)
            check('candidates and exclusions '+algorithm,known['eligible_candidates'] == catalog-len(seen) and all(p['stock_code'] not in seen for p in known['items']))
            fallback = get('/customers/'+unknown+'/recommendations',dict(release_id=rid,algorithm=algorithm,k=3))['data']
            pop = sql('SELECT stock_code FROM product_popularity WHERE release_id=%s ORDER BY `rank` LIMIT 3',(rid,))
            check('unknown fallback '+algorithm,not fallback['known_customer'] and fallback['model_run_id'] == bindings[algorithm] and fallback['fallback_used'] == (algorithm!='POP') and [p['stock_code'] for p in fallback['items']] == [p['stock_code'] for p in pop])
        get('/batches')
        quality = get('/batches/'+bid+'/quality')['data']
        expected = sql('SELECT metric_name,metric_value,description FROM data_quality WHERE batch_id=%s ORDER BY metric_name',(bid,))
        check('quality exact',quality['metrics'] == expected)
        runs = get('/runs',dict(batch_id=bid))['data']
        detail = get('/runs/'+runs['items'][0]['run_id'])['data']
        check('log metadata only',type(detail['log_available']) is bool and 'log_path' not in detail)
        check('task batch context',detail['batch_id'] == bid)
        get('/experiments',dict(batch_id=bid))
        selected = sql('SELECT algorithm,model_run_id FROM experiment_model WHERE experiment_id=%s',(eid,))
        for split in ('VALIDATION','TEST'):
            for cohort in ('WARM','ALL_ELIGIBLE'):
                metrics = get('/experiments/'+eid+'/metrics',dict(split=split,cohort=cohort))['data']['items']
                check('four selected EVAL models '+split+cohort,[p['algorithm'] for p in metrics] == ['POP','ITEMCF','ALS','NCF'] and {p['model_run_id'] for p in metrics} == {p['model_run_id'] for p in selected})
                for metric in metrics:
                    expected = sql('SELECT * FROM model_metric WHERE model_run_id=%s AND split_name=%s AND cohort=%s',(metric['model_run_id'],split,cohort))[0]
                    check('metric exact '+metric['algorithm']+split+cohort, all(metric[k] == expected[k] for k in metric if k not in ('algorithm','model_run_id')))
        selected_ncf = next(p['model_run_id'] for p in selected if p['algorithm']=='NCF')
        for mid in (selected_ncf,bindings['NCF'],bindings['POP']):
            epochs = get('/models/'+mid+'/epochs')['data']
            expected = sql('SELECT epoch,train_loss,validation_ndcg,elapsed_seconds,peak_gpu_mib FROM model_epoch WHERE model_run_id=%s ORDER BY epoch',(mid,))
            check('real epochs '+epochs['purpose']+epochs['algorithm'],epochs['items'] == expected)
        benchmark = get('/experiments/'+eid+'/benchmarks')['data']
        rows = sql('SELECT * FROM gpu_benchmark WHERE model_run_id=%s',(selected_ncf,))
        cpu = median(r['train_seconds'] for r in rows if r['device']=='CPU')
        gpu = median(r['train_seconds'] for r in rows if r['device']=='CUDA')
        check('benchmark actual six repeats',len(benchmark['items']) == 6 and benchmark['summary']['complete_repeats'] and benchmark['summary']['comparison_valid'])
        check('benchmark truthful ratio',benchmark['summary']['cpu_median_seconds'] == cpu and benchmark['summary']['gpu_median_seconds'] == gpu and benchmark['summary']['speedup'] == cpu/gpu)
        for path, params in [('/summary',{'unexpected':'x'}),('/products',{'sort':'net; DROP TABLE x'}),('/runs',{'release_id':rid}),('/summary',{'start_date':'2011-12-10','end_date':'2011-12-01'})]:
            check('invalid parameter envelope '+path,get(path,params,422)['error']['code'] == 'INVALID_PARAMETER')
        check('SQL injection literal',get('/summary',dict(release_id=rid,country="' OR 1=1 --"))['data']['gross_amount'] == '0.00')
        check('pagination beyond end',get('/products',dict(release_id=rid,page=10**20))['data']['items'] == [])
        denied = False
        try:
            # Zero-row operation: verifies denial without risking a data change.
            sql('UPDATE current_release SET updated_at=updated_at WHERE 1=0')
        except pymysql.MySQLError:
            denied = True
        check('actual reader UPDATE denied',denied)

        # Short five-client probe motivated by the observed product-query timeout.
        # This is W09 regression evidence, not the full W11 performance acceptance.
        def probe(index):
            path = '/products' if index % 2 else '/customers/'+customer_id+'/recommendations'
            start = time.monotonic()
            response = client.get(base+path,params={'release_id':rid})
            return dict(path=path,status=response.status_code,seconds=time.monotonic()-start)
        with ThreadPoolExecutor(max_workers=5) as pool:
            concurrent = list(pool.map(probe, range(15)))
        check('five-client probe no HTTP failures',all(r['status']==200 for r in concurrent))

    after = snapshot()
    check('all 23 reader-visible table digests unchanged',before == after)
    frozen = {}
    for name, expected in re.findall(r'\[([^\]]+\.md)\]\([^)]*\) \| `([0-9a-f]{64})`',(ROOT/'docs/BASELINE-MANIFEST.md').read_text()):
        check('frozen '+name,digest(ROOT/'docs'/name) == expected)
        frozen[name] = expected
    check('18 frozen baseline documents',len(frozen)==18)
    connection.close()
    times = sorted(r['seconds'] for r in concurrent)
    code = subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    tracked_dirty = bool(subprocess.check_output(['git','diff','HEAD','--name-only'],cwd=ROOT,text=True).strip())
    report = dict(status='PASS',baseline_version='HL-DOC-1.0.0',test_ids=['T-15','T-17'],
        observed_at=datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),code_commit=code,
        tracked_code_dirty=tracked_dirty,config_sha256=digest(args.config),uv_lock_sha256=digest(ROOT/'uv.lock'),
        environment=dict(python=platform.python_version(),packages={name:importlib.metadata.version(name) for name in ('fastapi','pydantic','uvicorn','PyMySQL','httpx')}),
        release_id=rid,batch_id=bid,experiment_id=eid,source_sha256=release['source_sha256'],
        summary=totals,benchmark=benchmark['summary'],checks=checks,check_count=len(checks),requests=requests,
        before_tables=before,after_tables=after,frozen_baseline=frozen,
        five_client_probe=dict(samples=len(concurrent),errors=sum(r['status']!=200 for r in concurrent),p95_seconds=times[math.ceil(.95*len(times))-1],requests=concurrent,
            scope='Short W09 list/recommendation regression probe; not full W11 T-18 acceptance'),
        scope='Real loopback HTTP + SELECT-only reader; all 20 endpoints; independent SQL comparison; no production fixture writes; digest covers all reader-visible columns, not inaccessible control fields')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('status','check_count','release_id','code_commit','tracked_code_dirty')}))


if __name__ == '__main__':
    main()
