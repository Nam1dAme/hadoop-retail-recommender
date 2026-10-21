"""T15/T17: actual MariaDB + HTTP contract; fixtures stay in the isolated DB.

No SQLite substitute, production writes, filesystem log endpoint or training calls.
"""
import json
import time
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pymysql
import pytest

from orchestration.database import open_connection, utcnow
from service.api import create_app
from service.database import ReadSession
from service.repository import Repository
from test_releases import release_fixture, install, another


PREFIX = '/api/v1'


@pytest.fixture
def api(db, release_fixture, mariadb_instance):
    c, manifest, directory = release_fixture
    install(c, manifest, directory)
    rid, bid, eid = (manifest[k] for k in ('release_id', 'batch_id', 'experiment_id'))
    with c.cursor() as q:
        q.execute("UPDATE data_batch SET min_source_date='2011-12-08',max_source_date='2011-12-10',quality_report_path='/isolated/private/quality.json' WHERE batch_id=%s", (bid,))
        q.execute("INSERT INTO data_quality VALUES (%s,'RAW_ROWS',2,'fixture rows',%s)", (bid, utcnow()))
        q.execute("UPDATE task_run SET started_at=%s,log_path='/private/secret.log' WHERE batch_id=%s", (utcnow(), bid))
        q.execute("SELECT model_run_id FROM experiment_model WHERE experiment_id=%s AND algorithm='NCF'", (eid,))
        ncf = q.fetchone()['model_run_id']
        q.execute('INSERT INTO model_epoch VALUES (%s,1,0.69,1.5,NULL,0.01,128)', (ncf,))
        q.execute('INSERT INTO model_epoch VALUES (%s,2,0.60,1.2,NULL,0.02,128)', (ncf,))
        q.execute("SELECT run_id FROM model_run WHERE model_run_id=%s", (ncf,))
        run = q.fetchone()['run_id']
    c.commit()
    def factory_for(role='reader'):
        credentials = dict(line.removeprefix('export ').split('=', 1) for line in (db[2]/(role+'.env')).read_text().splitlines())
        return open_connection(name=db[1], user=credentials['RETAIL_DB_USER'], password=credentials['RETAIL_DB_PASSWORD'],
                               unix_socket=mariadb_instance[1], read_timeout=2, write_timeout=2)
    factory = factory_for
    application = create_app(connection_factory=factory)
    with TestClient(application) as client:
        yield dict(c=c, client=client, app=application, factory=factory, factory_for=factory_for, rid=rid, bid=bid, eid=eid,
                   ncf=ncf, run=run, manifest=manifest, directory=directory, db=db)


def get(api, path, **params):
    result = api['client'].get(PREFIX+path, params=params)
    assert result.status_code == 200, (path, result.text)
    assert result.headers['content-type'] == 'application/json'
    data = result.json()
    assert str(UUID(result.headers['x-request-id'])) == data['meta']['request_id']
    assert data['meta']['schema_version'] == '1.0'
    return data


def error(client, path, status, code, **kwargs):
    result = client.get(PREFIX+path, **kwargs)
    assert result.status_code == status, (path, result.text)
    payload = result.json()
    assert payload['error']['code'] == code
    assert isinstance(payload['error']['details'], dict)
    assert payload['meta']['request_id'] == result.headers['x-request-id']
    assert result.headers['content-type'] == 'application/json'
    return payload


def test_all_twenty_endpoints_types_context_and_openapi(api):
    cases = [
        ('/health/live', {'status', 'service_version'}),
        ('/health/ready', {'status'}),
        ('/releases/current', {'release_id','batch_id','experiment_id','champion_algorithm','source_start','source_end','currency','published_at','baseline_version'}),
        ('/releases', {'items','page','page_size','total','has_next'}),
        ('/summary', {'currency','gross_amount','return_amount','net_amount','order_count','customer_count','average_order_value','sale_quantity','return_quantity'}),
        ('/sales/trend', {'points','currency'}),
        ('/sales/countries', {'items','page','page_size','total','has_next'}),
        ('/products', {'items','page','page_size','total','has_next'}),
        ('/customers', {'items','page','page_size','total','has_next'}),
        ('/customers/segments', {'as_of_date','items'}),
        ('/products/00001/similar', {'stock_code','description','items'}),
        ('/customers/U/recommendations', {'customer_id','known_customer','algorithm','model_run_id','eligible_candidates','returned_count','fallback_used','items'}),
        ('/batches', {'items','page','page_size','total','has_next'}),
        (f"/batches/{api['bid']}/quality", {'batch_id','status','metrics','report_available'}),
        ('/runs', {'items','page','page_size','total','has_next'}),
        (f"/runs/{api['run']}", {'run_id','batch_id','task_name','attempt','status','started_at','finished_at','application_id','error_code','error_message','parent_run_id','heartbeat_at','output_manifest_path','log_available'}),
        ('/experiments', {'items','page','page_size','total','has_next'}),
        (f"/experiments/{api['eid']}/metrics", {'experiment_id','split','cohort','k','candidate_policy_version','items'}),
        (f"/models/{api['ncf']}/epochs", {'model_run_id','purpose','algorithm','items'}),
        (f"/experiments/{api['eid']}/benchmarks", {'experiment_id','items','summary'}),
    ]
    ids = set()
    for path, fields in cases:
        payload = get(api, path)
        assert set(payload['data']) == fields, path
        assert payload['meta']['request_id'] not in ids
        ids.add(payload['meta']['request_id'])
        business = path.startswith(('/summary','/sales/','/products','/customers','/releases/current'))
        assert payload['meta']['release_id'] == (api['rid'] if business else None)
    spec = api['client'].get('/openapi.json').json()
    assert len(spec['paths']) == 20
    for path, methods in spec['paths'].items():
        assert set(methods) == {'get'}, path
        assert 'requestBody' not in methods['get']
        assert methods['get']['responses']['422']['content']['application/json']['schema']['$ref'].endswith('/ErrorResponse')
    current = get(api, '/releases/current')['data']
    assert current['published_at'].endswith('Z') and current['source_start'] == '2011-12-08'
    rec = get(api, '/customers/U/recommendations')['data']
    assert rec['known_customer'] is True and rec['fallback_used'] is False
    assert isinstance(rec['items'][0]['score'], float)
    assert get(api, f"/runs/{api['run']}")['data']['log_available'] is True
    assert '/private/' not in json.dumps(get(api, f"/runs/{api['run']}"))
    assert get(api, f"/batches/{api['bid']}/quality")['data']['report_available'] is True


def test_exact_money_empty_filters_boundaries_and_trend(api):
    summary = get(api, '/summary')['data']
    assert summary == dict(currency='GBP', gross_amount='37.19', return_amount='2.17', net_amount='35.02',
                           order_count=1, customer_count=1, average_order_value='37.19', sale_quantity=3, return_quantity=1)
    assert get(api, '/summary', start_date='2011-12-09', end_date='2011-12-09')['data'] == summary
    for params in [dict(country='unknown'), dict(country='UK '), dict(country='uk'), dict(start_date='2026-01-01'), dict(end_date='2000-01-01')]:
        empty = get(api, '/summary', **params)['data']
        assert empty['gross_amount'] == '0.00' and empty['average_order_value'] is None
        assert empty['customer_count'] == 0 and empty['order_count'] == 0
        assert get(api, '/products', **params)['data']['items'] == []
        assert get(api, '/sales/countries', **params)['data']['total'] == 0
    points = get(api, '/sales/trend')['data']['points']
    assert [r['period_start'] for r in points] == ['2011-12-08','2011-12-09','2011-12-10']
    assert [r['gross_amount'] for r in points] == ['0.00','37.19','0.00']
    assert get(api, '/sales/trend', country='absent')['data']['points'][1]['order_count'] == 0
    assert get(api, '/sales/trend', start_date='2020-01-01')['data']['points'] == []
    monthly = get(api, '/sales/trend', granularity='MONTH')['data']['points']
    assert len(monthly) == 1 and monthly[0]['period_start'] == '2011-12-01'
    assert monthly[0]['gross_amount'] == '37.19'
    # AOV must round half up; it is gross / positive orders, never net / orders.
    with api['c'].cursor() as q:
        for invoice in ('2','3','4','5','6'):
            q.execute("INSERT INTO sale_order VALUES (%s,%s,'2011-12-09','UK',NULL,0)", (api['rid'],invoice))
    api['c'].commit()
    assert get(api, '/summary')['data']['average_order_value'] == '6.20'


def test_pagination_stable_ties_and_full_product_distinct_orders(api):
    with api['c'].cursor() as q:
        q.execute("INSERT INTO sales_cube VALUES (%s,'2011-12-09','UK','00002',37.19,0,3,0)", (api['rid'],))
        q.execute("INSERT INTO sale_order_item VALUES (%s,'1','00002',37.19,3)", (api['rid'],))
        q.execute("INSERT INTO sales_cube VALUES (%s,'2011-12-10','UK','00001',1,0,1,0)", (api['rid'],))
        q.execute("INSERT INTO sale_order VALUES (%s,'2','2011-12-10','UK','U',1)", (api['rid'],))
        q.execute("INSERT INTO sale_order_item VALUES (%s,'2','00001',1,1)", (api['rid'],))
    api['c'].commit()
    first = get(api, '/products', page_size=1)['data']
    second = get(api, '/products', page_size=1, page=2)['data']
    assert first['total'] == second['total'] == 2
    assert first['has_next'] is True and second['has_next'] is False
    assert first['items'][0]['stock_code'] == '00001'
    assert first['items'][0]['order_count'] == 2
    assert second['items'][0]['order_count'] == 1
    filtered = get(api, '/products', stock_code='00001', end_date='2011-12-09')['data']['items'][0]
    assert filtered['gross_amount'] == '37.19' and filtered['order_count'] == 1
    for endpoint in ('/products','/customers','/sales/countries','/releases','/runs','/experiments','/batches'):
        empty = get(api, endpoint, page=10**30)['data']
        assert empty['items'] == [] and empty['total'] > 0 and empty['has_next'] is False
    assert get(api, '/products', stock_code='absent')['data']['total'] == 0
    assert get(api, '/products', stock_code='00001 ')['data']['total'] == 0
    assert get(api, '/customers', customer_id='U ')['data']['total'] == 0
    assert get(api, '/customers/U%20/recommendations')['data']['known_customer'] is False
    error(api['client'], '/products/00001%20/similar', 404, 'RESOURCE_NOT_FOUND')


def test_rfm_all_segments_missing_categories_and_null_description(api):
    segments = get(api, '/customers/segments')['data']
    assert segments['as_of_date'] == '2011-12-10'
    assert [r['segment'] for r in segments['items']] == ['CORE','AT_RISK','NEW','POTENTIAL','OTHER']
    assert [r['customer_count'] for r in segments['items']] == [0,0,1,0,0]
    assert get(api, '/customers', segment='CORE')['data']['total'] == 0
    customer = get(api, '/customers', customer_id='U')['data']['items'][0]
    assert customer['monetary'] == '37.19' and customer['currency'] == 'GBP'
    product = get(api, '/products')['data']['items'][0]
    assert product['description'] is None and product['is_merchandise'] is True
    assert get(api, '/products/00001/similar')['data']['items'][0]['related_stock_code'] == '00002'
    assert get(api, '/products/00002/similar')['data']['items'] == []


def test_all_algorithms_unknown_fallback_full_history_and_no_candidates(api):
    for algorithm in ('POP','ITEMCF','ALS','NCF'):
        for customer in ('U','unknown'):
            result = get(api, f'/customers/{customer}/recommendations', algorithm=algorithm, k=1)['data']
            assert result['algorithm'] == algorithm and result['model_run_id'] == api['manifest']['models'][algorithm]
            assert result['returned_count'] == 1 and result['items'][0]['rank'] == 1
            assert result['known_customer'] is (customer == 'U')
            if customer == 'unknown':
                assert result['eligible_candidates'] == 2
                assert result['fallback_used'] is (algorithm != 'POP')
                assert result['items'][0]['reason_code'] == ('POPULAR' if algorithm == 'POP' else 'FALLBACK_POPULAR')
            else:
                assert result['eligible_candidates'] == 1 and result['items'][0]['stock_code'] == '00002'
    # Existing customer with every candidate already purchased: 200 and genuinely empty.
    with api['c'].cursor() as q:
        q.execute('INSERT INTO customer_seen_item VALUES (%s,%s,%s)', (api['rid'],'U','00002'))
        q.execute('DELETE FROM customer_recommendation WHERE release_id=%s', (api['rid'],))
    api['c'].commit()
    empty = get(api, '/customers/U/recommendations')['data']
    assert empty['eligible_candidates'] == empty['returned_count'] == 0
    assert empty['items'] == [] and empty['fallback_used'] is False
    with api['c'].cursor() as q:
        q.execute('DELETE FROM product_popularity WHERE release_id=%s', (api['rid'],))
        q.execute('UPDATE product_dim SET is_candidate=0 WHERE release_id=%s', (api['rid'],))
    api['c'].commit()
    empty = get(api, '/customers/unknown/recommendations')['data']
    assert empty['eligible_candidates'] == 0 and empty['fallback_used'] is False and empty['items'] == []


def test_release_states_no_current_and_control_independence(api):
    for status, http, code in [('BUILDING',409,'RELEASE_NOT_READY'),('FAILED',409,'RELEASE_NOT_READY'),('RETIRED',410,'RELEASE_RETIRED')]:
        with api['c'].cursor() as q:
            q.execute('UPDATE data_release SET status=%s WHERE release_id=%s', (status,api['rid']))
        api['c'].commit()
        error(api['client'], '/summary', http, code, params={'release_id':api['rid']})
        assert get(api, '/releases')['data']['total'] == 0
    with api['c'].cursor() as q:
        q.execute("UPDATE data_release SET status='READY' WHERE release_id=%s", (api['rid'],))
        q.execute('UPDATE current_release SET release_id=NULL')
    api['c'].commit()
    for path in ('/summary','/releases/current'):
        error(api['client'], path, 404, 'NO_ACTIVE_RELEASE')
    assert get(api, '/health/ready')['data']['status'] == 'READY'
    assert get(api, '/summary', release_id=api['rid'])['meta']['release_id'] == api['rid']
    for path in ('/batches','/runs','/experiments',f"/models/{api['ncf']}/epochs",f"/batches/{api['bid']}/quality"):
        get(api, path)
        error(api['client'], path, 422, 'INVALID_PARAMETER', params={'release_id':api['rid']})


def test_known_rfm_customer_without_model_history_uses_stored_fallback(api):
    with api['c'].cursor() as q:
        q.execute("INSERT INTO customer_rfm VALUES (%s,'N','2011-12-09','2011-12-10',1,1,4,5,1,1,'NEW')", (api['rid'],))
        for algorithm in ('POP','ITEMCF','ALS','NCF'):
            fallback = algorithm != 'POP'
            for rank,code,score in [(1,'00001',2),(2,'00002',1)]:
                q.execute('INSERT INTO customer_recommendation VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                    (api['rid'],algorithm,'N',code,rank,score,'FALLBACK_POPULAR' if fallback else 'POPULAR',fallback))
    api['c'].commit()
    for algorithm in ('POP','ITEMCF','ALS','NCF'):
        result = get(api, '/customers/N/recommendations', algorithm=algorithm)['data']
        assert result['known_customer'] is True and result['eligible_candidates'] == 2
        assert result['returned_count'] == 2 and result['fallback_used'] is (algorithm != 'POP')
        assert result['model_run_id'] == api['manifest']['models'][algorithm]


def test_release_fixed_during_current_switch(api, monkeypatch):
    new = another(api['manifest'])
    install(api['c'], new, api['directory'])
    with api['c'].cursor() as q:
        q.execute('UPDATE sales_cube SET gross_amount=99 WHERE release_id=%s', (new['release_id'],))
        q.execute('UPDATE current_release SET release_id=%s', (api['rid'],))
    api['c'].commit()
    original = Repository.resolve
    calls = []
    def switch(repo, release_id=None):
        result = original(repo, release_id)
        calls.append(result['release_id'])
        with api['c'].cursor() as q:
            q.execute('UPDATE current_release SET release_id=%s', (new['release_id'],))
        api['c'].commit()
        return result
    monkeypatch.setattr(Repository, 'resolve', switch)
    first = get(api, '/summary')
    assert calls == [api['rid']] and first['data']['gross_amount'] == '37.19'
    assert first['meta']['release_id'] == api['rid']
    assert get(api, '/summary')['data']['gross_amount'] == '99.00'
    assert get(api, '/summary', release_id=api['rid'])['data']['gross_amount'] == '37.19'


def test_selected_eval_metrics_epochs_and_pending_results(api):
    for split in ('VALIDATION','TEST'):
        for cohort in ('WARM','ALL_ELIGIBLE'):
            result = get(api, f"/experiments/{api['eid']}/metrics", split=split, cohort=cohort)['data']
            assert [r['algorithm'] for r in result['items']] == ['POP','ITEMCF','ALS','NCF']
            assert result['k'] == 10 and result['candidate_policy_version'] == 'full-catalog-v1'
            assert all(r['model_run_id'] not in api['manifest']['models'].values() for r in result['items'])
            assert all(isinstance(r['ndcg_at_k'], float) and isinstance(r['user_count'], int) for r in result['items'])
    epochs = get(api, f"/models/{api['ncf']}/epochs")['data']
    assert epochs['purpose'] == 'EVAL' and [r['epoch'] for r in epochs['items']] == [1,2]
    mid = api['manifest']['models']['POP']
    assert get(api, f'/models/{mid}/epochs')['data']['items'] == []
    mid = api['manifest']['models']['NCF']
    with api['c'].cursor() as q:
        q.execute('INSERT INTO model_epoch VALUES (%s,1,0.6,1,NULL,NULL,128)', (mid,))
        q.execute('UPDATE experiment SET test_sealed_at=NULL WHERE experiment_id=%s', (api['eid'],))
    api['c'].commit()
    assert get(api, f'/models/{mid}/epochs')['data']['items'][0]['validation_ndcg'] is None
    error(api['client'], f"/experiments/{api['eid']}/metrics", 409, 'RESULT_NOT_READY')
    assert len(get(api, f"/experiments/{api['eid']}/metrics", split='VALIDATION')['data']['items']) == 4
    with api['c'].cursor() as q:
        q.execute("DELETE FROM model_metric WHERE model_run_id=%s AND split_name='VALIDATION'", (api['ncf'],))
    api['c'].commit()
    error(api['client'], f"/experiments/{api['eid']}/metrics", 409, 'RESULT_NOT_READY', params={'split':'VALIDATION'})


def test_benchmark_summary_complete_incomplete_and_inconsistent_hash(api):
    path = f"/experiments/{api['eid']}/benchmarks"
    with api['c'].cursor() as q:
        q.execute("UPDATE gpu_benchmark SET train_seconds=IF(device='CPU',10+repeat_index,20+repeat_index)")
    api['c'].commit()
    summary = get(api, path)['data']['summary']
    assert summary == dict(cpu_median_seconds=12.0,gpu_median_seconds=22.0,speedup=12/22,complete_repeats=True,comparison_valid=True)
    with api['c'].cursor() as q:
        q.execute("UPDATE gpu_benchmark SET dataset_sha256=%s WHERE device='CUDA' AND repeat_index=1", ('c'*64,))
    api['c'].commit()
    summary = get(api, path)['data']['summary']
    assert summary['complete_repeats'] is True and summary['comparison_valid'] is False
    assert summary['speedup'] is summary['cpu_median_seconds'] is summary['gpu_median_seconds'] is None
    with api['c'].cursor() as q:
        q.execute("DELETE FROM gpu_benchmark WHERE device='CUDA' AND repeat_index=1")
    api['c'].commit()
    summary = get(api, path)['data']['summary']
    assert summary['complete_repeats'] is False and summary['comparison_valid'] is False
    assert summary['speedup'] is None
    with api['c'].cursor() as q:
        q.execute('DELETE FROM gpu_benchmark')
    api['c'].commit()
    error(api['client'], path, 409, 'RESULT_NOT_READY')


def test_missing_resources_and_failed_quality_are_queryable(api):
    for path in ('/products/absent/similar',f'/runs/{uuid4()}',f'/models/{uuid4()}/epochs',
                 f'/experiments/{uuid4()}/metrics', f'/experiments/{uuid4()}/benchmarks',
                 '/batches/B20260101000000abcdef01/quality'):
        error(api['client'], path, 404, 'RESOURCE_NOT_FOUND')
    error(api['client'], '/summary', 404, 'RESOURCE_NOT_FOUND', params={'release_id':'R20260101000000abcdef01'})
    with api['c'].cursor() as q:
        q.execute("UPDATE data_batch SET status='FAILED' WHERE batch_id=%s", (api['bid'],))
        q.execute("UPDATE task_run SET error_code='WORKER_FAILED',error_message='SELECT password=SECRET FROM mysql.user',status='FAILED' WHERE run_id=%s", (api['run'],))
    api['c'].commit()
    quality = get(api, f"/batches/{api['bid']}/quality")['data']
    assert quality['status'] == 'FAILED' and quality['metrics'][0]['metric_value'] == 2
    task = get(api, f"/runs/{api['run']}")['data']
    assert task['error_code'] == 'WORKER_FAILED' and task['error_message']
    assert 'SECRET' not in json.dumps(task) and 'SELECT' not in json.dumps(task)
    assert get(api, '/runs', status='FAILED', batch_id=api['bid'])['data']['total'] == 1


def test_sql_injection_read_only_permissions_and_no_write_routes(api, caplog):
    injection = "' OR 1=1 --"
    assert get(api, '/summary', country=injection)['data']['gross_amount'] == '0.00'
    assert get(api, '/customers', customer_id=injection)['data']['total'] == 0
    assert get(api, '/products', stock_code=injection)['data']['total'] == 0
    assert get(api, '/customers/'+injection+'/recommendations')['data']['known_customer'] is False
    for path in ('/summary','/runs','/releases/current','/upload','/train','/publish','/sql'):
        for method in ('post','put','patch','delete'):
            result = api['client'].request(method.upper(), PREFIX+path, json={'sql':'DROP TABLE product_dim'})
            assert result.status_code in (404,405)
            assert result.headers.get('x-request-id') == result.json()['meta']['request_id']
    for path in ('/runs/%2e%2e%2fetc%2fpasswd','/products/a%5Cb/similar','/customers/%00/recommendations'):
        error(api['client'], path, 422, 'INVALID_PARAMETER')
    connection = api['factory']()
    try:
        for sql in ("UPDATE current_release SET release_id=NULL", "DELETE FROM customer_rfm", "CREATE TABLE should_fail (id INT)"):
            with pytest.raises(pymysql.MySQLError):
                with connection.cursor() as q:
                    q.execute(sql)
    finally:
        connection.close()
    assert 'SECRET' not in caplog.text


def test_schema_mismatch_non_reader_and_finite_response_enforcement(api, monkeypatch, caplog):
    with api['c'].cursor() as q:
        q.execute("UPDATE schema_migration SET checksum=%s", ('0'*64,))
    api['c'].commit()
    payload = error(api['client'], '/health/ready', 503, 'DEPENDENCY_UNAVAILABLE')
    assert payload['data']['status'] == 'NOT_READY'
    assert get(api, '/health/live')['data']['status'] == 'UP'
    for role in ('writer','migration'):
        # Real role check, before starting a read-only transaction.
        with TestClient(create_app(connection_factory=lambda: api['factory_for'](role))) as client:
            error(client, '/health/ready', 503, 'DEPENDENCY_UNAVAILABLE')
    def explode(repo, query):
        raise RuntimeError('password=SECRET; SELECT * FROM mysql.user')
    monkeypatch.setattr(Repository, 'summary', explode)
    # Restore migration to reach the handler; no exception text may reach body/log.
    from orchestration.migrations import catalog
    with api['c'].cursor() as q:
        q.execute('UPDATE schema_migration SET checksum=%s', (catalog()[0]['checksum'],))
    api['c'].commit()
    body = error(api['client'], '/summary', 500, 'INTERNAL_ERROR')
    assert 'SECRET' not in json.dumps(body) and 'SECRET' not in caplog.text and 'SELECT' not in caplog.text
    monkeypatch.setattr(Repository, 'summary', lambda repo, query: dict(currency='GBP',gross_amount='NaN'))
    error(api['client'], '/summary', 500, 'INTERNAL_ERROR')


def test_sql_timeout_and_transaction_read_only(api):
    with ReadSession(api['factory'], time.monotonic()+8) as session:
        assert session.one('SELECT @@max_statement_time t')['t'] <= 2
        with pytest.raises(pymysql.MySQLError):
            session.rows('UPDATE current_release SET release_id=NULL')
        started = time.monotonic()
        with pytest.raises(pymysql.MySQLError):
            session.rows('SELECT SLEEP(4)')
        assert 1.5 < time.monotonic()-started < 3.5


def test_day_limit_uses_intersection_and_month_gaps(api):
    with api['c'].cursor() as q:
        q.execute("UPDATE data_batch SET min_source_date='2010-01-01',max_source_date='2011-12-10' WHERE batch_id=%s", (api['bid'],))
    api['c'].commit()
    error(api['client'], '/sales/trend', 422, 'INVALID_PARAMETER')
    assert len(get(api, '/sales/trend', start_date='2011-12-08', end_date='9999-12-31')['data']['points']) == 3
    months = get(api, '/sales/trend', granularity='MONTH')['data']['points']
    assert len(months) == 24 and months[0]['gross_amount'] == '0.00'
    assert months[-1]['gross_amount'] == '37.19'


def test_hand_calculated_sales_orders_customers_and_product_ranking(api):
    """The 11-source-row example in 11, seeded only into an isolated API DB.

    This is a query fixture, not a publishable batch: its rejection ratio exceeds 5%.
    It must never be passed off as a successful real-data release or model result.
    """
    rid = api['rid']
    with api['c'].cursor() as q:
        for table in ('customer_recommendation','item_similarity','customer_seen_item','product_popularity',
                      'customer_rfm','sale_order_item','sale_order','sales_cube','product_dim'):
            q.execute('DELETE FROM '+table+' WHERE release_id=%s', (rid,))
        q.execute("UPDATE data_batch SET min_source_date='2011-08-01',max_source_date='2011-08-06' WHERE batch_id=%s", (api['bid'],))
        for code in ('10001','10002','10003','10004'):
            q.execute('INSERT INTO product_dim VALUES (%s,%s,NULL,1,%s)', (rid, code, int(code != '10004')))
        lines = [
            ('2011-08-01','10001',2,0,2,0),('2011-08-01','10002',3,0,1,0),
            ('2011-08-02','10001',1,0,1,0),('2011-08-02','10003',4,0,2,0),
            ('2011-08-03','10001',1,0,1,0),('2011-08-03','10002',3,0,1,0),
            ('2011-08-04','10002',0,3,0,1),('2011-08-05','10004',4,0,1,0),
            ('2011-08-06','10003',2,0,1,0),('2011-08-06','10001',1,0,1,0),
        ]
        for day,code,gross,returns,sq,rq in lines:
            q.execute("INSERT INTO sales_cube VALUES (%s,%s,'UK',%s,%s,%s,%s,%s)", (rid,day,code,gross,returns,sq,rq))
        orders = [('100001','2011-08-01','1',5,[('10001',2,2),('10002',3,1)]),
                  ('100002','2011-08-02','1',5,[('10001',1,1),('10003',4,2)]),
                  ('100003','2011-08-03','2',4,[('10001',1,1),('10002',3,1)]),
                  ('100005','2011-08-05',None,4,[('10004',4,1)]),
                  ('100007','2011-08-06','2',2,[('10003',2,1)]),
                  ('100008','2011-08-06','3',1,[('10001',1,1)])]
        for invoice,day,user,gross,items in orders:
            q.execute("INSERT INTO sale_order VALUES (%s,%s,%s,'UK',%s,%s)", (rid,invoice,day,user,gross))
            for code,amount,quantity in items:
                q.execute('INSERT INTO sale_order_item VALUES (%s,%s,%s,%s,%s)', (rid,invoice,code,amount,quantity))
    api['c'].commit()
    summary = get(api, '/summary')['data']
    assert summary == dict(currency='GBP',gross_amount='21.00',return_amount='3.00',net_amount='18.00',
        order_count=6,customer_count=3,average_order_value='3.50',sale_quantity=11,return_quantity=1)
    products = get(api, '/products')['data']['items']
    assert [r['stock_code'] for r in products] == ['10002','10003','10001','10004']
    assert [r['gross_amount'] for r in products] == ['6.00','6.00','5.00','4.00']
    assert [r['order_count'] for r in products] == [2,2,4,1]
    only_returns = get(api, '/summary',start_date='2011-08-04',end_date='2011-08-04')['data']
    assert only_returns['net_amount'] == '-3.00' and only_returns['order_count'] == 0
    assert only_returns['average_order_value'] is None
    countries = get(api, '/sales/countries',start_date='2011-08-04',end_date='2011-08-04')['data']['items']
    assert len(countries) == 1 and countries[0]['order_count'] == 0 and countries[0]['net_amount'] == '-3.00'
    points = get(api, '/sales/trend')['data']['points']
    assert [r['order_count'] for r in points] == [1,1,1,0,1,2]
    assert points[3]['net_amount'] == '-3.00'
    assert get(api, '/products/10001/similar')['data']['items'] == []


INVALID_QUERIES = [('/summary', {'unexpected':'x'}),('/summary', {'start_date':'no'}),
    ('/summary', {'start_date':'0'}),('/summary', {'end_date':'2011-12-09T00:00:00'}),
    ('/summary', {'start_date':'2011-12-10','end_date':'2011-12-01'}),('/summary', {'release_id':'../x'}),
    ('/products', {'sort':'gross_amount; DROP TABLE x'}),('/products', {'page':0}),('/products', {'page_size':101}),
    ('/products', {'stock_code':'x'*33}),('/products', {'stock_code':'a/b'}),('/customers', {'segment':'INVALID'}),
    ('/sales/trend', {'granularity':'YEAR'}),('/sales/countries', {'sort':'QUANTITY_DESC'}),
    ('/customers/U/recommendations', {'algorithm':'UNKNOWN'}),('/customers/U/recommendations', {'k':11}),
    ('/products/00001/similar', {'k':0}),('/runs', {'status':'READY'}),('/runs', {'batch_id':'invalid'}),
    ('/batches', {'release_id':'R20260101000000abcdef01'}),('/health/live', {'x':'1'}),('/health/ready', {'release_id':'x'}),
    ('/releases/current', {'release_id':'R20260101000000abcdef01'}),
    ('/experiments/00000000-0000-0000-0000-000000000001/metrics', {'cohort':'unknown'}),
    ('/experiments/00000000-0000-0000-0000-000000000001/metrics', {'split':'TRAIN'}),
    ('/runs/not-a-uuid', {}),('/batches/invalid/quality', {}),
    ('/customers/'+'x'*33+'/recommendations', {}),
]


@pytest.mark.parametrize('path,params', INVALID_QUERIES)
def test_invalid_parameters_rejected_before_database(path, params):
    def forbidden():
        pytest.fail('Invalid request reached the database')
    with TestClient(create_app(connection_factory=forbidden)) as client:
        error(client, path, 422, 'INVALID_PARAMETER', params=params)


def test_request_body_duplicates_unavailable_and_total_timeout(caplog):
    def unavailable_factory():
        raise pymysql.OperationalError(2003, 'password=SECRET')
    with TestClient(create_app(connection_factory=unavailable_factory)) as client:
        assert client.get(PREFIX+'/health/live').status_code == 200
        for path in ('/summary','/health/ready'):
            error(client, path, 503, 'DEPENDENCY_UNAVAILABLE')
        error(client, '/summary?page=1&page=2', 422, 'INVALID_PARAMETER')
        error(client, '/products?page=1&page=2', 422, 'INVALID_PARAMETER')
        result = client.request('GET', PREFIX+'/summary', json={'country':'UK'})
        assert result.status_code == 422
    assert 'SECRET' not in caplog.text
    def slow():
        time.sleep(0.15)
        raise pymysql.OperationalError(2003, 'password=SECRET')
    application = create_app(connection_factory=slow)
    # Exercise the same production timeout middleware with a short test budget.
    application.user_middleware[0].kwargs['timeout'] = 0.05
    with TestClient(application) as client:
        start = time.monotonic()
        error(client, '/summary', 503, 'DEPENDENCY_UNAVAILABLE')
        assert time.monotonic()-start < 0.13
