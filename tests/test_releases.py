"""T14/T19 against real isolated MariaDB; fixtures are not formal model evidence."""
from copy import deepcopy
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
from uuid import uuid4

import pytest

from etl.ingest import sha256
from ml.data import write_tsv
from orchestration.control import ControlStore
from orchestration.database import named_lock, LockBusy, utcnow
from orchestration.manifests import ManifestError
from orchestration.publication import publish
from orchestration.recovery import verify_backup, recovery_root, physical_backup_gate
from orchestration.release_data import TABLES, normalize, compare_database
from orchestration.releases import (import_bundle, current, verify_local, rollback_local,
                                     existing_release, verify_bindings)

H = 'a'*64
C = 'b'*40


@pytest.fixture
def release_fixture(db, tmp_path):
    connection = db[0]['migration']
    store = ControlStore(connection)
    bid = store.register_batch(source_sha256=H, source_name='fixture.xlsx', source_url='https://example.invalid/fixture',
        source_bytes=100, raw_hdfs_path='/retail/v1/raw/batch_id={batch_id}/source.xlsx',
        normalized_hdfs_path='/retail/v1/normalized/batch_id={batch_id}')['batch_id']
    eid = store.create_experiment(bid, H)
    with connection.cursor() as q:
        q.execute("UPDATE data_batch SET status='SUCCEEDED',raw_rows=2,sale_rows=1,return_rows=1,rejected_rows=0 WHERE batch_id=%s", (bid,))
        q.execute("UPDATE experiment SET status='SUCCEEDED',champion_algorithm='ALS',test_sealed_at=%s WHERE experiment_id=%s", (utcnow(), eid))
        models = {}
        for purpose in ('EVAL', 'FULLFIT'):
            for algorithm in ('POP', 'ITEMCF', 'ALS', 'NCF'):
                run, mid = str(uuid4()), str(uuid4())
                q.execute('''INSERT INTO task_run (run_id,batch_id,task_name,attempt,status,config_sha256,code_commit,baseline_version,created_at)
                    VALUES (%s,%s,%s,1,'SUCCEEDED',%s,%s,'HL-DOC-1.0.0',%s)''', (run, bid, purpose+algorithm, H, C, utcnow()))
                q.execute('''INSERT INTO model_run (model_run_id,batch_id,experiment_id,run_id,algorithm,purpose,device,status,
                    config_sha256,mapping_sha256,artifact_path,code_commit,environment_sha256,baseline_version,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'SUCCEEDED',%s,%s,%s,%s,%s,'HL-DOC-1.0.0',%s)''',
                    (mid, bid, eid, run, algorithm, purpose, 'CUDA' if algorithm == 'NCF' else 'CPU', H, H,
                     '/retail/v1/models/model_run_id='+mid, C, H, utcnow()))
                if purpose == 'FULLFIT':
                    models[algorithm] = mid
                    continue
                q.execute('INSERT INTO experiment_model VALUES (%s,%s,%s,%s)', (eid, algorithm, mid, utcnow()))
                for split in ('VALIDATION', 'TEST'):
                    for cohort in ('WARM', 'ALL_ELIGIBLE'):
                        q.execute('''INSERT INTO model_metric VALUES
                            (%s,%s,%s,10,1,2,1,0,0,0,0,0,0,0,0,'full-catalog-v1',%s)''', (mid, split, cohort, utcnow()))
                if algorithm == 'NCF':
                    for device in ('CPU', 'CUDA'):
                        for repeat in (1, 2, 3):
                            q.execute('''INSERT INTO gpu_benchmark (benchmark_id,model_run_id,device,repeat_index,seed,
                                batch_size,epochs,warmup_steps,train_seconds,samples_per_second,hardware,torch_version,
                                dataset_sha256,config_sha256,created_at) VALUES (%s,%s,%s,%s,42,1024,5,20,1,1,'fixture','fixture',%s,%s,%s)''',
                                (str(uuid4()), mid, device, repeat, H, H, utcnow()))
    connection.commit()
    rows = {
        'product_dim': [('00001', None, 1, 1), ('00002', 'item', 1, 1)],
        'sales_cube': [('2011-12-09', 'UK', '00001', '37.19', '2.17', 3, 1)],
        'sale_order': [('1', '2011-12-09', 'UK', 'U', '37.19')],
        'sale_order_item': [('1', '00001', '37.19', 3)],
        'customer_rfm': [('U', '2011-12-09', '2011-12-10', 1, 1, '37.19', 5, 1, 1, 'NEW')],
        'customer_seen_item': [('U', '00001')],
        'item_similarity': [('00001', '00002', '.12345678912345678', '.25', '1.5', 3, 1)],
        'product_popularity': [('00001', 2, 1), ('00002', 1, 2)],
        'customer_recommendation': [(a, 'U', '00002', 1, '.98765432112345678', reason, 0)
            for a, reason in [('ALS', 'LATENT_FACTOR'), ('ITEMCF', 'CO_PURCHASE'), ('NCF', 'NEURAL_CF'), ('POP', 'POPULAR')]],
    }
    tables = {}
    for name in TABLES:
        path = tmp_path/(name+'.tsv')
        ref = write_tsv(path, (normalize(name, row) for row in rows[name]))
        tables[name] = ref | {'file': path.name, 'columns': TABLES[name].columns.split(','), 'bytes': path.stat().st_size}
    manifest = {'schema_version': '1.0', 'baseline_version': 'HL-DOC-1.0.0',
                'release_id': 'R20261021000000'+uuid4().hex[:8], 'batch_id': bid, 'experiment_id': eid,
                'champion_algorithm': 'ALS', 'mapping_sha256': H, 'source_sha256': H,
                'code_dirty': False, 'synthetic': False, 'models': models, 'tables': tables,
                'audit': {'gross_pence': 3719, 'return_pence': 217, 'sale_quantity': 3, 'return_quantity': 1}}
    return connection, manifest, tmp_path


def install(connection, manifest, directory, **kwargs):
    path = directory/(manifest['release_id']+'.json')
    path.write_text(json.dumps(manifest, sort_keys=True))
    return import_bundle(connection, manifest, directory, '/retail/v1/releases/release_id='+manifest['release_id']+'/release.json',
                         sha256(path), kwargs.pop('expected_current', current(connection)), **kwargs), sha256(path)


def another(manifest):
    return deepcopy(manifest) | {'release_id': 'R20261021000000'+uuid4().hex[:8]}


def test_publish_exact_rows_idempotency_and_readonly(db, release_fixture):
    c, m, directory = release_fixture
    with named_lock(c):
        result, digest = install(c, m, directory)
    assert result['status'] == 'PASS' and current(c) == m['release_id']
    assert existing_release(c, m['batch_id'], m['experiment_id'], m['models']) == (m['release_id'], 'READY')
    for _ in range(2):
        assert verify_local(db[0]['reader'], m['release_id'], m, directory, digest)['tables'] == result['tables']
    with c.cursor() as q:
        q.execute('SELECT COUNT(*) n FROM release_event'); assert q.fetchone()['n'] == 1
        q.execute('SELECT COUNT(*) n FROM customer_recommendation'); assert q.fetchone()['n'] == 4
    c.rollback()


@pytest.mark.parametrize('stage', ['table:product_dim', 'table:sale_order_item', 'verified', 'switch'])
def test_interrupted_import_and_switch_leave_old_current(release_fixture, stage):
    c, old, directory = release_fixture
    with named_lock(c):
        _, old_hash = install(c, old, directory)
        new = another(old)
        def fail(actual):
            if actual == stage:
                raise RuntimeError('Injected publication interruption')
        with pytest.raises(RuntimeError):
            install(c, new, directory, fault=fail)
        assert current(c) == old['release_id']
        verify_local(c, old['release_id'], old, directory, old_hash)
        with c.cursor() as q:
            q.execute('SELECT status FROM data_release WHERE release_id=%s', (new['release_id'],))
            assert q.fetchone()['status'] == 'FAILED'
            q.execute('SELECT COUNT(*) n FROM release_event'); assert q.fetchone()['n'] == 1
        c.rollback()
        retry = another(old)
        install(c, retry, directory)
        assert current(c) == retry['release_id']
        assert compare_database(c, retry['release_id'], retry['tables'])['customer_recommendation']['rows'] == 4
        c.rollback()


def test_concurrent_publisher_lock_and_stale_pointer_conflict(db, release_fixture):
    c, m, directory = release_fixture
    with named_lock(c):
        with pytest.raises(LockBusy):
            publish(db[0]['writer'], {}, m['batch_id'], m['experiment_id'], list(m['models'].values()))
        install(c, m, directory)
        with pytest.raises(LockBusy):
            install(c, another(m), directory, expected_current=None)
        assert current(c) == m['release_id']


def test_rollback_verifies_target_and_preserves_both_versions(db, release_fixture):
    c, old, directory = release_fixture
    with named_lock(c):
        _, digest = install(c, old, directory)
        new = another(old)
        install(c, new, directory)
        assert rollback_local(c, old['release_id'], old, directory, digest, '测试旧版本恢复', expected_current=new['release_id'])['previous_release_id'] == new['release_id']
        assert current(c) == old['release_id']
        compare_database(db[0]['reader'], new['release_id'], new['tables'])
        db[0]['reader'].rollback()
        with c.cursor() as q:
            q.execute("SELECT * FROM release_event WHERE action='ROLLBACK'")
            row = q.fetchone()
            assert row['reason'] == '测试旧版本恢复' and row['from_release_id'] == new['release_id'] and row['to_release_id'] == old['release_id']
        c.rollback()
        with pytest.raises(ManifestError):
            rollback_local(c, new['release_id'], new, directory, 'f'*64, '坏清单', expected_current=old['release_id'])
        assert current(c) == old['release_id']


def test_wrong_batch_models_missing_gpu_and_changed_ready_rows_rejected(release_fixture):
    c, m, directory = release_fixture
    broken = deepcopy(m) | {'mapping_sha256': 'f'*64}
    with pytest.raises(ManifestError):
        install(c, broken, directory)
    with c.cursor() as q:
        q.execute('DELETE FROM gpu_benchmark WHERE repeat_index=3')
    c.commit()
    with pytest.raises(ManifestError):
        verify_bindings(c, m)
    assert current(c) is None


@pytest.mark.parametrize('change', ['batch', 'device', 'metrics'])
def test_control_gates_do_not_accept_cross_batch_cpu_ncf_or_missing_metrics(release_fixture, change):
    c, m, directory = release_fixture
    with c.cursor() as q:
        if change == 'batch':
            q.execute('''INSERT INTO data_batch SELECT %s, %s, source_name,source_url,source_bytes,policy_version,
                baseline_version,schema_version,status,raw_hdfs_path,normalized_hdfs_path,created_at,updated_at,
                min_source_date,max_source_date,raw_rows,sale_rows,return_rows,rejected_rows,quality_report_path,error_code,error_message
                FROM data_batch WHERE batch_id=%s''', ('B20261021000000abcdef12', 'e'*64, m['batch_id']))
            q.execute('UPDATE model_run SET batch_id=%s WHERE model_run_id=%s', ('B20261021000000abcdef12', m['models']['POP']))
        elif change == 'device':
            q.execute("UPDATE model_run SET device='CPU' WHERE model_run_id=%s", (m['models']['NCF'],))
        else:
            q.execute("DELETE FROM model_metric WHERE split_name='TEST'")
    c.commit()
    with pytest.raises(ManifestError):
        install(c, m, directory)
    assert current(c) is None


def test_ready_import_cannot_be_overwritten_and_tampering_is_detected(release_fixture):
    import pymysql
    c, m, directory = release_fixture
    with named_lock(c):
        _, digest = install(c, m, directory)
        with pytest.raises(pymysql.IntegrityError):
            install(c, m, directory)
        assert current(c) == m['release_id']
        with c.cursor() as q:
            q.execute('UPDATE sale_order SET gross_amount=99 WHERE release_id=%s', (m['release_id'],))
        c.commit()
        with pytest.raises(ManifestError):
            verify_local(c, m['release_id'], m, directory, digest)


def test_tampered_export_and_nonfinite_scores_rejected(release_fixture):
    c, m, directory = release_fixture
    (directory/'sale_order.tsv').write_text('tampered\n')
    with pytest.raises(ManifestError):
        install(c, m, directory)
    assert current(c) is None
    with pytest.raises(ManifestError):
        normalize('customer_recommendation', ['POP', 'U', '00001', 1, 'nan', 'POPULAR', 0])
    assert normalize('sales_cube', [date(2011, 1, 1), 'UK', '00001', Decimal('999999999999999999.99'), '0', 1, 0])[3] == '999999999999999999.99'


def test_real_dump_restore_all_business_rows_and_audit(release_fixture, mariadb_instance):
    from orchestration.db_backup import backup_database
    from orchestration.database import open_connection
    from orchestration.migrations import require_current
    import getpass
    import subprocess
    c, m, directory = release_fixture
    with named_lock(c):
        _, digest = install(c, m, directory)
    dump = backup_database(c, directory/'backup')
    admin, socket, _ = mariadb_instance
    name = 'restored_'+uuid4().hex[:10]
    with admin.cursor() as q:
        q.execute('CREATE DATABASE `'+name+'` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
    with Path(dump['path']).open('rb') as stream:
        subprocess.run(['mariadb', '--no-defaults', '--protocol=socket', '--socket='+socket,
                        '--user='+getpass.getuser(), name], stdin=stream, check=True, capture_output=True, timeout=30)
    restored = open_connection(name=name, user=getpass.getuser(), unix_socket=socket)
    try:
        require_current(restored)
        assert current(restored) == m['release_id']
        report = verify_local(restored, m['release_id'], m, directory, digest)
        assert all(report['tables'][name]['sha256'] == ref['sha256'] for name, ref in m['tables'].items())
        with restored.cursor() as q:
            q.execute('SELECT COUNT(*) n FROM release_event')
            assert q.fetchone()['n'] == 1
    finally:
        restored.close()


def test_recovery_guards_and_archive_checksums(tmp_path):
    config = {'hdfs': {'root': '/retail/v1'}}
    for root in ['/retail/v1', '/retail/v1/recovery/a', '/retail/recovery/../v1', '/']:
        with pytest.raises(ManifestError):
            recovery_root(config, root)
    assert recovery_root(config, '/retail/recovery/test') == '/retail/recovery/test'
    assert not physical_backup_gate(tmp_path)['passed']
    dump = tmp_path/'dump.sql'
    dump.write_text('fixture')
    (tmp_path/'backup.json').write_text(json.dumps({'schema_version': '1.0', 'status': 'PASS', 'database_dump': 'dump.sql',
        'files': [{'file': 'dump.sql', 'sha256': sha256(dump), 'bytes': dump.stat().st_size}]}))
    verify_backup(tmp_path)
    dump.write_text('changed')
    with pytest.raises(ManifestError):
        verify_backup(tmp_path)


@pytest.mark.parametrize('published', [False, True])
def test_receipt_error_after_commit_keeps_task_recoverable(release_fixture, published):
    from orchestration.publication import fail_unpublished_task
    from orchestration.runtime import process_start
    from orchestration.manifests import LocalStore
    from test_database import run_manifest
    import os
    c, m, directory = release_fixture
    store = ControlStore(c)
    run = store.create_task(m['batch_id'], 'publish_receipt_fixture', H, C, experiment_id=m['experiment_id'])
    store.start_task(run, pid=os.getpid(), process_start_utc=process_start(os.getpid()))
    with named_lock(c):
        if published:
            install(c, m, directory)
        fail_unpublished_task(c, store, run, m['release_id'])
    assert store.task_status(run)['status'] == ('RUNNING' if published else 'FAILED')
    if published:
        assert current(c) == m['release_id']
        _, path = run_manifest(directory/'receipt', m['batch_id'], run)
        store.finish_task(run, 'SUCCEEDED', manifest_path=str(path), store=LocalStore(directory))
        assert store.task_status(run)['status'] == 'SUCCEEDED'
