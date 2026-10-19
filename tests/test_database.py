"""W02 verified against MariaDB 10.11, not SQLite or a DB mock."""
from pathlib import Path
from uuid import uuid4
import json
import hashlib
import os
import subprocess
import pytest
import pymysql
from orchestration.control import ControlStore, ControlError
from orchestration.database import named_lock, LockBusy, utcnow, open_connection
from orchestration.db_backup import backup_database
from orchestration.manifests import LocalStore, DATASETS, verify_manifest, ManifestError
from orchestration.migrations import migrate, inspect, MigrationError, require_current

H='a'*64
C='b'*40


def batch(store):
    return store.register_batch(source_sha256=H,source_name='fixture.xlsx',source_url='https://example.invalid/fixture',
        source_bytes=100,raw_hdfs_path='/retail/v1/raw/batch_id={batch_id}/source.xlsx',
        normalized_hdfs_path='/retail/v1/normalized/batch_id={batch_id}')['batch_id']


def running(db):
    connection=db[0]['writer']; store=ControlStore(connection)
    bid=batch(store); store.transition_batch(bid,'PROCESSING')
    rid=store.create_task(bid,'normalize',H,C); store.start_task(rid)
    return store,bid,rid


def run_manifest(directory,bid,rid):
    directory.mkdir(parents=True,exist_ok=True)
    source=directory/'source.tsv'; source.write_bytes(b'input\n')
    output=directory/'part-00000'; output.write_bytes(b'output\n')
    marker=directory/'_SUCCESS'; marker.touch()
    def item(path): return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'rows':1}
    data=dict(schema_version='1.0',baseline_version='HL-DOC-1.0.0',batch_id=bid,run_id=rid,
        status='SUCCEEDED',code_commit=C,config_sha256=H,created_at='2026-10-20T00:00:00Z',
        finished_at='2026-10-20T00:00:01Z',dataset='normalized',schema=DATASETS['normalized'].split(','),
        inputs=[item(source)],outputs=[item(output)],record_count=1,classification_counts={},
        application_id=None,success_marker=str(marker))
    manifest=directory/'manifest.json'; manifest.write_text(json.dumps(data))
    return data,manifest


def test_schema_and_permissions(db):
    connections,name,credentials,directory=db
    migration=connections['migration']; writer=connections['writer']; reader=connections['reader']
    require_current(migration)
    with migration.cursor() as cursor:
        cursor.execute('SELECT VERSION() AS v')
        assert cursor.fetchone()['v'].startswith('10.11.')
        cursor.execute('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s',(name,))
        assert cursor.fetchone()['n']==23
        cursor.execute('SELECT * FROM current_release')
        assert cursor.fetchone()['release_id'] is None
        cursor.execute('SELECT DISTINCT DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=%s',(name,))
        assert {row['DELETE_RULE'] for row in cursor.fetchall()}=={'RESTRICT'}
        cursor.execute("SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND COLUMN_NAME='stock_code'",(name,))
        assert {r['COLLATION_NAME'] for r in cursor.fetchall()}=={'utf8mb4_bin'}
    with reader.cursor() as cursor:
        cursor.execute('SELECT run_id,error_code,error_message FROM task_run')
        for sql in ("UPDATE current_release SET release_id=NULL",'CREATE TABLE forbidden (id INT)',
                    'SELECT source_url FROM data_batch','SELECT log_path FROM task_run','SELECT pid FROM task_run'):
            with pytest.raises(pymysql.MySQLError): cursor.execute(sql)
    with writer.cursor() as cursor:
        for sql in ('CREATE TABLE forbidden (id INT)','DELETE FROM schema_migration',
                    f"GRANT SELECT ON `{name}`.* TO 'nobody'@'localhost'"):
            with pytest.raises(pymysql.MySQLError): cursor.execute(sql)
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in credentials.glob('*.env'))
    assert list((directory/'backups').glob('*.sql'))


def test_real_constraints_and_binary_identifiers(db):
    migration=db[0]['migration']; store=ControlStore(db[0]['writer']); bid=batch(store)
    with migration.cursor() as cursor:
        for sql,args in [
            ('INSERT INTO current_release VALUES (2,NULL,%s)',(utcnow(),)),
            ("UPDATE data_batch SET status='UNKNOWN' WHERE batch_id=%s",(bid,)),
            ('UPDATE data_batch SET source_bytes=-1 WHERE batch_id=%s',(bid,)),
            ('UPDATE current_release SET release_id=%s',('R20261020000000abcdef12',)),
            ('INSERT INTO data_quality VALUES (%s,%s,%s,%s,%s)',(bid,'RAW_ROWS',-1,'invalid',utcnow())),
        ]:
            with pytest.raises(pymysql.MySQLError) as rejected: cursor.execute(sql,args)
            assert rejected.value.args[0] in (4025,1452)
        cursor.execute('INSERT INTO data_quality VALUES (%s,%s,1,%s,%s)',(bid,'CASE','one',utcnow()))
        cursor.execute('INSERT INTO data_quality VALUES (%s,%s,1,%s,%s)',(bid,'case','two',utcnow()))
        with pytest.raises(pymysql.IntegrityError): cursor.execute('INSERT INTO data_quality VALUES (%s,%s,1,%s,%s)',(bid,'CASE','dup',utcnow()))
        with pytest.raises(pymysql.IntegrityError): cursor.execute('DELETE FROM data_batch WHERE batch_id=%s',(bid,))
    migration.rollback()


def test_migration_backup_rollback_reapply_and_drift(db,tmp_path):
    connection=db[0]['migration']; directory=db[3]/uuid4().hex
    assert migrate(connection,lambda:pytest.fail('idempotent migration must not write'))==[]
    assert migrate(connection,lambda:pytest.fail('dry run must not write'),direction='down',dry_run=True)==['001']
    backup=lambda:backup_database(connection,directory)
    assert migrate(connection,backup,direction='down')==['001']
    with pytest.raises(MigrationError): require_current(connection)
    assert migrate(connection,backup)==['001']
    require_current(connection)
    # Both directions are protected by the persisted checksum.
    root=tmp_path/'migrations'; root.mkdir()
    from orchestration.migrations import ROOT
    for path in ROOT.glob('*.sql'): (root/path.name).write_bytes(path.read_bytes())
    with (root/'001_initial.down.sql').open('a') as f: f.write('\n-- changed\n')
    with pytest.raises(MigrationError): migrate(connection,backup,root=root)
    bid=batch(ControlStore(db[0]['writer']))
    with pytest.raises(MigrationError): migrate(connection,backup,direction='down')


def test_partial_schema_and_backup_failure_stop_ddl(mariadb_instance):
    admin,socket,directory=mariadb_instance
    name='partial_'+uuid4().hex[:10]
    with admin.cursor() as cursor: cursor.execute(f'CREATE DATABASE `{name}`')
    import getpass
    connection=open_connection(name=name,user=getpass.getuser(),unix_socket=socket)
    try:
        def failed_backup(): raise RuntimeError('backup unavailable')
        with pytest.raises(RuntimeError): migrate(connection,failed_backup)
        with connection.cursor() as cursor:
            cursor.execute('SHOW TABLES'); assert not cursor.fetchall()
            cursor.execute('CREATE TABLE task_run (run_id INT)')
        with pytest.raises(MigrationError): migrate(connection,lambda:None)
    finally: connection.close()


def test_batch_task_retry_heartbeat_and_manifest(db,tmp_path):
    store,bid,rid=running(db)
    assert batch(store)==bid
    store.heartbeat(rid)
    with store.connection.cursor() as cursor:
        cursor.execute("UPDATE task_run SET heartbeat_at='2020-01-01' WHERE run_id=%s",(rid,))
    store.connection.commit()
    assert store.task_status(rid)['heartbeat_stale']
    assert store.task_status(rid)['status']=='RUNNING'
    with pytest.raises(ControlError): store.create_task(bid,'normalize',H,C)
    with pytest.raises(ControlError): store.finish_task(rid,'SUCCEEDED')
    store.finish_task(rid,'FAILED',error_code='TEST_FAILURE')
    with pytest.raises(ControlError): store.heartbeat(rid)
    retry=store.create_task(bid,'normalize',H,C)
    assert retry!=rid
    status=store.task_status(retry)
    assert status['attempt']==2 and status['parent_run_id']==rid
    store.start_task(retry)
    data,path=run_manifest(tmp_path,bid,retry)
    store.finish_task(retry,'SUCCEEDED',manifest_path=str(path),store=LocalStore(tmp_path))
    with pytest.raises(ControlError): store.finish_task(retry,'FAILED')
    with pytest.raises(ControlError): store.create_task(bid,'normalize',H,C)
    store.quality(bid,{'RAW_ROWS':1,'SALE_ROWS':1},counts=dict(raw_rows=1,sale_rows=1,return_rows=0,rejected_rows=0))
    store.transition_batch(bid,'SUCCEEDED')
    with pytest.raises(ControlError): store.quality(bid,{'RAW_ROWS':2})
    with pytest.raises(ControlError): store.transition_batch(bid,'PROCESSING')


def test_external_live_handle_blocks_terminal_and_retry(db):
    store,bid,rid=running(db)
    store.record_application(rid,'application_123_0001')
    with pytest.raises(ControlError): store.record_application(rid,'application_123_0002')
    with pytest.raises(ControlError): store.finish_task(rid,'FAILED')
    with pytest.raises(ControlError): store.finish_task(rid,'FAILED',terminal_check=lambda row:'RUNNING')
    assert store.task_status(rid)['status']=='RUNNING'
    store.finish_task(rid,'FAILED',terminal_check=lambda row:'FAILED',error_code='YARN_FAILED')
    assert store.create_task(bid,'normalize',H,C)


def test_named_lock_contention(db):
    writer=db[0]['writer']; migration=db[0]['migration']
    with named_lock(writer):
        with pytest.raises(LockBusy):
            with named_lock(migration): pass
    with named_lock(migration): pass


def test_experiment_idempotency_and_sealing_gate(db):
    store,bid,rid=running(db)
    eid=store.create_experiment(bid,H)
    assert store.create_experiment(bid,H,eid)==eid
    with pytest.raises(ControlError): store.create_experiment(bid,'c'*64,eid)
    with pytest.raises(ControlError): store.transition_experiment(eid,'SUCCEEDED')
    store.transition_experiment(eid,'RUNNING')
    with pytest.raises(ControlError): store.seal_test(eid)
    with pytest.raises(ControlError): store.transition_experiment(eid,'SUCCEEDED','POP','report.json')
    with pytest.raises(ControlError): store.register_model(batch_id=bid,experiment_id=eid,run_id=rid,
        algorithm='NCF',purpose='EVAL',device='CPU',config_sha256=H,mapping_sha256=H,artifact_path='/retail/v1/models',code_commit=C,environment_sha256=H)
    store.transition_experiment(eid,'FAILED')
    with pytest.raises(ControlError): store.transition_experiment(eid,'RUNNING')


def test_quality_and_task_identity_validation(db):
    store,bid,rid=running(db)
    with pytest.raises(ControlError): store.quality(bid,{'made_up':1})
    with pytest.raises(ControlError): store.quality(bid,{'RAW_ROWS':-1})
    with pytest.raises(ControlError): store.quality(bid,{},counts=dict(raw_rows=2,sale_rows=1,return_rows=0,rejected_rows=0))
    with pytest.raises(ControlError): store.transition_batch(bid,'SUCCEEDED')
    with pytest.raises(ControlError): store.create_task(bid,'unsafe name',H,C)
    with pytest.raises(ControlError): store.create_task(bid,'test',H,'development')
    with pytest.raises(ControlError): store.start_task(rid,pid=123)


def test_four_model_selection_metrics_and_sealing(db,tmp_path):
    """Isolated fixture artifacts prove control flow only, never actual model quality."""
    from orchestration.control import ALGORITHMS
    store,bid,rid=running(db)
    eid=store.create_experiment(bid,H); store.transition_experiment(eid,'RUNNING')
    models={}
    for algorithm in ALGORITHMS:
        directory=tmp_path/algorithm; directory.mkdir()
        artifact_names=['config.json','mapping.tsv','environment.lock','hardware.json','inputs.json','metrics.json']
        artifact_names+=['model.pt','training_history.tsv'] if algorithm=='NCF' else (['rules.json'] if algorithm in ('POP','ITEMCF') else ['spark_model/metadata/part-00000'])
        artifacts=[]
        for name in artifact_names:
            file=directory/name;file.parent.mkdir(parents=True,exist_ok=True)
            file.write_bytes(b'isolated control fixture\n')
            artifacts.append(dict(path=str(file),sha256=hashlib.sha256(file.read_bytes()).hexdigest(),bytes=file.stat().st_size))
        hash_value=artifacts[0]['sha256']
        mid=store.register_model(batch_id=bid,experiment_id=eid,run_id=rid,algorithm=algorithm,purpose='EVAL',
            device='CUDA' if algorithm=='NCF' else 'CPU',config_sha256=hash_value,mapping_sha256=hash_value,
            artifact_path=str(directory),code_commit=C,environment_sha256=hash_value)
        models[algorithm]=mid
        store.transition_model(mid,'RUNNING')
        if algorithm=='NCF':
            store.record_epoch(mid,epoch=1,train_loss=0.7,elapsed_seconds=0.1,validation_ndcg=0.0)
            with pytest.raises(ControlError): store.record_epoch(mid,epoch=2,train_loss=float('nan'),elapsed_seconds=1.0)
        source=directory/'source.tsv';source.write_bytes(b'input\n')
        manifest=dict(schema_version='1.0',baseline_version='HL-DOC-1.0.0',batch_id=bid,model_run_id=mid,
            experiment_id=eid,algorithm=algorithm,purpose='EVAL',status='SUCCEEDED',created_at='2026-10-20T00:00:00Z',
            finished_at='2026-10-20T00:00:01Z',code_commit=C,config_sha256=hash_value,mapping_sha256=hash_value,
            environment_sha256=hash_value,inputs=[dict(path=str(source),sha256=hashlib.sha256(source.read_bytes()).hexdigest(),rows=1)],artifacts=artifacts)
        path=directory/'manifest.json';path.write_text(json.dumps(manifest))
        store.transition_model(mid,'SUCCEEDED',manifest_path=str(path),store=LocalStore(directory))
        metric=dict(user_count=1,catalog_count=2,window_user_count=1,precision_at_k=0.1,recall_at_k=1.0,
            ndcg_at_k=1.0,coverage=0.5,cold_user_count=0,cold_item_count=0,cold_interaction_count=0,excluded_user_count=0)
        with pytest.raises(ControlError): store.select_model(eid,algorithm,mid)
        for cohort in ('WARM','ALL_ELIGIBLE'): store.record_metric(mid,split_name='VALIDATION',cohort=cohort,**metric)
        store.select_model(eid,algorithm,mid)
    for algorithm,mid in models.items():
        for cohort in ('WARM','ALL_ELIGIBLE'): store.record_metric(mid,split_name='TEST',cohort=cohort,**metric)
    store.seal_test(eid)
    with pytest.raises(ControlError): store.select_model(eid,'POP',models['POP'])
    with pytest.raises(ControlError): store.record_metric(models['POP'],split_name='TEST',cohort='WARM',**metric)
    store.transition_experiment(eid,'SUCCEEDED','POP','isolated-fixture-report.json')
    with store.connection.cursor() as cursor:
        cursor.execute('SELECT status,test_sealed_at FROM experiment WHERE experiment_id=%s',(eid,))
        row=cursor.fetchone(); assert row['status']=='SUCCEEDED' and row['test_sealed_at']


def test_backup_restores_same_control_rows(db,mariadb_instance):
    connections,name,_,directory=db
    store,bid,rid=running(db)
    store.finish_task(rid,'FAILED',error_code='ISOLATED_FIXTURE')
    evidence=backup_database(connections['migration'],directory/'restore-backups')
    path=Path(evidence['path'])
    assert hashlib.sha256(path.read_bytes()).hexdigest()==evidence['sha256']
    admin,socket,_=mariadb_instance
    target='restore_'+uuid4().hex[:10]
    with admin.cursor() as cursor: cursor.execute(f'CREATE DATABASE `{target}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
    import getpass
    with path.open('rb') as stream:
        subprocess.run(['mariadb','--no-defaults','--protocol=socket','--socket='+socket,
            '--user='+getpass.getuser(),target],stdin=stream,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,check=True,timeout=30)
    restored=open_connection(name=target,user=getpass.getuser(),unix_socket=socket)
    try:
        require_current(restored)
        with restored.cursor() as cursor:
            cursor.execute('SELECT run_id,batch_id,status,error_code FROM task_run WHERE run_id=%s',(rid,))
            assert cursor.fetchone()==dict(run_id=rid,batch_id=bid,status='FAILED',error_code='ISOLATED_FIXTURE')
    finally: restored.close()


def test_composite_release_foreign_keys_and_candidate_check(db):
    store,bid,rid=running(db)
    eid=store.create_experiment(bid,H)
    first='R20261020000000abcdef11'; second='R20261020000000abcdef22'
    connection=db[0]['writer']
    with connection.cursor() as cursor:
        for release in (first,second):
            cursor.execute('''INSERT INTO data_release (release_id,batch_id,experiment_id,status,champion_algorithm,
                manifest_path,manifest_sha256,baseline_version,created_at)
                VALUES (%s,%s,%s,'BUILDING','POP','fixture-manifest',%s,'HL-DOC-1.0.0',%s)''',(release,bid,eid,H,utcnow()))
        cursor.execute('INSERT INTO product_dim VALUES (%s,%s,NULL,1,1)',(first,'10001'))
        with pytest.raises(pymysql.MySQLError) as rejected:
            cursor.execute('INSERT INTO product_dim VALUES (%s,%s,NULL,0,1)',(first,'10002'))
        assert rejected.value.args[0]==4025
        cursor.execute("INSERT INTO sale_order VALUES (%s,'invoice','2011-08-01','UK',NULL,1.00)",(second,))
        with pytest.raises(pymysql.IntegrityError) as rejected:
            cursor.execute("INSERT INTO sale_order_item VALUES (%s,'invoice','10001',1.00,1)",(second,))
        assert rejected.value.args[0]==1452
        cursor.execute('INSERT INTO product_dim VALUES (%s,%s,NULL,1,1)',(second,'10001'))
        cursor.execute("INSERT INTO sale_order_item VALUES (%s,'invoice','10001',1.00,1)",(second,))
    connection.commit()
