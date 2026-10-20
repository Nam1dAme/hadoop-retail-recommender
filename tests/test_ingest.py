"""T-01 and recovery: real MariaDB, isolated file store; real HDFS is separately verified."""
from copy import deepcopy
from pathlib import Path
from uuid import uuid4
import json
import pytest
from etl.ingest import ingest
from etl.storage import FileStorage
from orchestration.database import named_lock,LockBusy
from orchestration.manifests import ManifestError,verify_manifest,fingerprint,LocalStore
from orchestration.settings import load_config,ConfigError
from test_normalization import workbook,BASE

URL='https://example.invalid/isolated-fixture'


@pytest.fixture
def setup_ingest(db,tmp_path):
    config=load_config('config/development.toml')
    config['paths']['work_dir']=str(tmp_path)
    config['database']['name']=db[1]
    config['hdfs']['root']=str(tmp_path/'hdfs')
    source=workbook(tmp_path/'source.xlsx',[BASE.copy()])
    storage=FileStorage(config['hdfs']['root'])
    return db[0]['writer'],config,source,storage


def run(setup,**kwargs):
    connection,config,source,storage=setup
    return ingest(connection,config,source,URL,synthetic=True,storage=storage,**kwargs)


def test_dry_run_then_same_file_idempotency_and_changed_source(setup_ingest):
    connection,config,source,storage=setup_ingest
    plan=run(setup_ingest,dry_run=True)
    assert plan['dry_run'] and plan['existing_batch_id'] is None
    assert not (Path(config['paths']['work_dir'])/'ingest').exists()
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) AS n FROM data_batch');assert cursor.fetchone()['n']==0
    first=run(setup_ingest);second=run(setup_ingest)
    assert first['batch_id']==second['batch_id'] and first['run_id']==second['run_id'] and second['reused']
    assert Path(first['raw_hdfs_path']).read_bytes()==source.read_bytes()
    with connection.cursor() as cursor:
        cursor.execute('SELECT status,raw_rows FROM data_batch');row=cursor.fetchone()
        assert row['status']=='PROCESSING' and row['raw_rows']==1
        cursor.execute('SELECT COUNT(*) AS n FROM task_run');assert cursor.fetchone()['n']==1
    changed=BASE.copy();changed[3]=2;workbook(source,[changed])
    third=run(setup_ingest)
    assert third['batch_id']!=first['batch_id']


def test_immutable_hashes_and_configuration_conflicts(setup_ingest):
    first=run(setup_ingest)
    with pytest.raises(ManifestError):run(setup_ingest,date_format='%Y-%m-%d')
    normalized=Path(first['normalized_hdfs_path'])/'normalized.tsv'
    normalized.write_text('tampered\n')
    with pytest.raises(ManifestError):run(setup_ingest)
    assert normalized.read_text()=='tampered\n'


def test_failed_io_retries_same_batch_with_new_attempt(setup_ingest,monkeypatch):
    storage=setup_ingest[-1]
    original=storage.put
    def fail(local,path):raise OSError('isolated injected failure')
    monkeypatch.setattr(storage,'put',fail)
    with pytest.raises(OSError):run(setup_ingest)
    connection=setup_ingest[0]
    with connection.cursor() as cursor:
        cursor.execute('SELECT batch_id,status FROM data_batch');before=cursor.fetchone()
        cursor.execute('SELECT run_id,status FROM task_run');previous=cursor.fetchone()
    assert before['status']=='FAILED' and previous['status']=='FAILED'
    monkeypatch.setattr(storage,'put',original)
    result=run(setup_ingest)
    assert result['batch_id']==before['batch_id'] and result['run_id']!=previous['run_id']
    with connection.cursor() as cursor:
        cursor.execute('SELECT attempt,parent_run_id,status FROM task_run ORDER BY attempt DESC LIMIT 1');row=cursor.fetchone()
        assert row==dict(attempt=2,parent_run_id=previous['run_id'],status='SUCCEEDED')


def test_published_output_recovers_after_dead_owner_database_gap(setup_ingest,monkeypatch):
    from orchestration.control import ControlStore
    original=ControlStore.finish_task
    def fail(self,*args,**kwargs):raise RuntimeError('isolated crash after publication')
    monkeypatch.setattr(ControlStore,'finish_task',fail)
    with pytest.raises(RuntimeError):run(setup_ingest)
    connection=setup_ingest[0]
    with connection.cursor() as cursor:
        cursor.execute('SELECT run_id,status FROM task_run');row=cursor.fetchone()
        assert row['status']=='RUNNING'
    # An owner whose process is still alive must never be restarted merely because it is stale.
    monkeypatch.setattr(ControlStore,'finish_task',original)
    with pytest.raises(LockBusy):run(setup_ingest)
    with connection.cursor() as cursor:
        cursor.execute('UPDATE task_run SET pid=999999999 WHERE run_id=%s',(row['run_id'],))
    connection.commit()
    recovered=run(setup_ingest)
    assert recovered['reused'] and recovered['recovered'] and recovered['run_id']==row['run_id']
    with connection.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) AS n FROM task_run');assert cursor.fetchone()['n']==1


def test_lock_contention_quality_failure_and_production_guards(setup_ingest,db):
    with named_lock(db[0]['migration']):
        with pytest.raises(LockBusy):run(setup_ingest)
    connection,config,source,storage=setup_ingest
    data=BASE.copy();data[3]=0;workbook(source,[BASE.copy(),data])
    result=run(setup_ingest)
    assert not result['quality_gate_passed']
    with connection.cursor() as cursor:
        cursor.execute('SELECT status FROM data_batch');assert cursor.fetchone()['status']=='FAILED'
    assert run(setup_ingest)['reused']
    bad=deepcopy(config);bad['database']['name']='retail_analytics'
    with pytest.raises(ConfigError):ingest(connection,bad,source,URL,synthetic=True,storage=storage)
    with pytest.raises(ConfigError):ingest(connection,config,'/etc/passwd',URL,synthetic=True,storage=storage)


def test_xlsx_rows_are_excel_rows_not_newline_bytes(setup_ingest):
    connection,config,source,storage=setup_ingest
    first=run(setup_ingest)
    manifest=json.loads((Path(first['normalized_hdfs_path'])/'manifest.json').read_text())
    assert manifest['inputs'][0]['format']=='XLSX'
    expected={k:manifest[k] for k in ('batch_id','run_id','config_sha256','code_commit')}
    verify_manifest(manifest,storage,expected,formal=False)
    with pytest.raises(ManifestError):verify_manifest(manifest,storage,expected,formal=True)
    manifest['inputs'][0]['rows']=2
    with pytest.raises(ManifestError):verify_manifest(manifest,storage,expected,formal=False)


def test_recover_pending_attempt_without_submitted_work(setup_ingest):
    from orchestration.control import ControlStore
    connection,config,source,storage=setup_ingest
    plan=run(setup_ingest,dry_run=True)
    control=ControlStore(connection)
    root=config['hdfs']['root']
    batch=control.register_batch(source_sha256=plan['source_sha256'],source_name=source.name,
        source_url=URL,source_bytes=source.stat().st_size,
        raw_hdfs_path=root+'/raw/batch_id={batch_id}/source.xlsx',
        normalized_hdfs_path=root+'/normalized/batch_id={batch_id}',hdfs_root=root)
    control.transition_batch(batch['batch_id'],'PROCESSING')
    previous=control.create_task(batch['batch_id'],'ingest',plan['config_sha256'],'f'*40)
    result=run(setup_ingest)
    assert result['batch_id']==batch['batch_id'] and result['run_id']!=previous
    with connection.cursor() as cursor:
        cursor.execute('SELECT status FROM task_run WHERE run_id=%s',(previous,));assert cursor.fetchone()['status']=='FAILED'
        cursor.execute('SELECT parent_run_id,attempt FROM task_run WHERE run_id=%s',(result['run_id'],))
        assert cursor.fetchone()==dict(parent_run_id=previous,attempt=2)


def test_raw_archive_is_reused_after_normalized_publish_failure(setup_ingest,monkeypatch):
    storage=setup_ingest[-1]
    original=storage.move
    def fail_normalized(source,target):
        if '/normalized/' in target:raise OSError('isolated failure after raw archive publication')
        return original(source,target)
    monkeypatch.setattr(storage,'move',fail_normalized)
    with pytest.raises(OSError):run(setup_ingest)
    connection=setup_ingest[0]
    with connection.cursor() as cursor:
        cursor.execute('SELECT raw_hdfs_path FROM data_batch');raw=Path(cursor.fetchone()['raw_hdfs_path'])
    before=raw.stat().st_mtime_ns
    monkeypatch.setattr(storage,'move',original)
    result=run(setup_ingest)
    assert result['quality_gate_passed'] and raw.stat().st_mtime_ns==before


def test_lost_connection_lock_stops_normalization_before_publication(setup_ingest,monkeypatch):
    import etl.ingest as module
    connection=setup_ingest[0]
    original=module.convert
    def lose_lock(*args,**kwargs):
        with connection.cursor() as cursor:cursor.execute("SELECT RELEASE_LOCK('retail-heavy-job')")
        args[-1](force=True)
        pytest.fail('Lost lock must stop before converter runs')
    monkeypatch.setattr(module,'convert',lose_lock)
    with pytest.raises(LockBusy):run(setup_ingest)
    assert not (Path(setup_ingest[1]['hdfs']['root'])/'raw').exists()
