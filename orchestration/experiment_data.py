"""W05 immutable experiment preparation, backed by the successful W04 basket run."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from uuid import UUID
import zipfile

from etl.ingest import ROOT, read_json, sha256, write_json
from etl.storage import HdfsStorage, publish
from ml.data import prepare
from orchestration.control import ControlStore
from orchestration.database import named_lock, utcnow, LockBusy
from orchestration.manifests import ManifestError, fingerprint, verify_manifest
from orchestration.migrations import require_current
from orchestration.pipeline import expected, stream_rows, task_row, validate_stage_bundle
from orchestration.runtime import Heartbeat, process_start, process_alive


def configuration(batch, inputs):
    return {'baseline_version': 'HL-DOC-1.0.0', 'source_sha256': batch['source_sha256'],
            'policy_version': batch['policy_version'], 'inputs': inputs,
            'experiment_parameters_sha256': sha256(ROOT / 'config/experiment.toml'),
            'train_feature_jar_sha256': sha256(ROOT / 'mapreduce/target/retail-mapreduce-0.1.0-SNAPSHOT.jar')}


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def experiment_task(experiment_id):
    if str(UUID(experiment_id)) != experiment_id:
        raise ValueError('Noncanonical experiment ID')
    return 'experiment_' + UUID(experiment_id).hex


def prepare_experiment(connection, config, batch_id, experiment_id, *, dry_run=False):
    task_name = experiment_task(experiment_id)
    require_current(connection)
    control = ControlStore(connection)
    storage = HdfsStorage(config)
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    # Formal evidence must identify exactly the committed implementation.
    dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT))
    with named_lock(connection):
        tick = Heartbeat(connection, control)
        storage.heartbeat = tick
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM data_batch WHERE batch_id=%s', (batch_id,))
            batch = cursor.fetchone()
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name='J06baskets' AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1", (batch_id,))
            source = cursor.fetchone()
            cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s ORDER BY attempt DESC LIMIT 1', (batch_id, task_name))
            previous = cursor.fetchone()
        connection.rollback()
        if not batch or batch['status'] != 'SUCCEEDED' or not source:
            raise ManifestError('W05 requires the successful W04 batch and complete basket manifest')
        manifest = read_json(storage, source['output_manifest_path'])
        validate_stage_bundle(manifest, source)
        verify_manifest(manifest, storage, expected(source), heartbeat=tick)
        inputs = manifest['datasets']['baskets']['files']
        params = configuration(batch, inputs)
        config_hash = json_hash(params)
        if dry_run:
            return {'dry_run': True, 'experiment_id': experiment_id, 'config_sha256': config_hash,
                    'plan': ['verify complete untruncated W04 baskets', 'split before training aggregation',
                             'independent EVAL/FULLFIT mappings', 'shared cohorts and frozen TEST labels']}
        if previous and previous['status'] == 'SUCCEEDED':
            if previous['config_sha256'] != config_hash:
                raise ManifestError('Successful experiment data has different inputs or configuration')
            bundle = read_json(storage, previous['output_manifest_path'])
            verify_manifest(bundle, storage, expected(previous), heartbeat=tick)
            return bundle['execution']['result'] | {'reused': True}
        if dirty:
            raise ManifestError('Commit the experiment implementation before formal preparation')
        control.create_experiment(batch_id, config_hash, experiment_id)
        if previous and previous['status'] in ('RUNNING', 'PENDING'):
            if process_alive(previous):
                raise LockBusy('Experiment preparation process is still alive')
            destination = f"{storage.root}/features/batch_id={batch_id}/run_id={previous['run_id']}"
            if storage.isdir(destination):
                bundle = read_json(storage, destination + '/manifest.json')
                if previous['status'] == 'PENDING':
                    control.start_task(previous['run_id'])
                control.finish_task(previous['run_id'], 'SUCCEEDED', manifest_path=destination + '/manifest.json', store=storage, heartbeat=tick)
                return bundle['execution']['result'] | {'reused': True, 'recovered': True}
            if previous['status'] == 'PENDING':
                control.start_task(previous['run_id'])
            control.finish_task(previous['run_id'], 'FAILED', error_code='INCOMPLETE_EXPERIMENT_DATA', terminal_check=lambda _: 'FAILED')
        # Apply the TRAIN cutoff in the clean/order mappers before J06 aggregation,
        # and run J07/J08 on those TRAIN baskets rather than full-history features.
        from orchestration.experiment_features import train_features
        features = train_features(connection, config, experiment_id, batch_id, control, storage, tick, commit)
        run_id = control.create_task(batch_id, task_name, config_hash, commit)
        control.start_task(run_id, pid=os.getpid(), process_start_utc=process_start(os.getpid()))
        tick.run_id = run_id
        tick(force=True)
        local = Path(config['paths']['work_dir']) / 'experiments' / experiment_id / run_id
        local.mkdir(parents=True, mode=0o700)
        destination = f'{storage.root}/features/batch_id={batch_id}/run_id={run_id}'
        published = False
        try:
            summary = prepare(stream_rows(storage, inputs, 4, tick), local / 'data', tick)
            from ml.data import TrainingData
            train = TrainingData.load(local / 'data/eval')
            independent = {(u, i): (n, at[:10]) for u, history in train.history.items() for i, (n, at) in history.items()}
            actual = {}
            for user, item, count, at in stream_rows(storage, features['datasets']['interactions']['files'], 4, tick):
                key = (user, item)
                if key in actual:
                    raise ManifestError('Duplicate TRAIN YARN interaction')
                actual[key] = (int(count), at[:10])
            if actual != independent:
                raise ManifestError('TRAIN YARN interactions differ from independent time split')
            summary['train_yarn_interactions_verified'] = len(actual)
            write_json(local / 'config.json', params)
            shutil.copyfile(ROOT / 'uv.lock', local / 'environment.lock')
            sources = [*ROOT.glob('ml/*.py'), *ROOT.glob('orchestration/*.py'), ROOT / 'config/experiment.toml']
            source_hashes = {str(p.relative_to(ROOT)): sha256(p) for p in sources}
            with zipfile.ZipFile(local / 'source-code.zip', 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for name in sorted(source_hashes):
                    archive.write(ROOT / name, name)
            files = {str(p.relative_to(local / 'data')).replace('/', '.'): p for p in (local / 'data').rglob('*')
                     if p.is_file() and p.suffix != '.sqlite'}
            files.update({name: local / name for name in ('config.json', 'environment.lock', 'source-code.zip')})
            (local / 'summary.tsv').write_text('status\tPASS\nexperiment_id\t' + experiment_id + '\n')
            files['summary.tsv'] = local / 'summary.tsv'
            (local / '_SUCCESS').touch()
            files['_SUCCESS'] = local / '_SUCCESS'
            refs = {name: {'path': destination + '/' + name, 'sha256': sha256(p),
                           'rows': sum(1 for _ in p.open('rb'))} for name, p in files.items()
                    if p.suffix in ('.tsv', '.json')}
            result = {'status': 'PASS', 'batch_id': batch_id, 'experiment_id': experiment_id,
                      'run_id': run_id, 'manifest_path': destination + '/manifest.json',
                      'summary': summary, 'files': refs, 'train_features': features}
            row = task_row(connection, run_id)
            output = refs['summary.tsv']
            bundle = {'schema_version': '1.0', 'baseline_version': 'HL-DOC-1.0.0',
                      'batch_id': batch_id, 'run_id': run_id, 'status': 'SUCCEEDED', 'code_commit': commit,
                      'code_dirty': False, 'source_code_sha256': source_hashes, 'config_sha256': config_hash,
                      'created_at': row['created_at'].isoformat() + 'Z', 'finished_at': utcnow().isoformat() + 'Z',
                      'dataset': 'experiment_summary', 'schema': ['key', 'value'],
                      'inputs': inputs + [f for ds in features['datasets'].values() for f in ds['files']],
                      'outputs': [output], 'artifacts': [
                          {'path': destination + '/' + name, 'sha256': sha256(p), 'bytes': p.stat().st_size}
                          for name, p in files.items() if name != 'summary.tsv'],
                      'record_count': output['rows'], 'classification_counts': {}, 'application_id': None,
                      'success_marker': destination + '/_SUCCESS',
                      'execution': {'experiment_id': experiment_id, 'result': result}}
            write_json(local / 'manifest.json', bundle)
            files['manifest.json'] = local / 'manifest.json'
            publish(storage, files, destination, run_id, tick)
            published = True
            control.finish_task(run_id, 'SUCCEEDED', manifest_path=destination + '/manifest.json', store=storage, heartbeat=tick)
            return result | {'reused': False}
        except Exception:
            if not published:
                tick(force=True)
                control.finish_task(run_id, 'FAILED', error_code='EXPERIMENT_DATA_FAILED', terminal_check=lambda _: 'FAILED')
            raise


def materialize(storage, result, destination, tick=lambda: None):
    """Verified local cache. Every consumption rechecks immutable HDFS references."""
    destination = Path(destination)
    for purpose in ('eval', 'fullfit', 'validation', 'test'):
        (destination / purpose).mkdir(parents=True, exist_ok=True)
    for name, ref in result['files'].items():
        prefix, _, tail = name.partition('.')
        if prefix not in ('eval', 'fullfit', 'validation', 'test'):
            continue
        path = destination / prefix / tail
        digest = hashlib.sha256()
        with path.open('wb') as stream:
            for chunk in storage.chunks(ref['path']):
                tick()
                digest.update(chunk)
                stream.write(chunk)
        if digest.hexdigest() != ref['sha256']:
            raise ManifestError('Experiment cache differs from immutable input')
    return destination
