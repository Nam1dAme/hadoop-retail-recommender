"""W08 provenance collection and public publish/verify/rollback commands."""
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
from uuid import UUID, uuid4

from etl.ingest import ROOT, sha256, write_json
from etl.storage import HdfsStorage
from orchestration.control import ControlStore, ALGORITHMS
from orchestration.database import named_lock, utcnow, LockBusy
from orchestration.experiment_data import json_hash
from orchestration.manifests import ManifestError, DATASETS
from orchestration.pipeline import STAGES, task_row
from orchestration.pipeline_audit import audit_database, audit_data
from orchestration.release_data import build_exports
from orchestration.releases import (current, existing_release, import_bundle, verify_local,
                                     rollback_local, verify_bindings, validate_bundle)
from orchestration.runtime import Heartbeat, process_alive, process_start
from orchestration.training import SnapshotStore, fetch, upload_bundle
from orchestration.w07 import Context


def require(value, message):
    if not value:
        raise ManifestError(message)


def check_cuda(result):
    audit = result['details']['first_batch_device_audit']
    history = result['details']['epochs']
    require(all(audit[k] for k in ('gradients_on_device', 'gradients_finite', 'nonzero_gradient', 'fp32'))
            and all(audit[k].startswith('cuda') for k in
                    ('model_device', 'input_device', 'labels_device', 'output_device', 'loss_device'))
            and history and all(math.isfinite(r['train_loss']) for r in history)
            and history[-1]['train_loss'] < history[0]['train_loss'], 'Actual FULLFIT CUDA evidence failed')


def dependencies(ctx):
    files = {}
    for prefix, directory in ctx.snapshots.bindings:
        for path in directory.rglob('*'):
            if not path.is_file() or path.name.endswith('.crc'):
                continue
            remote = prefix+str(path.relative_to(directory))
            ref = {'path': remote, 'sha256': sha256(path), 'bytes': path.stat().st_size}
            require(remote not in files or files[remote] == ref, 'Downloaded dependency changed during publication')
            files[remote] = ref
    return [files[k] for k in sorted(files)]


def benchmark_gate(ctx):
    with ctx.connection.cursor() as cursor:
        cursor.execute('SELECT * FROM gpu_benchmark WHERE model_run_id=%s', (ctx.selected['NCF']['model_run_id'],))
        entries = cursor.fetchall()
    ctx.connection.rollback()
    evidence = []
    for row in entries:
        task = ctx.query_task('benchmark_'+ctx.experiment_id.replace('-', '')+'_'+row['device']+'_'+str(row['repeat_index']), succeeded=True)
        require(task is not None, 'Missing successful benchmark task')
        local = ctx.download(str(PurePosixPath(task['output_manifest_path']).parent))
        ctx.verify_run(json.loads((local/'manifest.json').read_text()), task)
        params = json.loads((local/'config.json').read_text())
        result = json.loads((local/'benchmark-result.json').read_text())
        shared = {k: v for k, v in params.items() if k not in ('device', 'repeat_index', 'comparison_config_sha256')}
        require(json_hash(shared) == params['comparison_config_sha256'] == row['config_sha256']
                and json_hash(params['sample_files']) == params['dataset_sha256'] == row['dataset_sha256'],
                'Benchmark input/configuration hash differs')
        require(all(row[k] == result[k] for k in ('device', 'repeat_index', 'train_seconds', 'samples_per_second', 'peak_memory_mib'))
                and params['model_run_id'] == row['model_run_id'] == ctx.selected['NCF']['model_run_id']
                and params['epochs'] == result['epochs'] == row['epochs'] == 5
                and result['optimizer_reset']
                and result['initial_state_sha256'] == result['reset_state_sha256'] != result['warmed_state_sha256'],
                'Benchmark result differs from persisted protocol')
        for audit in (result['first_batch_device_audit'], result['warmup_device_audit']):
            require(all(audit[k] for k in ('gradients_on_device', 'gradients_finite', 'nonzero_gradient', 'fp32'))
                    and all(audit[k].startswith(row['device'].lower()) for k in
                            ('model_device', 'input_device', 'labels_device', 'output_device', 'loss_device')),
                    'Benchmark actual graph device evidence failed')
        history = result['history']
        require([r['epoch'] for r in history] == list(range(1, 6))
                and all(math.isfinite(r['train_loss']) for r in history)
                and history[-1]['train_loss'] < history[0]['train_loss'], 'Benchmark loss/epoch evidence failed')
        if row['device'] == 'CUDA':
            require(result['peak_memory_mib'] > 0 and row['driver_version']
                    and row['cuda_runtime'] and result['hardware']['gpu_name'], 'Missing actual CUDA hardware evidence')
        evidence.append({'benchmark_id': row['benchmark_id'], 'device': row['device'],
                         'repeat_index': row['repeat_index'], 'dataset_sha256': row['dataset_sha256'],
                         'config_sha256': row['config_sha256'], 'train_seconds': row['train_seconds']})
    return evidence


def prepare_bundle(ctx, models, task, release_id):
    bundle = ctx.directory/'release-bundle'
    bundle.mkdir()
    datasets = {}
    for stage in STAGES:
        if stage == 'pipeline':
            continue
        manifest = ctx.stage(stage)
        for name, dataset in manifest['datasets'].items():
            require(name not in datasets, 'Ambiguous business dataset')
            datasets[name] = dataset
    recs = {}
    selected = {}
    for algorithm, model in ctx.selected.items():
        selected[algorithm] = ctx.model_bundle(model)
    for algorithm, model in models.items():
        root, params = ctx.model_bundle(model)
        require(model['mapping_sha256'] == ctx.train.mapping_sha256
                and params['source_eval_model_run_id'] == ctx.selected[algorithm]['model_run_id'],
                'FULLFIT mapping or selected EVAL model differs')
        expected_params = dict(ctx.report['selected'][algorithm]['hyperparameters'])
        if algorithm == 'NCF':
            for key in ('max_epochs', 'patience', 'min_delta'):
                expected_params.pop(key)
            chosen_epoch = json.loads((selected[algorithm][0]/'worker-result.json').read_text())['details']['best_epoch']
            expected_params |= {'epochs': chosen_epoch, 'early_stopping': False}
            result = json.loads((root/'fullfit-result.json').read_text())
            check_cuda(result)
            require(len(result['details']['epochs']) == chosen_epoch and not result['details']['early_stopping'],
                    'FULLFIT NCF did not use the validation-selected fixed epoch count')
        require(params['hyperparameters'] == expected_params, 'FULLFIT parameters differ from sealed selection')
        rec_task = ctx.query_task('recommend_'+model['model_run_id'].replace('-', ''), succeeded=True)
        require(rec_task is not None, 'Missing successful FULLFIT recommendation task')
        rec_root = ctx.download(str(PurePosixPath(rec_task['output_manifest_path']).parent))
        ctx.verify_run(json.loads((rec_root/'manifest.json').read_text()), rec_task)
        recs[algorithm] = (model['model_run_id'], rec_root/'recommendations.tsv')
    with ctx.connection.cursor() as cursor:
        cursor.execute('SELECT * FROM data_batch WHERE batch_id=%s', (ctx.batch_id,))
        batch = cursor.fetchone()
    ctx.connection.rollback()
    # Re-run W04's independent source-vs-result checks, rather than accepting a
    # stored PASS label. Keep its disk tables for the publication projection.
    with audit_database(ctx.directory/'release-audit.sqlite', ctx.tick) as db:
        audit = audit_data(db, ctx.snapshots, batch, datasets, ctx.tick)
        expected_users = {r[0] for r in db.execute('SELECT DISTINCT customer_id FROM interactions')}
        expected_items = {r[0] for r in db.execute('SELECT DISTINCT stock_code FROM interactions')}
        require(expected_users == set(ctx.train.users) and expected_items == set(ctx.train.items),
                'FULLFIT mapping differs from full-history business interactions')
        for user, item, count, at in db.execute('SELECT * FROM interactions'):
            require(ctx.train.history[user][item] == (count, at), 'FULLFIT history differs from W04 interactions')
        require(sum(map(len, ctx.train.history.values())) == audit['counts']['interactions'], 'FULLFIT history contains extra pairs')
        tables = build_exports(db, bundle, recs, ctx.train, ctx.tick)
    gate = benchmark_gate(ctx)
    # Pin the original XLSX too, including first publications where no parent
    # happened to reference the raw directory directly.
    raw = ctx.download(str(PurePosixPath(batch['raw_hdfs_path']).parent))
    require(sha256(raw/'source.xlsx') == batch['source_sha256'], 'Original XLSX hash differs from batch')
    subprocess.run(['git', 'archive', '--format=tar', '--output='+str(bundle/'code.tar'), ctx.commit], cwd=ROOT, check=True)
    shutil.copyfile(ROOT/'uv.lock', bundle/'environment.lock')
    shutil.copyfile(ROOT/'config/development.toml', bundle/'development.toml')
    shutil.copyfile(ROOT/'config/experiment.toml', bundle/'experiment.toml')
    files = [{'file': p.name, 'sha256': sha256(p), 'bytes': p.stat().st_size}
             for p in sorted(bundle.iterdir()) if p.suffix != '.tsv']
    manifest = {'schema_version': '1.0', 'baseline_version': ctx.config['baseline_version'],
                'release_id': release_id, 'batch_id': ctx.batch_id, 'experiment_id': ctx.experiment_id,
                'champion_algorithm': ctx.experiment['champion_algorithm'], 'code_commit': ctx.commit,
                'code_dirty': False, 'synthetic': False, 'config_sha256': task['config_sha256'],
                'created_at': utcnow().isoformat()+'Z', 'models': {a: m['model_run_id'] for a, m in models.items()},
                'mapping_sha256': ctx.train.mapping_sha256, 'source_sha256': batch['source_sha256'],
                'tables': tables, 'audit': audit, 'gpu_gate': gate, 'dependencies': dependencies(ctx), 'files': files,
                'baseline_sha256': {str(p.relative_to(ROOT)): sha256(p) for p in sorted((ROOT/'docs').glob('*.md'))}}
    verify_bindings(ctx.connection, manifest)
    write_json(bundle/'release.json', manifest)
    (bundle/'summary.tsv').write_text('status\tPASS\nrelease_id\t'+release_id+'\n')
    (bundle/'_SUCCESS').touch()
    destination = str(ctx.storage.root)+'/releases/release_id='+release_id
    output = {'path': destination+'/summary.tsv', 'sha256': sha256(bundle/'summary.tsv'), 'rows': 2}
    mapping = ctx.prepared['files']['fullfit.mapping.tsv']
    run_manifest = {'schema_version': '1.0', 'baseline_version': ctx.config['baseline_version'],
        'batch_id': ctx.batch_id, 'run_id': task['run_id'], 'status': 'SUCCEEDED', 'code_commit': ctx.commit,
        'config_sha256': task['config_sha256'], 'code_dirty': False, 'synthetic': False,
        'source_code_sha256': {}, 'created_at': task['created_at'].isoformat()+'Z', 'finished_at': utcnow().isoformat()+'Z',
        'dataset': 'model_summary', 'schema': DATASETS['model_summary'].split(','), 'inputs': [mapping],
        'outputs': [output], 'record_count': 2, 'classification_counts': {}, 'application_id': None,
        'success_marker': destination+'/_SUCCESS', 'execution': {'release_id': release_id},
        'artifacts': [{'path': destination+'/'+p.name, 'sha256': sha256(p), 'bytes': p.stat().st_size}
                      for p in sorted(bundle.iterdir()) if p.is_file() and p.name != 'summary.tsv']}
    write_json(bundle/'manifest.json', run_manifest)
    validate_bundle(manifest, bundle)
    upload_bundle(ctx.storage, bundle, destination, task['run_id'])
    downloaded = ctx.download(destination)
    ctx.verify_run(json.loads((downloaded/'manifest.json').read_text()), task)
    return manifest, downloaded, destination


def dependency_root(path, root):
    parts = PurePosixPath(path).parts
    for i, part in enumerate(parts):
        if part.startswith(('run_id=', 'model_run_id=')):
            return str(PurePosixPath(*parts[:i+1]))
    value = PurePosixPath(path).relative_to(root).parts
    if len(value) >= 2 and value[0] in ('raw', 'normalized') and value[1].startswith('batch_id='):
        return str(PurePosixPath(root)/value[0]/value[1])
    return str(PurePosixPath(path).parent)


def load_release(connection, config, release_id, *, tick=lambda: None, check_dependencies=True, restore_root=None):
    require(bool(__import__('re').fullmatch(r'R[0-9]{14}[0-9a-f]{8}', release_id)), 'Invalid release ID')
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM data_release WHERE release_id=%s', (release_id,))
        row = cursor.fetchone()
    connection.rollback()
    require(row and row['status'] == 'READY', 'Unknown or non-READY release')
    if restore_root:
        from orchestration.recovery import RelocatedStorage
        storage = RelocatedStorage(config, restore_root)
    else:
        storage = HdfsStorage(config)
    storage.heartbeat = tick
    directory = Path(config['paths']['work_dir'])/'w08-checks'/str(uuid4())
    snapshots = SnapshotStore(storage)
    root = str(PurePosixPath(row['manifest_path']).parent)
    require(root == str(storage.root)+'/releases/release_id='+release_id
            and PurePosixPath(row['manifest_path']).name == 'release.json', 'Noncanonical release path')
    local = fetch(storage, root, directory)
    manifest = json.loads((local/'release.json').read_text())
    require(sha256(local/'release.json') == row['manifest_sha256'], 'Release manifest hash differs')
    validate_bundle(manifest, local)
    if check_dependencies:
        for ref in manifest['dependencies']:
            storage.check(ref['path'])
            if snapshots.local(ref['path']) is None:
                parent = dependency_root(ref['path'], storage.root)
                snapshots.bind(parent, fetch(storage, parent, directory))
            path = snapshots.local(ref['path'])
            require(sha256(path) == ref['sha256'] and path.stat().st_size == ref['bytes'], 'Release dependency hash differs')
            tick()
    return manifest, local, row['manifest_sha256']


def publish(connection, config, batch_id, experiment_id, model_ids, *, dry_run=False):
    require(len(model_ids) == len(set(model_ids)) == 4 and all(str(UUID(i)) == i for i in model_ids),
            'Four distinct canonical model UUIDs are required')
    before = current(connection)
    with named_lock(connection):
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM model_run WHERE model_run_id IN (%s,%s,%s,%s)', tuple(model_ids))
            models = {r['algorithm']: r for r in cursor.fetchall()}
        connection.rollback()
        require(set(models) == set(ALGORITHMS), 'Exactly one FULLFIT model per algorithm is required')
        bindings = {a: m['model_run_id'] for a, m in models.items()}
        prior = existing_release(connection, batch_id, experiment_id, bindings)
        if prior and prior[1] == 'READY':
            manifest, local, digest = load_release(connection, config, prior[0])
            verify_bindings(connection, manifest)
            result = verify_local(connection, prior[0], manifest, local, digest)
            # A process can die after the pointer transaction committed and
            # before its final task receipt. Reconcile that receipt only after
            # verifying the complete READY release; never import it again.
            params = {'batch_id': batch_id, 'experiment_id': experiment_id, 'models': bindings, 'policy': 'release-v1'}
            name = 'publish_'+json_hash(params)[:48]
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s AND status IN ('PENDING','RUNNING')", (batch_id, name))
                unfinished = cursor.fetchall()
            connection.rollback()
            if not dry_run:
                for task in unfinished:
                    if process_alive(task) is not False:
                        raise LockBusy('The publication task process is still alive or unverified')
                    snapshots = SnapshotStore(HdfsStorage(config))
                    remote = str(PurePosixPath(config['hdfs']['root'])/'releases'/('release_id='+prior[0]))
                    snapshots.bind(remote, local)
                    control = ControlStore(connection)
                    if task['status'] == 'PENDING':
                        control.start_task(task['run_id'])
                    control.finish_task(task['run_id'], 'SUCCEEDED', manifest_path=remote+'/manifest.json', store=snapshots)
            return result | {'reused': True, 'current_release_id': current(connection)}
        if dry_run:
            return {'dry_run': True, 'batch_id': batch_id, 'experiment_id': experiment_id,
                    'models': bindings, 'previous_release_id': before,
                    'plan': 'verify sources, BUILDING import, compare nine tables, atomically publish READY'}
        ctx = Context(connection, config, experiment_id, batch_id=batch_id)
        require(not ctx.dirty, 'Commit the W08 implementation before formal publication')
        params = {'batch_id': batch_id, 'experiment_id': experiment_id, 'models': bindings, 'policy': 'release-v1'}
        name = 'publish_'+json_hash(params)[:48]
        control = ControlStore(connection)
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM task_run WHERE status IN ('PENDING','RUNNING')")
            unfinished = cursor.fetchall()
        connection.rollback()
        for previous in unfinished:
            if previous['task_name'] != name or process_alive(previous) is not False:
                raise LockBusy('An unfinished task requires authoritative process verification')
            if previous['status'] == 'PENDING':
                control.start_task(previous['run_id'])
            control.finish_task(previous['run_id'], 'FAILED', error_code='INTERRUPTED_PUBLICATION', terminal_check=lambda _: 'FAILED')
        if prior:
            with control.transaction() as cursor:
                cursor.execute("UPDATE data_release SET status='FAILED',failure_reason='Interrupted BUILDING import; retry creates a new immutable release' WHERE release_id=%s AND status='BUILDING'", (prior[0],))
        run_id = control.create_task(batch_id, name, json_hash(params), ctx.commit, experiment_id=experiment_id)
        control.start_task(run_id, pid=os.getpid(), process_start_utc=process_start(os.getpid()))
        ctx.tick.run_id = run_id
        task = task_row(connection, run_id)
        release_id = 'R'+task['created_at'].strftime('%Y%m%d%H%M%S')+uuid4().hex[:8]
        try:
            manifest, local, destination = prepare_bundle(ctx, models, task, release_id)
            require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)
                    and subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == ctx.commit,
                    'Code changed during release preparation')
            result = import_bundle(connection, manifest, local, destination+'/release.json', sha256(local/'release.json'),
                                   before, tick=ctx.tick)
            control.finish_task(run_id, 'SUCCEEDED', manifest_path=destination+'/manifest.json',
                                store=ctx.snapshots, heartbeat=ctx.tick)
            ctx.tick.run_id = None
            return result | {'run_id': run_id, 'manifest_path': destination+'/release.json'}
        except BaseException:
            control.finish_task(run_id, 'FAILED', error_code='PUBLICATION_FAILED', terminal_check=lambda _: 'FAILED')
            raise


def verify(connection, config, release_id, *, restore_root=None):
    manifest, local, digest = load_release(connection, config, release_id, restore_root=restore_root)
    return verify_local(connection, release_id, manifest, local, digest)


def rollback(connection, config, release_id, reason, *, dry_run=False):
    before = current(connection)
    with named_lock(connection):
        tick = Heartbeat(connection, ControlStore(connection))
        manifest, local, digest = load_release(connection, config, release_id, tick=tick)
        verify_local(connection, release_id, manifest, local, digest, tick)
        if dry_run:
            return {'dry_run': True, 'release_id': release_id, 'previous_release_id': before}
        return rollback_local(connection, release_id, manifest, local, digest, reason, expected_current=before, tick=tick)
