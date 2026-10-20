"""Serial four-algorithm evaluation, durable validation selection and TEST sealing."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import tempfile
import time
from uuid import uuid4
import zipfile

from etl.ingest import ROOT, read_json, sha256, write_json
from etl.storage import HdfsStorage
from ml.data import TrainingData
from ml.evaluation import choose
from orchestration.control import ControlStore, ALGORITHMS
from orchestration.database import named_lock, utcnow, LockBusy
from orchestration.experiment_data import prepare_experiment, experiment_task, materialize, json_hash
from orchestration.manifests import ManifestError, verify_manifest
from orchestration.pipeline import expected, task_row
from orchestration.runtime import Heartbeat, process_alive, process_start


class SnapshotStore:
    """Verify a fresh bulk HDFS download while retaining canonical HDFS references.

    Bindings are populated only by dfs -get during this invocation. Unknown paths
    still stream from HDFS. Downloading directories avoids one JVM per tiny file.
    """
    def __init__(self, storage):
        self.storage = storage
        self.bindings = []

    def bind(self, remote, local):
        self.storage.check(remote)
        self.bindings.append((remote.rstrip('/') + '/', Path(local)))

    def local(self, path):
        self.storage.check(path)
        for prefix, local in reversed(self.bindings):
            if path.startswith(prefix):
                value = (local / path[len(prefix):]).resolve(strict=True)
                if not value.is_relative_to(local.resolve()):
                    raise ManifestError('Snapshot path escapes download root')
                return value

    def chunks(self, path):
        local = self.local(path)
        if local is None:
            yield from self.storage.chunks(path)
        else:
            with local.open('rb') as stream:
                while chunk := stream.read(1024**2):
                    yield chunk

    def exists(self, path):
        local = self.local(path)
        return local.is_file() if local is not None else self.storage.exists(path)


def fetch(storage, remote, parent):
    parent = Path(parent)
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / str(uuid4())
    storage.run('-get', remote, str(target))
    return target


def upload_bundle(storage, local, destination, run_id):
    temporary = str(PurePosixPath(destination).parent) + '/.model-' + run_id + '.tmp'
    if storage.isdir(destination) or storage.isdir(temporary):
        raise ManifestError('Immutable model destination/staging already exists')
    storage.mkdir(str(PurePosixPath(destination).parent))
    storage.run('-put', str(local), temporary)
    # Hadoop's symbolic-mode parser rejects an empty o= permission set.
    storage.run('-chmod', '-R', 'u=rwX,g=rX,o-rwx', temporary)
    storage.move(temporary, destination)


def worker_alive(run_id):
    for path in Path('/proc').glob('[0-9]*/cmdline'):
        try:
            args = path.read_bytes().split(b'\0')
        except OSError:
            continue
        if any(name in args for name in (b'ml.worker', b'ml.w07_worker')) and run_id.encode() in args:
            return True
    return False


def run_worker(config, control, tick, task, algorithm, data, directory, *, mode='train', model_id=None,
               module='ml.worker'):
    command = [os.sys.executable, '-m', module, '--data', str(data), '--directory', str(directory),
               '--algorithm', algorithm, '--mode', mode, '--run-id', task['run_id']]
    environment = os.environ | {'JAVA_HOME': config['runtime']['java_home'], 'SPARK_LOCAL_IP': '127.0.0.1',
        'PYSPARK_PYTHON': os.sys.executable, 'CUBLAS_WORKSPACE_CONFIG': ':4096:8',
        'PYSPARK_SUBMIT_ARGS': '--driver-memory 2g pyspark-shell', 'OPENBLAS_NUM_THREADS': '2'}
    environment['PATH'] = config['runtime']['java_home'] + '/bin:' + environment['PATH']
    last_epoch = 0
    if model_id:
        with control.connection.cursor() as cursor:
            cursor.execute('SELECT COALESCE(MAX(epoch),0) AS n FROM model_epoch WHERE model_run_id=%s', (model_id,))
            last_epoch = cursor.fetchone()['n']
        control.connection.rollback()
    log = directory / (mode + '-driver.log')
    with log.open('wb') as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, env=environment,
                                   cwd=ROOT, start_new_session=True)
        write_json(directory / (mode + '-process.json'), {'pid': process.pid,
            'process_start_utc': process_start(process.pid).isoformat() + 'Z', 'run_id': task['run_id']})
        try:
            while True:
                tick()
                progress = directory / 'progress.json'
                if model_id and algorithm == 'NCF' and mode in ('train','fullfit') and progress.exists():
                    try:
                        rows = json.loads(progress.read_text())['epochs']
                    except json.JSONDecodeError:
                        rows = []
                    for row in rows:
                        if row['epoch'] > last_epoch:
                            control.record_epoch(model_id, **{k: row[k] for k in (
                                'epoch', 'train_loss', 'validation_ndcg', 'elapsed_seconds', 'peak_gpu_mib')})
                            last_epoch = row['epoch']
                if process.poll() is not None:
                    break
                time.sleep(1)
        except BaseException:
            # This process group belongs to this exact Popen, including Spark children.
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=30)
            raise
    if process.returncode:
        raise RuntimeError('Training worker failed; inspect the private driver log')
    if model_id and algorithm == 'NCF' and mode in ('train','fullfit'):
        for row in json.loads((directory / 'progress.json').read_text())['epochs']:
            if row['epoch'] > last_epoch:
                control.record_epoch(model_id, **{k: row[k] for k in (
                    'epoch','train_loss','validation_ndcg','elapsed_seconds','peak_gpu_mib')})
                last_epoch = row['epoch']
    result_file = 'worker-result.json' if mode == 'train' else mode + '-result.json'
    return json.loads((directory / result_file).read_text())


def grids():
    import tomllib
    frozen = tomllib.loads((ROOT / 'config/experiment.toml').read_text())
    if frozen != {'split_policy_version':'time-split-v1','candidate_policy_version':'full-catalog-v1',
        'seed':42,'k':10,'train_end_exclusive':'2011-09-01','validation_end_exclusive':'2011-11-01',
        'als':{'rank':[16,32],'reg_param':[.01,.1],'max_iter':10,'alpha':10.,'implicit_prefs':True,'nonnegative':True},
        'ncf':{'learning_rate':[.001,.0005],'embedding_dim':16,'hidden_layers':[64,32,16],'batch_size':1024,
               'max_epochs':20,'patience':3,'min_delta':1e-6,'negatives':4,'weight_decay':1e-5}}:
        raise ManifestError('Experiment configuration differs from frozen HL-DOC-1.0.0 grids')
    return {'POP': [{}], 'ITEMCF': [{'max_seeds': 20, 'min_co_count': 3, 'max_neighbors': 10}],
        'ALS': [{'rank': rank, 'reg_param': reg, 'max_iter': 10, 'alpha': 10., 'implicit_prefs': True,
                 'nonnegative': True, 'num_user_blocks': 2, 'num_item_blocks': 2, 'driver_memory': '2g',
                 'master': 'local[2]', 'shuffle_partitions': 4}
                for rank in (16, 32) for reg in (.01, .1)],
        'NCF': [{'learning_rate': lr, 'embedding_dim': 16, 'hidden_layers': [64, 32, 16],
                 'weight_decay': 1e-5, 'batch_size': 1024, 'max_epochs': 20, 'patience': 3,
                 'min_delta': 1e-6, 'negatives': 4, 'precision': 'FP32', 'amp': False, 'tf32': False}
                for lr in (.001, .0005)]}


def metric_rows(control, model_id, metrics):
    """Crash-safe insertion: exact existing rows are reused, never replaced."""
    for metric in metrics:
        with control.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM model_metric WHERE model_run_id=%s AND split_name=%s AND cohort=%s AND k=10',
                           (model_id, metric['split_name'], metric['cohort']))
            existing = cursor.fetchone()
        control.connection.rollback()
        if existing:
            if any(existing[k] != value for k, value in metric.items()):
                raise ManifestError('Existing metric differs from verified immutable evidence')
        else:
            control.record_metric(model_id, **metric)


def candidate(connection, config, storage, snapshots, control, tick, prepared, data, algorithm, grid_index, hyperparameters, commit):
    experiment_id, batch_id = prepared['experiment_id'], prepared['batch_id']
    train = TrainingData.load(data / 'eval')
    task_name = 'eval_' + experiment_id.replace('-', '') + '_' + algorithm + '_' + str(grid_index)
    params = {'algorithm': algorithm, 'purpose': 'EVAL', 'experiment_id': experiment_id, 'batch_id': batch_id,
        'seed': 42, 'k': 10, 'candidate_policy_version': 'full-catalog-v1', 'split_policy_version': 'time-split-v1',
        'mapping_sha256': train.mapping_sha256, 'environment_sha256': sha256(ROOT / 'uv.lock'),
        'experiment_data_manifest': prepared['manifest_path'], 'hyperparameters': hyperparameters,
        'validation_cohorts': prepared['summary']['validation']['cohorts']}
    digest = json_hash(params)
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s ORDER BY attempt DESC LIMIT 1', (batch_id, task_name))
        previous = cursor.fetchone()
    connection.rollback()
    if previous and previous['config_sha256'] != digest:
        raise ManifestError('Candidate configuration changed under the same experiment')
    if previous:
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM model_run WHERE run_id=%s', (previous['run_id'],))
            model = cursor.fetchone()
        connection.rollback()
        if previous['status'] in ('PENDING', 'RUNNING'):
            if process_alive(previous) or worker_alive(previous['run_id']):
                raise LockBusy('Original training parent or worker remains alive')
            if model and storage.isdir(model['artifact_path']):
                local = fetch(storage, model['artifact_path'], data.parent / 'downloads')
                snapshots.bind(model['artifact_path'], local)
                if model['status'] == 'RUNNING':
                    control.transition_model(model['model_run_id'], 'SUCCEEDED',
                        manifest_path=model['artifact_path'] + '/manifest.json', store=snapshots)
                control.finish_task(previous['run_id'], 'SUCCEEDED',
                    manifest_path=model['artifact_path'] + '/task-manifest.json', store=snapshots, heartbeat=tick)
                previous = task_row(connection, previous['run_id'])
            else:
                if previous['status'] == 'PENDING':
                    control.start_task(previous['run_id'])
                if model and model['status'] in ('CREATED', 'RUNNING'):
                    if model['status'] == 'CREATED':
                        control.transition_model(model['model_run_id'], 'RUNNING')
                    control.transition_model(model['model_run_id'], 'FAILED')
                control.finish_task(previous['run_id'], 'FAILED', error_code='ABANDONED_TRAINING', terminal_check=lambda _: 'FAILED')
                previous = None
        if previous and previous['status'] == 'SUCCEEDED':
            local = fetch(storage, model['artifact_path'], data.parent / 'downloads')
            snapshots.bind(model['artifact_path'], local)
            manifest = json.loads((local / 'manifest.json').read_text())
            verify_manifest(manifest, snapshots, {k: model[k] for k in (
                'batch_id','model_run_id','experiment_id','algorithm','purpose','config_sha256','mapping_sha256','environment_sha256','code_commit')}, heartbeat=tick)
            verify_manifest(json.loads((local / 'task-manifest.json').read_text()), snapshots, expected(previous), heartbeat=tick)
            result = json.loads((local / 'worker-result.json').read_text())
            metric_rows(control, model['model_run_id'], result['metrics'])
            return {'model_run_id': model['model_run_id'], 'run_id': previous['run_id'], 'local': str(local),
                'artifact_path': model['artifact_path'], 'warm': result['metrics'][0],
                'parameter_count': result['details']['parameter_count'], 'grid_index': grid_index,
                'hyperparameters': hyperparameters, 'reused': True, 'details': result['details']}
    run_id = control.create_task(batch_id, task_name, digest, commit, experiment_id=experiment_id)
    control.start_task(run_id, pid=os.getpid(), process_start_utc=process_start(os.getpid()))
    tick.run_id = run_id
    tick(force=True)
    model_id = str(uuid4())
    destination = f'{storage.root}/models/model_run_id={model_id}'
    model_id = control.register_model(batch_id=batch_id, experiment_id=experiment_id, run_id=run_id,
        algorithm=algorithm, purpose='EVAL', device='CUDA' if algorithm == 'NCF' else 'CPU',
        config_sha256=digest, mapping_sha256=train.mapping_sha256, artifact_path=destination,
        code_commit=commit, environment_sha256=sha256(ROOT / 'uv.lock'), model_run_id=model_id)
    control.transition_model(model_id, 'RUNNING')
    local = data.parent / 'candidates' / model_id
    local.mkdir(parents=True)
    # ModelManifest binds config_sha256 to the actual config.json bytes.
    (local / 'config.json').write_text(json.dumps(params, sort_keys=True, separators=(',', ':')))
    shutil.copyfile(data / 'eval/mapping.tsv', local / 'mapping.tsv')
    shutil.copyfile(ROOT / 'uv.lock', local / 'environment.lock')
    inputs = [prepared['files'][name] for name in (
        'eval.mapping.tsv', 'eval.positive_interactions.tsv', 'eval.popularity.tsv',
        'validation.truth.tsv', 'validation.cohorts.json')]
    if algorithm == 'ITEMCF':
        inputs += prepared['train_features']['datasets']['similarity']['files']
    write_json(local / 'inputs.json', {'train_end_exclusive': '2011-09-01',
        'validation_end_exclusive': '2011-11-01', 'inputs': inputs,
        'training_context': prepared['summary']['contexts']['eval'],
        'validation_cohorts': prepared['summary']['validation']['cohorts']})
    task = task_row(connection, run_id)
    published = False
    try:
        sources = [*ROOT.glob('ml/*.py'), *ROOT.glob('orchestration/*.py'), ROOT / 'config/experiment.toml']
        source_hashes = {str(p.relative_to(ROOT)): sha256(p) for p in sources}
        result = run_worker(config, control, tick, task, algorithm, data, local, model_id=model_id)
        if any(sha256(ROOT / name) != digest for name, digest in source_hashes.items()):
            raise ManifestError('Training implementation changed while the worker ran')
        bundle = local / 'bundle'
        bundle.mkdir()
        for source in local.rglob('*'):
            if (not source.is_file() or bundle in source.parents or 'spark-tmp' in source.relative_to(local).parts
                    or source.name in ('progress.json',) or source.name.endswith('.crc')):
                continue
            target = bundle / source.relative_to(local)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        with zipfile.ZipFile(bundle / 'source-code.zip', 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for source in sources:
                archive.write(source, str(source.relative_to(ROOT)))
        (bundle / 'summary.tsv').write_text('model_run_id\t' + model_id + '\nstatus\tPASS\n')
        (bundle / '_SUCCESS').touch()
        artifacts = [{'path': destination + '/' + str(p.relative_to(bundle)), 'sha256': sha256(p), 'bytes': p.stat().st_size}
                     for p in sorted(bundle.rglob('*')) if p.is_file()]
        manifest = {'schema_version': '1.0', 'baseline_version': 'HL-DOC-1.0.0', 'batch_id': batch_id,
            'model_run_id': model_id, 'experiment_id': experiment_id, 'algorithm': algorithm, 'purpose': 'EVAL',
            'status': 'SUCCEEDED', 'code_commit': commit, 'code_dirty': False,
            'source_code_sha256': source_hashes, 'config_sha256': digest,
            'mapping_sha256': train.mapping_sha256, 'environment_sha256': sha256(ROOT / 'uv.lock'),
            'created_at': task['created_at'].isoformat() + 'Z', 'finished_at': utcnow().isoformat() + 'Z',
            'inputs': inputs, 'artifacts': artifacts}
        write_json(bundle / 'manifest.json', manifest)
        summary_file = bundle / 'summary.tsv'
        task_manifest = {'schema_version': '1.0', 'baseline_version': 'HL-DOC-1.0.0', 'batch_id': batch_id,
            'run_id': run_id, 'status': 'SUCCEEDED', 'code_commit': commit, 'code_dirty': False, 'config_sha256': digest,
            'created_at': task['created_at'].isoformat() + 'Z', 'finished_at': utcnow().isoformat() + 'Z',
            'dataset': 'model_summary', 'schema': ['key','value'], 'inputs': inputs,
            'outputs': [{'path': destination + '/summary.tsv', 'sha256': sha256(summary_file), 'rows': 2}],
            'artifacts': [a for a in artifacts if a['path'] != destination + '/summary.tsv'] + [
                {'path': destination + '/manifest.json', 'sha256': sha256(bundle / 'manifest.json'), 'bytes': (bundle / 'manifest.json').stat().st_size}],
            'record_count': 2, 'classification_counts': {}, 'application_id': None,
            'success_marker': destination + '/_SUCCESS', 'execution': {'model_run_id': model_id, 'algorithm': algorithm}}
        write_json(bundle / 'task-manifest.json', task_manifest)
        upload_bundle(storage, bundle, destination, run_id)
        published = True
        downloaded = fetch(storage, destination, data.parent / 'downloads')
        snapshots.bind(destination, downloaded)
        verify_manifest(read_json(snapshots, destination + '/manifest.json'), snapshots,
            {k: manifest[k] for k in ('batch_id','model_run_id','experiment_id','algorithm','purpose','config_sha256','mapping_sha256','environment_sha256','code_commit')}, heartbeat=tick)
        control.transition_model(model_id, 'SUCCEEDED', manifest_path=destination + '/manifest.json', store=snapshots)
        metric_rows(control, model_id, result['metrics'])
        with control.transaction() as cursor:
            cursor.execute('UPDATE model_run SET best_epoch=%s,validation_ndcg=%s,hardware_summary=%s WHERE model_run_id=%s',
                (result['details'].get('best_epoch'), result['metrics'][0]['ndcg_at_k'],
                 json.dumps(result['hardware'], ensure_ascii=False)[:1024], model_id))
        control.finish_task(run_id, 'SUCCEEDED', manifest_path=destination + '/task-manifest.json', store=snapshots, heartbeat=tick)
        return {'model_run_id': model_id, 'run_id': run_id, 'local': str(downloaded), 'artifact_path': destination,
            'warm': result['metrics'][0], 'parameter_count': result['details']['parameter_count'], 'grid_index': grid_index,
            'hyperparameters': hyperparameters, 'reused': False, 'details': result['details']}
    except LockBusy:
        raise
    except Exception:
        if not published:
            tick(force=True)
            control.transition_model(model_id, 'FAILED')
            control.finish_task(run_id, 'FAILED', error_code='MODEL_TRAINING_FAILED', terminal_check=lambda _: 'FAILED')
        raise


def evaluate_experiment(connection, config, batch_id, experiment_id, *, dry_run=False):
    # W05 has its own lock and immutable reuse validation before this lock scope.
    prepared = prepare_experiment(connection, config, batch_id, experiment_id, dry_run=dry_run)
    if dry_run:
        return prepared | {'grids': grids(), 'formal_ncf_device': 'CUDA', 'test_after_validation_freeze': True}
    commit = subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()
    if subprocess.check_output(['git','status','--porcelain'], cwd=ROOT):
        raise ManifestError('Commit the training implementation before formal evaluation')
    storage, control = HdfsStorage(config), ControlStore(connection)
    with named_lock(connection):
        tick = Heartbeat(connection, control)
        storage.heartbeat = tick
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM task_run WHERE status IN ('PENDING','RUNNING')")
            unfinished = cursor.fetchall()
        connection.rollback()
        token = experiment_id.replace('-', '')
        if any(token not in task['task_name'] for task in unfinished):
            raise LockBusy('Another unfinished task requires authoritative handle verification')
        snapshots = SnapshotStore(storage)
        directory = Path(config['paths']['work_dir']) / 'evaluation' / experiment_id / str(uuid4())
        directory.mkdir(parents=True, mode=0o700)
        pack = fetch(storage, str(PurePosixPath(prepared['manifest_path']).parent), directory / 'downloads')
        snapshots.bind(str(PurePosixPath(prepared['manifest_path']).parent), pack)
        data = materialize(snapshots, prepared, directory / 'data', tick)
        (data / 'similarity').mkdir()
        for number, ref in enumerate(prepared['train_features']['datasets']['similarity']['files']):
            path = data / 'similarity' / f'part-{number:05d}.tsv'
            content = b''.join(storage.chunks(ref['path']))
            if hashlib.sha256(content).hexdigest() != ref['sha256']:
                raise ManifestError('TRAIN similarity input hash differs')
            path.write_bytes(content)
            # Parent directories contain other partition files. Bind each complete
            # original directory only after fetching it, not to a partial cache.
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM experiment WHERE experiment_id=%s', (experiment_id,))
            experiment = cursor.fetchone()
        connection.rollback()
        if experiment['status'] == 'SUCCEEDED':
            report = read_json(storage, experiment['report_path'])
            return report | {'reused': True}
        if experiment['status'] == 'FAILED':
            raise ManifestError('Failed experiment needs a new explicit experiment identity')
        if experiment['status'] == 'CREATED':
            control.transition_experiment(experiment_id, 'RUNNING')
        selected, candidates = {}, {}
        for algorithm in ALGORITHMS:
            entries = [candidate(connection, config, storage, snapshots, control, tick, prepared, data,
                       algorithm, index, parameters, commit) for index, parameters in enumerate(grids()[algorithm])]
            candidates[algorithm] = entries
            selected[algorithm] = choose(entries)
            with connection.cursor() as cursor:
                cursor.execute('SELECT model_run_id FROM experiment_model WHERE experiment_id=%s AND algorithm=%s', (experiment_id, algorithm))
                old = cursor.fetchone()
            connection.rollback()
            if old and old['model_run_id'] == selected[algorithm]['model_run_id']:
                continue
            control.select_model(experiment_id, algorithm, selected[algorithm]['model_run_id'])
        # Champion tie-break uses validation only; algorithm order is the last tie.
        champion_entries = [selected[a] | {'grid_index': n, 'parameter_count': 0, 'algorithm': a}
                            for n, a in enumerate(ALGORITHMS)]
        champion = choose(champion_entries)['algorithm']
        selection = {'experiment_id': experiment_id, 'batch_id': batch_id, 'champion_algorithm': champion,
            'models': {a: {k: v[k] for k in ('model_run_id','hyperparameters','warm')} for a,v in selected.items()},
            'test_catalog_sha256': prepared['summary']['test']['catalog_sha256'],
            'test_cohorts': prepared['summary']['test']['cohorts'], 'test_truth_sha256': prepared['summary']['test']['truth_sha256']}
        # Publish selection before a TEST worker starts. Its bytes can never change.
        selection_path = f'{storage.root}/features/batch_id={batch_id}/experiment-{experiment_id}-selection.json'
        if storage.exists(selection_path):
            if read_json(storage, selection_path) != selection:
                raise ManifestError('Validation-frozen selection differs; refuse TEST')
        else:
            write_json(directory / 'selection.json', selection)
            storage.put(directory / 'selection.json', selection_path)
        test_results = {}
        for algorithm in ALGORITHMS:
            entry = selected[algorithm]
            task_name = 'test_' + experiment_id.replace('-', '') + '_' + algorithm
            digest = json_hash({'selection': selection, 'algorithm': algorithm})
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s ORDER BY attempt DESC LIMIT 1', (batch_id, task_name))
                previous = cursor.fetchone()
            connection.rollback()
            if previous and previous['config_sha256'] != digest:
                raise ManifestError('TEST retry configuration changed')
            if previous and previous['status'] in ('RUNNING','PENDING'):
                if process_alive(previous) or worker_alive(previous['run_id']):
                    raise LockBusy('Original TEST worker remains alive')
                if previous['status'] == 'PENDING':
                    control.start_task(previous['run_id'])
                final = f"{storage.root}/features/batch_id={batch_id}/run_id={previous['run_id']}"
                if storage.isdir(final):
                    recovered = read_json(storage, final + '/manifest.json')
                    if recovered['execution']['model_run_id'] != entry['model_run_id'] or recovered['execution']['selection_sha256'] != json_hash(selection):
                        raise ManifestError('Recovered TEST report differs from frozen selection')
                    control.finish_task(previous['run_id'],'SUCCEEDED',manifest_path=final+'/manifest.json',store=storage,heartbeat=tick)
                    previous = task_row(connection, previous['run_id'])
                else:
                    control.finish_task(previous['run_id'],'FAILED',error_code='INTERRUPTED_TEST',terminal_check=lambda _: 'FAILED')
                    previous = None
            if previous and previous['status'] == 'SUCCEEDED':
                report = read_json(storage, previous['output_manifest_path'])
                verify_manifest(report, storage, expected(previous), heartbeat=tick)
                result = report['execution']['test_result']
            else:
                run_id = control.create_task(batch_id, task_name, digest, commit, experiment_id=experiment_id)
                control.start_task(run_id, pid=os.getpid(), process_start_utc=process_start(os.getpid()))
                tick.run_id = run_id
                task = task_row(connection, run_id)
                local = directory / 'tests' / run_id
                shutil.copytree(entry['local'], local)
                try:
                    result = run_worker(config, control, tick, task, algorithm, data, local, mode='test')
                except LockBusy:
                    raise
                except Exception:
                    tick(force=True)
                    control.finish_task(run_id,'FAILED',error_code='TEST_SCORING_FAILED',terminal_check=lambda _: 'FAILED')
                    raise
                destination = f'{storage.root}/features/batch_id={batch_id}/run_id={run_id}'
                test_bundle = local / 'test-bundle'
                test_bundle.mkdir()
                shutil.copyfile(local / 'test-result.json', test_bundle / 'test-result.json')
                shutil.copyfile(local / 'test-driver.log', test_bundle / 'test-driver.log')
                (test_bundle / 'summary.tsv').write_text('status\tPASS\nalgorithm\t' + algorithm + '\n')
                (test_bundle / '_SUCCESS').touch()
                artifact = test_bundle / 'test-result.json'
                report = {'schema_version':'1.0','baseline_version':'HL-DOC-1.0.0','batch_id':batch_id,
                    'run_id':run_id,'status':'SUCCEEDED','code_commit':commit,'config_sha256':digest,
                    'created_at':task['created_at'].isoformat()+'Z','finished_at':utcnow().isoformat()+'Z',
                    'dataset':'model_summary','schema':['key','value'],
                    'inputs':[prepared['files']['test.truth.tsv'],prepared['files']['test.cohorts.json']],
                    'outputs':[{'path':destination+'/summary.tsv','sha256':sha256(test_bundle/'summary.tsv'),'rows':2}],
                    'artifacts':[{'path':destination+'/test-result.json','sha256':sha256(artifact),'bytes':artifact.stat().st_size}],
                    'record_count':2,'classification_counts':{},'application_id':None,'success_marker':destination+'/_SUCCESS',
                    'execution':{'model_run_id':entry['model_run_id'],'selection_sha256':json_hash(selection),'test_result':result}}
                write_json(test_bundle / 'manifest.json', report)
                upload_bundle(storage, test_bundle, destination, run_id)
                control.finish_task(run_id,'SUCCEEDED',manifest_path=destination+'/manifest.json',store=storage,heartbeat=tick)
            metric_rows(control, entry['model_run_id'], result['metrics'])
            test_results[algorithm] = result
        with connection.cursor() as cursor:
            cursor.execute('SELECT test_sealed_at FROM experiment WHERE experiment_id=%s',(experiment_id,))
            sealed = cursor.fetchone()['test_sealed_at']
        connection.rollback()
        if not sealed:
            control.seal_test(experiment_id)
        report = {'status':'PASS','batch_id':batch_id,'experiment_id':experiment_id,
            'champion_algorithm':champion,'selection_path':selection_path,
            'selected':{a:{k:v for k,v in entry.items() if k not in ('local','reused')} for a,entry in selected.items()},
            'candidate_count':sum(len(v) for v in candidates.values()),'test_results':test_results,
            'test_sealed':True,'code_commit':commit,'finished_at':utcnow().isoformat()+'Z'}
        report_path = f'{storage.root}/features/batch_id={batch_id}/experiment-{experiment_id}-report.json'
        if storage.exists(report_path):
            previous = read_json(storage, report_path)
            if {k: previous[k] for k in ('experiment_id','selected','test_results')} != {k:report[k] for k in ('experiment_id','selected','test_results')}:
                raise ManifestError('Existing experiment report differs')
            report = previous
        else:
            write_json(directory / 'report.json', report)
            storage.put(directory / 'report.json', report_path)
        control.transition_experiment(experiment_id,'SUCCEEDED',champion_algorithm=champion,report_path=report_path)
        return report | {'reused':False}
