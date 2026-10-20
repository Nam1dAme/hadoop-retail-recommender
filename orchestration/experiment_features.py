"""TRAIN-only J06/J07/J08 on real YARN; isolated immutable runs per experiment."""
import os
from pathlib import Path, PurePosixPath
import shutil

from etl.ingest import ROOT, read_json, sha256, write_json
from orchestration.database import utcnow, LockBusy
from orchestration.manifests import DATASETS, ManifestError, fingerprint, verify_manifest
from orchestration.pipeline import STAGES, expected, hdfs_files, task_row
from orchestration.runtime import process_start, process_alive
from orchestration.yarn import Yarn


def train_features(connection, config, experiment_id, batch_id, control, storage, tick, commit):
    from orchestration.experiment_data import json_hash
    sources = {}
    with connection.cursor() as cursor:
        for stage in ('J01', 'J02'):
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1", (batch_id, stage))
            task = cursor.fetchone()
            if not task:
                raise ManifestError('Missing successful clean/order source')
            sources[stage] = task
    connection.rollback()
    outputs = {}
    for stage, task in sources.items():
        manifest = read_json(storage, task['output_manifest_path'])
        verify_manifest(manifest, storage, expected(task), heartbeat=tick)
        outputs.update(manifest['datasets'])
    jar = ROOT / 'mapreduce/target/retail-mapreduce-0.1.0-SNAPSHOT.jar'
    jar_hash = sha256(jar)
    yarn = Yarn(config)
    runs = []
    basket_count = None

    def parent(dataset):
        return str(PurePosixPath(outputs[dataset]['files'][0]['path']).parent)

    for stage in ('J06baskets', 'J06interactions', 'J07baskets', 'J07', 'J08'):
        task_name = 'ml_' + experiment_id.replace('-', '') + '_' + stage
        dependencies = {'J06baskets': ('clean', 'order_fact'), 'J06interactions': ('clean',),
                        'J07baskets': ('baskets',), 'J07': ('occurrences',), 'J08': ('item_pairs', 'item_frequency')}[stage]
        inputs = [f for name in dependencies for f in outputs[name]['files']]
        options = {'input': parent(dependencies[0])}
        if stage == 'J06baskets':
            options.update({'orders': parent('order_fact'), 'train-end': '2011-09-01'})
        if stage == 'J06interactions':
            options['train-end'] = '2011-09-01'
        if stage == 'J08':
            options.update({'cache': ','.join(f['path'] for f in outputs['item_frequency']['files']), 'baskets': basket_count})
        params = {'stage': stage, 'purpose': 'EVAL', 'experiment_id': experiment_id,
                  'train_end_exclusive': '2011-09-01', 'inputs': inputs, 'options': options,
                  'jar_sha256': jar_hash, 'reducers': 2}
        digest = json_hash(params)
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s ORDER BY attempt DESC LIMIT 1', (batch_id, task_name))
            previous = cursor.fetchone()
        connection.rollback()
        if previous and previous['config_sha256'] != digest:
            raise ManifestError('TRAIN feature inputs/parameters changed for the experiment')
        if previous and previous['status'] in ('RUNNING', 'PENDING'):
            from etl.ingest import live_child
            if process_alive(previous) or live_child(previous['run_id']):
                raise LockBusy('Original TRAIN feature process is alive')
            app = yarn.app(previous['application_id']) if previous['application_id'] else yarn.find('retail-' + task_name + '-' + previous['run_id'])
            if app and app['state'] not in ('FINISHED', 'FAILED', 'KILLED'):
                raise LockBusy('Original TRAIN YARN application is still active')
            if previous['status'] == 'PENDING':
                control.start_task(previous['run_id'])
            if app and not previous['application_id']:
                control.record_application(previous['run_id'], app['id'])
                previous['application_id'] = app['id']
            final = f"{storage.root}/features/batch_id={batch_id}/run_id={previous['run_id']}"
            if app and app['finalStatus'] == 'SUCCEEDED' and storage.isdir(final):
                control.finish_task(previous['run_id'], 'SUCCEEDED', manifest_path=final + '/manifest.json', store=storage, heartbeat=tick,
                                    terminal_check=lambda _: 'SUCCEEDED')
                previous = task_row(connection, previous['run_id'])
            else:
                control.finish_task(previous['run_id'], 'FAILED', error_code='INCOMPLETE_TRAIN_FEATURES', terminal_check=lambda _: 'FAILED')
                previous = None
        if previous and previous['status'] == 'SUCCEEDED':
            manifest = read_json(storage, previous['output_manifest_path'])
            verify_manifest(manifest, storage, expected(previous), heartbeat=tick)
            if manifest['execution']['parameters'] != params:
                raise ManifestError('TRAIN feature manifest parameters differ')
            outputs.update(manifest['datasets'])
            runs.append(previous['run_id'])
            if stage == 'J07baskets':
                basket_count = manifest['execution']['counters']['ELIGIBLE_BASKET_COUNT']
            continue
        run_id = control.create_task(batch_id, task_name, digest, commit, experiment_id=experiment_id)
        control.start_task(run_id, pid=os.getpid(), process_start_utc=process_start(os.getpid()))
        tick.run_id = run_id
        tick(force=True)
        local = Path(config['paths']['work_dir']) / 'experiments' / experiment_id / run_id
        local.mkdir(parents=True, mode=0o700)
        destination = f'{storage.root}/features/batch_id={batch_id}/run_id={run_id}'
        temporary = str(PurePosixPath(destination).parent) + '/.run_id=' + run_id + '.tmp'
        task = task_row(connection, run_id)
        published = False
        app = None
        try:
            shutil.copyfile(jar, local / 'business.jar')
            command = [str(Path(config['runtime']['hadoop_home']) / 'bin/hadoop'), 'jar', str(local / 'business.jar'),
                       'cn.hadooplab.retail.RetailJobs', '-Dmapreduce.framework.name=yarn', '-Dfs.defaultFS=' + config['hdfs']['uri'],
                       '--stage', stage, '--task-name', task_name, '--run-id', run_id, '--output', temporary, '--reducers', '2']
            for name, value in options.items():
                command.extend(['--' + name, str(value)])
            app, counters = yarn.run(command, task, control, tick, local,
                                     storage.environment | {'HADOOP_MAPRED_HOME': config['runtime']['hadoop_home']})
            paths = hdfs_files(storage, temporary)
            datasets = {}
            for name in STAGES[stage][1:]:
                files = [p for p in paths if str(PurePosixPath(p).parent) == temporary + '/' + name and PurePosixPath(p).name.startswith('part-')]
                if not files:
                    (local / 'empty.tsv').touch()
                    storage.mkdir(temporary + '/' + name)
                    storage.put(local / 'empty.tsv', temporary + '/' + name + '/part-00000')
                    files = [temporary + '/' + name + '/part-00000']
                refs = []
                for path in files:
                    sha, _, rows = fingerprint(storage, path, heartbeat=tick)
                    refs.append({'path': path.replace(temporary + '/', destination + '/', 1), 'sha256': sha, 'rows': rows})
                datasets[name] = {'schema': DATASETS[name].split(','), 'files': refs}
            write_json(local / 'config.json', params)
            shutil.copyfile(ROOT / 'uv.lock', local / 'environment.lock')
            artifacts = []
            for name in ('config.json', 'environment.lock', 'business.jar', 'driver.log'):
                p = local / name
                storage.put(p, temporary + '/' + name)
                artifacts.append({'path': destination + '/' + name, 'sha256': sha256(p), 'bytes': p.stat().st_size})
            primary = STAGES[stage][1]
            manifest = {'schema_version': '1.0', 'baseline_version': 'HL-DOC-1.0.0', 'batch_id': batch_id,
                'run_id': run_id, 'status': 'SUCCEEDED', 'code_commit': commit, 'code_dirty': False,
                'config_sha256': digest, 'created_at': task['created_at'].isoformat() + 'Z', 'finished_at': utcnow().isoformat() + 'Z',
                'dataset': primary, 'schema': DATASETS[primary].split(','), 'inputs': inputs,
                'outputs': datasets[primary]['files'], 'datasets': datasets, 'artifacts': artifacts,
                'record_count': sum(f['rows'] for f in datasets[primary]['files']), 'classification_counts': {},
                'application_id': app['id'], 'success_marker': destination + '/_SUCCESS',
                'execution': {'purpose': 'EVAL', 'experiment_id': experiment_id, 'parameters': params,
                              'application': app, 'counters': counters}}
            write_json(local / 'manifest.json', manifest)
            storage.put(local / 'manifest.json', temporary + '/manifest.json')
            tick(force=True)
            storage.move(temporary, destination)
            published = True
            control.finish_task(run_id, 'SUCCEEDED', manifest_path=destination + '/manifest.json', store=storage,
                                terminal_check=lambda _: 'SUCCEEDED', heartbeat=tick)
            outputs.update(datasets)
            runs.append(run_id)
            if stage == 'J07baskets':
                basket_count = counters['ELIGIBLE_BASKET_COUNT']
        except LockBusy:
            # Preserve RUNNING when ownership or observation was lost.
            raise
        except Exception:
            if not published:
                # Yarn.run waits for an authoritative terminal state before raising.
                current = task_row(connection, run_id)
                if not current['application_id'] or yarn.app(current['application_id'])['state'] in ('FINISHED', 'FAILED', 'KILLED'):
                    control.finish_task(run_id, 'FAILED', error_code='TRAIN_FEATURE_FAILED', terminal_check=lambda _: 'FAILED')
            raise
    tick.run_id = None
    return {'run_ids': runs, 'datasets': {k: outputs[k] for k in ('baskets', 'interactions', 'similarity', 'item_frequency', 'item_pairs')},
            'eligible_basket_count': basket_count, 'jar_sha256': jar_hash}
