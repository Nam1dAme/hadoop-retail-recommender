"""W07 FULLFIT, service Top10 and reproducible CPU/CUDA benchmark orchestration."""
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import statistics
import subprocess
from uuid import uuid4
import zipfile

from etl.ingest import ROOT, read_json, sha256, write_json
from etl.storage import HdfsStorage
from ml.data import TrainingData, write_tsv
from orchestration.control import ControlStore, ALGORITHMS, ControlError
from orchestration.database import named_lock, utcnow, LockBusy
from orchestration.experiment_data import experiment_task, materialize, json_hash
from orchestration.manifests import ManifestError, verify_manifest, DATASETS
from orchestration.migrations import require_current
from orchestration.pipeline import expected, task_row
from orchestration.runtime import Heartbeat, process_start, process_alive
from orchestration.training import SnapshotStore, fetch, run_worker, worker_alive, upload_bundle, grids


def canonical(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))


class Context:
    def __init__(self, connection, config, experiment_id, *, batch_id=None):
        require_current(connection)
        self.connection, self.config = connection, config
        self.control = ControlStore(connection)
        self.tick = Heartbeat(connection, self.control)
        self.storage = HdfsStorage(config)
        self.storage.heartbeat = self.tick
        self.snapshots = SnapshotStore(self.storage)
        self.commit = subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
        self.dirty = bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT))
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM experiment WHERE experiment_id=%s',(experiment_id,))
            self.experiment = cursor.fetchone()
            if (not self.experiment or self.experiment['status'] != 'SUCCEEDED'
                    or not self.experiment['test_sealed_at']
                    or batch_id is not None and self.experiment['batch_id'] != batch_id):
                raise ManifestError('W07 requires the matching completed, TEST-sealed W06 experiment')
            self.batch_id, self.experiment_id = self.experiment['batch_id'], experiment_id
            cursor.execute('SELECT status FROM data_batch WHERE batch_id=%s',(self.batch_id,))
            if cursor.fetchone()['status'] != 'SUCCEEDED':
                raise ManifestError('W07 requires a successful business batch')
            cursor.execute('''SELECT m.* FROM experiment_model e JOIN model_run m USING(model_run_id)
                WHERE e.experiment_id=%s''',(experiment_id,))
            self.selected = {r['algorithm']:r for r in cursor.fetchall()}
        connection.rollback()
        if set(self.selected) != set(ALGORITHMS) or any(
                m['purpose'] != 'EVAL' or m['status'] != 'SUCCEEDED'
                or m['batch_id'] != self.batch_id or m['experiment_id'] != experiment_id
                for m in self.selected.values()):
            raise ManifestError('Four successful, consistently selected EVAL models are required')
        self.directory = Path(config['paths']['work_dir'])/'w07-runs'/experiment_id/str(uuid4())
        self.directory.mkdir(parents=True,mode=0o700)
        preparation = self.query_task(experiment_task(experiment_id), succeeded=True)
        if not preparation:
            raise ManifestError('Missing W05 experiment preparation')
        pack = self.download(str(PurePosixPath(preparation['output_manifest_path']).parent))
        manifest = json.loads((pack/'manifest.json').read_text())
        self.verify_run(manifest, preparation)
        self.prepared = manifest['execution']['result']
        self.data = materialize(self.snapshots,self.prepared,self.directory/'data',self.tick)
        self.train = TrainingData.load(self.data/'fullfit')
        self.report = read_json(self.storage,self.experiment['report_path'])
        for algorithm, model in self.selected.items():
            if self.report['selected'][algorithm]['model_run_id'] != model['model_run_id']:
                raise ManifestError('Experiment report differs from selected models')
        self.loaded_models = {}

    def query_task(self, name, *, succeeded=False):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s'
                           + (" AND status='SUCCEEDED'" if succeeded else '')
                           + ' ORDER BY attempt DESC LIMIT 1',(self.batch_id,name))
            row = cursor.fetchone()
        self.connection.rollback()
        return row

    def download(self, remote):
        # Each binding is a fresh complete download during this invocation.
        for prefix, local in self.snapshots.bindings:
            if prefix.rstrip('/') == remote:
                return local
        local = fetch(self.storage,remote,self.directory/'downloads')
        self.snapshots.bind(remote,local)
        return local

    def cache_inputs(self, manifest):
        for ref in manifest['inputs']:
            if self.snapshots.local(ref['path']) is None:
                self.download(str(PurePosixPath(ref['path']).parent))

    def verify_run(self, manifest, task):
        self.cache_inputs(manifest)
        verify_manifest(manifest,self.snapshots,expected(task),heartbeat=self.tick)

    def model_bundle(self, model):
        if model['model_run_id'] in self.loaded_models:
            return self.loaded_models[model['model_run_id']]
        local = self.download(model['artifact_path'])
        manifest = json.loads((local/'manifest.json').read_text())
        self.cache_inputs(manifest)
        verify_manifest(manifest,self.snapshots,{k:model[k] for k in (
            'batch_id','model_run_id','experiment_id','algorithm','purpose','config_sha256',
            'mapping_sha256','environment_sha256','code_commit')},heartbeat=self.tick)
        task = task_row(self.connection,model['run_id'])
        if task['status'] != 'SUCCEEDED':
            raise ManifestError('Successful model requires a successful task')
        self.verify_run(json.loads((local/'task-manifest.json').read_text()),task)
        parameters = json.loads((local/'config.json').read_text())
        if model['purpose'] == 'EVAL':
            if (parameters['hyperparameters'] not in grids()[model['algorithm']]
                    or parameters['hyperparameters'] != self.report['selected'][model['algorithm']]['hyperparameters']):
                raise ManifestError('Selected hyperparameters differ from frozen W06 evidence')
        self.loaded_models[model['model_run_id']] = (local,parameters)
        return local,parameters

    def stage(self, name):
        task = self.query_task(name,succeeded=True)
        if not task:
            raise ManifestError('Missing successful W04 stage '+name)
        local = self.download(str(PurePosixPath(task['output_manifest_path']).parent))
        manifest = json.loads((local/'manifest.json').read_text())
        self.verify_run(manifest,task)
        return manifest

    def sources(self):
        return [*ROOT.glob('ml/*.py'),*ROOT.glob('orchestration/*.py'),ROOT/'config/experiment.toml']

    def task(self, name, params, destination_for_run):
        digest = json_hash(params)
        previous = self.query_task(name)
        if previous and previous['config_sha256'] != digest:
            raise ManifestError('Successful/retried W07 configuration cannot change under the same identity')
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM task_run WHERE status IN ('PENDING','RUNNING')")
            unfinished = cursor.fetchall()
        self.connection.rollback()
        if any(not previous or r['run_id'] != previous['run_id'] for r in unfinished):
            raise LockBusy('Another unfinished task requires authoritative handle verification')
        if previous and previous['status'] in ('PENDING','RUNNING'):
            if process_alive(previous) or worker_alive(previous['run_id']):
                raise LockBusy('Original W07 parent or worker is still alive')
            # Recover only an atomically published, fully verified bundle.
            with self.connection.cursor() as cursor:
                cursor.execute('SELECT * FROM model_run WHERE run_id=%s',(previous['run_id'],))
                model = cursor.fetchone()
            self.connection.rollback()
            destination = model['artifact_path'] if model else destination_for_run(previous['run_id'])
            if destination and self.storage.isdir(destination):
                local = self.download(destination)
                self.tick.run_id = previous['run_id']
                if previous['status'] == 'PENDING':
                    self.control.start_task(previous['run_id'])
                if model:
                    manifest = json.loads((local/'manifest.json').read_text())
                    self.cache_inputs(manifest)
                    if model['status'] == 'CREATED':
                        self.control.transition_model(model['model_run_id'],'RUNNING')
                    if model['status'] == 'RUNNING':
                        self.control.transition_model(model['model_run_id'],'SUCCEEDED',
                            manifest_path=destination+'/manifest.json',store=self.snapshots)
                manifest_path = destination+('/task-manifest.json' if model else '/manifest.json')
                self.control.finish_task(previous['run_id'],'SUCCEEDED',manifest_path=manifest_path,
                                         store=self.snapshots,heartbeat=self.tick)
                self.tick.run_id = None
                previous = task_row(self.connection,previous['run_id'])
            else:
                if previous['status'] == 'PENDING':
                    self.control.start_task(previous['run_id'])
                if model and model['status'] in ('CREATED','RUNNING'):
                    if model['status'] == 'CREATED':
                        self.control.transition_model(model['model_run_id'],'RUNNING')
                    self.control.transition_model(model['model_run_id'],'FAILED')
                self.control.finish_task(previous['run_id'],'FAILED',error_code='INTERRUPTED_W07',terminal_check=lambda _: 'FAILED')
                previous = None
        if previous and previous['status'] == 'SUCCEEDED':
            local = self.download(str(PurePosixPath(previous['output_manifest_path']).parent))
            self.verify_run(json.loads((local/PurePosixPath(previous['output_manifest_path']).name).read_text()),previous)
            return previous,local
        if self.dirty:
            raise ManifestError('Commit the W07 implementation before formal execution')
        run_id = self.control.create_task(self.batch_id,name,digest,self.commit,experiment_id=self.experiment_id)
        self.control.start_task(run_id,pid=os.getpid(),process_start_utc=process_start(os.getpid()))
        self.tick.run_id = run_id
        self.tick(force=True)
        return task_row(self.connection,run_id),None

    def bundle(self, task, work, destination, inputs, result, *, model=None):
        bundle = work/'bundle'
        bundle.mkdir()
        for source in work.rglob('*'):
            if (not source.is_file() or bundle in source.parents or 'spark-tmp' in source.relative_to(work).parts
                    or source.name == 'progress.json' or source.name.endswith('.crc')):
                continue
            target = bundle/source.relative_to(work)
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(source,target)
        sources = self.sources()
        hashes = {str(p.relative_to(ROOT)):sha256(p) for p in sources}
        with zipfile.ZipFile(bundle/'source-code.zip','w',compression=zipfile.ZIP_DEFLATED) as archive:
            for name in sorted(hashes):
                archive.write(ROOT/name,name)
        canonical(bundle/'result.json',result)
        (bundle/'summary.tsv').write_text('status\tPASS\nrun_id\t'+task['run_id']+'\n')
        (bundle/'_SUCCESS').touch()
        common = {'schema_version':'1.0','baseline_version':'HL-DOC-1.0.0','batch_id':self.batch_id,
                  'status':'SUCCEEDED','code_commit':task['code_commit'],'code_dirty':False,
                  'source_code_sha256':hashes,'config_sha256':task['config_sha256'],
                  'created_at':task['created_at'].isoformat()+'Z','finished_at':utcnow().isoformat()+'Z'}
        def artifacts():
            return [{'path':destination+'/'+str(p.relative_to(bundle)),'sha256':sha256(p),'bytes':p.stat().st_size}
                    for p in sorted(bundle.rglob('*')) if p.is_file()]
        if model:
            manifest = common | {k:model[k] for k in ('model_run_id','experiment_id','algorithm','purpose',
                        'mapping_sha256','environment_sha256')}
            manifest |= {'inputs':inputs,'artifacts':artifacts()}
            write_json(bundle/'manifest.json',manifest)
        output_name = 'recommendations.tsv' if (bundle/'recommendations.tsv').is_file() else 'summary.tsv'
        output = {'path':destination+'/'+output_name,'sha256':sha256(bundle/output_name),
                  'rows':result['coverage']['recommendation_count'] if output_name == 'recommendations.tsv' else 2}
        dataset = 'recommendations' if output_name == 'recommendations.tsv' else 'model_summary'
        task_manifest = common | {'run_id':task['run_id'],'dataset':dataset,'schema':DATASETS[dataset].split(','),
            'inputs':inputs,'outputs':[output],'artifacts':[a for a in artifacts() if a['path'] != output['path']],
            'record_count':output['rows'],'classification_counts':{},'application_id':None,
            'success_marker':destination+'/_SUCCESS','execution':{'result':result}}
        name = 'task-manifest.json' if model else 'manifest.json'
        write_json(bundle/name,task_manifest)
        upload_bundle(self.storage,bundle,destination,task['run_id'])
        downloaded = self.download(destination)
        self.verify_run(json.loads((downloaded/name).read_text()),task)
        if model:
            self.control.transition_model(model['model_run_id'],'SUCCEEDED',
                manifest_path=destination+'/manifest.json',store=self.snapshots)
        self.control.finish_task(task['run_id'],'SUCCEEDED',manifest_path=destination+'/'+name,
                                store=self.snapshots,heartbeat=self.tick)
        self.tick.run_id = None
        return result

    def run(self, task, params, algorithm, mode, inputs, destination, *, model=None):
        work = self.directory/'workers'/task['run_id']
        work.mkdir(parents=True)
        canonical(work/'config.json',params)
        if model:
            shutil.copyfile(self.data/'fullfit/mapping.tsv',work/'mapping.tsv')
            shutil.copyfile(ROOT/'uv.lock',work/'environment.lock')
            write_json(work/'inputs.json',{'inputs':inputs,'training_context':self.prepared['summary']['contexts']['fullfit'],
                                          'source_eval_model_run_id':params['source_eval_model_run_id']})
        before = {str(p.relative_to(ROOT)):sha256(p) for p in self.sources()}
        try:
            worker = run_worker(self.config,self.control,self.tick,task,algorithm,self.data,work,mode=mode,
                                model_id=model['model_run_id'] if model else None,module='ml.w07_worker')
            if any(sha256(ROOT/n) != h for n,h in before.items()):
                raise ManifestError('Implementation changed while the W07 worker ran')
            result = {'status':'PASS','batch_id':self.batch_id,'experiment_id':self.experiment_id,
                      'run_id':task['run_id'],'algorithm':algorithm,'mode':mode,'artifact_path':destination,
                      'manifest_path':destination+('/task-manifest.json' if model else '/manifest.json'),
                      'code_commit':task['code_commit'],'worker':worker}
            if model:
                result['model_run_id'] = model['model_run_id']
            if mode == 'recommend':
                result['coverage'] = worker['coverage']
            return self.bundle(task,work,destination,inputs,result,model=model)
        except LockBusy:
            raise
        except Exception:
            # Published evidence is recoverable; never mark it failed because a DB commit was interrupted.
            if not self.storage.isdir(destination):
                if model:
                    self.control.transition_model(model['model_run_id'],'FAILED')
                self.control.finish_task(task['run_id'],'FAILED',error_code='W07_WORKER_FAILED',terminal_check=lambda _: 'FAILED')
            raise


@contextmanager
def session(connection,config,experiment_id,*,batch_id=None):
    with named_lock(connection):
        context = Context(connection,config,experiment_id,batch_id=batch_id)
        yield context


def train_fullfit(connection,config,batch_id,experiment_id,algorithm,device,*,dry_run=False):
    algorithm,device = algorithm.upper(),device.lower()
    if algorithm not in ALGORITHMS or device != ('cuda' if algorithm == 'NCF' else 'cpu'):
        raise ValueError('FULLFIT requires CUDA for NCF and CPU for POP/ITEMCF/ALS')
    with session(connection,config,experiment_id,batch_id=batch_id) as ctx:
        selected = ctx.selected[algorithm]
        source,source_params = ctx.model_bundle(selected)
        hyperparameters = dict(source_params['hyperparameters'])
        if algorithm == 'NCF':
            best = json.loads((source/'worker-result.json').read_text())['details']['best_epoch']
            if selected['best_epoch'] != best or type(best) is not int or not 1 <= best <= 20:
                raise ManifestError('Selected NCF best epoch is inconsistent')
            for key in ('max_epochs','patience','min_delta'):
                hyperparameters.pop(key)
            hyperparameters |= {'epochs':best,'early_stopping':False}
        inputs = [ctx.prepared['files']['fullfit.'+n] for n in ('mapping.tsv','positive_interactions.tsv','popularity.tsv')]
        if algorithm == 'ITEMCF':
            similarity = ctx.stage('J08')['datasets']['similarity']['files']
            (ctx.data/'fullfit-similarity').mkdir()
            for number,ref in enumerate(similarity):
                shutil.copyfile(ctx.snapshots.local(ref['path']),ctx.data/'fullfit-similarity'/f'part-{number:05d}.tsv')
            inputs += similarity
        params = {'algorithm':algorithm,'purpose':'FULLFIT','batch_id':batch_id,'experiment_id':experiment_id,
                  'seed':42,'k':10,'mapping_sha256':ctx.train.mapping_sha256,
                  'environment_sha256':sha256(ROOT/'uv.lock'),'source_eval_model_run_id':selected['model_run_id'],
                  'source_eval_config_sha256':selected['config_sha256'],
                  'hyperparameters':hyperparameters,'inputs':inputs}
        if dry_run:
            return {'dry_run':True,'parameters':params,'plan':'full-history training without validation/test evaluation'}
        name = 'fullfit_'+experiment_id.replace('-','')+'_'+algorithm
        task,old = ctx.task(name,params,lambda _: '')
        if old:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM model_run WHERE run_id=%s',(task['run_id'],))
                model = cursor.fetchone()
            connection.rollback()
            if not model or model['status'] != 'SUCCEEDED' or model['purpose'] != 'FULLFIT':
                raise ManifestError('FULLFIT task/model association is incomplete')
            ctx.model_bundle(model)
            return json.loads((old/'result.json').read_text()) | {'reused':True}
        model_id = str(uuid4())
        destination = f'{ctx.storage.root}/models/model_run_id={model_id}'
        ctx.control.register_model(batch_id=batch_id,experiment_id=experiment_id,run_id=task['run_id'],
            algorithm=algorithm,purpose='FULLFIT',device=device.upper(),config_sha256=task['config_sha256'],
            mapping_sha256=ctx.train.mapping_sha256,artifact_path=destination,code_commit=ctx.commit,
            environment_sha256=sha256(ROOT/'uv.lock'),model_run_id=model_id)
        ctx.control.transition_model(model_id,'RUNNING')
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM model_run WHERE model_run_id=%s',(model_id,))
            model = cursor.fetchone()
        connection.rollback()
        return ctx.run(task,params,algorithm,'fullfit',inputs,destination,model=model) | {'reused':False}


def recommend_fullfit(connection,config,model_run_id,*,dry_run=False):
    from uuid import UUID
    if str(UUID(model_run_id)) != model_run_id:
        raise ValueError('Noncanonical model ID')
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM model_run WHERE model_run_id=%s',(model_run_id,))
        model = cursor.fetchone()
    connection.rollback()
    if not model or model['status'] != 'SUCCEEDED' or model['purpose'] != 'FULLFIT':
        raise ManifestError('Service recommendations require a successful FULLFIT model')
    with session(connection,config,model['experiment_id'],batch_id=model['batch_id']) as ctx:
        local,parameters = ctx.model_bundle(model)
        if model['mapping_sha256'] != ctx.train.mapping_sha256:
            raise ManifestError('Service mapping differs from FULLFIT')
        rfm = ctx.stage('J05')['datasets']['rfm']['files']
        from ml.data import read_tsv
        users = []
        for ref in rfm:
            users += [row[0] for row in read_tsv(ctx.snapshots.local(ref['path']),9)]
        if len(set(users)) != len(users) or not set(ctx.train.users) <= set(users):
            raise ManifestError('RFM users are duplicated or omit full-history customers')
        write_tsv(ctx.data/'service-users.tsv',((u,) for u in sorted(users,key=lambda u:u.encode())))
        target = ctx.data/'models'/model_run_id
        target.parent.mkdir()
        shutil.copytree(local,target)
        model_input = {'path':model['artifact_path']+'/manifest.json','sha256':sha256(local/'manifest.json'),
                       'rows':sum(1 for _ in (local/'manifest.json').open('rb'))}
        inputs = [model_input,*rfm,*[ctx.prepared['files']['fullfit.'+n]
                    for n in ('mapping.tsv','positive_interactions.tsv','popularity.tsv')]]
        params = {'model_run_id':model_run_id,'algorithm':model['algorithm'],'mapping_sha256':ctx.train.mapping_sha256,
                  'model_config_sha256':model['config_sha256'],'k':10,'inputs':inputs,
                  'expected_user_count':len(users)}
        if dry_run:
            return {'dry_run':True,'parameters':params,'plan':'all RFM customers, full-history exclusion, POP fallback'}
        destination = lambda run:f'{ctx.storage.root}/results/batch_id={model["batch_id"]}/run_id={run}'
        task,old = ctx.task('recommend_'+model_run_id.replace('-',''),params,destination)
        if old:
            return json.loads((old/'result.json').read_text()) | {'reused':True}
        return ctx.run(task,params,model['algorithm'],'recommend',inputs,destination(task['run_id'])) | {'reused':False}


def record_benchmark(connection,model_id,params,worker):
    """Exactly reuse matching rows; seal-aware benchmark writes never touch EVAL metrics."""
    if (worker['device'] != params['device'] or worker['repeat_index'] != params['repeat_index']
            or any(worker[k] != v for k,v in {'seed':42,'epochs':5,'batch_size':1024,'warmup_steps':20}.items())
            or any(not math.isfinite(worker[k]) or worker[k] <= 0 for k in ('train_seconds','samples_per_second'))):
        raise ManifestError('Invalid benchmark result')
    hardware = worker['hardware']
    history = worker['history']
    if (len(history) != 5 or [r['epoch'] for r in history] != list(range(1,6))
            or any(not math.isfinite(r['train_loss']) for r in history)
            or history[-1]['train_loss'] >= history[0]['train_loss']
            or worker['initial_state_sha256'] != worker['reset_state_sha256']
            or not worker['optimizer_reset'] or hardware['torch_cpu_threads'] != 4
            or hardware['tf32'] or hardware['amp']
            or [r['sha256'] for r in worker['sample_files']] != [r['sha256'] for r in params['sample_files']]
            or worker['samples_processed'] != sum(r['rows'] for r in params['sample_files'])
            or not math.isclose(worker['samples_per_second'],worker['samples_processed']/worker['train_seconds'],rel_tol=1e-12)):
        raise ManifestError('Benchmark protocol, samples, reset or loss evidence differs')
    for audit in (worker['first_batch_device_audit'],worker['warmup_device_audit']):
        if (any(not audit[k] for k in ('gradients_on_device','gradients_finite','nonzero_gradient','fp32'))
                or any(not audit[k].startswith(params['device'].lower()) for k in (
                    'model_device','input_device','labels_device','output_device','loss_device'))):
            raise ManifestError('Benchmark graph/device audit failed')
    driver = (hardware['nvidia_smi'] or '').split(',')
    values = {'model_run_id':model_id,**{k:worker[k] for k in ('device','repeat_index','seed','batch_size','epochs',
        'warmup_steps','train_seconds','samples_per_second','peak_memory_mib')},
        'hardware':json.dumps({'gpu_name':hardware['gpu_name'],'cpu_model':hardware.get('cpu_model'),
            'platform':hardware['platform'],
            'cpu_threads':hardware['torch_cpu_threads'],'cpu_peak_rss_mib':worker['cpu_peak_rss_mib'],
            'cpu_memory_scope':worker['cpu_memory_scope'],'gpu_memory_scope':worker['gpu_memory_scope']},sort_keys=True),
        'torch_version':hardware['torch_version'],'cuda_runtime':hardware['cuda_runtime'] if params['device']=='CUDA' else None,
        'driver_version':driver[1].strip() if params['device']=='CUDA' and len(driver)>1 else None,
        'dataset_sha256':params['dataset_sha256'],'config_sha256':params['comparison_config_sha256']}
    control = ControlStore(connection)
    with control.transaction() as cursor:
        cursor.execute('''SELECT m.*,e.status AS experiment_status,e.test_sealed_at,s.model_run_id AS selected_id
            FROM model_run m JOIN experiment e USING(experiment_id)
            JOIN experiment_model s ON s.experiment_id=m.experiment_id AND s.algorithm='NCF'
            WHERE m.model_run_id=%s FOR UPDATE''',(model_id,))
        model = cursor.fetchone()
        if (not model or model['purpose'] != 'EVAL' or model['algorithm'] != 'NCF'
                or model['status'] != 'SUCCEEDED' or model['experiment_status'] != 'SUCCEEDED'
                or not model['test_sealed_at'] or model['selected_id'] != model_id):
            raise ControlError('Benchmark must reference the selected, completed EVAL NCF')
        cursor.execute('SELECT * FROM gpu_benchmark WHERE model_run_id=%s AND device=%s AND repeat_index=%s',
                       (model_id,params['device'],params['repeat_index']))
        existing = cursor.fetchone()
        if existing:
            if any(existing[k] != value for k,value in values.items()):
                raise ManifestError('Persisted benchmark differs from immutable evidence')
            return existing['benchmark_id']
        benchmark_id = str(uuid4())
        columns = ['benchmark_id',*values,'created_at']
        cursor.execute('INSERT INTO gpu_benchmark ('+','.join(columns)+') VALUES ('+','.join(['%s']*len(columns))+')',
                       (benchmark_id,*values.values(),utcnow()))
        return benchmark_id


def benchmark(connection,config,experiment_id,model_run_id,*,dry_run=False):
    with session(connection,config,experiment_id) as ctx:
        model = ctx.selected['NCF']
        if model['model_run_id'] != model_run_id:
            raise ManifestError('Benchmark must use the validation-selected EVAL NCF')
        source,source_params = ctx.model_bundle(model)
        sample_dir = ctx.data/'benchmark-samples'
        sample_dir.mkdir()
        sample_inputs = [ctx.prepared['files'][n] for n in (
            'eval.mapping.tsv','eval.positive_interactions.tsv')]
        sample_params = {'model_run_id':model_run_id,'mapping_sha256':model['mapping_sha256'],
                         'seed':42,'epochs':5,'negatives':4,'inputs':sample_inputs}
        if dry_run:
            return {'dry_run':True,'sample_parameters':sample_params,'hyperparameters':source_params['hyperparameters'],
                    'plan':'five immutable TRAIN sample files; fresh CPU/CUDA children, five epochs and three repeats each'}
        destination_for_samples = lambda run:f'{ctx.storage.root}/features/batch_id={ctx.batch_id}/run_id={run}'
        sample_task,old_samples = ctx.task('benchmark_samples_'+experiment_id.replace('-',''),
                                          sample_params,destination_for_samples)
        if old_samples:
            sample_result = json.loads((old_samples/'result.json').read_text())
        else:
            from ml.sampling import sample_epoch,save_samples
            work = ctx.directory/'workers'/sample_task['run_id']
            (work/'samples').mkdir(parents=True)
            canonical(work/'config.json',sample_params)
            sample_destination = destination_for_samples(sample_task['run_id'])
            refs = []
            try:
                training = TrainingData.load(ctx.data/'eval')
                for epoch in range(1,6):
                    values,excluded = sample_epoch(training,epoch)
                    name = f'epoch-{epoch:03d}.tsv'
                    reference = save_samples(work/'samples'/name,values,epoch)
                    refs.append({'path':sample_destination+'/samples/'+name,**reference})
                    ctx.tick(force=True)
                sample_result = {'status':'PASS','batch_id':ctx.batch_id,'experiment_id':experiment_id,
                    'run_id':sample_task['run_id'],'mode':'benchmark_samples','sample_files':refs,
                    'mapping_sha256':training.mapping_sha256,'excluded_no_negative_users':excluded,
                    'manifest_path':sample_destination+'/manifest.json','artifact_path':sample_destination}
                ctx.bundle(sample_task,work,sample_destination,sample_inputs,sample_result)
            except Exception:
                if not ctx.storage.isdir(sample_destination):
                    ctx.control.finish_task(sample_task['run_id'],'FAILED',error_code='BENCHMARK_SAMPLES_FAILED',
                                            terminal_check=lambda _: 'FAILED')
                raise
        samples = sample_result['sample_files']
        for epoch,ref in enumerate(samples,1):
            require_path = ctx.snapshots.local(ref['path'])
            shutil.copyfile(require_path,sample_dir/f'epoch-{epoch:03d}.tsv')
        shared = {'model_run_id':model_run_id,'mapping_sha256':model['mapping_sha256'],
                  'hyperparameters':source_params['hyperparameters'],'seed':42,'epochs':5,'warmup_steps':20,
                  'batch_size':1024,'cpu_threads':4,'precision':'FP32','amp':False,'tf32':False,
                  'early_stopping':False,'sample_files':samples,'dataset_sha256':json_hash(samples),
                  'environment_sha256':sha256(ROOT/'uv.lock')}
        shared['comparison_config_sha256'] = json_hash(shared)
        inputs = [*samples,*[ctx.prepared['files'][n] for n in (
            'eval.mapping.tsv','eval.positive_interactions.tsv','eval.popularity.tsv','validation.truth.tsv','validation.cohorts.json')]]
        entries = []
        for repeat in range(1,4):
            for device in ('CPU','CUDA'):
                params = shared | {'device':device,'repeat_index':repeat}
                name = 'benchmark_'+experiment_id.replace('-','')+'_'+device+'_'+str(repeat)
                destination = lambda run:f'{ctx.storage.root}/features/batch_id={ctx.batch_id}/run_id={run}'
                task,old = ctx.task(name,params,destination)
                ctx.tick.run_id = task['run_id'] if not old else None
                if old:
                    result = json.loads((old/'result.json').read_text())
                else:
                    result = ctx.run(task,params,'NCF','benchmark',inputs,destination(task['run_id']))
                bid = record_benchmark(connection,model_run_id,params,result['worker'])
                entries.append(result | {'benchmark_id':bid,'reused':old is not None})
        cpu = statistics.median(e['worker']['train_seconds'] for e in entries if e['worker']['device']=='CPU')
        gpu = statistics.median(e['worker']['train_seconds'] for e in entries if e['worker']['device']=='CUDA')
        return {'status':'PASS','experiment_id':experiment_id,'model_run_id':model_run_id,'entries':entries,
                'cpu_median_seconds':cpu,'cuda_median_seconds':gpu,'speedup':cpu/gpu,
                'dataset_sha256':shared['dataset_sha256'],'comparison_config_sha256':shared['comparison_config_sha256'],
                'reused':all(e['reused'] for e in entries)}
