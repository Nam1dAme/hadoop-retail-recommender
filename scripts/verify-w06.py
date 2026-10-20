#!/usr/bin/env python3
"""Audit all real EVAL bundles, validation selection, CUDA history and sealed TEST rows."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import subprocess
import tempfile
import zipfile

from etl.ingest import ROOT, read_json, sha256, write_json
from etl.storage import HdfsStorage
from ml.data import TrainingData, read_tsv
from ml.evaluation import choose
from orchestration.control import ControlStore, ALGORITHMS
from orchestration.database import connect, named_lock
from orchestration.experiment_data import experiment_task, materialize, json_hash
from orchestration.manifests import verify_manifest, ManifestError
from orchestration.pipeline import expected
from orchestration.runtime import Heartbeat
from orchestration.settings import load_config
from orchestration.training import SnapshotStore, fetch, grids


def verify(connection, config, experiment_id):
    storage=HdfsStorage(config)
    control=ControlStore(connection)
    with named_lock(connection):
        tick=Heartbeat(connection,control)
        storage.heartbeat=tick
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM experiment WHERE experiment_id=%s',(experiment_id,))
            experiment=cursor.fetchone()
            if not experiment or experiment['status']!='SUCCEEDED' or not experiment['test_sealed_at']:
                raise ManifestError('Experiment has not completed and sealed TEST')
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1",(experiment['batch_id'],experiment_task(experiment_id)))
            preparation=cursor.fetchone()
            cursor.execute("SELECT * FROM model_run WHERE experiment_id=%s AND purpose='EVAL' ORDER BY algorithm,created_at",(experiment_id,))
            models=cursor.fetchall()
            cursor.execute('SELECT * FROM experiment_model WHERE experiment_id=%s',(experiment_id,))
            selected={r['algorithm']:r['model_run_id'] for r in cursor.fetchall()}
            cursor.execute('SELECT t.* FROM task_run t JOIN model_run m ON m.run_id=t.run_id WHERE m.experiment_id=%s',(experiment_id,))
            tasks={r['run_id']:r for r in cursor.fetchall()}
        connection.rollback()
        succeeded=[m for m in models if m['status']=='SUCCEEDED']
        if Counter(m['algorithm'] for m in succeeded)!=Counter({'POP':1,'ITEMCF':1,'ALS':4,'NCF':2}) or set(selected)!=set(ALGORITHMS):
            raise ManifestError('Missing mandatory algorithm/grid candidates')
        prepared=read_json(storage,preparation['output_manifest_path'])['execution']['result']
        report=read_json(storage,experiment['report_path'])
        selection=read_json(storage,report['selection_path'])
        if selection['champion_algorithm']!=experiment['champion_algorithm'] or selection['models'].keys()!=selected.keys():
            raise ManifestError('Frozen selection/champion differs from DB')
        bundles=[]
        with tempfile.TemporaryDirectory(dir=config['paths']['work_dir'],prefix='verify-w06-') as temporary:
            temporary=Path(temporary)
            snapshots=SnapshotStore(storage)
            preparation_root=str(Path(prepared['manifest_path']).parent)
            pack=fetch(storage,preparation_root,temporary/'downloads')
            snapshots.bind(preparation_root,pack)
            data=materialize(snapshots,prepared,temporary/'data',tick)
            train=TrainingData.load(data/'eval')
            item_indices={item:i for i,item in enumerate(train.items)}
            expected_positive={(u,item_indices[item]) for u,user in enumerate(train.users)
                if len(train.history[user])<len(train.items) for item in train.history[user]}
            candidates={a:[] for a in ALGORITHMS}
            for model in succeeded:
                tick()
                directory=fetch(storage,model['artifact_path'],temporary/'downloads')
                snapshots.bind(model['artifact_path'],directory)
                manifest=json.loads((directory/'manifest.json').read_text())
                verify_manifest(manifest,snapshots,{k:model[k] for k in ('batch_id','model_run_id','experiment_id','algorithm','purpose','config_sha256','mapping_sha256','environment_sha256','code_commit')},heartbeat=tick)
                task=tasks[model['run_id']]
                if task['status']!='SUCCEEDED':
                    raise ManifestError('Successful model lacks a successful task')
                verify_manifest(json.loads((directory/'task-manifest.json').read_text()),snapshots,expected(task),heartbeat=tick)
                parameters=json.loads((directory/'config.json').read_text())
                algorithm=model['algorithm']
                if parameters['hyperparameters'] not in grids()[algorithm] or model['mapping_sha256']!=train.mapping_sha256:
                    raise ManifestError('Candidate parameters/mapping differ from frozen experiment')
                result=json.loads((directory/'worker-result.json').read_text())
                if result['status']!='PASS' or result['details']['reload_max_error']>1e-6:
                    raise ManifestError('Model did not pass actual save/load score comparison')
                if result['cohorts']!=prepared['summary']['validation']['cohorts'] or result['catalog_sha256']!=prepared['summary']['validation']['catalog_sha256']:
                    raise ManifestError('Candidate evaluated on a different cohort/catalog')
                with connection.cursor() as cursor:
                    cursor.execute('SELECT * FROM model_metric WHERE model_run_id=%s',(model['model_run_id'],))
                    metrics=cursor.fetchall()
                    cursor.execute('SELECT * FROM model_epoch WHERE model_run_id=%s ORDER BY epoch',(model['model_run_id'],))
                    epochs=cursor.fetchall()
                connection.rollback()
                for metric in result['metrics']:
                    matches=[r for r in metrics if r['split_name']==metric['split_name'] and r['cohort']==metric['cohort']]
                    if len(matches)!=1 or any(matches[0][k]!=v for k,v in metric.items()):
                        raise ManifestError('Persisted validation metric differs from artifact')
                grid_index=grids()[algorithm].index(parameters['hyperparameters'])
                candidates[algorithm].append({'model_run_id':model['model_run_id'],'warm':result['metrics'][0],
                    'parameter_count':result['details']['parameter_count'],'grid_index':grid_index})
                details={'model_run_id':model['model_run_id'],'algorithm':algorithm,'grid_index':grid_index,
                    'mapping_sha256':model['mapping_sha256'],'code_commit':model['code_commit'],
                    'validation_warm':result['metrics'][0],'reload_max_error':result['details']['reload_max_error']}
                if algorithm=='ALS':
                    metadata=json.loads(next((directory/'spark_model/metadata').glob('part-*')).read_text())
                    params=metadata['paramMap']
                    if (params['rank']!=parameters['hyperparameters']['rank'] or params['regParam']!=parameters['hyperparameters']['reg_param']
                            or params['maxIter']!=10 or params['alpha']!=10 or not params['implicitPrefs'] or not params['nonnegative'] or params['seed']!=42):
                        raise ManifestError('Persisted Spark metadata differs from frozen ALS parameters')
                if algorithm=='NCF':
                    if model['device']!='CUDA' or not all(result['details']['first_batch_cuda_audit'].values()) or not result['details']['device'].startswith('cuda'):
                        raise ManifestError('Formal NCF lacks CUDA graph evidence')
                    history=result['details']['epochs']
                    if len(epochs)!=len(history) or [r['epoch'] for r in history]!=list(range(1,len(history)+1)) or not 1<=len(history)<=20:
                        raise ManifestError('NCF epoch records differ')
                    for actual,original in zip(epochs,history):
                        if any(actual[k]!=original[k] for k in ('epoch','train_loss','validation_ndcg','elapsed_seconds','peak_gpu_mib')):
                            raise ManifestError('Database epoch differs from real training history')
                    if not history[-1]['train_loss']<history[0]['train_loss'] or any(not math.isfinite(r['train_loss']) for r in history):
                        raise ManifestError('NCF loss is nonfinite or did not decrease')
                    for epoch in history:
                        positive=Counter()
                        negative=Counter()
                        for number,(u,i,label,ep,seed) in enumerate(read_tsv(directory/'samples'/f"epoch-{epoch['epoch']:03d}.tsv",5),1):
                            u,i,label,ep,seed=map(int,(u,i,label,ep,seed))
                            if not 0<=u<len(train.users) or not 0<=i<len(train.items) or ep!=epoch['epoch'] or seed!=42+ep or label not in (0,1):
                                raise ManifestError('Invalid NCF sample ID/epoch/seed')
                            seen=train.items[i] in train.history[train.users[u]]
                            if seen!=bool(label):
                                raise ManifestError('Negative sample contains a TRAIN purchase')
                            if label:
                                positive[(u,i)]+=1
                            else:
                                negative[u]+=1
                            if number%8192==0:
                                tick()
                        if set(positive)!=expected_positive or any(v!=1 for v in positive.values()):
                            raise ManifestError('NCF positives are not exactly one per train interaction')
                        counts=Counter(u for u,i in expected_positive)
                        if negative!={u:4*n for u,n in counts.items()}:
                            raise ManifestError('NCF negative ratio differs from four')
                    details['gpu_name']=result['hardware']['gpu_name']
                    details['best_epoch']=result['details']['best_epoch']
                    details['epoch_count']=len(history)
                    details['first_loss']=history[0]['train_loss']
                    details['last_loss']=history[-1]['train_loss']
                with zipfile.ZipFile(directory/'source-code.zip') as source:
                    for name,digest in manifest['source_code_sha256'].items():
                        committed=subprocess.check_output(['git','show',model['code_commit']+':'+name],cwd=ROOT)
                        if hashlib.sha256(source.read(name)).hexdigest()!=digest or hashlib.sha256(committed).hexdigest()!=digest:
                            raise ManifestError('Model source snapshot differs from committed implementation')
                if selected[algorithm]==model['model_run_id']:
                    test=report['test_results'][algorithm]
                    if test['cohorts']!=prepared['summary']['test']['cohorts'] or test['truth_sha256']!=prepared['summary']['test']['truth_sha256']:
                        raise ManifestError('TEST report uses different frozen labels/cohorts')
                    for metric in test['metrics']:
                        matches=[r for r in metrics if r['split_name']==metric['split_name'] and r['cohort']==metric['cohort']]
                        if len(matches)!=1 or any(matches[0][k]!=v for k,v in metric.items()):
                            raise ManifestError('Persisted TEST metric differs from sealed report')
                    if len(metrics)!=4:
                        raise ManifestError('Selected model requires exactly four cohort metrics')
                    details['test_metrics']=test['metrics']
                elif len(metrics)!=2:
                    raise ManifestError('Unselected candidate must have validation metrics only')
                bundles.append(details)
            for algorithm,entries in candidates.items():
                chosen=choose(entries)
                if chosen['model_run_id']!=selected[algorithm] or selection['models'][algorithm]['model_run_id']!=selected[algorithm]:
                    raise ManifestError('Selected model is not validation winner')
            champions=[choose(candidates[a])|{'algorithm':a,'parameter_count':0,'grid_index':n} for n,a in enumerate(ALGORITHMS)]
            if choose(champions)['algorithm']!=experiment['champion_algorithm']:
                raise ManifestError('Champion differs from validation-only tie rules')
            return {'status':'PASS','batch_id':experiment['batch_id'],'experiment_id':experiment_id,
                'champion_algorithm':experiment['champion_algorithm'],'test_sealed_at':str(experiment['test_sealed_at']),
                'successful_candidate_count':len(succeeded),'failed_candidate_count':len(models)-len(succeeded),
                'selected':selected,'models':bundles,
                'verification_code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--db-defaults-file',required=True,type=Path)
    parser.add_argument('--experiment-id',required=True)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    connection=None
    try:
        config=load_config(args.config)
        connection=connect(config,args.db_defaults_file)
        report=verify(connection,config,args.experiment_id)
    except Exception as exc:
        report={'status':'FAIL','error_type':type(exc).__name__}
        if isinstance(exc,ManifestError):
            report['error_message']=str(exc)
    finally:
        if connection is not None:
            connection.close()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    write_json(args.output,report)
    print(json.dumps({'status':report['status'],'evidence':str(args.output)}))
    return 0 if report['status']=='PASS' else 6


if __name__=='__main__':
    raise SystemExit(main())
