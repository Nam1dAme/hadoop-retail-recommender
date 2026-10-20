#!/usr/bin/env python3
"""Read-only W07 audit: real artifacts, six repeats, FULLFIT and every service row."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess
import tempfile
import zipfile
import numpy as np

from etl.ingest import ROOT, sha256, write_json
from ml.data import TrainingData, read_tsv
from ml.models import load_model
from ml.evaluation import recommend, evaluate
from ml.sampling import sample_epoch, save_samples
from ml.worker import labels
from orchestration.database import connect
from orchestration.manifests import ManifestError
from orchestration.settings import load_config
from orchestration.w07 import session
from orchestration.experiment_data import json_hash


def require(condition,message):
    if not condition:
        raise ManifestError(message)


def sources(directory,manifest,tick):
    with zipfile.ZipFile(directory/'source-code.zip') as archive:
        require(set(archive.namelist())==set(manifest['source_code_sha256']),'Source archive membership differs')
        for name,digest in manifest['source_code_sha256'].items():
            tick()
            content=archive.read(name)
            committed=subprocess.check_output(['git','show',manifest['code_commit']+':'+name],cwd=ROOT)
            require(hashlib.sha256(content).hexdigest()==digest and content==committed,'Archived source differs from its code commit')


def samples(directory,train,history,tick):
    """Rebuild seed+epoch samples independently of the saved sample audit fields."""
    evidence=[]
    with tempfile.TemporaryDirectory(dir=ROOT/'var',prefix='w07-samples-') as temporary:
        path=Path(temporary)/'expected.tsv'
        for row in history:
            tick()
            values,excluded=sample_epoch(train,row['epoch'])
            reference=save_samples(path,values,row['epoch'])
            actual=directory/'samples'/f"epoch-{row['epoch']:03d}.tsv"
            require(sha256(actual)==reference['sha256'],'Actual sample bytes/order differ from seed+epoch regeneration')
            require(reference['rows']==row['sample_count'] and excluded==row['excluded_no_negative_users'],
                    'Sample count or excluded users differs')
            evidence.append({'epoch':row['epoch'],**reference,'excluded_no_negative_users':excluded})
    return evidence


def verify(connection,config,experiment_id):
    from ml.service_recommendations import REASONS
    with session(connection,config,experiment_id) as ctx:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM model_run WHERE experiment_id=%s AND purpose='FULLFIT' AND status='SUCCEEDED'",(experiment_id,))
            models=cursor.fetchall()
            cursor.execute('SELECT * FROM gpu_benchmark WHERE model_run_id=%s ORDER BY repeat_index,device',(ctx.selected['NCF']['model_run_id'],))
            benchmarks=cursor.fetchall()
            cursor.execute("SELECT COUNT(*) AS n FROM task_run WHERE status IN ('PENDING','RUNNING')")
            active=cursor.fetchone()['n']
        connection.rollback()
        require(active==0,'Unfinished tasks remain')
        require(Counter(m['algorithm'] for m in models)==Counter({a:1 for a in REASONS}),
                'Exactly four successful FULLFIT algorithms are required')
        require(len(benchmarks)==6 and {(r['device'],r['repeat_index']) for r in benchmarks}==
                {(d,r) for d in ('CPU','CUDA') for r in range(1,4)},'Six CPU/CUDA repeats are required')
        stage=ctx.stage('J05')
        users=sorted([row[0] for ref in stage['datasets']['rfm']['files']
                      for row in read_tsv(ctx.snapshots.local(ref['path']),9)],key=lambda u:u.encode())
        require(len(set(users))==len(users),'Duplicate RFM customers')
        training,full=TrainingData.load(ctx.data/'eval'),ctx.train
        eval_source,eval_params=ctx.model_bundle(ctx.selected['NCF'])
        selected_epochs=json.loads((eval_source/'worker-result.json').read_text())['details']['best_epoch']
        model_evidence=[]
        for model in models:
            ctx.tick()
            directory,params=ctx.model_bundle(model)
            manifest=json.loads((directory/'manifest.json').read_text())
            sources(directory,manifest,ctx.tick)
            algorithm=model['algorithm']
            require(model['mapping_sha256']==full.mapping_sha256 and model['mapping_sha256']!=training.mapping_sha256,
                    'FULLFIT mapping is mixed with EVAL')
            require(params['source_eval_model_run_id']==ctx.selected[algorithm]['model_run_id'],
                    'FULLFIT did not use the selected EVAL configuration')
            expected_hyper=dict(ctx.report['selected'][algorithm]['hyperparameters'])
            if algorithm=='NCF':
                for key in ('max_epochs','patience','min_delta'):
                    expected_hyper.pop(key)
                expected_hyper|={'epochs':selected_epochs,'early_stopping':False}
            require(params['hyperparameters']==expected_hyper,'FULLFIT hyperparameters differ from validation selection')
            require(all('/fullfit.' in ref['path'] or '/similarity/' in ref['path'] for ref in manifest['inputs']),
                    'FULLFIT consumed EVAL/validation/test inputs')
            require(not (directory/'metrics.json').exists(),'FULLFIT must not claim validation/test metrics')
            with connection.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) AS n FROM model_metric WHERE model_run_id=%s',(model['model_run_id'],))
                require(cursor.fetchone()['n']==0,'FULLFIT wrote evaluation metrics')
                cursor.execute('SELECT * FROM model_epoch WHERE model_run_id=%s ORDER BY epoch',(model['model_run_id'],))
                epochs=cursor.fetchall()
            connection.rollback()
            worker=json.loads((directory/'fullfit-result.json').read_text())
            require(worker['status']=='PASS' and worker['purpose']=='FULLFIT' and worker['details']['reload_max_error']<=1e-6,
                    'FULLFIT save/load evidence failed')
            details={'algorithm':algorithm,'model_run_id':model['model_run_id'],'run_id':model['run_id'],
                     'artifact_path':model['artifact_path'],'code_commit':model['code_commit'],
                     'source_eval_model_run_id':params['source_eval_model_run_id'],'hyperparameters':params['hyperparameters'],
                     'reload_max_error':worker['details']['reload_max_error']}
            if algorithm=='NCF':
                history=worker['details']['epochs']
                require(model['device']=='CUDA' and len(history)==selected_epochs==len(epochs)
                        and [r['epoch'] for r in history]==list(range(1,selected_epochs+1)),
                        'FULLFIT NCF did not run the selected fixed epoch count')
                require(history[-1]['train_loss']<history[0]['train_loss'] and not worker['details']['early_stopping'],
                        'FULLFIT NCF loss/early-stop protocol differs')
                audit=worker['details']['first_batch_device_audit']
                require(all(audit[k] for k in ('gradients_on_device','gradients_finite','nonzero_gradient','fp32'))
                        and all(audit[k].startswith('cuda') for k in ('model_device','input_device','labels_device','output_device','loss_device')),
                        'FULLFIT lacks real CUDA forward/backward evidence')
                serialized=list(read_tsv(directory/'training_history.tsv',5))
                require(len(serialized)==len(history),'Incomplete FULLFIT history TSV')
                for actual,row,tsv in zip(epochs,history,serialized):
                    require(all(actual[k]==row[k] for k in ('epoch','train_loss','validation_ndcg','elapsed_seconds','peak_gpu_mib'))
                            and actual['validation_loss'] is None and row['validation_ndcg'] is None,
                            'FULLFIT history/DB mismatch or non-NULL validation')
                    require(tsv[2] is None and [int(tsv[0]),float(tsv[1]),float(tsv[3]),float(tsv[4])]==
                            [row[k] for k in ('epoch','train_loss','elapsed_seconds','peak_gpu_mib')],
                            'FULLFIT serialized history differs')
                details|={'cuda_audit':audit,'history':history,'samples':samples(directory,full,history,ctx.tick)}
            elif algorithm=='ALS':
                runtime=json.loads((directory/'als-runtime.json').read_text())
                actual=runtime['parameters']
                require(actual['rank']==expected_hyper['rank'] and actual['regParam']==expected_hyper['reg_param']
                        and actual['maxIter']==10 and actual['alpha']==10 and actual['implicitPrefs'] and actual['nonnegative']
                        and actual['seed']==42 and runtime['master']=='local[2]' and runtime['spark_version']=='3.5.7'
                        and runtime['java_version'].startswith('1.8.') and runtime['driver_memory']=='2g'
                        and runtime['shuffle_partitions']=='4','Actual FULLFIT Spark estimator/runtime differs')
                details['als_runtime']=runtime
            task=ctx.query_task('recommend_'+model['model_run_id'].replace('-',''),succeeded=True)
            require(task is not None,'Missing successful service recommendation task')
            root=ctx.download(str(Path(task['output_manifest_path']).parent))
            rec_manifest=json.loads((root/'manifest.json').read_text())
            ctx.verify_run(rec_manifest,task)
            sources(root,rec_manifest,ctx.tick)
            result=json.loads((root/'result.json').read_text())
            coverage=result['coverage']
            rows={}
            for mid,user,item,rank,score,reason,fallback in read_tsv(root/'recommendations.tsv',7):
                require(mid==model['model_run_id'] and user in set(users),'Wrong model/customer reference')
                rows.setdefault(user,[]).append((item,int(rank),float(score),reason,int(fallback)))
            serialized_coverage=list(read_tsv(root/'coverage.tsv',5))
            coverage_rows={u:tuple(map(int,values)) for u,*values in serialized_coverage}
            require(set(coverage_rows)==set(users)
                    and len(serialized_coverage)==len(coverage_rows)==coverage['coverage']['rows']==coverage['expected_user_count']==len(users),
                    'Service coverage omits zero-result/no-history RFM customers')
            scorer=load_model(algorithm,full,directory)
            totals=Counter()
            catalog=set(full.items)
            covered=set()
            for offset in range(0,len(users),32):
                predictions=recommend(full,users[offset:offset+32],scorer,tick=ctx.tick)
                for user in users[offset:offset+32]:
                    actual=rows.get(user,[])
                    expected=predictions[user]
                    require(len(actual)==len(expected)==min(10,len(catalog)-len(full.history.get(user,{}))),
                            'Incomplete or excessive service Top10')
                    seen=set()
                    for rank,(a,e) in enumerate(zip(actual,expected),1):
                        item,actual_rank,score,reason,fallback=a
                        expected_item,expected_score,expected_fallback=e
                        expected_fallback=int(expected_fallback and algorithm!='POP')
                        expected_reason='FALLBACK_POPULAR' if expected_fallback else REASONS[algorithm]
                        require(item==expected_item and actual_rank==rank and reason==expected_reason and fallback==expected_fallback
                                and math.isfinite(score) and math.isclose(score,expected_score,rel_tol=1e-6,abs_tol=1e-6),
                                'Service ranking, reason, fallback or score differs from loaded FULLFIT model')
                        require(item in catalog and item not in full.history.get(user,{}) and item not in seen,
                                'Service result contains seen/duplicate/noncatalog item')
                        seen.add(item);covered.add(item)
                    fallback_count=sum(r[4] for r in actual)
                    available=len(catalog)-len(full.history.get(user,{}))
                    require(coverage_rows[user]==(available,len(actual),fallback_count,int(user in full.history)),
                            'Per-customer coverage differs from full-history candidates')
                    totals['recommendation_count']+=len(actual)
                    totals['fallback_item_count']+=fallback_count
                    totals['fallback_user_count']+=int(fallback_count>0)
                    totals['recommended_user_count']+=int(bool(actual))
                    totals['no_candidate_user_count']+=int(available==0)
                    totals['short_list_user_count']+=int(len(actual)<10)
            require(all(coverage[k]==v for k,v in totals.items()) and coverage['covered_item_count']==len(covered)
                    and coverage['no_history_user_count']==len(set(users)-set(full.users)),
                    'Coverage summary differs from actual service rows')
            details|={'recommendation_run_id':task['run_id'],'recommendation_manifest':task['output_manifest_path'],
                      'coverage':coverage,'all_rows_recomputed_from_reloaded_model':True}
            model_evidence.append(details)
        benchmark_evidence=[]
        initial_states=set()
        data_hashes=set();config_hashes=set()
        truth,stats=labels(ctx.data/'validation')
        sample_task=ctx.query_task('benchmark_samples_'+experiment_id.replace('-',''),succeeded=True)
        require(sample_task is not None,'Missing immutable benchmark sample task')
        sample_root=ctx.download(str(Path(sample_task['output_manifest_path']).parent))
        sample_manifest=json.loads((sample_root/'manifest.json').read_text())
        ctx.verify_run(sample_manifest,sample_task)
        sources(sample_root,sample_manifest,ctx.tick)
        common_sample_refs=json.loads((sample_root/'result.json').read_text())['sample_files']
        require(len(common_sample_refs)==5,'Benchmark requires exactly five common sample files')
        # Regenerate the five common TRAIN sample files, including their order.
        with tempfile.TemporaryDirectory(dir=ROOT/'var',prefix='w07-benchmark-samples-') as temporary:
            for epoch in range(1,6):
                values,_=sample_epoch(training,epoch)
                reference=save_samples(Path(temporary)/'expected.tsv',values,epoch)
                require(sha256(sample_root/'samples'/f'epoch-{epoch:03d}.tsv')==reference['sha256']
                        and common_sample_refs[epoch-1]['sha256']==reference['sha256'],
                        'Common TRAIN samples differ from frozen seed/epoch protocol')
        for dbrow in benchmarks:
            task=ctx.query_task('benchmark_'+experiment_id.replace('-','')+'_'+dbrow['device']+'_'+str(dbrow['repeat_index']),succeeded=True)
            require(task is not None,'Missing successful benchmark task')
            root=ctx.download(str(Path(task['output_manifest_path']).parent))
            manifest=json.loads((root/'manifest.json').read_text())
            ctx.verify_run(manifest,task)
            sources(root,manifest,ctx.tick)
            params=json.loads((root/'config.json').read_text())
            result=json.loads((root/'benchmark-result.json').read_text())
            shared={k:v for k,v in params.items() if k not in ('device','repeat_index','comparison_config_sha256')}
            require(json_hash(shared)==params['comparison_config_sha256']==dbrow['config_sha256']
                    and json_hash(params['sample_files'])==params['dataset_sha256']==dbrow['dataset_sha256'],
                    'Benchmark comparison config/dataset hashes differ')
            require(params['model_run_id']==dbrow['model_run_id']==ctx.selected['NCF']['model_run_id']
                    and params['hyperparameters']==eval_params['hyperparameters'],'Benchmark used another EVAL configuration')
            require(params['epochs']==result['epochs']==dbrow['epochs']==5
                    and params['warmup_steps']==result['warmup_steps']==dbrow['warmup_steps']==20
                    and result['seed']==dbrow['seed']==42 and result['batch_size']==dbrow['batch_size']==1024,
                    'Benchmark protocol differs from T-13')
            for k in ('device','repeat_index','train_seconds','samples_per_second','peak_memory_mib'):
                require(dbrow[k]==result[k],'Benchmark DB row differs from immutable result')
            history=result['history']
            require([r['epoch'] for r in history]==list(range(1,6))
                    and all(math.isfinite(r['train_loss']) for r in history)
                    and history[-1]['train_loss']<history[0]['train_loss'],'Benchmark loss/epoch evidence failed')
            hardware=result['hardware']
            require(hardware['cpu_model'] and hardware['torch_cpu_threads']==4 and not hardware['amp'] and not hardware['tf32']
                    and hardware['deterministic_algorithms'] and hardware['torch_version']=='2.6.0+cu124',
                    'Benchmark precision/thread/determinism/runtime differs')
            require(result['initial_state_sha256']==result['reset_state_sha256']!=result['warmed_state_sha256']
                    and result['optimizer_reset'] and result['initial_torch_rng_sha256']==result['reset_torch_rng_sha256'],
                    'Warmup reset evidence failed')
            for audit in (result['first_batch_device_audit'],result['warmup_device_audit']):
                require(all(audit[k] for k in ('gradients_on_device','gradients_finite','nonzero_gradient','fp32'))
                        and all(audit[k].startswith(dbrow['device'].lower()) for k in
                            ('model_device','input_device','labels_device','output_device','loss_device')),
                        'Benchmark actual graph device differs')
            if dbrow['device']=='CUDA':
                require(result['peak_memory_mib']>0 and hardware['gpu_name'] and hardware['gpu_memory_bytes']>0
                        and dbrow['driver_version'] and dbrow['cuda_runtime']=='12.4','Missing actual CUDA hardware/memory evidence')
            require(result['cpu_peak_rss_mib']>0 and result['end_to_end_seconds']>=result['train_seconds'],
                    'Invalid CPU memory/end-to-end timing')
            require(result['samples_processed']==sum(r['rows'] for r in params['sample_files'])
                    and math.isclose(result['samples_per_second'],result['samples_processed']/result['train_seconds'],rel_tol=1e-12),
                    'Benchmark throughput calculation differs')
            for epoch,(expected_ref,actual_ref) in enumerate(zip(params['sample_files'],result['sample_files']),1):
                require(expected_ref==common_sample_refs[epoch-1]
                        and expected_ref['sha256']==actual_ref['sha256']==sha256(sample_root/'samples'/f'epoch-{epoch:03d}.tsv')
                        and expected_ref['rows']==actual_ref['rows'],'CPU/CUDA samples/order differ')
            require(result['validation_cohorts']==stats['cohorts'] and result['truth_sha256']==stats['truth_sha256']
                    and result['catalog_sha256']==stats['catalog_sha256'],'Benchmark validation cohort differs')
            import torch
            from ml.ncf import NeuMF,NeuralScorer,seed_everything
            seed_everything()
            neural=NeuMF(len(training.users),len(training.items)).to('cuda')
            from ml.neural_training import state_hash
            require(state_hash(neural)==result['initial_state_sha256'],'Benchmark reset initialization cannot be reproduced')
            neural.load_state_dict(torch.load(root/'benchmark-model.pt',map_location='cuda',weights_only=True))
            require(state_hash(neural)==result['final_state_sha256'],'Benchmark final weights differ from actual training state')
            scorer=NeuralScorer(training,neural,'cuda')
            predictions=recommend(training,sorted(truth,key=lambda u:u.encode()),scorer,tick=ctx.tick)
            metrics=evaluate(training,truth,stats,predictions,'VALIDATION')
            for actual,expected_metric in zip(metrics,result['validation_metrics']):
                require(all(actual[k]==v if type(v) not in (float,) else abs(actual[k]-v)<=1e-6
                            for k,v in expected_metric.items()),'Benchmark ranking metrics cannot be reproduced from saved weights')
            initial_states.add(result['initial_state_sha256'])
            data_hashes.add(dbrow['dataset_sha256']);config_hashes.add(dbrow['config_sha256'])
            benchmark_evidence.append({'benchmark_id':dbrow['benchmark_id'],'run_id':task['run_id'],
                'manifest_path':task['output_manifest_path'],'code_commit':task['code_commit'],**result})
        require(len(initial_states)==len(data_hashes)==len(config_hashes)==1,'CPU/CUDA comparisons do not share initialization/data/config')
        for device in ('CPU','CUDA'):
            repeats=[r for r in benchmark_evidence if r['device']==device]
            require(len({r['final_state_sha256'] for r in repeats})==1,
                    'Same-device deterministic benchmark repeats produced different weights')
        cpu=statistics.median(r['train_seconds'] for r in benchmark_evidence if r['device']=='CPU')
        cuda=statistics.median(r['train_seconds'] for r in benchmark_evidence if r['device']=='CUDA')
        return {'status':'PASS','baseline_version':'HL-DOC-1.0.0','experiment_id':experiment_id,'batch_id':ctx.batch_id,
                'champion_algorithm':ctx.experiment['champion_algorithm'],'test_sealed_at':str(ctx.experiment['test_sealed_at']),
                'test_ids':['T-09','T-11','T-12','T-13'],'fullfit_mapping_sha256':full.mapping_sha256,
                'eval_mapping_sha256':training.mapping_sha256,'expected_user_count':len(users),
                'fullfit_models':model_evidence,'benchmarks':benchmark_evidence,
                'cpu_median_seconds':cpu,'cuda_median_seconds':cuda,'speedup':cpu/cuda,
                'environment_sha256':sha256(ROOT/'uv.lock'),'active_tasks':active,
                'w08_release':'NOT_STARTED','w09_api':'NOT_STARTED','w10_ui':'NOT_STARTED'}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--db-defaults-file',required=True,type=Path)
    parser.add_argument('--experiment-id',required=True)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    config=load_config(args.config)
    connection=connect(config,args.db_defaults_file)
    try:
        result=verify(connection,config,args.experiment_id)
        write_json(args.output,result)
        print(json.dumps({'status':result['status'],'fullfit_models':len(result['fullfit_models']),
                          'benchmarks':len(result['benchmarks']),'speedup':result['speedup'],'output':str(args.output)}))
    finally:
        connection.close()


if __name__=='__main__':
    main()
