"""Service coverage, independent benchmark reset and real isolated DB invariants."""
import copy
import json
from pathlib import Path
import numpy as np
import pytest
import torch

from ml.data import TrainingData, read_tsv, prepare, cohort, set_hash, write_tsv
from ml.evaluation import recommend
from ml.models import Popularity, ItemCF
from ml.service_recommendations import generate
from ml.neural_training import initialize, batches, train_epoch, state_hash
from ml.sampling import sample_epoch, save_samples
from ml.benchmark import run
from orchestration.w07 import record_benchmark, train_fullfit, recommend_fullfit
from orchestration.manifests import ManifestError
from orchestration.control import ControlError


def small():
    return TrainingData(['u','v'],['10001','10002','10003'],
        {'u':{'10001':(1,'2011-08-01')},'v':{'10002':(1,'2011-08-01')}},
        [('10001',3),('10002',2),('10003',1)],'a'*64)


@pytest.mark.parametrize('algorithm',['POP','ITEMCF'])
def test_service_covers_no_history_exhausted_and_short_lists(tmp_path,algorithm):
    train = small()
    train.history['u'] = {i:(1,'2011-08-01') for i in train.items}
    model = Popularity(train) if algorithm == 'POP' else ItemCF(train,{'10002':[('10003',.1)]})
    summary = generate(train,['v','no_history','u'],model,algorithm,'model',tmp_path)
    coverage = {r[0]:list(map(int,r[1:])) for r in read_tsv(tmp_path/'coverage.tsv',5)}
    assert coverage['u'] == [0,0,0,1]
    assert coverage['no_history'] == [3,3,0 if algorithm=='POP' else 3,0]
    assert summary['expected_user_count']==3 and summary['no_candidate_user_count']==1
    assert summary['recommendation_count']==5 and summary['short_list_user_count']==3
    rows=list(read_tsv(tmp_path/'recommendations.tsv',7))
    assert all(r[0]=='model' for r in rows)
    assert not any(r[1]=='u' for r in rows)
    if algorithm=='POP':
        assert all(r[5:] == ['POPULAR','0'] for r in rows)
    else:
        v=[r for r in rows if r[1]=='v']
        assert [r[2] for r in v]==['10003','10001']  # Main result precedes higher POP score.
        assert [r[5] for r in v]==['CO_PURCHASE','FALLBACK_POPULAR']
    with pytest.raises(ValueError,match='every'):
        generate(train,['u'],model,algorithm,'model',tmp_path)


def test_service_scores_in_bounded_blocks(tmp_path):
    train=small()
    calls=[]
    class Model:
        positive_only=False
        def score(self,users,items):
            calls.append((len(users),len(items)))
            return np.zeros((len(users),len(items)))
    generate(train,['u','v',*[str(i) for i in range(100)]],Model(),'ALS','id',tmp_path)
    assert calls and all(u<=32 and i<=512 for u,i in calls)


def test_neural_training_resets_and_audits_actual_cpu_graph():
    train=small()
    model,optimizer=initialize(train,.001,'cpu')
    initial=state_hash(model)
    samples,_=sample_epoch(train,1)
    loss,audit=train_epoch(model,optimizer,batches(samples),'cpu',audit=True)
    assert np.isfinite(loss) and audit['gradients_finite'] and audit['model_device']=='cpu'
    assert state_hash(model)!=initial and len(optimizer.state)>0
    repeated,new_optimizer=initialize(train,.001,'cpu')
    assert state_hash(repeated)==initial and len(new_optimizer.state)==0


def test_actual_cpu_benchmark_fixed_five_epochs_and_warmup_reset(tmp_path):
    train=small()
    sample_dir=tmp_path/'benchmark-samples'; sample_dir.mkdir()
    refs=[]
    for epoch in range(1,6):
        samples,_=sample_epoch(train,epoch)
        refs.append(save_samples(sample_dir/f'epoch-{epoch:03d}.tsv',samples,epoch))
    truth,stats=cohort(train,{'u':{'10002'},'v':{'10003'},'cold':{'10001'}})
    stats['truth_sha256']=set_hash((u,i) for u in sorted(truth) for i in sorted(truth[u]))
    params={'device':'CPU','epochs':5,'warmup_steps':20,'batch_size':1024,'repeat_index':1,
            'sample_files':refs,'hyperparameters':{'learning_rate':.001}}
    result=run(train,tmp_path,tmp_path,params,truth,stats)
    assert result['epochs']==5 and len(result['history'])==5
    assert result['initial_state_sha256']==result['reset_state_sha256']!=result['warmed_state_sha256']
    assert result['optimizer_reset'] and result['samples_processed']==50
    assert result['samples_per_second']==pytest.approx(50/result['train_seconds'])
    assert result['peak_memory_mib'] is None and result['cpu_peak_rss_mib']>0
    assert {r['cohort'] for r in result['validation_metrics']}=={'WARM','ALL_ELIGIBLE'}
    params['sample_files'][0]['sha256']='0'*64
    with pytest.raises(ValueError,match='hash'):
        run(train,tmp_path,tmp_path,params,truth,stats)


def test_invalid_fullfit_devices_and_eval_recommendation_rejected(db):
    from orchestration.settings import ConfigError
    for algorithm,device in [('NCF','cpu'),('ALS','cuda'),('POP','cuda')]:
        with pytest.raises(ConfigError,match='requires'):
            train_fullfit(db[0]['writer'],{},'batch','experiment',algorithm,device)
    with pytest.raises(ManifestError,match='FULLFIT'):
        recommend_fullfit(db[0]['writer'],{},'00000000-0000-0000-0000-000000000000')


def test_actual_fullfit_and_recommend_workers_need_no_evaluation_labels(db,tmp_path):
    from orchestration.control import ControlStore
    from orchestration.database import named_lock
    from orchestration.runtime import Heartbeat
    from orchestration.training import run_worker
    from orchestration.pipeline import task_row
    from orchestration.settings import load_config
    from test_database import batch,H,C
    data=tmp_path/'data'; (data/'fullfit').mkdir(parents=True)
    write_tsv(data/'fullfit/mapping.tsv',[('USER','u',0),('USER','v',1),('ITEM','10001',0),('ITEM','10002',1)])
    write_tsv(data/'fullfit/positive_interactions.tsv',[(0,0,1,'2011-12-01'),(1,1,1,'2011-12-02')])
    write_tsv(data/'fullfit/popularity.tsv',[('10001',2),('10002',1)])
    training=TrainingData.load(data/'fullfit')
    directory=tmp_path/'fullfit-worker'; directory.mkdir()
    (directory/'config.json').write_text(json.dumps({'mapping_sha256':training.mapping_sha256,'hyperparameters':{}}))
    control=ControlStore(db[0]['writer']); bid=batch(control); control.transition_batch(bid,'PROCESSING')
    rid=control.create_task(bid,'fullfit_worker',H,C); control.start_task(rid)
    with named_lock(control.connection):
        tick=Heartbeat(control.connection,control); tick.run_id=rid
        result=run_worker(load_config('config/development.toml'),control,tick,task_row(control.connection,rid),
                          'POP',data,directory,mode='fullfit',module='ml.w07_worker')
        assert result['purpose']=='FULLFIT' and 'metrics' not in result
        assert not (directory/'metrics.json').exists()
        model_id='00000000-0000-0000-0000-000000000001'
        import shutil
        shutil.copytree(directory,data/'models'/model_id)
        write_tsv(data/'service-users.tsv',[(u,) for u in ('u','v','no_history')])
        rec=tmp_path/'recommend-worker'; rec.mkdir()
        (rec/'config.json').write_text(json.dumps({'mapping_sha256':training.mapping_sha256,'model_run_id':model_id}))
        result=run_worker(load_config('config/development.toml'),control,tick,task_row(control.connection,rid),
                          'POP',data,rec,mode='recommend',module='ml.w07_worker')
        assert result['coverage']['expected_user_count']==3 and result['coverage']['recommendation_count']==4
        assert result['coverage']['fallback_item_count']==0


def test_benchmark_db_rows_exactly_reuse_and_reject_changes(db,tmp_path):
    # Fake artifact fixture is only for DB association tests; never GPU evidence.
    from test_database import test_four_model_selection_metrics_and_sealing
    test_four_model_selection_metrics_and_sealing(db,tmp_path)
    connection=db[0]['writer']
    with connection.cursor() as cur:
        cur.execute("SELECT model_run_id FROM model_run WHERE algorithm='NCF'")
        model_id=cur.fetchone()['model_run_id']
        cur.execute("SELECT model_run_id FROM model_run WHERE algorithm='ALS'")
        als=cur.fetchone()['model_run_id']
    connection.rollback()
    audit={k:'cpu' for k in ('model_device','input_device','labels_device','output_device','loss_device')}
    audit|={k:True for k in ('gradients_on_device','gradients_finite','nonzero_gradient','fp32')}
    params={'device':'CPU','repeat_index':1,'dataset_sha256':'c'*64,'comparison_config_sha256':'d'*64,
            'sample_files':[{'sha256':str(i)*64,'rows':10} for i in range(5)]}
    worker={'device':'CPU','repeat_index':1,'seed':42,'epochs':5,'batch_size':1024,'warmup_steps':20,
        'train_seconds':2.,'samples_per_second':25.,'peak_memory_mib':None,'samples_processed':50,
        'cpu_peak_rss_mib':100.,'cpu_memory_scope':'isolated fixture','gpu_memory_scope':'isolated fixture',
        'sample_files':params['sample_files'],'initial_state_sha256':'e'*64,'reset_state_sha256':'e'*64,
        'optimizer_reset':True,'first_batch_device_audit':audit,'warmup_device_audit':audit,
        'history':[{'epoch':i,'train_loss':1/i} for i in range(1,6)],
        'hardware':{'nvidia_smi':None,'gpu_name':None,'platform':'isolated fixture','torch_cpu_threads':4,
                    'torch_version':'fixture','tf32':False,'amp':False}}
    bid=record_benchmark(connection,model_id,params,worker)
    assert record_benchmark(connection,model_id,params,worker)==bid
    with pytest.raises(ControlError,match='selected'):
        record_benchmark(connection,als,params,worker)
    changed=copy.deepcopy(worker); changed['train_seconds']=1.; changed['samples_per_second']=50.
    with pytest.raises(ManifestError,match='differs'):
        record_benchmark(connection,model_id,params,changed)
    for key,value in [('train_seconds',float('nan')),('optimizer_reset',False)]:
        changed=copy.deepcopy(worker);changed[key]=value
        with pytest.raises(ManifestError):
            record_benchmark(connection,model_id,params,changed)
    with connection.cursor() as cur:
        cur.execute('SELECT COUNT(*) AS n FROM gpu_benchmark')
        assert cur.fetchone()['n']==1


def minimal_context(db,tmp_path):
    from orchestration.w07 import Context
    from orchestration.control import ControlStore
    from orchestration.runtime import Heartbeat
    from etl.storage import FileStorage
    from test_database import batch,C
    context=Context.__new__(Context)
    context.connection=db[0]['writer']; context.control=ControlStore(context.connection)
    context.batch_id=batch(context.control);context.control.transition_batch(context.batch_id,'PROCESSING')
    context.experiment_id=context.control.create_experiment(context.batch_id,'a'*64)
    context.commit=C;context.dirty=False
    context.storage=FileStorage(tmp_path/'archive')
    context.tick=Heartbeat(context.connection,context.control)
    return context


def test_w07_retry_preserves_dead_parent_record_and_creates_new_attempt(db,tmp_path):
    from orchestration.database import named_lock,utcnow
    from orchestration.experiment_data import json_hash
    from orchestration.pipeline import task_row
    context=minimal_context(db,tmp_path)
    params={'request':'isolated interruption test'}
    run=context.control.create_task(context.batch_id,'fullfit_isolated',json_hash(params),context.commit)
    missing_pid=4194303
    assert not Path(f'/proc/{missing_pid}').exists()
    context.control.start_task(run,pid=missing_pid,process_start_utc=utcnow())
    with named_lock(context.connection):
        new,local=context.task('fullfit_isolated',params,lambda _: '')
    assert local is None and new['attempt']==2 and new['parent_run_id']==run
    assert new['run_id']!=run and new['status']=='RUNNING'
    previous=task_row(context.connection,run)
    assert previous['status']=='FAILED' and previous['error_code']=='INTERRUPTED_W07'


def test_w07_live_parent_blocks_retry_without_using_heartbeat_age(db,tmp_path):
    import os
    from orchestration.database import named_lock,LockBusy
    from orchestration.runtime import process_start
    from orchestration.experiment_data import json_hash
    from orchestration.pipeline import task_row
    context=minimal_context(db,tmp_path)
    params={'request':'isolated live-parent test'}
    run=context.control.create_task(context.batch_id,'fullfit_isolated',json_hash(params),context.commit)
    context.control.start_task(run,pid=os.getpid(),process_start_utc=process_start(os.getpid()))
    with context.connection.cursor() as q:
        q.execute("UPDATE task_run SET heartbeat_at='2020-01-01' WHERE run_id=%s",(run,))
    context.connection.commit()
    with named_lock(context.connection):
        with pytest.raises(LockBusy,match='still alive'):
            context.task('fullfit_isolated',params,lambda _: '')
    assert task_row(context.connection,run)['status']=='RUNNING'


def test_independent_w07_sample_audit_rejects_seen_negatives_and_missing_positives(tmp_path):
    import importlib.util
    spec=importlib.util.spec_from_file_location('w07_verifier',Path(__file__).resolve().parents[1]/'scripts/verify-w07.py')
    verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
    training=small();values,_=sample_epoch(training,1)
    path=tmp_path/'samples.tsv';save_samples(path,values,1)
    audit=verifier.audit_samples(path,training,1)
    assert audit['positive_rows']==2 and audit['negative_rows']==8
    corrupted=values.copy()
    negative=int(np.flatnonzero(corrupted[:,2]==0)[0])
    user=int(corrupted[negative,0]);seen=next(iter(training.history[training.users[user]]))
    corrupted[negative,1]=training.items.index(seen)
    save_samples(path,corrupted,1)
    with pytest.raises(ManifestError,match='history'):
        verifier.audit_samples(path,training,1)
    positive=int(np.flatnonzero(values[:,2]==1)[0])
    save_samples(path,np.delete(values,positive,axis=0),1)
    with pytest.raises(ManifestError,match='Positives'):
        verifier.audit_samples(path,training,1)
