"""Transactional batch, task and experiment control. No training or auto-retry here."""
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
import re
import secrets
from uuid import uuid4
from orchestration.database import utcnow
from orchestration.manifests import verify_manifest
from orchestration.settings import BASELINE

ALGORITHMS = ('POP','ITEMCF','ALS','NCF')
BATCH_TRANSITIONS = {'REGISTERED':{'PROCESSING'},'PROCESSING':{'SUCCEEDED','FAILED'},'FAILED':{'PROCESSING'},'SUCCEEDED':set()}
EXPERIMENT_TRANSITIONS = {'CREATED':{'RUNNING'},'RUNNING':{'SUCCEEDED','FAILED'},'SUCCEEDED':set(),'FAILED':set()}


class ControlError(ValueError):
    pass


def digest(value):
    if not isinstance(value,str) or not re.fullmatch('[a-f0-9]{64}',value):
        raise ControlError('Invalid SHA-256')
    return value


def commit_id(value):
    if not isinstance(value,str) or not re.fullmatch('[a-f0-9]{40}',value):
        raise ControlError('A real source commit is required')
    return value


class ControlStore:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def transaction(self):
        self.connection.begin()
        try:
            with self.connection.cursor() as cursor:
                yield cursor
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    @staticmethod
    def row(cursor, table, key, value):
        cursor.execute(f'SELECT * FROM {table} WHERE {key}=%s FOR UPDATE',(value,))
        row=cursor.fetchone()
        if not row:
            raise ControlError('Unknown '+table)
        return row

    def register_batch(self, *, source_sha256, source_name, source_url, source_bytes,
                       raw_hdfs_path, normalized_hdfs_path, policy_version='retail-clean-v1', hdfs_root='/retail/v1'):
        digest(source_sha256)
        if type(source_bytes) is not int or source_bytes <= 0 or policy_version != 'retail-clean-v1':
            raise ControlError('Invalid source metadata/policy')
        batch_id='B'+utcnow().strftime('%Y%m%d%H%M%S')+secrets.token_hex(4)
        # Paths must be derived from the resulting ID by the caller template.
        raw=raw_hdfs_path.format(batch_id=batch_id)
        normalized=normalized_hdfs_path.format(batch_id=batch_id)
        if not hdfs_root.startswith('/') or '..' in hdfs_root.split('/') or hdfs_root=='/':
            raise ControlError('Invalid batch storage root')
        if raw != f'{hdfs_root}/raw/batch_id={batch_id}/source.xlsx' or normalized != f'{hdfs_root}/normalized/batch_id={batch_id}':
            raise ControlError('Noncanonical batch storage paths')
        now=utcnow()
        with self.transaction() as cursor:
            cursor.execute('''INSERT INTO data_batch
                (batch_id,source_sha256,source_name,source_url,source_bytes,policy_version,baseline_version,
                 schema_version,status,raw_hdfs_path,normalized_hdfs_path,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'1.0','REGISTERED',%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE batch_id=batch_id''',
                (batch_id,source_sha256,source_name,source_url,source_bytes,policy_version,BASELINE,raw,normalized,now,now))
            cursor.execute('SELECT * FROM data_batch WHERE source_sha256=%s AND policy_version=%s FOR UPDATE',
                           (source_sha256,policy_version))
            row=cursor.fetchone()
            if row['source_bytes'] != source_bytes:
                raise ControlError('Source hash/size conflict')
            return row

    def transition_batch(self,batch_id,status):
        with self.transaction() as cursor:
            row=self.row(cursor,'data_batch','batch_id',batch_id)
            if status not in BATCH_TRANSITIONS[row['status']]:
                raise ControlError('Illegal batch transition')
            if status == 'SUCCEEDED':
                counts=[row[k] for k in ('raw_rows','sale_rows','return_rows','rejected_rows')]
                if any(v is None for v in counts) or counts[0] != sum(counts[1:]) or counts[1] == 0:
                    raise ControlError('Missing or invalid batch quality counts')
                cursor.execute("SELECT COUNT(*) AS n FROM task_run WHERE batch_id=%s AND status IN ('PENDING','RUNNING')",(batch_id,))
                if cursor.fetchone()['n']:
                    raise ControlError('Batch still has unfinished tasks')
            cursor.execute('UPDATE data_batch SET status=%s,updated_at=%s WHERE batch_id=%s',(status,utcnow(),batch_id))

    def quality(self,batch_id,metrics, *, counts=None, report_path=None):
        allowed={'RAW_ROWS','SALE_ROWS','RETURN_ROWS','REJECTED_ROWS','DUPLICATE_EXCESS_ROWS',
            'NON_MERCHANDISE_SALE_ROWS','TRUNCATED_BASKETS','TRUNCATED_BASKET_ITEMS'}
        from orchestration.quality import REJECTIONS, WARNINGS
        allowed.update(v+'_ROWS' for v in REJECTIONS+WARNINGS)
        if any(k not in allowed or type(v) is not int or v < 0 for k,v in metrics.items()):
            raise ControlError('Unknown/invalid quality metric')
        with self.transaction() as cursor:
            row=self.row(cursor,'data_batch','batch_id',batch_id)
            if row['status'] == 'SUCCEEDED':
                raise ControlError('Successful batch quality is immutable')
            for name,value in metrics.items():
                cursor.execute('''INSERT INTO data_quality (batch_id,metric_name,metric_value,description,updated_at)
                    VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE metric_value=VALUES(metric_value),updated_at=VALUES(updated_at)''',
                    (batch_id,name,value,name,utcnow()))
            if counts is not None:
                if set(counts) != {'raw_rows','sale_rows','return_rows','rejected_rows'} or any(type(v) is not int or v<0 for v in counts.values()) or counts['raw_rows'] != sum(counts[k] for k in ('sale_rows','return_rows','rejected_rows')):
                    raise ControlError('Classification conservation failed')
                for key,value in counts.items():
                    if key.upper() in metrics and metrics[key.upper()] != value:
                        raise ControlError('Conflicting quality counts')
                cursor.execute('UPDATE data_batch SET raw_rows=%s,sale_rows=%s,return_rows=%s,rejected_rows=%s,quality_report_path=%s,updated_at=%s WHERE batch_id=%s',
                    (*(counts[k] for k in ('raw_rows','sale_rows','return_rows','rejected_rows')),report_path,utcnow(),batch_id))

    def create_task(self,batch_id,task_name,config_sha256,code_commit, *, experiment_id=None):
        digest(config_sha256); commit_id(code_commit)
        if not re.fullmatch('[a-zA-Z0-9_-]{1,64}',task_name):
            raise ControlError('Invalid task name')
        with self.transaction() as cursor:
            batch=self.row(cursor,'data_batch','batch_id',batch_id)
            if batch['status'] != 'PROCESSING':
                if batch['status'] != 'SUCCEEDED' or experiment_id is None:
                    raise ControlError('Batch must be PROCESSING or successful with an explicit experiment')
                experiment=self.row(cursor,'experiment','experiment_id',experiment_id)
                if experiment['batch_id'] != batch_id or experiment['status'] == 'FAILED':
                    raise ControlError('Downstream task requires a matching active/completed experiment')
            cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s ORDER BY attempt DESC LIMIT 1 FOR UPDATE',(batch_id,task_name))
            previous=cursor.fetchone()
            if previous and previous['status'] not in ('FAILED','CANCELLED'):
                raise ControlError('Task exists; inspect live handle or reuse successful artifacts')
            run_id=str(uuid4())
            cursor.execute('''INSERT INTO task_run (run_id,batch_id,parent_run_id,task_name,attempt,status,config_sha256,code_commit,baseline_version,created_at)
                VALUES (%s,%s,%s,%s,%s,'PENDING',%s,%s,%s,%s)''',
                (run_id,batch_id,previous['run_id'] if previous else None,task_name,previous['attempt']+1 if previous else 1,config_sha256,code_commit,BASELINE,utcnow()))
            return run_id

    def start_task(self,run_id, *, pid=None,process_start_utc=None,application_id=None):
        if (pid is None) != (process_start_utc is None):
            raise ControlError('PID requires process start identity')
        if application_id is not None and not re.fullmatch('application_[0-9]+_[0-9]+',application_id):
            raise ControlError('Invalid YARN application ID')
        with self.transaction() as cursor:
            row=self.row(cursor,'task_run','run_id',run_id)
            if row['status'] != 'PENDING':
                raise ControlError('Task is not pending')
            now=utcnow()
            cursor.execute("UPDATE task_run SET status='RUNNING',started_at=%s,heartbeat_at=%s,pid=%s,process_start_utc=%s,application_id=%s WHERE run_id=%s",
                (now,now,pid,process_start_utc,application_id,run_id))

    def record_application(self,run_id,application_id):
        if not re.fullmatch('application_[0-9]+_[0-9]+',application_id):
            raise ControlError('Invalid application ID')
        with self.transaction() as cursor:
            row=self.row(cursor,'task_run','run_id',run_id)
            if row['status'] != 'RUNNING' or row['application_id'] not in (None,application_id):
                raise ControlError('Cannot replace task application identity')
            cursor.execute('UPDATE task_run SET application_id=%s WHERE run_id=%s',(application_id,run_id))

    def heartbeat(self,run_id):
        with self.transaction() as cursor:
            row=self.row(cursor,'task_run','run_id',run_id)
            if row['status'] != 'RUNNING':
                raise ControlError('Only running tasks have heartbeats')
            cursor.execute('UPDATE task_run SET heartbeat_at=%s WHERE run_id=%s',(utcnow(),run_id))

    def finish_task(self,run_id,status, *, manifest_path=None,store=None,error_code=None,terminal_check=None,
                    formal=True,heartbeat=lambda:None):
        if status not in ('SUCCEEDED','FAILED','CANCELLED'):
            raise ControlError('Invalid terminal status')
        with self.transaction() as cursor:
            row=self.row(cursor,'task_run','run_id',run_id)
            if row['status'] != 'RUNNING':
                raise ControlError('Task is not running; terminal states are immutable')
            if row['application_id'] or (status != 'SUCCEEDED' and row['pid']):
                if terminal_check is None or terminal_check(row) != status:
                    raise ControlError('External task terminal state is not confirmed')
        # Content verification can take minutes on a real XLSX. Do not hold a row
        # transaction open: the owner must keep committing its ten-second heartbeat.
        if status == 'SUCCEEDED':
            if not manifest_path or store is None:
                raise ControlError('Successful task requires verified manifest')
            data=json.loads(b''.join(store.chunks(manifest_path)))
            verify_manifest(data,store,{k:row[k] for k in ('batch_id','run_id','config_sha256','code_commit')} | {'application_id':row['application_id']},
                            formal=formal,heartbeat=heartbeat)
        if error_code is not None and not re.fullmatch('[A-Z0-9_]{1,64}',error_code):
            raise ControlError('Invalid error code')
        with self.transaction() as cursor:
            current=self.row(cursor,'task_run','run_id',run_id)
            if current['status']!='RUNNING' or any(current[k]!=row[k] for k in ('batch_id','config_sha256','code_commit','application_id')):
                raise ControlError('Task state changed during artifact verification')
            cursor.execute('UPDATE task_run SET status=%s,finished_at=%s,output_manifest_path=%s,error_code=%s WHERE run_id=%s',
                (status,utcnow(),manifest_path,error_code,run_id))

    def task_status(self,run_id):
        with self.transaction() as cursor:
            row=self.row(cursor,'task_run','run_id',run_id)
            row['heartbeat_stale']=row['status']=='RUNNING' and row['heartbeat_at'] is not None and utcnow()-row['heartbeat_at']>timedelta(seconds=60)
            return row

    def create_experiment(self,batch_id,config_sha256,experiment_id=None):
        digest(config_sha256)
        experiment_id=experiment_id or str(uuid4())
        from uuid import UUID
        if str(UUID(experiment_id)) != experiment_id:
            raise ControlError('Invalid experiment ID')
        with self.transaction() as cursor:
            self.row(cursor,'data_batch','batch_id',batch_id)
            cursor.execute('SELECT * FROM experiment WHERE experiment_id=%s FOR UPDATE',(experiment_id,))
            old=cursor.fetchone()
            if old:
                if old['batch_id'] != batch_id or old['config_sha256'] != config_sha256:
                    raise ControlError('Experiment configuration is immutable')
                return experiment_id
            cursor.execute('''INSERT INTO experiment (experiment_id,batch_id,status,split_policy_version,train_end_exclusive,
                validation_end_exclusive,seed,config_sha256,baseline_version,created_at)
                VALUES (%s,%s,'CREATED','time-split-v1','2011-09-01','2011-11-01',42,%s,%s,%s)''',
                (experiment_id,batch_id,config_sha256,BASELINE,utcnow()))
            return experiment_id

    def transition_experiment(self,experiment_id,status,champion_algorithm=None,report_path=None):
        with self.transaction() as cursor:
            row=self.row(cursor,'experiment','experiment_id',experiment_id)
            if status not in EXPERIMENT_TRANSITIONS[row['status']]:
                raise ControlError('Illegal experiment transition')
            if status == 'SUCCEEDED':
                self._four_models(cursor,row)
                if row['test_sealed_at'] is None or champion_algorithm not in ALGORITHMS or not report_path:
                    raise ControlError('Successful experiment needs sealed test and validation champion/report')
            cursor.execute('UPDATE experiment SET status=%s,champion_algorithm=%s,report_path=%s,finished_at=%s WHERE experiment_id=%s',
                (status,champion_algorithm,report_path,utcnow() if status in ('SUCCEEDED','FAILED') else None,experiment_id))

    def register_model(self, *, batch_id,experiment_id,run_id,algorithm,purpose,device,
                       config_sha256,mapping_sha256,artifact_path,code_commit,environment_sha256,model_run_id=None):
        for value in (config_sha256,mapping_sha256,environment_sha256): digest(value)
        commit_id(code_commit)
        if algorithm not in ALGORITHMS or purpose not in ('EVAL','FULLFIT') or device != ('CUDA' if algorithm=='NCF' else 'CPU'):
            raise ControlError('Invalid model configuration/device')
        with self.transaction() as cursor:
            experiment=self.row(cursor,'experiment','experiment_id',experiment_id)
            task=self.row(cursor,'task_run','run_id',run_id)
            if experiment['batch_id'] != batch_id or task['batch_id'] != batch_id or task['status'] != 'RUNNING':
                raise ControlError('Model/task/experiment batch mismatch or inactive task')
            if purpose == 'EVAL':
                self._ensure_no_test(cursor,experiment_id)
            if purpose == 'EVAL' and (experiment['test_sealed_at'] or experiment['status'] != 'RUNNING'):
                raise ControlError('Cannot train EVAL after test sealing')
            if purpose == 'FULLFIT' and experiment['status'] != 'SUCCEEDED':
                raise ControlError('FULLFIT requires completed evaluation')
            model_id=model_run_id or str(uuid4())
            from uuid import UUID
            if str(UUID(model_id)) != model_id:
                raise ControlError('Invalid model identity')
            if model_run_id is not None:
                from pathlib import PurePosixPath
                if PurePosixPath(artifact_path).name != 'model_run_id='+model_id:
                    raise ControlError('Explicit model identity must match its immutable artifact directory')
            cursor.execute('''INSERT INTO model_run (model_run_id,batch_id,experiment_id,run_id,algorithm,purpose,device,status,
                config_sha256,mapping_sha256,artifact_path,code_commit,environment_sha256,baseline_version,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'CREATED',%s,%s,%s,%s,%s,%s,%s)''',
                (model_id,batch_id,experiment_id,run_id,algorithm,purpose,device,config_sha256,mapping_sha256,artifact_path,code_commit,environment_sha256,BASELINE,utcnow()))
            return model_id

    def transition_model(self,model_run_id,status, *, manifest_path=None,store=None):
        with self.transaction() as cursor:
            row=self.row(cursor,'model_run','model_run_id',model_run_id)
            if status not in EXPERIMENT_TRANSITIONS[row['status']]:
                raise ControlError('Illegal model transition')
            if status == 'SUCCEEDED':
                if not manifest_path or store is None:
                    raise ControlError('Successful model requires verified artifacts')
                data=json.loads(b''.join(store.chunks(manifest_path)))
                verify_manifest(data,store,{k:row[k] for k in ('batch_id','model_run_id','experiment_id','algorithm','purpose','config_sha256','mapping_sha256','environment_sha256','code_commit')})
            cursor.execute('UPDATE model_run SET status=%s,finished_at=%s WHERE model_run_id=%s',
                (status,utcnow() if status in ('SUCCEEDED','FAILED') else None,model_run_id))

    def select_model(self,experiment_id,algorithm,model_run_id):
        with self.transaction() as cursor:
            experiment=self.row(cursor,'experiment','experiment_id',experiment_id)
            model=self.row(cursor,'model_run','model_run_id',model_run_id)
            self._ensure_no_test(cursor,experiment_id)
            if experiment['status'] != 'RUNNING' or experiment['test_sealed_at'] or model['status'] != 'SUCCEEDED' or model['purpose'] != 'EVAL' or model['algorithm'] != algorithm or model['experiment_id'] != experiment_id or model['batch_id'] != experiment['batch_id']:
                raise ControlError('Invalid selected model or sealed experiment')
            cursor.execute("SELECT COUNT(*) AS n FROM model_metric WHERE model_run_id=%s AND split_name='VALIDATION'",(model_run_id,))
            if cursor.fetchone()['n'] != 2:
                raise ControlError('Selected model needs both validation cohorts')
            cursor.execute('INSERT INTO experiment_model (experiment_id,algorithm,model_run_id,selected_at) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE model_run_id=VALUES(model_run_id),selected_at=VALUES(selected_at)',
                (experiment_id,algorithm,model_run_id,utcnow()))

    @staticmethod
    def _ensure_no_test(cursor,experiment_id):
        cursor.execute("SELECT COUNT(*) AS n FROM model_metric t JOIN model_run m ON m.model_run_id=t.model_run_id WHERE m.experiment_id=%s AND t.split_name='TEST'",(experiment_id,))
        if cursor.fetchone()['n']:
            raise ControlError('Test evaluation has started; model selection is frozen')

    @staticmethod
    def _four_models(cursor,experiment):
        cursor.execute('''SELECT m.* FROM experiment_model s JOIN model_run m ON m.model_run_id=s.model_run_id
            WHERE s.experiment_id=%s FOR UPDATE''',(experiment['experiment_id'],))
        rows=cursor.fetchall()
        if len(rows)!=4 or {r['algorithm'] for r in rows}!=set(ALGORITHMS) or any(r['status']!='SUCCEEDED' or r['purpose']!='EVAL' or r['batch_id']!=experiment['batch_id'] or r['experiment_id']!=experiment['experiment_id'] for r in rows):
            raise ControlError('Experiment requires four successful same-batch EVAL models')
        return rows

    def seal_test(self,experiment_id):
        with self.transaction() as cursor:
            experiment=self.row(cursor,'experiment','experiment_id',experiment_id)
            if experiment['status']!='RUNNING' or experiment['test_sealed_at']:
                raise ControlError('Experiment is not open for sealing')
            for model in self._four_models(cursor,experiment):
                cursor.execute('SELECT COUNT(*) AS n FROM model_metric WHERE model_run_id=%s',(model['model_run_id'],))
                if cursor.fetchone()['n'] != 4:
                    raise ControlError('Missing validation/test cohorts')
            cursor.execute('UPDATE experiment SET test_sealed_at=%s WHERE experiment_id=%s',(utcnow(),experiment_id))

    def record_metric(self,model_run_id, *, split_name,cohort,user_count,catalog_count,window_user_count,
                      precision_at_k,recall_at_k,ndcg_at_k,coverage,cold_user_count,cold_item_count,
                      cold_interaction_count,excluded_user_count):
        import math
        counts=(user_count,catalog_count,window_user_count,cold_user_count,cold_item_count,cold_interaction_count,excluded_user_count)
        scores=(precision_at_k,recall_at_k,ndcg_at_k,coverage)
        if split_name not in ('VALIDATION','TEST') or cohort not in ('WARM','ALL_ELIGIBLE') or any(type(v) is not int or v<0 for v in counts) or not user_count or not catalog_count or any(type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=1 for v in scores):
            raise ControlError('Invalid metric/cohort/counts or nonfinite score')
        with self.transaction() as cursor:
            # Consistent lock order: experiment, then model, shared with selection/sealing.
            cursor.execute('SELECT experiment_id FROM model_run WHERE model_run_id=%s',(model_run_id,))
            model=cursor.fetchone()
            if not model:
                raise ControlError('Unknown model')
            experiment=self.row(cursor,'experiment','experiment_id',model['experiment_id'])
            model=self.row(cursor,'model_run','model_run_id',model_run_id)
            if experiment['status']!='RUNNING' or experiment['test_sealed_at'] or model['purpose']!='EVAL' or model['status']!='SUCCEEDED':
                raise ControlError('Metrics require successful EVAL model in unsealed running experiment')
            if split_name=='TEST':
                self._four_models(cursor,experiment)
                cursor.execute('SELECT model_run_id FROM experiment_model WHERE experiment_id=%s AND algorithm=%s',(experiment['experiment_id'],model['algorithm']))
                if cursor.fetchone()['model_run_id']!=model_run_id:
                    raise ControlError('Test metrics only belong to validation-selected models')
            # No UPSERT: replacing reported metrics requires a new auditable run.
            cursor.execute('''INSERT INTO model_metric (model_run_id,split_name,cohort,k,user_count,catalog_count,
                window_user_count,precision_at_k,recall_at_k,ndcg_at_k,coverage,cold_user_count,cold_item_count,
                cold_interaction_count,excluded_user_count,candidate_policy_version,created_at)
                VALUES (%s,%s,%s,10,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'full-catalog-v1',%s)''',
                (model_run_id,split_name,cohort,user_count,catalog_count,window_user_count,*scores,
                 cold_user_count,cold_item_count,cold_interaction_count,excluded_user_count,utcnow()))

    def record_epoch(self,model_run_id, *, epoch,train_loss,elapsed_seconds,validation_loss=None,validation_ndcg=None,peak_gpu_mib=None):
        import math
        values=(train_loss,elapsed_seconds,validation_loss,validation_ndcg,peak_gpu_mib)
        if type(epoch) is not int or epoch<1 or any(v is not None and (type(v) not in (int,float) or not math.isfinite(v) or v<0) for v in values) or (validation_ndcg is not None and validation_ndcg>1):
            raise ControlError('Invalid epoch data')
        with self.transaction() as cursor:
            model=self.row(cursor,'model_run','model_run_id',model_run_id)
            if model['algorithm']!='NCF' or model['status']!='RUNNING' or (model['purpose']=='FULLFIT' and (validation_loss is not None or validation_ndcg is not None)):
                raise ControlError('Epoch data requires active NCF model with correct purpose')
            cursor.execute('''INSERT INTO model_epoch (model_run_id,epoch,train_loss,elapsed_seconds,validation_loss,validation_ndcg,peak_gpu_mib)
                VALUES (%s,%s,%s,%s,%s,%s,%s)''',(model_run_id,epoch,*values))
