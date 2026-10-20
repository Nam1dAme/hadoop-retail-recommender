"""W04 serial business pipeline: one task and immutable output bundle per YARN application."""
from datetime import date,timedelta
import hashlib
import json
import os
from pathlib import Path,PurePosixPath
import re
import shutil
import subprocess
import zipfile

from etl.ingest import ROOT,read_json,write_json,sha256,live_child
from etl.storage import HdfsStorage,FileStorage
from etl.tsv import parse_line,line
from orchestration.control import ControlStore
from orchestration.database import named_lock,utcnow,LockBusy
from orchestration.manifests import DATASETS,ManifestError,fingerprint,verify_manifest
from orchestration.migrations import require_current
from orchestration.runtime import Heartbeat,process_alive,process_start
from orchestration.settings import ConfigError
from orchestration.yarn import Yarn,TERMINAL

STAGES={
    'J01':('clean','clean','rejected','warnings'),
    'J02':('results','order_fact'), 'J03':('results','sales_cube'),
    'J04':('results','product_order_fact'), 'J05base':('features','rfm_base'),
    'J05distribution':('features','rfm_scores'), 'J05':('results','rfm'),
    'J06baskets':('features','baskets'), 'J06interactions':('features','interactions','customer_seen'),
    'J07baskets':('features','occurrences'), 'J07':('features','item_pairs','item_frequency'),
    'J08':('results','similarity'), 'pipeline':('results','pipeline_summary'),
}


def stream_rows(storage,files,columns,heartbeat=lambda:None):
    for file in files:
        buffer=b''
        for chunk in storage.chunks(file['path']):
            heartbeat()
            lines=(buffer+chunk).split(b'\n');buffer=lines.pop()
            for raw in lines:yield parse_line(raw.decode('utf-8')+'\n',columns)
        if buffer:raise ManifestError('TSV must end in LF')


def hdfs_files(storage,directory):
    _,listing=storage.run('-ls','-R',directory)
    return sorted(line.split(None,7)[7] for line in listing.decode().splitlines() if line.startswith('-'))


def expected(task):
    return {k:task[k] for k in ('batch_id','run_id','code_commit','config_sha256','application_id')}


def validate_stage_bundle(manifest,task):
    stage=task['task_name']
    if (manifest.get('dataset')!=STAGES[stage][1] or
            set(manifest.get('datasets',{}))!=set(STAGES[stage][1:]) or
            manifest.get('execution',{}).get('stage')!=stage):
        raise ManifestError('Manifest output bundle does not match task stage')
    application=manifest['execution'].get('application')
    if stage=='pipeline':
        if application is not None or task['application_id'] is not None:raise ManifestError('Audit task cannot claim a YARN application')
    elif not application or application.get('id')!=task['application_id'] or application.get('name')!='retail-'+stage+'-'+task['run_id'] or application.get('finalStatus')!='SUCCEEDED':
        raise ManifestError('Manifest application association differs')


def task_row(connection,run_id):
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM task_run WHERE run_id=%s',(run_id,));row=cursor.fetchone()
    connection.rollback()
    return row


def recover_task(control,yarn,task,storage,tick,formal=True):
    """Never infer death from age. Resolve submission gaps by the exact run name."""
    if process_alive(task) or live_child(task['run_id']):raise LockBusy('Original pipeline process is alive')
    if task['task_name']=='pipeline':
        app=None
    else:
        app=yarn.app(task['application_id']) if task['application_id'] else yarn.find('retail-'+task['task_name']+'-'+task['run_id'])
        if app:
            if app['name']!='retail-'+task['task_name']+'-'+task['run_id']:raise LockBusy('Application identity mismatch')
            if app['state'] not in TERMINAL:raise LockBusy('Original YARN application is still active')
    if task['status']=='PENDING':
        control.start_task(task['run_id'])
    if app and not task['application_id']:
        control.record_application(task['run_id'],app['id']);task['application_id']=app['id']
    destination=stage_path(storage,task['batch_id'],task['task_name'],task['run_id'])
    if storage.isdir(destination):
        manifest=read_json(storage,destination+'/manifest.json')
        validate_stage_bundle(manifest,task)
        verify_manifest(manifest,storage,expected(task),heartbeat=tick,formal=formal)
        if app and app['finalStatus']!='SUCCEEDED':raise ManifestError('Published output has unsuccessful application')
        control.finish_task(task['run_id'],'SUCCEEDED',store=storage,manifest_path=destination+'/manifest.json',
            terminal_check=lambda _: 'SUCCEEDED',heartbeat=tick,formal=formal)
    else:
        # No final bundle: even a successful application cannot be reused without its
        # schema/hash/metadata bundle. Retain its staging directory and retry a new run.
        control.finish_task(task['run_id'],'FAILED',error_code='INCOMPLETE_PIPELINE_RUN',terminal_check=lambda _: 'FAILED')


def stage_path(storage,batch_id,stage,run_id):
    return str(storage.root)+'/'+STAGES[stage][0]+'/batch_id='+batch_id+'/run_id='+run_id


def stage_parameters(stage,files,options,jar_hash,reducers):
    return {'stage':stage,'baseline_version':'HL-DOC-1.0.0','policy_version':'retail-clean-v1',
        'jar_sha256':jar_hash,'reducers':0 if stage=='J05' else reducers,
        'inputs':files,'options':options,'schemas':{name:DATASETS[name] for name in STAGES[stage][1:]}}


def pipeline(connection,config,batch_id,*,reducers=2,dry_run=False,synthetic=False,storage=None,yarn=None):
    if not re.fullmatch(r'B[0-9]{14}[0-9a-f]{8}',batch_id) or type(reducers)is not int or not 1<=reducers<=4:
        raise ValueError('Invalid batch or reducer count')
    require_current(connection)
    if synthetic and (config['hdfs']['root']=='/retail/v1' or config['database']['name']=='retail_analytics'):
        raise ConfigError('Synthetic pipeline requires isolated database and storage root')
    if (isinstance(storage,FileStorage) or yarn is not None) and not synthetic:
        raise ConfigError('Formal pipeline requires real HDFS and YARN')
    storage=storage or HdfsStorage(config);control=ControlStore(connection);yarn=yarn or Yarn(config)
    work=Path(config['paths']['work_dir']).resolve()
    jar=ROOT/'mapreduce/target/retail-mapreduce-0.1.0-SNAPSHOT.jar'
    if not jar.is_file():raise RuntimeError('Build the MapReduce JAR before running pipeline')
    jar_hash=sha256(jar)
    hadoop_hashes={p.name:sha256(p) for p in Path(config['runtime']['hadoop_conf_dir']).glob('*.xml')}
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    sources=[*ROOT.glob('mapreduce/src/main/java/**/*.java'),*ROOT.glob('orchestration/*.py'),*ROOT.glob('etl/*.py'),ROOT/'mapreduce/pom.xml']
    source_paths={str(p.relative_to(ROOT)):p for p in sources}
    source_paths.update({'runtime-config/'+p.name:p for p in Path(config['runtime']['hadoop_conf_dir']).glob('*.xml')})
    source_hashes={name:sha256(path) for name,path in source_paths.items()}
    with named_lock(connection):
        tick=Heartbeat(connection,control);storage.heartbeat=tick
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM data_batch WHERE batch_id=%s',(batch_id,));batch=cursor.fetchone()
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name='ingest' AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1",(batch_id,));ingest=cursor.fetchone()
            cursor.execute("SELECT * FROM task_run WHERE status IN ('PENDING','RUNNING')");unfinished=cursor.fetchall()
        connection.rollback()
        if not batch or not ingest:raise ManifestError('Successful W03 ingest is required')
        normalized=read_json(storage,ingest['output_manifest_path'])
        verify_manifest(normalized,storage,expected(ingest),heartbeat=tick,formal=not synthetic)
        if bool(normalized.get('synthetic'))!=synthetic:raise ManifestError('Pipeline and input synthetic policies differ')
        preflight=read_json(storage,batch['normalized_hdfs_path']+'/quality.json')
        if not preflight['quality_gate_passed']:raise ManifestError('W03 quality gate failed')
        if dry_run:
            return {'batch_id':batch_id,'stages':list(STAGES),'reducers':reducers,'dry_run':True,'jar_sha256':jar_hash}
        for task in unfinished:
            if task['batch_id']!=batch_id or task['task_name'] not in STAGES:raise LockBusy('Another unfinished task requires handle verification')
            recover_task(control,yarn,task,storage,tick,formal=not synthetic)
        if batch['status'] in ('REGISTERED','FAILED'):control.transition_batch(batch_id,'PROCESSING')
        outputs={};runs=[];stage_reports={};reference=None
        def files(name):return outputs[name]['files']
        def directory(name):return str(PurePosixPath(files(name)[0]['path']).parent)
        def artifact_input(path):
            digest,_,rows=fingerprint(storage,path,heartbeat=tick)
            return {'path':path,'sha256':digest,'rows':rows}
        warnings=artifact_input(batch['normalized_hdfs_path']+'/warnings.tsv')
        def execute(stage,input_files,options):
            input_files=sorted(input_files,key=lambda f:f['path'])
            params=stage_parameters(stage,input_files,options,jar_hash,reducers)
            params['hadoop_config_sha256']=hadoop_hashes
            config_hash=hashlib.sha256(json.dumps(params,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s ORDER BY attempt DESC LIMIT 1',(batch_id,stage));previous=cursor.fetchone()
            connection.rollback()
            if previous and previous['status']=='SUCCEEDED':
                if previous['config_sha256']!=config_hash:raise ManifestError('Successful stage has different parameters or inputs')
                manifest=read_json(storage,previous['output_manifest_path'])
                validate_stage_bundle(manifest,previous)
                verify_manifest(manifest,storage,expected(previous),heartbeat=tick,formal=not synthetic)
                outputs.update(manifest['datasets']);runs.append(previous['run_id']);stage_reports[stage]=manifest['execution']
                return manifest
            if batch['status']=='SUCCEEDED':raise ManifestError('Successful batch has a missing stage')
            run_id=control.create_task(batch_id,stage,config_hash,commit)
            control.start_task(run_id,pid=os.getpid(),process_start_utc=process_start(os.getpid()))
            tick.run_id=run_id;tick(force=True)
            task=task_row(connection,run_id)
            local=work/'pipeline'/batch_id/run_id;local.mkdir(parents=True,mode=0o700)
            with control.transaction() as cursor:cursor.execute('UPDATE task_run SET log_path=%s WHERE run_id=%s',(str(local/'driver.log'),run_id))
            destination=stage_path(storage,batch_id,stage,run_id)
            temporary=str(PurePosixPath(destination).parent)+'/.run_id='+run_id+'.tmp'
            published=False
            def event(name,level='INFO',error_code=None):
                with (local/'events.jsonl').open('a') as log:
                    log.write(json.dumps({'timestamp':utcnow().isoformat()+'Z','level':level,'component':'pipeline',
                        'event':name,'run_id':run_id,'batch_id':batch_id,'model_run_id':None,'release_id':None,
                        'duration_ms':max(0,int((utcnow()-task['created_at']).total_seconds()*1000)),
                        'error_code':error_code},sort_keys=True)+'\n')
            event('stage_started')
            try:
                write_json(local/'config.json',params)
                shutil.copyfile(jar,local/'business.jar');shutil.copyfile(ROOT/'uv.lock',local/'environment.lock')
                with zipfile.ZipFile(local/'source-code.zip','w',compression=zipfile.ZIP_DEFLATED) as archive:
                    for name in sorted(source_hashes):archive.write(source_paths[name],name)
                if sha256(local/'business.jar')!=jar_hash:raise ManifestError('JAR changed while taking snapshot')
                counters={};app=None
                if stage=='pipeline':
                    from orchestration.pipeline_audit import audit
                    report=audit(storage,batch,outputs,local,tick)
                    counters=stage_reports['J01']['counters']|{k:stage_reports['J07baskets']['counters'].get(k,0) for k in ('TRUNCATED_BASKETS','TRUNCATED_BASKET_ITEMS')}
                    report['metrics']=preflight['metrics']|counters
                    # ELIGIBLE_BASKET_COUNT is manifest metadata, not a frozen DB metric.
                    report['metrics'].pop('ELIGIBLE_BASKET_COUNT',None)
                    if any(counters[k]!=report[v] for k,v in (('TRUNCATED_BASKETS','truncated_baskets'),('TRUNCATED_BASKET_ITEMS','truncated_basket_items'))):
                        raise ManifestError('MR basket counters differ from independent audit')
                    write_json(local/'quality.json',report)
                    (local/'summary.tsv').write_text(line(['status','PASS'])+line(['batch_id',batch_id]))
                    storage.mkdir(temporary+'/pipeline_summary');storage.put(local/'summary.tsv',temporary+'/pipeline_summary/part-00000')
                    storage.put(local/'quality.json',temporary+'/quality.json')
                    (local/'_SUCCESS').touch();storage.put(local/'_SUCCESS',temporary+'/_SUCCESS')
                else:
                    command=[str(Path(config['runtime']['hadoop_home'])/'bin/hadoop'),'jar',str(local/'business.jar'),'cn.hadooplab.retail.RetailJobs',
                        '-Dmapreduce.framework.name=yarn','-Dfs.defaultFS='+config['hdfs']['uri'],
                        '--stage',stage,'--run-id',run_id,'--output',temporary,'--reducers',str(reducers)]
                    for name,value in options.items():command.extend(['--'+name,str(value)])
                    environment=storage.environment|{'HADOOP_MAPRED_HOME':config['runtime']['hadoop_home']}
                    app,counters=yarn.run(command,task,control,tick,local,environment)
                if any(sha256(source_paths[name])!=digest for name,digest in source_hashes.items()):raise ManifestError('Source changed while pipeline was executing')
                if stage=='J01':
                    from orchestration.quality import REJECTIONS,WARNINGS
                    checked=('RAW_ROWS','SALE_ROWS','RETURN_ROWS','REJECTED_ROWS','NON_MERCHANDISE_SALE_ROWS')+tuple(name+'_ROWS' for name in REJECTIONS+WARNINGS)
                    for k in checked:
                        if counters.get(k,0)!=preflight['metrics'][k]:raise ManifestError('MR classification differs from input preflight')
                datasets={}
                existing=hdfs_files(storage,temporary)
                for name in STAGES[stage][1:]:
                    paths=[p for p in existing if str(PurePosixPath(p).parent)==temporary+'/'+name and PurePosixPath(p).name.startswith('part-')]
                    if not paths:
                        empty=local/'empty.tsv';empty.touch();storage.mkdir(temporary+'/'+name);storage.put(empty,temporary+'/'+name+'/part-00000');paths=[temporary+'/'+name+'/part-00000']
                    refs=[]
                    for path in paths:
                        digest,_,rows=fingerprint(storage,path,heartbeat=tick)
                        refs.append({'path':destination+path[len(temporary):],'sha256':digest,'rows':rows})
                    datasets[name]={'schema':DATASETS[name].split(','),'files':refs}
                execution={'stage':stage,'counters':counters,'reducers':0 if stage=='J05' else reducers,
                    'job_counters':app.get('job_counters',{}) if app else {},
                    'application':{k:app[k] for k in ('id','name','state','finalStatus','startedTime','finishedTime','elapsedTime','memorySeconds','vcoreSeconds') if k in app} if app else None}
                write_json(local/'execution.json',execution)
                artifacts=[]
                referenced={f['path'] for d in datasets.values() for f in d['files']}
                for path in existing:
                    final=destination+path[len(temporary):]
                    if final not in referenced and PurePosixPath(path).name not in ({'_SUCCESS','quality.json'} if stage=='pipeline' else {'_SUCCESS'}):
                        digest,size,rows=fingerprint(storage,path,heartbeat=tick)
                        if rows:raise ManifestError('Unexpected nonempty MR output outside declared datasets')
                        artifacts.append({'path':final,'sha256':digest,'bytes':size})
                for name in ('config.json','environment.lock','business.jar','source-code.zip','execution.json'):
                    storage.put(local/name,temporary+'/'+name)
                    artifacts.append({'path':destination+'/'+name,'sha256':sha256(local/name),'bytes':(local/name).stat().st_size})
                if stage=='pipeline':artifacts.append({'path':destination+'/quality.json','sha256':sha256(local/'quality.json'),'bytes':(local/'quality.json').stat().st_size})
                primary=STAGES[stage][1]
                manifest={'schema_version':'1.0','baseline_version':'HL-DOC-1.0.0','batch_id':batch_id,'run_id':run_id,'status':'SUCCEEDED',
                    'code_commit':commit,'config_sha256':config_hash,'code_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),
                    'source_code_sha256':source_hashes,'synthetic':synthetic,'created_at':task['created_at'].isoformat()+'Z','finished_at':utcnow().isoformat()+'Z',
                    'dataset':primary,'schema':DATASETS[primary].split(','),'inputs':input_files,'outputs':datasets[primary]['files'],
                    'artifacts':artifacts,'datasets':datasets,'execution':execution,
                    'record_count':sum(f['rows'] for f in datasets[primary]['files']),'classification_counts':{},
                    'application_id':app['id'] if app else None,'success_marker':destination+'/_SUCCESS'}
                write_json(local/'manifest.json',manifest);storage.put(local/'manifest.json',temporary+'/manifest.json')
                storage.move(temporary,destination);published=True
                task=task_row(connection,run_id)
                control.finish_task(run_id,'SUCCEEDED',store=storage,manifest_path=destination+'/manifest.json',
                    terminal_check=yarn.terminal if app else None,heartbeat=tick,formal=not synthetic)
                event('stage_succeeded')
                outputs.update(datasets);runs.append(run_id);stage_reports[stage]=execution
                return manifest
            except Exception:
                event('stage_interrupted','ERROR','PIPELINE_STAGE_INTERRUPTED')
                if not published:
                    task=task_row(connection,run_id)
                    observed=yarn.app(task['application_id']) if task['application_id'] else yarn.find('retail-'+stage+'-'+run_id)
                    if observed is None or observed['state'] in TERMINAL:
                        control.finish_task(run_id,'FAILED',error_code='PIPELINE_STAGE_FAILED',terminal_check=lambda _: 'FAILED')
                        control.transition_batch(batch_id,'FAILED')
                raise
            finally:tick.run_id=None
        execute('J01',normalized['outputs']+[warnings],{'input':batch['normalized_hdfs_path']+'/normalized.tsv','warnings':warnings['path'],'source-sha':batch['source_sha256']})
        execute('J02',files('clean'),{'input':directory('clean')})
        maximum=max(row[1] for row in stream_rows(storage,files('order_fact'),5,tick))
        reference=(date.fromisoformat(maximum)+timedelta(days=1)).isoformat()
        execute('J03',files('clean'),{'input':directory('clean')})
        execute('J04',files('clean'),{'input':directory('clean')})
        execute('J05base',files('order_fact'),{'input':directory('order_fact')})
        customers=sum(f['rows'] for f in files('rfm_base'))
        execute('J05distribution',files('rfm_base'),{'input':directory('rfm_base'),'customers':customers})
        execute('J05',files('rfm_base')+files('rfm_scores'),{'input':directory('rfm_base'),'customers':customers,
            'cache':','.join(f['path'] for f in files('rfm_scores')),'reference-date':reference})
        execute('J06baskets',files('clean')+files('order_fact'),{'input':directory('clean'),'orders':directory('order_fact')})
        execute('J06interactions',files('clean'),{'input':directory('clean')})
        execute('J07baskets',files('baskets'),{'input':directory('baskets')})
        execute('J07',files('occurrences'),{'input':directory('occurrences')})
        baskets=stage_reports['J07baskets']['counters'].get('ELIGIBLE_BASKET_COUNT',0)
        execute('J08',files('item_pairs')+files('item_frequency'),{'input':directory('item_pairs'),
            'cache':','.join(f['path'] for f in files('item_frequency')),'baskets':baskets})
        summary=execute('pipeline',[f for d in outputs.values() for f in d['files']],{'input_manifests':runs.copy()})
        quality=read_json(storage,str(PurePosixPath(summary['success_marker']).parent)+'/quality.json')
        if batch['status']!='SUCCEEDED':
            control.quality(batch_id,quality['metrics'],counts={key:quality['metrics'][key.upper()] for key in ('raw_rows','sale_rows','return_rows','rejected_rows')},
                report_path=str(PurePosixPath(summary['success_marker']).parent)+'/quality.json')
            control.transition_batch(batch_id,'SUCCEEDED')
        return {'batch_id':batch_id,'status':'SUCCEEDED','run_ids':runs,'quality_report':str(PurePosixPath(summary['success_marker']).parent)+'/quality.json',
            'datasets':outputs,'dry_run':False}
