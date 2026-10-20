"""W03 ingest: immutable source archive, streaming normalization and recoverable control state."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile
from xml.etree.ElementTree import ParseError
from openpyxl.utils.exceptions import InvalidFileException
from urllib.parse import urlsplit

from etl.audit import convert
from etl.storage import HdfsStorage,FileStorage,publish,StorageError,DependencyUnavailable
from etl.workbook import rows
from orchestration.control import ControlStore,ControlError
from orchestration.database import named_lock,utcnow,LockBusy
from orchestration.manifests import ManifestError,LocalStore,DATASETS,fingerprint,verify_manifest
from orchestration.migrations import require_current
from orchestration.runtime import Heartbeat,process_start,process_alive
from orchestration.settings import ConfigError

ROOT=Path(__file__).resolve().parents[1]


def write_json(path,data):
    Path(path).write_text(json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)+'\n')


def sha256(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk:=stream.read(1024**2):digest.update(chunk)
    return digest.hexdigest()


def source_path(path,work):
    try:
        path=Path(path).resolve(strict=True)
        if not path.is_file() or not path.is_relative_to(work):
            raise ConfigError('Source path must be a file inside the configured work directory')
        return path
    except OSError:
        raise ConfigError('Source file is inaccessible') from None


def read_json(storage,path):
    return json.loads(b''.join(storage.chunks(path)))


def live_child(run_id):
    for item in Path('/proc').glob('[0-9]*/cmdline'):
        try:arguments=item.read_bytes().split(b'\0')
        except OSError:continue
        if arguments and Path(arguments[0].decode(errors='ignore')).name=='java' and any(run_id.encode() in arg for arg in arguments):
            return True
    return False


def apply_quality(control,batch_id,report,report_path):
    with control.transaction() as cursor:
        row=control.row(cursor,'data_batch','batch_id',batch_id)
        if row['status']=='SUCCEEDED':return
    counts={name:report['metrics'][name.upper()] for name in ('raw_rows','sale_rows','return_rows','rejected_rows')}
    control.quality(batch_id,report['metrics'],counts=counts,report_path=report_path)
    with control.transaction() as cursor:
        cursor.execute('UPDATE data_batch SET min_source_date=%s,max_source_date=%s,error_code=%s,error_message=NULL,updated_at=%s WHERE batch_id=%s',
            (report['min_source_date'],report['max_source_date'],None if report['quality_gate_passed'] else 'QUALITY_GATE_FAILED',utcnow(),batch_id))
    if not report['quality_gate_passed']:
        with control.transaction() as cursor:
            row=control.row(cursor,'data_batch','batch_id',batch_id)
        if row['status']=='PROCESSING':control.transition_batch(batch_id,'FAILED')


def ingest(connection,config,source_file,source_url, *, sheet=None,date_format=None,source_metadata=None,
           synthetic=False,dry_run=False,storage=None):
    work=Path(config['paths']['work_dir']).resolve()
    source=source_path(source_file,work)
    if source.suffix.lower()!='.xlsx':raise ConfigError('Source must be an XLSX workbook')
    url=urlsplit(source_url)
    if url.scheme!='https' or not url.hostname or url.username or url.password or len(source_url)>1024:
        raise ConfigError('Source URL must be a public HTTPS provenance URL')
    if not synthetic and (url.hostname!='archive.ics.uci.edu' or not any(part in url.path for part in ('/352/','/352','/00352/'))):
        raise ConfigError('Formal input must identify UCI Online Retail 352')
    hdfs_root=config['hdfs']['root']
    if synthetic and (hdfs_root=='/retail/v1' or config['database']['name']=='retail_analytics'):
        raise ConfigError('Synthetic input requires both an isolated database and an isolated storage root')
    if isinstance(storage,FileStorage) and not synthetic:
        raise ConfigError('Formal ingestion requires real HDFS')
    if len(source.name)>255:raise ConfigError('Source filename exceeds schema limit')
    digest=sha256(source);size=source.stat().st_size
    try:
        with rows(source,sheet) as (actual_sheet,_):pass
    except (ValueError,InvalidFileException,zipfile.BadZipFile,ParseError):
        raise ManifestError('Invalid source workbook or worksheet schema') from None
    origin={}
    if source_metadata is not None:
        origin=json.loads(source_path(source_metadata,work).read_text())
        if origin.get('source_sha256')!=digest or origin.get('source_bytes')!=size or origin.get('source_url')!=source_url:
            raise ManifestError('Source metadata does not match the actual source')
        stamp=origin.get('downloaded_at','')
        if not stamp.endswith('Z'):
            raise ManifestError('Download audit time must use UTC Z')
        datetime.fromisoformat(stamp.replace('Z','+00:00'))
    elif not synthetic:
        raise ConfigError('Formal source requires --source-metadata with observed download time and file hash')
    storage=storage or HdfsStorage(config)
    try:storage.ready()
    except StorageError:raise DependencyUnavailable('HDFS is unavailable') from None
    require_current(connection)
    params={'config':config,'sheet':actual_sheet,'source_date_format':date_format,'synthetic':synthetic,
            'policy_version':'retail-clean-v1','schema_version':'1.0'}
    config_hash=hashlib.sha256(json.dumps(params,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    control=ControlStore(connection)
    with connection.cursor() as cursor:
        cursor.execute('SELECT batch_id FROM data_batch WHERE source_sha256=%s AND policy_version=%s',(digest,'retail-clean-v1'))
        existing=cursor.fetchone()
    connection.rollback()
    if dry_run:
        return {'dry_run':True,'source_sha256':digest,'source_bytes':size,'sheet':actual_sheet,
                'existing_batch_id':existing['batch_id'] if existing else None,'config_sha256':config_hash,
                'plan':['archive immutable XLSX','stream normalized TSV and preflight audit','verify staged hashes and atomic rename','record task and quality; batch remains PROCESSING until W04']}
    with named_lock(connection):
        tick=Heartbeat(connection,control)
        storage.heartbeat=tick
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM task_run WHERE status IN ('PENDING','RUNNING')")
            unfinished=cursor.fetchall()
        connection.rollback()
        for task in unfinished:
            if task['task_name']!='ingest' or not existing or task['batch_id']!=existing['batch_id'] or process_alive(task) is not False or live_child(task['run_id']):
                # A just-registered PENDING ingest has not submitted work. The code
                # below can recover it only while owning this same global DB lock.
                if not (task['task_name']=='ingest' and existing and task['batch_id']==existing['batch_id'] and task['status']=='PENDING' and task['pid'] is None):
                    raise LockBusy('Unfinished task needs authoritative handle verification')
        batch=control.register_batch(source_sha256=digest,source_name=source.name,source_url=source_url,source_bytes=size,
            raw_hdfs_path=hdfs_root+'/raw/batch_id={batch_id}/source.xlsx',
            normalized_hdfs_path=hdfs_root+'/normalized/batch_id={batch_id}',hdfs_root=hdfs_root)
        bid=batch['batch_id'];raw_dir=hdfs_root+'/raw/batch_id='+bid
        normalized=hdfs_root+'/normalized/batch_id='+bid
        if batch['raw_hdfs_path']!=raw_dir+'/source.xlsx' or batch['normalized_hdfs_path']!=normalized:
            raise ManifestError('Existing batch uses a different storage namespace')
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name='ingest' ORDER BY attempt DESC LIMIT 1",(bid,))
            previous=cursor.fetchone()
        connection.rollback()
        if previous and previous['config_sha256']!=config_hash:
            raise ManifestError('Same file/policy cannot silently change normalization configuration')
        if storage.isdir(normalized):
            if not previous or previous['status'] not in ('SUCCEEDED','RUNNING'):
                raise ManifestError('Existing output has no recoverable task identity')
            manifest=read_json(storage,normalized+'/manifest.json')
            expected={k:previous[k] for k in ('batch_id','run_id','config_sha256','code_commit')}
            verify_manifest(manifest,storage,expected,formal=not synthetic,heartbeat=tick)
            if previous['status']=='RUNNING':
                control.finish_task(previous['run_id'],'SUCCEEDED',manifest_path=normalized+'/manifest.json',store=storage,formal=not synthetic,heartbeat=tick)
            report=read_json(storage,normalized+'/quality.json')
            apply_quality(control,bid,report,normalized+'/quality.json')
            return {'batch_id':bid,'run_id':previous['run_id'],'reused':True,'recovered':previous['status']=='RUNNING',
                    'raw_hdfs_path':raw_dir+'/source.xlsx','normalized_hdfs_path':normalized,
                    'raw_rows':report['raw_rows'],'quality_gate_passed':report['quality_gate_passed']}
        if previous and previous['status']=='SUCCEEDED':
            raise ManifestError('Successful ingest artifact is missing; refuse implicit regeneration')
        if previous and previous['status'] in ('PENDING','RUNNING'):
            if previous['status']=='PENDING':
                control.start_task(previous['run_id'],pid=os.getpid(),process_start_utc=process_start(os.getpid()))
            control.finish_task(previous['run_id'],'FAILED',error_code='ABANDONED_INGEST',terminal_check=lambda row:'FAILED')
        if batch['status'] in ('REGISTERED','FAILED'):control.transition_batch(bid,'PROCESSING')
        run_id=control.create_task(bid,'ingest',config_hash,subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip())
        control.start_task(run_id,pid=os.getpid(),process_start_utc=process_start(os.getpid()))
        tick.run_id=run_id;tick(force=True)
        directory=work/'ingest'/bid/run_id
        directory.mkdir(parents=True,exist_ok=False,mode=0o700)
        complete_output=False
        try:
            sources=[*ROOT.glob('etl/*.py'),*ROOT.glob('orchestration/*.py')]
            source_hashes={str(p.relative_to(ROOT)):sha256(p) for p in sources}
            with zipfile.ZipFile(directory/'source-code.zip','w',compression=zipfile.ZIP_DEFLATED) as archive:
                for name in sorted(source_hashes):archive.write(ROOT/name,name)
            snapshot=directory/'source.xlsx';shutil.copyfile(source,snapshot)
            if sha256(snapshot)!=digest:raise ManifestError('Source changed while taking the immutable snapshot')
            report=convert(snapshot,directory,digest,actual_sheet,date_format,tick)
            report['synthetic']=synthetic
            report['batch_id']=bid;report['run_id']=run_id
            write_json(directory/'quality.json',report)
            source_info=origin|{'source_sha256':digest,'source_bytes':size,'source_name':source.name,
                'source_url':batch['source_url'],'batch_id':bid,'schema_version':'1.0','baseline_version':'HL-DOC-1.0.0',
                'sheet':actual_sheet,'raw_rows':report['raw_rows'],'synthetic':synthetic,
                'registered_at':batch['created_at'].isoformat()+'Z'}
            if storage.isdir(raw_dir):
                if fingerprint(storage,raw_dir+'/source.xlsx',heartbeat=tick)[:2]!=(digest,size):
                    raise ManifestError('Existing raw archive differs from source')
                archived=read_json(storage,raw_dir+'/source.json')
                if archived.get('source_sha256')!=digest or archived.get('raw_rows')!=report['raw_rows'] or archived.get('batch_id')!=bid:
                    raise ManifestError('Existing raw provenance differs')
                source_info=archived
            write_json(directory/'source.json',source_info)
            if not storage.isdir(raw_dir):
                publish(storage,{'source.xlsx':snapshot,'source.json':directory/'source.json'},raw_dir,run_id,tick)
            write_json(directory/'config.json',params)
            shutil.copyfile(ROOT/'uv.lock',directory/'environment.lock')
            def artifact(name,path=None):
                local=directory/name
                return {'path':path or normalized+'/'+name,'sha256':sha256(local),'bytes':local.stat().st_size}
            files={name:directory/name for name in ('normalized.tsv','warnings.tsv','rejected-audit.jsonl','order-conflicts.tsv','quality.json','config.json','environment.lock','source-code.zip')}
            with connection.cursor() as cursor:
                cursor.execute('SELECT created_at,code_commit FROM task_run WHERE run_id=%s',(run_id,))
                task=cursor.fetchone()
            connection.rollback()
            if any(sha256(ROOT/name)!=digest for name,digest in source_hashes.items()):
                raise RuntimeError('Source code changed during ingestion')
            manifest={'schema_version':'1.0','baseline_version':'HL-DOC-1.0.0','batch_id':bid,'run_id':run_id,
                'model_run_id':None,'status':'SUCCEEDED','code_commit':task['code_commit'],'config_sha256':config_hash,
                'code_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),
                'source_code_sha256':source_hashes,
                'created_at':task['created_at'].isoformat()+'Z','finished_at':utcnow().isoformat()+'Z',
                'synthetic':synthetic,'dataset':'normalized','schema':DATASETS['normalized'].split(','),
                'inputs':[{'path':raw_dir+'/source.xlsx','sha256':digest,'rows':report['raw_rows'],'format':'XLSX','sheet':actual_sheet}],
                'outputs':[{'path':normalized+'/normalized.tsv','sha256':sha256(directory/'normalized.tsv'),'rows':report['raw_rows']}],
                'artifacts':[artifact(name) for name in files if name!='normalized.tsv']+[artifact('source.json',raw_dir+'/source.json')],
                'record_count':report['raw_rows'],'classification_counts':report['classification_counts'],
                'application_id':None,'success_marker':normalized+'/_SUCCESS'}
            write_json(directory/'manifest.json',manifest)
            (directory/'_SUCCESS').touch()
            files['manifest.json']=directory/'manifest.json';files['_SUCCESS']=directory/'_SUCCESS'
            publish(storage,files,normalized,run_id,tick)
            complete_output=True
            control.finish_task(run_id,'SUCCEEDED',manifest_path=normalized+'/manifest.json',store=storage,formal=not synthetic,heartbeat=tick)
            tick.run_id=None
            apply_quality(control,bid,report,normalized+'/quality.json')
            return {'batch_id':bid,'run_id':run_id,'reused':False,'recovered':False,'raw_hdfs_path':raw_dir+'/source.xlsx',
                'normalized_hdfs_path':normalized,'raw_rows':report['raw_rows'],'quality_gate_passed':report['quality_gate_passed']}
        except Exception:
            if not complete_output:
                try:
                    control.finish_task(run_id,'FAILED',error_code='INGEST_FAILED',terminal_check=lambda row:'FAILED')
                    control.transition_batch(bid,'FAILED')
                except Exception:pass  # Lost connections leave the original run for handle-based recovery.
            raise
        finally:
            tick.run_id=None
            storage.heartbeat=lambda:None
