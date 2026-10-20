#!/usr/bin/env python3
"""Read-only independent verification of W03 source, TSV structure, trace and control records."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from etl.storage import HdfsStorage
from etl.tsv import parse_line
from orchestration.database import connect
from orchestration.manifests import RunManifest,DATASETS,fingerprint,ManifestError
from orchestration.settings import load_config
from orchestration.migrations import require_current


def scan_normalized(storage,path,source_sha256,expected_rows,index_path):
    database=sqlite3.connect(index_path)
    database.execute('PRAGMA cache_size=-8192')
    database.execute('CREATE TABLE duplicate_row (key TEXT PRIMARY KEY,n INTEGER NOT NULL)')
    count=0;buffer=b''
    try:
        for chunk in storage.chunks(path):
            parts=(buffer+chunk).split(b'\n');buffer=parts.pop()
            for raw in parts:
                if count==0 and raw.startswith(b'\xef\xbb\xbf'):raise ManifestError('TSV BOM forbidden')
                fields=parse_line(raw.decode('utf-8')+'\n',11)
                count+=1
                if fields[1]!=str(count+1):raise ManifestError('Source row sequence differs')
                expected=hashlib.sha256(f'{source_sha256}:{count+1}'.encode()).hexdigest()
                if fields[0]!=expected:raise ManifestError('row_id does not trace to the source row')
                for index in (5,7):
                    if fields[index] is not None:
                        number=int(fields[index])
                        if str(number)!=fields[index] or not -(2**63)<=number<2**63:
                            raise ManifestError('Noncanonical or overflowing normalized integer')
                if fields[6] is not None:
                    value=datetime.strptime(fields[6],'%Y-%m-%dT%H:%M:%S')
                    if value.isoformat(timespec='seconds')!=fields[6]:raise ManifestError('Invalid source-local timestamp')
                key=hashlib.sha256(json.dumps(fields[2:10],ensure_ascii=False).encode()).hexdigest()
                database.execute('INSERT INTO duplicate_row VALUES (?,1) ON CONFLICT(key) DO UPDATE SET n=n+1',(key,))
                if count%10000==0:database.commit()
        if buffer:raise ManifestError('TSV must end in LF')
        if count!=expected_rows:raise ManifestError('Normalized record count differs')
        excess=database.execute('SELECT COALESCE(SUM(n-1),0) FROM duplicate_row').fetchone()[0]
        groups=database.execute('SELECT COUNT(*) FROM duplicate_row WHERE n>1').fetchone()[0]
        return {'rows':count,'source_row_ids_verified':count,'duplicate_excess_rows':excess,'duplicate_groups':groups}
    finally:database.close()


def verify(config,connection,batch_id):
    require_current(connection)
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM data_batch WHERE batch_id=%s',(batch_id,));batch=cursor.fetchone()
        if not batch:raise ManifestError('Batch not found')
        cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name='ingest' ORDER BY attempt",(batch_id,));tasks=cursor.fetchall()
        cursor.execute('SELECT metric_name,metric_value FROM data_quality WHERE batch_id=%s',(batch_id,));metrics={r['metric_name']:r['metric_value'] for r in cursor.fetchall()}
    if not tasks or tasks[-1]['status']!='SUCCEEDED':raise ManifestError('Ingest task is not successful')
    task=tasks[-1];storage=HdfsStorage(config)
    def read(path):return json.loads(b''.join(storage.chunks(path)))
    manifest_data=read(task['output_manifest_path']);manifest=RunManifest.model_validate(manifest_data)
    if manifest.synthetic or manifest.dataset!='normalized' or manifest.dataset_schema!=DATASETS['normalized'].split(','):
        raise ManifestError('Not a formal normalized manifest')
    for name in ('batch_id','run_id','config_sha256','code_commit','application_id'):
        if getattr(manifest,name)!=task[name]:raise ManifestError('Task/manifest association differs')
    if not storage.exists(manifest.success_marker):raise ManifestError('Success marker missing')
    verified_files=[]
    for file in [*manifest.inputs,*manifest.outputs,*manifest.artifacts]:
        digest,size,rows=fingerprint(storage,file.path)
        if digest!=file.sha256 or (hasattr(file,'bytes') and size!=file.bytes):
            raise ManifestError('Archived content hash/size differs')
        if hasattr(file,'format') and file.format=='TSV' and rows!=file.rows:
            raise ManifestError('Archived TSV row count differs')
        verified_files.append({'path':file.path,'sha256':digest,'bytes':size})
    if len(manifest.inputs)!=1 or manifest.inputs[0].path!=batch['raw_hdfs_path'] or manifest.inputs[0].sha256!=batch['source_sha256'] or manifest.inputs[0].format!='XLSX':
        raise ManifestError('Raw source association differs')
    normalized=batch['normalized_hdfs_path']+'/normalized.tsv'
    if len(manifest.outputs)!=1 or manifest.outputs[0].path!=normalized:raise ManifestError('Normalized path differs')
    source=read(str(Path(batch['raw_hdfs_path']).parent)+'/source.json')
    quality=read(batch['normalized_hdfs_path']+'/quality.json')
    params=read(batch['normalized_hdfs_path']+'/config.json')
    config_digest=hashlib.sha256(json.dumps(params,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    if config_digest!=task['config_sha256']:raise ManifestError('Parameters do not match recorded hash')
    if source['source_sha256']!=batch['source_sha256'] or source['batch_id']!=batch_id or source['raw_rows']!=batch['raw_rows']:
        raise ManifestError('Source audit differs from database')
    if quality['metrics']!={key:metrics.get(key) for key in quality['metrics']} or quality['batch_id']!=batch_id or quality['run_id']!=task['run_id']:
        raise ManifestError('Quality report differs from database')
    if not quality['quality_gate_passed']:raise ManifestError('Quality preflight failed')
    count=batch['raw_rows']
    if count!=sum(batch[k] for k in ('sale_rows','return_rows','rejected_rows')) or count!=manifest.record_count:
        raise ManifestError('Classification conservation failed')
    root=Path(config['paths']['work_dir'])/'tmp';root.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='w03-verify-',dir=root) as temporary:
        scan=scan_normalized(storage,normalized,batch['source_sha256'],count,Path(temporary)/'index.sqlite')
    if scan['duplicate_excess_rows']!=metrics['DUPLICATE_EXCESS_ROWS'] or scan['duplicate_groups']!=quality['duplicate_groups']:
        raise ManifestError('Independent duplicate count differs')
    return {'batch_id':batch_id,'run_id':task['run_id'],'source_sha256':batch['source_sha256'],
        'source_bytes':batch['source_bytes'],'batch_status':batch['status'],'task_status':task['status'],
        'attempts':len(tasks),'classification_counts':quality['classification_counts'],
        'metrics':metrics,'quality_gate_passed':True,'conflicting_orders':quality['conflicting_orders'],
        'files':verified_files,'normalized_scan':scan,'source':source,
        'raw_excel_rows_evidence':'Successful ingest and reuse both fully verify XLSX sheet rows; this independent scan rechecks immutable source SHA and normalized row trace.',
        'status':'PASS'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--db-defaults-file',required=True,type=Path)
    parser.add_argument('--batch-id',required=True)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    report={'baseline_version':'HL-DOC-1.0.0','observed_at':datetime.now(timezone.utc).isoformat().replace('+00:00','Z')}
    connection=None
    try:
        config=load_config(args.config);connection=connect(config,args.db_defaults_file)
        report.update(verify(config,connection,args.batch_id))
    except Exception as error:
        report.update(status='FAIL',error_type=type(error).__name__)
    finally:
        if connection is not None:connection.close()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,default=str,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'evidence':str(args.output)}))
    return 0 if report['status']=='PASS' else 6


if __name__=='__main__':raise SystemExit(main())
