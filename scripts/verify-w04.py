#!/usr/bin/env python3
"""Verify formal W04 application identities, immutable bundles and business results."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import io
import subprocess
import zipfile
from pathlib import Path
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from etl.ingest import read_json
from etl.storage import HdfsStorage
from orchestration.database import connect,named_lock
from orchestration.control import ControlStore
from orchestration.manifests import verify_manifest,ManifestError
from orchestration.pipeline import STAGES,expected
from orchestration.pipeline_audit import audit
from orchestration.runtime import Heartbeat
from orchestration.settings import load_config
from orchestration.yarn import Yarn


def verify(config,connection,batch_id):
    storage=HdfsStorage(config);yarn=Yarn(config)
    with named_lock(connection):
        tick=Heartbeat(connection,ControlStore(connection));storage.heartbeat=tick
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM data_batch WHERE batch_id=%s',(batch_id,));batch=cursor.fetchone()
            cursor.execute('SELECT * FROM task_run WHERE batch_id=%s ORDER BY created_at',(batch_id,));tasks=cursor.fetchall()
            cursor.execute('SELECT metric_name,metric_value FROM data_quality WHERE batch_id=%s',(batch_id,));metrics={r['metric_name']:r['metric_value'] for r in cursor.fetchall()}
        connection.rollback()
        if not batch or batch['status']!='SUCCEEDED':raise ManifestError('Formal batch is not successful')
        outputs={};applications=[];manifests=[];latest={t['task_name']:t for t in tasks}
        for stage in STAGES:
            task=latest.get(stage)
            if task is None or task['status']!='SUCCEEDED':raise ManifestError('Missing successful pipeline stage')
            manifest=read_json(storage,task['output_manifest_path'])
            verify_manifest(manifest,storage,expected(task),heartbeat=tick)
            if manifest['execution']['stage']!=stage or set(manifest['datasets'])!=set(STAGES[stage][1:]):raise ManifestError('Stage dataset bundle differs')
            if manifest.get('code_dirty'):raise ManifestError('W04 acceptance requires committed execution code')
            archived=next(a for a in manifest['artifacts'] if a['path'].endswith('/source-code.zip'))
            with zipfile.ZipFile(io.BytesIO(b''.join(storage.chunks(archived['path'])))) as archive:
                if set(archive.namelist())!=set(manifest['source_code_sha256']):raise ManifestError('Source snapshot membership differs')
                for name,digest in manifest['source_code_sha256'].items():
                    if hashlib.sha256(archive.read(name)).hexdigest()!=digest:raise ManifestError('Snapshot source hash differs')
                    if not name.startswith('runtime-config/'):
                        committed=subprocess.check_output(['git','show',task['code_commit']+':'+name],cwd=ROOT,stderr=subprocess.DEVNULL)
                        if hashlib.sha256(committed).hexdigest()!=digest:raise ManifestError('Executed source differs from committed source')
            params=next(a for a in manifest['artifacts'] if a['path'].endswith('/config.json'))
            actual=read_json(storage,params['path'])
            if hashlib.sha256(json.dumps(actual,sort_keys=True,separators=(',',':')).encode()).hexdigest()!=task['config_sha256']:
                raise ManifestError('Stage parameter digest differs')
            jar=next(a for a in manifest['artifacts'] if a['path'].endswith('/business.jar'))
            if jar['sha256']!=actual['jar_sha256']:raise ManifestError('Executed JAR differs from parameters')
            if stage!='pipeline':
                if not task['application_id']:raise ManifestError('Business stage lacks application identity')
                if yarn.terminal(task)!='SUCCEEDED':raise ManifestError('Business application failed')
                app=manifest['execution']['application']
                if app['id']!=task['application_id'] or app['finalStatus']!='SUCCEEDED':raise ManifestError('Manifest application differs')
                applications.append(manifest['execution'])
            outputs.update(manifest['datasets'])
            manifests.append({'stage':stage,'run_id':task['run_id'],'attempt':task['attempt'],'application_id':task['application_id'],
                'path':task['output_manifest_path'],'code_commit':task['code_commit'],'config_sha256':task['config_sha256']})
        temporary=Path(config['paths']['work_dir'])/'tmp';temporary.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='w04-independent-',dir=temporary) as directory:
            result=audit(storage,batch,outputs,Path(directory),tick)
        if metrics['TRUNCATED_BASKETS']!=result['truncated_baskets'] or metrics['TRUNCATED_BASKET_ITEMS']!=result['truncated_basket_items']:
            raise ManifestError('Truncation metrics differ from independent calculation')
        original=read_json(storage,batch['normalized_hdfs_path']+'/quality.json')
        if any(metrics.get(k)!=v for k,v in original['metrics'].items()):raise ManifestError('W03 quality metrics drifted')
        quality=read_json(storage,batch['quality_report_path'])
        for key in ('counts','gross_pence','return_pence','net_pence','order_count','valid_customers','rfm_reference_date','eligible_basket_count'):
            if result[key]!=quality[key]:raise ManifestError('Stored final quality report differs from independent verification')
        return {'status':'PASS','batch_id':batch_id,'source_sha256':batch['source_sha256'],'source_rows':batch['raw_rows'],
            'batch_status':batch['status'],'applications':applications,'manifests':manifests,'independent_business_verification':result,
            'metrics':metrics,'quality_report_path':batch['quality_report_path'],'yarn_business_gate':'PASS',
            'scope':'W04 only; no model training, release or application page delivered'}


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
    except Exception as error:report.update(status='FAIL',error_type=type(error).__name__)
    finally:
        if connection is not None:connection.close()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,default=str,indent=2,allow_nan=False)+'\n')
    print(json.dumps({'status':report['status'],'evidence':str(args.output)}))
    return 0 if report['status']=='PASS' else 6


if __name__=='__main__':raise SystemExit(main())
