#!/usr/bin/env python3
"""Independent real-data W05 audit: reconstruct both contexts directly from clean SALE."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import zipfile

from etl.ingest import read_json, sha256, write_json, ROOT
from etl.storage import HdfsStorage
from etl.tsv import line
from ml.data import read_tsv
from orchestration.control import ControlStore
from orchestration.database import connect, named_lock
from orchestration.experiment_data import experiment_task, materialize
from orchestration.manifests import ManifestError, verify_manifest
from orchestration.pipeline import expected, stream_rows
from orchestration.runtime import Heartbeat
from orchestration.settings import load_config
from orchestration.yarn import Yarn


def verify(connection, config, experiment_id):
    storage = HdfsStorage(config)
    control = ControlStore(connection)
    with named_lock(connection):
        tick = Heartbeat(connection, control)
        storage.heartbeat = tick
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM experiment WHERE experiment_id=%s', (experiment_id,))
            experiment = cursor.fetchone()
            if not experiment:
                raise ManifestError('Unknown experiment')
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name=%s AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1",
                           (experiment['batch_id'], experiment_task(experiment_id)))
            task = cursor.fetchone()
            cursor.execute("SELECT * FROM task_run WHERE batch_id=%s AND task_name='J01' AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1", (experiment['batch_id'],))
            clean_task = cursor.fetchone()
        connection.rollback()
        if not task or not clean_task:
            raise ManifestError('Missing successful preparation/clean tasks')
        manifest = read_json(storage, task['output_manifest_path'])
        verify_manifest(manifest, storage, expected(task), heartbeat=tick)
        result = manifest['execution']['result']
        clean = read_json(storage, clean_task['output_manifest_path'])
        verify_manifest(clean, storage, expected(clean_task), heartbeat=tick)
        applications = []
        for run_id in result['train_features']['run_ids']:
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM task_run WHERE run_id=%s', (run_id,))
                row = cursor.fetchone()
            connection.rollback()
            bundle = read_json(storage, row['output_manifest_path'])
            verify_manifest(bundle, storage, expected(row), heartbeat=tick)
            app = Yarn(config).app(row['application_id'])
            if app['state'] != 'FINISHED' or app['finalStatus'] != 'SUCCEEDED' or app['name'] != 'retail-'+row['task_name']+'-'+run_id:
                raise ManifestError('TRAIN YARN application association/state differs')
            parameters = bundle['execution']['parameters']
            if parameters['purpose'] != 'EVAL' or parameters['train_end_exclusive'] != '2011-09-01':
                raise ManifestError('TRAIN feature window differs')
            if parameters['stage'] in ('J06baskets', 'J06interactions') and parameters['options']['train-end'] != '2011-09-01':
                raise ManifestError('Mapper cutoff missing')
            applications.append({'run_id':run_id,'application_id':app['id'],'stage':parameters['stage'],'final_status':app['finalStatus']})
        if {a['stage'] for a in applications} != {'J06baskets','J06interactions','J07baskets','J07','J08'}:
            raise ManifestError('Incomplete TRAIN feature chain')
        with tempfile.TemporaryDirectory(dir=config['paths']['work_dir'], prefix='verify-w05-') as temporary:
            temporary = Path(temporary)
            data = materialize(storage, result, temporary/'data', tick)
            database = sqlite3.connect(temporary/'audit.sqlite')
            database.set_progress_handler(lambda: (tick() or 0), 10000)
            database.execute('CREATE TABLE baskets(invoice TEXT,item TEXT,customer TEXT,day TEXT,PRIMARY KEY(invoice,item)) WITHOUT ROWID')
            for number, fields in enumerate(stream_rows(storage, clean['datasets']['clean']['files'], 15, tick), 1):
                if fields[13] != 'SALE' or fields[14] != 'true':
                    continue
                database.execute('''INSERT INTO baskets VALUES (?,?,?,?) ON CONFLICT(invoice,item)
                    DO UPDATE SET customer=COALESCE(customer,excluded.customer)''', (fields[2],fields[3],fields[11],fields[8]))
                if number % 4096 == 0:
                    tick()
            contexts = {}
            for purpose in ('eval','fullfit'):
                predicate = "day<'2011-09-01'" if purpose == 'eval' else '1=1'
                users = [r[0] for r in database.execute(f'SELECT DISTINCT customer FROM baskets WHERE {predicate} AND customer IS NOT NULL ORDER BY customer COLLATE BINARY')]
                items = [r[0] for r in database.execute(f'SELECT DISTINCT item FROM baskets WHERE {predicate} AND customer IS NOT NULL ORDER BY item COLLATE BINARY')]
                mapping = list(read_tsv(data/purpose/'mapping.tsv', 3))
                required = [['USER',u,str(i)] for i,u in enumerate(users)] + [['ITEM',item,str(i)] for i,item in enumerate(items)]
                if mapping != required:
                    raise ManifestError('Mapping differs from clean source window')
                actual = {}
                for u,i,count,last in read_tsv(data/purpose/'positive_interactions.tsv',4):
                    key=(users[int(u)],items[int(i)])
                    if key in actual:
                        raise ManifestError('Duplicate positive interaction')
                    actual[key]=(int(count),last)
                required = {(u,i):(n,last) for u,i,n,last in database.execute(f'''SELECT customer,item,COUNT(*),MAX(day) FROM baskets
                    WHERE {predicate} AND customer IS NOT NULL GROUP BY customer,item''')}
                if actual != required:
                    raise ManifestError('Interaction counts/dates differ from independent source reconstruction')
                catalog=set(items)
                popularity=[(i,int(n)) for i,n in read_tsv(data/purpose/'popularity.tsv',2)]
                required_pop=[(i,n) for i,n in database.execute(f'''SELECT item,COUNT(*) FROM baskets WHERE {predicate}
                    GROUP BY item ORDER BY COUNT(*) DESC,item COLLATE BINARY''') if i in catalog]
                if popularity != required_pop:
                    raise ManifestError('Popularity differs from source including anonymous baskets')
                contexts[purpose]={'user_count':len(users),'catalog_count':len(items),'positive_interactions':len(actual),
                                   'mapping_sha256':sha256(data/purpose/'mapping.tsv')}
                if purpose == 'eval':
                    train_history=defaultdict(set)
                    for u,i in actual:
                        train_history[u].add(i)
                    train_users=set(users)
                    train_catalog=catalog
            windows={}
            for split,predicate in (('validation',"day>='2011-09-01' AND day<'2011-11-01'"),('test',"day>='2011-11-01'")):
                purchases=defaultdict(set)
                for u,i in database.execute(f'SELECT DISTINCT customer,item FROM baskets WHERE {predicate} AND customer IS NOT NULL'):
                    purchases[u].add(i)
                relevant={u:(v & train_catalog)-train_history.get(u,set()) for u,v in purchases.items()}
                expected_truth=[(u,i) for u in sorted(relevant,key=lambda x:x.encode()) for i in sorted(relevant[u],key=lambda x:x.encode())]
                actual_truth=[tuple(r) for r in read_tsv(data/split/'truth.tsv',2)]
                if actual_truth != expected_truth:
                    raise ManifestError('Shared relevance labels differ from source and TRAIN exclusions')
                stats=json.loads((data/split/'cohorts.json').read_text())
                counts={'window_user_count':len(purchases),'cold_user_count':len(purchases.keys()-train_users),
                    'cold_item_count':len(set().union(*(v-train_catalog for v in purchases.values()))),
                    'cold_interaction_count':sum(len(v-train_catalog) for v in purchases.values()),
                    'excluded_user_count':sum(not v for v in relevant.values())}
                if any(stats[k] != v for k,v in counts.items()):
                    raise ManifestError('Cold/excluded/window counts differ')
                windows[split]=counts | {'warm_users':sum(bool(v) and u in train_users for u,v in relevant.items()),
                                         'all_eligible_users':sum(bool(v) for v in relevant.values())}
            database.close()
            source_zip=next(a for a in manifest['artifacts'] if a['path'].endswith('/source-code.zip'))
            archive=temporary/'source-code.zip'
            archive.write_bytes(b''.join(storage.chunks(source_zip['path'])))
            with zipfile.ZipFile(archive) as source:
                for name,digest in manifest['source_code_sha256'].items():
                    original=subprocess.check_output(['git','show',task['code_commit']+':'+name],cwd=ROOT)
                    if hashlib.sha256(source.read(name)).hexdigest() != digest or hashlib.sha256(original).hexdigest() != digest:
                        raise ManifestError('Prepared source snapshot differs from committed implementation')
            return {'status':'PASS','experiment_id':experiment_id,'batch_id':experiment['batch_id'],
                'preparation_run_id':task['run_id'],'preparation_code_commit':task['code_commit'],
                'contexts':contexts,'windows':windows,'applications':applications,
                'checks':['committed source snapshot','immutable hashes and schemas','TRAIN cutoff before J06/J07',
                          'both mappings from clean','both interaction counts/dates','anonymous-aware popularity',
                          'shared future labels minus TRAIN history','window/cold/excluded counts']}


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
    finally:
        if connection is not None:
            connection.close()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    write_json(args.output,report)
    print(json.dumps({'status':report['status'],'evidence':str(args.output)}))
    return 0 if report['status']=='PASS' else 6


if __name__=='__main__':
    raise SystemExit(main())
