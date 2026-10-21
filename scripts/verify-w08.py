#!/usr/bin/env python3
"""Independent W08 audit: source sales/recommendations, old controls and baseline."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
import pymysql.cursors
from etl.ingest import ROOT, sha256
from etl.storage import HdfsStorage
from ml.data import read_tsv
from orchestration.database import connect
from orchestration.settings import load_config
from orchestration.training import fetch


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--config',required=True,type=Path)
    p.add_argument('--db-defaults-file',required=True,type=Path)
    p.add_argument('--release-id',required=True)
    p.add_argument('--output',required=True,type=Path)
    p.add_argument('--before-controls',type=Path)
    a=p.parse_args()
    config=load_config(a.config)
    c=connect(config,a.db_defaults_file)
    storage=HdfsStorage(config)
    directory=Path(config['paths']['work_dir'])/'w08-independent'/str(time.time_ns())
    directory.mkdir(parents=True)
    def query(sql,args=()):
        with c.cursor() as q:q.execute(sql,args);return q.fetchall()
    def task(name):
        row=query("SELECT output_manifest_path FROM task_run WHERE batch_id=%s AND task_name=%s AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1",(release['batch_id'],name))[0]
        root=str(Path(row['output_manifest_path']).parent)
        local=fetch(storage,root,directory)
        source_manifest=json.loads((local/'manifest.json').read_text())
        assert sha256(local/'manifest.json')==dependencies[row['output_manifest_path']]['sha256']
        return root,local,source_manifest
    release=query('SELECT * FROM data_release WHERE release_id=%s',(a.release_id,))[0]
    assert release['status']=='READY'
    release_root=str(Path(release['manifest_path']).parent)
    local=fetch(storage,release_root,directory)
    manifest=json.loads((local/'release.json').read_text())
    assert sha256(local/'release.json')==release['manifest_sha256']
    dependencies={f['path']:f for f in manifest['dependencies']}
    bindings={r['algorithm']:r['model_run_id'] for r in query('SELECT * FROM release_model WHERE release_id=%s',(a.release_id,))}
    assert bindings==manifest['models'] and len(bindings)==4
    db=sqlite3.connect(directory/'source-comparison.sqlite')
    db.execute('CREATE TABLE sales(day TEXT,country TEXT,item TEXT,gross INTEGER,returns INTEGER,sq INTEGER,rq INTEGER,PRIMARY KEY(day,country,item))')
    root,source,m=task('J03')
    for ref in m['datasets']['sales_cube']['files']:
        path=source/Path(ref['path']).relative_to(root)
        assert sha256(path)==ref['sha256']==dependencies[ref['path']]['sha256']
        db.executemany('INSERT INTO sales VALUES (?,?,?,?,?,?,?)',
            ((day,country,item,*map(int,values)) for day,country,item,*values in read_tsv(path,7)))
    source_sales=iter(db.execute('SELECT * FROM sales ORDER BY day,country,item'))
    sales_count=0
    with c.cursor(pymysql.cursors.SSCursor) as q:
        q.execute('SELECT source_date,country,stock_code,gross_amount,return_amount,sale_quantity,return_quantity FROM sales_cube WHERE release_id=%s ORDER BY source_date,country,stock_code',(a.release_id,))
        for day,country,item,gross,returns,sq,rq in q:
            assert next(source_sales)==(day.isoformat(),country,item,int(gross*100),int(returns*100),sq,rq)
            sales_count+=1
    assert next(source_sales,None) is None
    total=query('SELECT SUM(gross_amount) gross,SUM(return_amount) returns,SUM(sale_quantity) sq,SUM(return_quantity) rq FROM sales_cube WHERE release_id=%s',(a.release_id,))[0]
    assert int(total['gross']*100)==manifest['audit']['gross_pence']
    assert int(total['returns']*100)==manifest['audit']['return_pence']
    assert total['sq']==manifest['audit']['sale_quantity'] and total['rq']==manifest['audit']['return_quantity']
    recommendations={}
    for algorithm,mid in sorted(bindings.items()):
        root,source,m=task('recommend_'+mid.replace('-',''))
        path=source/'recommendations.tsv'
        assert sha256(path)==dependencies[root+'/recommendations.tsv']['sha256']
        expected=iter(read_tsv(path,7))
        count=0
        with c.cursor(pymysql.cursors.SSCursor) as q:
            q.execute('SELECT customer_id,stock_code,`rank`,score,reason_code,is_fallback FROM customer_recommendation WHERE release_id=%s AND algorithm=%s ORDER BY customer_id,`rank`',(a.release_id,algorithm))
            for user,item,rank,score,reason,fallback in q:
                model,eu,ei,er,es,ereason,ef=next(expected)
                assert model==mid and (user,item,rank,score,reason,fallback)==(eu,ei,int(er),float(es),ereason,int(ef))
                count+=1
        assert next(expected,None) is None
        recommendations[algorithm]={'rows':count,'source_sha256':sha256(path),'exact_items_order_scores':True}
    frozen={}
    for name,digest in re.findall(r'\[([^\]]+\.md)\]\([^)]*\) \| `([0-9a-f]{64})`',(ROOT/'docs/BASELINE-MANIFEST.md').read_text()):
        assert sha256(ROOT/'docs'/name)==digest
        frozen[name]=digest
    assert len(frozen)==18
    controls={}
    if a.before_controls:
        before=json.loads(a.before_controls.read_text())
        for table,rows in before.items():
            if not rows or table=='current_release':continue
            now=query('SELECT * FROM `'+table+'`')
            actual={json.dumps(r,sort_keys=True,default=str) for r in now}
            assert all(json.dumps(r,sort_keys=True) in actual for r in rows),table+' prior rows changed'
            controls[table]={'prior_rows':len(rows),'unchanged':True}
    current_id=query('SELECT release_id FROM current_release WHERE singleton_id=1')[0]['release_id']
    events=query('SELECT * FROM release_event ORDER BY created_at')
    report={'status':'PASS','release_id':a.release_id,'current_release_id':current_id,
        'manifest_sha256':release['manifest_sha256'],'sales_cube_rows_compared_to_J03':sales_count,
        'gross_gbp':format(total['gross'],'.2f'),'return_gbp':format(total['returns'],'.2f'),
        'net_gbp':format(total['gross']-total['returns'],'.2f'),'recommendations':recommendations,
        'old_control_rows':controls,'frozen_baseline':frozen,'events':events,
        'scope':'Fresh HDFS source-vs-MariaDB comparison of every sales cube and recommendation row; all prior control rows preserved'}
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(report,default=str,sort_keys=True,indent=2)+'\n')
    print(json.dumps({'status':'PASS','release_id':a.release_id,'sales_rows':sales_count,
                      'recommendation_rows':sum(r['rows'] for r in recommendations.values())}))
    db.close();c.rollback();c.close()


if __name__=='__main__':main()
