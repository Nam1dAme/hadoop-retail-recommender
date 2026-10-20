"""Disk-backed preflight audit; results never replace W04 Java/YARN computations."""
from collections import Counter
from datetime import date,datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from etl.normalize import normalize,MAX_I64,HEADERS
from etl.tsv import line
from etl.workbook import rows
from orchestration.quality import REJECTIONS,WARNINGS


def raw_value(value):
    if isinstance(value,(datetime,date)):
        return value.isoformat()
    if isinstance(value,Decimal) or (isinstance(value,float) and not math.isfinite(value)):
        return str(value)
    return value


def convert(source, directory, source_sha256, sheet=None, date_format=None, heartbeat=lambda:None):
    directory = Path(directory)
    directory.mkdir(parents=True,exist_ok=True)
    metrics = Counter({name:0 for name in ('RAW_ROWS','SALE_ROWS','RETURN_ROWS','REJECTED_ROWS',
                      'DUPLICATE_EXCESS_ROWS','NON_MERCHANDISE_SALE_ROWS')})
    metrics.update({name+'_ROWS':0 for name in (*REJECTIONS,*WARNINGS)})
    index = sqlite3.connect(directory/'audit.sqlite')
    index.execute('PRAGMA cache_size=-8192')
    index.executescript('''CREATE TABLE duplicate_row (digest TEXT PRIMARY KEY,n INTEGER NOT NULL);
        CREATE TABLE invoice (invoice TEXT PRIMARY KEY,day TEXT NOT NULL,country TEXT NOT NULL,customer TEXT);
        CREATE TABLE conflict (invoice TEXT NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(invoice,reason));''')
    totals = {'gross_pence':0,'return_pence':0,'sale_quantity':0,'return_quantity':0}
    minimum=maximum=None
    overflow=False
    try:
        with (directory/'normalized.tsv').open('w',encoding='utf-8',newline='') as output, \
             (directory/'warnings.tsv').open('w',encoding='utf-8',newline='') as warnings, \
             (directory/'rejected-audit.jsonl').open('w',encoding='utf-8',newline='') as rejected:
            with rows(source,sheet) as (sheet, records):
                for number,raw,cell_error in records:
                    heartbeat()
                    normalized = normalize(raw,source_sha256,number,date_format,cell_error)
                    values = normalized.fields
                    output.write(line(values))
                    metrics['RAW_ROWS'] += 1
                    metrics[normalized.record_class+'_ROWS'] += 1
                    if normalized.primary_reason:
                        metrics[normalized.primary_reason+'_ROWS'] += 1
                        rejected.write(json.dumps({'row_id':values[0],'source_row_number':number,
                            'primary_reason':normalized.primary_reason,'field_errors':normalized.field_errors,
                            'raw':dict(zip(HEADERS,map(raw_value,raw)))},ensure_ascii=False,allow_nan=False)+'\n')
                    for warning in normalized.warnings:
                        metrics[warning+'_ROWS'] += 1
                        warnings.write(line((values[0],warning)))
                    duplicate_values = values[2:10]
                    # The frozen duplicate definition uses the eight normalized
                    # fields, including NULL. Raw rejected values remain in the audit.
                    key = hashlib.sha256(json.dumps(duplicate_values,ensure_ascii=False,sort_keys=True,allow_nan=False).encode()).hexdigest()
                    index.execute('INSERT INTO duplicate_row VALUES (?,1) ON CONFLICT(digest) DO UPDATE SET n=n+1',(key,))
                    if values[6]:
                        day=values[6][:10]
                        minimum=min(minimum or day,day);maximum=max(maximum or day,day)
                    if normalized.record_class in ('SALE','RETURN'):
                        prefix='sale' if normalized.record_class=='SALE' else 'return'
                        totals['gross_pence' if prefix=='sale' else 'return_pence'] += abs(normalized.amount)
                        totals[prefix+'_quantity'] += abs(values[5])
                        if any(value>MAX_I64 for value in totals.values()):overflow=True
                    if normalized.record_class=='SALE':
                        invoice,country,customer=values[2],values[9],values[8]
                        day=values[6][:10]
                        old=index.execute('SELECT day,country,customer FROM invoice WHERE invoice=?',(invoice,)).fetchone()
                        if old:
                            for field,left,right in zip(('SOURCE_DATE','COUNTRY','CUSTOMER'),old,(day,country,customer)):
                                if left is not None and right is not None and left!=right:
                                    index.execute('INSERT OR IGNORE INTO conflict VALUES (?,?)',(invoice,field))
                            if old[2] is None and customer is not None:
                                index.execute('UPDATE invoice SET customer=? WHERE invoice=?',(customer,invoice))
                        else:index.execute('INSERT INTO invoice VALUES (?,?,?,?)',(invoice,day,country,customer))
                        if not normalized.is_merchandise:metrics['NON_MERCHANDISE_SALE_ROWS'] += 1
                    if metrics['RAW_ROWS']%5000==0:index.commit()
        index.commit()
        metrics['DUPLICATE_EXCESS_ROWS']=index.execute('SELECT COALESCE(SUM(n-1),0) FROM duplicate_row').fetchone()[0]
        duplicate_groups=index.execute('SELECT COUNT(*) FROM duplicate_row WHERE n>1').fetchone()[0]
        conflicts=index.execute('SELECT COUNT(DISTINCT invoice) FROM conflict').fetchone()[0]
        with (directory/'order-conflicts.tsv').open('w',encoding='utf-8',newline='') as output:
            for invoice,reason in index.execute('SELECT invoice,reason FROM conflict ORDER BY invoice,reason'):
                output.write(line((invoice,reason)))
        raw=metrics['RAW_ROWS']
        assert raw==sum(metrics[name+'_ROWS'] for name in ('SALE','RETURN','REJECTED'))
        failures=[]
        if not metrics['SALE_ROWS']:failures.append('NO_SALE')
        if raw and metrics['REJECTED_ROWS']*20>raw:failures.append('REJECTION_RATE_EXCEEDED')
        if conflicts:failures.append('ORDER_IDENTITY_CONFLICT')
        if overflow:failures.append('AGGREGATE_OVERFLOW')
        report={'schema_version':'1.0','baseline_version':'HL-DOC-1.0.0','policy_version':'retail-clean-v1',
            'source_sha256':source_sha256,'sheet':sheet,'raw_rows':raw,
            'classification_counts':{name:metrics[name+'_ROWS'] for name in ('SALE','RETURN','REJECTED')},
            'metrics':dict(metrics),'duplicate_groups':duplicate_groups,'conflicting_orders':conflicts,
            'min_source_date':minimum,'max_source_date':maximum,'preflight_totals':totals,
            'quality_gate_passed':not failures,'quality_gate_failures':failures,
            'scope':'Normalization preflight only. W04 must independently perform actual business filtering and aggregation on YARN.'}
        (directory/'quality.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        return report
    finally:
        index.close()
