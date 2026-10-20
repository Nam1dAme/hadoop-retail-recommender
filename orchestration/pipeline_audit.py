"""Independent, disk-backed relational checks of W04 outputs before batch success."""
from collections import Counter
from contextlib import contextmanager
from datetime import date,timedelta
import math
import re
from pathlib import Path
import sqlite3

from orchestration.manifests import DATASETS,ManifestError
from orchestration.pipeline import stream_rows

INTEGERS={'source_row_number','quantity','quantity_abs','unit_price_pence','signed_amount_pence',
    'gross_pence','return_pence','sale_quantity','return_quantity','recency_days','frequency',
    'monetary_pence','r_score','f_score','m_score','order_count','co_count','basket_count','rank','count','value','score'}


@contextmanager
def audit_database(path,heartbeat=lambda:None):
    """Keep lock checks/heartbeats running inside long SQLite VM operations."""
    db=sqlite3.connect(path)
    interrupted=[]
    def progress():
        try:
            heartbeat()
            return 0
        except Exception as error:
            interrupted.append(error)
            return 1
    db.set_progress_handler(progress,10000)
    try:
        yield db
    except sqlite3.OperationalError:
        if interrupted:raise interrupted[0]
        raise
    finally:db.close()


def audit(storage,batch,datasets,directory,heartbeat=lambda:None):
    with audit_database(Path(directory)/'audit.sqlite',heartbeat) as db:
        return audit_data(db,storage,batch,datasets,heartbeat)


def audit_data(db,storage,batch,datasets,heartbeat):
    db.execute('PRAGMA cache_size=-16384');db.execute('PRAGMA temp_store=FILE')
    counts={}
    checks=[]
    def check(condition,label):
        if not condition:raise ManifestError('Independent W04 check failed: '+label)
        checks.append(label)
    def scalar(sql):return db.execute(sql).fetchone()[0]
    def equal(actual,expected,label):
        # Bidirectional set difference plus key uniqueness assertions below; do not
        # accept equal totals as proof that individual groups are correct.
        check(db.execute('SELECT * FROM ('+actual+' EXCEPT '+expected+') LIMIT 1').fetchone() is None and
              db.execute('SELECT * FROM ('+expected+' EXCEPT '+actual+') LIMIT 1').fetchone() is None,label)
    for name,dataset in datasets.items():
        if name in ('occurrences','rfm_base','rfm_scores','pipeline_summary'):continue
        schema=DATASETS[name].split(',')
        columns=','.join('"'+column+'" '+('INTEGER' if column in INTEGERS else 'TEXT') for column in schema)
        db.execute('CREATE TABLE "'+name+'" ('+columns+')')
        count=0;pending=[]
        for row in stream_rows(storage,dataset['files'],len(schema),heartbeat):
            for i,column in enumerate(schema):
                if column in INTEGERS and row[i] is not None:
                    value=int(row[i])
                    if str(value)!=row[i] or not -(2**63)<=value<2**63:raise ManifestError('Noncanonical/overflowing output integer')
                    row[i]=value
            if name=='clean':
                if row[14] != ('true' if re.fullmatch(r'[0-9]{5,6}[A-Z]{0,2}',row[3] or '',flags=re.ASCII) else 'false'):
                    raise ManifestError('Merchandise eligibility differs from stock code')
            pending.append(row);count+=1
            if len(pending)>=4000:
                db.executemany('INSERT INTO "'+name+'" VALUES ('+','.join('?' for _ in schema)+')',pending);pending=[];heartbeat()
        if pending:db.executemany('INSERT INTO "'+name+'" VALUES ('+','.join('?' for _ in schema)+')',pending)
        check(count==sum(f['rows'] for f in dataset['files']),name+'_row_count')
        counts[name]=count;db.commit()
    for table,columns in {'clean':'row_id','rejected':'row_id','warnings':'row_id,warning_code',
            'order_fact':'invoice_no','product_order_fact':'invoice_no,stock_code','sales_cube':'source_date,country,stock_code',
            'rfm':'customer_id','baskets':'invoice_no,stock_code','interactions':'customer_id,stock_code',
            'customer_seen':'customer_id,stock_code','item_pairs':'item_i,item_j',
            'item_frequency':'stock_code','similarity':'stock_code,related_stock_code'}.items():
        db.execute('CREATE UNIQUE INDEX "'+table+'_identity" ON "'+table+'" ('+columns+')')
    db.execute('CREATE INDEX clean_invoice ON clean(invoice_no)')
    db.execute('CREATE INDEX clean_customer ON clean(customer_id)')
    check(scalar('SELECT COUNT(*) FROM clean JOIN rejected USING(row_id)')==0,'disjoint_clean_rejected')
    check(counts['clean']+counts['rejected']==batch['raw_rows'],'classification_conservation')
    check(scalar("SELECT COUNT(*) FROM clean WHERE record_class='SALE'")==batch['sale_rows'],'sale_count')
    check(scalar("SELECT COUNT(*) FROM clean WHERE record_class='RETURN'")==batch['return_rows'],'return_count')
    check(counts['rejected']==batch['rejected_rows'],'rejected_count')
    check(scalar("SELECT COUNT(*) FROM clean WHERE record_class NOT IN ('SALE','RETURN') OR quantity_abs<=0 OR unit_price_pence<=0 OR signed_amount_pence != CASE record_class WHEN 'SALE' THEN quantity_abs*unit_price_pence ELSE -quantity_abs*unit_price_pence END")==0,'clean_amount_sign')
    check(scalar('SELECT COUNT(*) FROM warnings w WHERE NOT EXISTS(SELECT 1 FROM clean c WHERE c.row_id=w.row_id) AND NOT EXISTS(SELECT 1 FROM rejected r WHERE r.row_id=w.row_id)')==0,'warning_references')
    sale="SELECT * FROM clean WHERE record_class='SALE'"
    check(scalar('SELECT COUNT(*) FROM (SELECT invoice_no FROM ('+sale+') GROUP BY invoice_no HAVING COUNT(DISTINCT source_date)>1 OR COUNT(DISTINCT country)>1 OR COUNT(DISTINCT customer_id)>1)')==0,'order_identity')
    equal('SELECT * FROM order_fact','SELECT invoice_no,source_date,country,MAX(customer_id),SUM(signed_amount_pence) FROM ('+sale+') GROUP BY invoice_no','order_facts')
    equal('SELECT * FROM sales_cube',"SELECT source_date,country,stock_code,SUM(CASE WHEN record_class='SALE' THEN signed_amount_pence ELSE 0 END),SUM(CASE WHEN record_class='RETURN' THEN -signed_amount_pence ELSE 0 END),SUM(CASE WHEN record_class='SALE' THEN quantity_abs ELSE 0 END),SUM(CASE WHEN record_class='RETURN' THEN quantity_abs ELSE 0 END) FROM clean GROUP BY source_date,country,stock_code",'sales_groups')
    equal('SELECT * FROM product_order_fact','SELECT invoice_no,stock_code,source_date,country,SUM(signed_amount_pence),SUM(quantity) FROM ('+sale+') GROUP BY invoice_no,stock_code','product_order_distinct')
    gross=scalar('SELECT SUM(gross_pence) FROM sales_cube') or 0
    returns=scalar('SELECT SUM(return_pence) FROM sales_cube') or 0
    sale_quantity=scalar('SELECT SUM(sale_quantity) FROM sales_cube') or 0
    return_quantity=scalar('SELECT SUM(return_quantity) FROM sales_cube') or 0
    check(gross==(scalar('SELECT SUM(gross_pence) FROM order_fact') or 0)==(scalar('SELECT SUM(gross_pence) FROM product_order_fact') or 0),'gross_conservation')
    check(all(-(2**63)<=value<2**63 for value in (gross,returns,gross-returns,sale_quantity,return_quantity)),'global_amount_int64')
    base=scalar('SELECT MAX(source_date) FROM order_fact')
    reference=date.fromisoformat(base)+timedelta(days=1)
    db.execute('CREATE TEMP TABLE expected_rfm AS SELECT customer_id,MAX(source_date) AS last,COUNT(*) AS frequency,SUM(gross_pence) AS monetary FROM order_fact WHERE customer_id IS NOT NULL GROUP BY customer_id')
    # Frequency distributions, not transaction details, fit in memory. Customers
    # here are already aggregated; no source record list is collected.
    n=scalar('SELECT COUNT(*) FROM expected_rfm')
    distributions=[Counter(),Counter(),Counter()]
    for customer,last,f,m in db.execute('SELECT * FROM expected_rfm'):
        for distribution,value in zip(distributions,((reference-date.fromisoformat(last)).days,f,m)):distribution[value]+=1
    scores=[]
    for i,distribution in enumerate(distributions):
        less=0;mapping={}
        for value,count in sorted(distribution.items()):
            q=min(5,1+5*less//n);mapping[value]=6-q if i==0 else q;less+=count
        scores.append(mapping)
    check(counts['rfm']==n,'rfm_customer_count')
    for customer,last,f,m in db.execute('SELECT * FROM expected_rfm'):
        recency=(reference-date.fromisoformat(last)).days
        r,fs,ms=(mapping[value] for mapping,value in zip(scores,(recency,f,m)))
        segment='CORE' if r>=4 and fs>=4 and ms>=4 else 'AT_RISK' if r<=2 and fs>=4 else 'NEW' if r>=4 and fs<=2 else 'POTENTIAL' if r>=3 else 'OTHER'
        actual=db.execute('SELECT * FROM rfm WHERE customer_id=?',(customer,)).fetchone()
        check(actual==(customer,last,recency,f,m,r,fs,ms,segment),'rfm_'+customer)
    eligible="SELECT * FROM clean WHERE record_class='SALE' AND is_merchandise='true'"
    db.execute('CREATE TEMP TABLE expected_baskets AS SELECT c.invoice_no,o.customer_id,c.invoice_at,c.stock_code FROM ('+eligible+') c JOIN order_fact o USING(invoice_no) GROUP BY c.invoice_no,c.stock_code HAVING c.source_row_number=MIN(c.source_row_number)')
    equal('SELECT * FROM baskets','SELECT * FROM expected_baskets','basket_dedup_and_identity')
    equal('SELECT * FROM interactions','SELECT customer_id,stock_code,COUNT(DISTINCT invoice_no),MAX(source_date) FROM ('+eligible+') WHERE customer_id IS NOT NULL GROUP BY customer_id,stock_code','model_interactions_untruncated')
    equal('SELECT * FROM customer_seen','SELECT customer_id,stock_code FROM interactions','seen_history')
    # SQLite window ordering uses BINARY UTF-8 ordering, matching frozen IDs.
    db.execute('CREATE TEMP TABLE truncated AS SELECT invoice_no,stock_code FROM (SELECT invoice_no,stock_code,ROW_NUMBER() OVER(PARTITION BY invoice_no ORDER BY stock_code COLLATE BINARY) AS position FROM expected_baskets) WHERE position<=100')
    db.execute('CREATE INDEX trunc_invoice ON truncated(invoice_no)')
    basket_count=scalar('SELECT COUNT(DISTINCT invoice_no) FROM truncated')
    truncations=scalar('SELECT COUNT(*) FROM (SELECT invoice_no FROM expected_baskets GROUP BY invoice_no HAVING COUNT(*)>100)')
    excess=counts['baskets']-scalar('SELECT COUNT(*) FROM truncated')
    equal('SELECT * FROM item_frequency','SELECT stock_code,COUNT(*) FROM truncated GROUP BY stock_code','item_frequency_same_baskets')
    db.execute('CREATE TEMP TABLE expected_pairs AS SELECT a.stock_code AS item_i,b.stock_code AS item_j,COUNT(*) AS co_count FROM truncated a JOIN truncated b ON a.invoice_no=b.invoice_no AND a.stock_code<b.stock_code GROUP BY a.stock_code,b.stock_code')
    equal('SELECT * FROM item_pairs','SELECT * FROM expected_pairs','pair_counts')
    frequencies=dict(db.execute('SELECT stock_code,basket_count FROM item_frequency'))
    db.execute('CREATE INDEX expected_pair_i ON expected_pairs(item_i)');db.execute('CREATE INDEX expected_pair_j ON expected_pairs(item_j)')
    expected_similarity_count=0
    for stock in sorted(frequencies):
        heartbeat();candidates=[]
        for other,co in db.execute('SELECT item_j,co_count FROM expected_pairs WHERE item_i=? AND co_count>=3 UNION ALL SELECT item_i,co_count FROM expected_pairs WHERE item_j=? AND co_count>=3',(stock,stock)):
            sim=co/math.sqrt(frequencies[stock]*frequencies[other]);confidence=co/frequencies[stock];lift=co*basket_count/(frequencies[stock]*frequencies[other])
            candidates.append((other,sim,co,confidence,lift))
        candidates.sort(key=lambda r:(-r[1],-r[2],r[0].encode()))
        rows=list(db.execute('SELECT related_stock_code,similarity,co_count,confidence,lift,rank FROM similarity WHERE stock_code=? ORDER BY rank',(stock,)))
        check(len(rows)==min(10,len(candidates)),'similarity_count_'+stock)
        expected_similarity_count+=len(rows)
        for rank,(actual,want) in enumerate(zip(rows,candidates[:10]),1):
            check(actual[0]==want[0] and actual[2]==want[2] and actual[5]==rank and all(math.isfinite(float(actual[i])) and abs(float(actual[i])-want[i])<=1e-12 for i in (1,3,4)),'similarity_formula_'+stock+'_'+str(rank))
    check(counts['similarity']==expected_similarity_count,'similarity_no_unknown_items')
    return {'status':'PASS','batch_id':batch['batch_id'],'baseline_version':'HL-DOC-1.0.0',
        'counts':counts,'gross_pence':gross,'return_pence':returns,'net_pence':gross-returns,
        'sale_quantity':sale_quantity,'return_quantity':return_quantity,
        'order_count':counts['order_fact'],'valid_customers':counts['rfm'],'rfm_reference_date':reference.isoformat(),
        'eligible_basket_count':basket_count,'truncated_baskets':truncations,'truncated_basket_items':excess,
        'checks_passed':len(checks),'checks':sorted(set(label.split('_')[0] if label.startswith(('rfm_','similarity_')) else label for label in checks)),
        'verification_method':'Independent SQLite relational aggregation, frequency scoring and item-pair formula checks; disk-backed source rows'}
