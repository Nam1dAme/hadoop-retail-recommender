"""Real Java LocalJobRunner contract tests; YARN acceptance is recorded separately."""
from datetime import datetime
import os
from pathlib import Path
import subprocess
import pytest
from etl.normalize import normalize
from etl.tsv import line,parse_line

ROOT=Path(__file__).resolve().parents[1]
SHA='a'*64


def dataset(directory,name):
    return [parse_line(row,len(row.rstrip('\n').split('\t'))) for p in sorted((directory/name).glob('part-*')) for row in p.open()]


@pytest.fixture(scope='module')
def java_runner(tmp_path_factory):
    env=os.environ|{'JAVA_HOME':'/usr/lib/jvm/java-8-openjdk-amd64','HADOOP_CONF_DIR':str(ROOT/'config/hadoop'),'HADOOP_MAPRED_HOME':'/usr/local/hadoop'}
    env['PATH']=env['JAVA_HOME']+'/bin:'+env['PATH']
    subprocess.run(['mvn','-q','-Dmaven.repo.local='+str(ROOT/'var/cache/maven'),'-f',str(ROOT/'mapreduce/pom.xml'),'package'],check=True,env=env,timeout=180)
    root=tmp_path_factory.mktemp('java-pipeline')
    def run(stage,input_path,expect_failure=False,**options):
        directory=root/(stage+'-'+str(len(list(root.iterdir()))))
        command=['/usr/local/hadoop/bin/hadoop','jar',str(ROOT/'mapreduce/target/retail-mapreduce-0.1.0-SNAPSHOT.jar'),'cn.hadooplab.retail.RetailJobs',
            '-Dmapreduce.framework.name=local','-Dfs.defaultFS=file:///', '-Dmapreduce.jobtracker.staging.root.dir='+str(root/'staging'),
            '--stage',stage,'--run-id','test-'+directory.name,'--input',str(input_path),'--output',str(directory),'--reducers','1']
        for k,v in options.items():command.extend(['--'+k.replace('_','-'),str(v)])
        result=subprocess.run(command,capture_output=True,text=True,env=env,timeout=120)
        (root/(directory.name+'.log')).write_text(result.stdout+result.stderr)
        assert (result.returncode!=0 if expect_failure else result.returncode==0),(result.stdout+result.stderr)[-10000:]
        return directory,result.stdout
    return root,run


@pytest.fixture(scope='module')
def hand_calculation(java_runner):
    root,run=java_runner
    spec=[('08-01','100001','1','10001',2,1),('08-01','100001','1','10002',1,3),
      ('08-02','100002','1','10001',1,1),('08-02','100002','1','10003',2,2),
      ('08-03','100003','2','10001',1,1),('08-03','100003','2','10002',1,3),
      ('08-04','C100004','1','10002',-1,3),('08-05','100005',None,'10004',1,4),
      ('08-05','100006','3','10001',0,1),('08-06','100007','2','10003',1,2),
      ('08-06','100008','3','10001',1,1)]
    normalized=root/'normalized.tsv';warnings=root/'warnings.tsv'
    with normalized.open('w') as n,warnings.open('w') as w:
        for i,(day,invoice,customer,stock,q,p) in enumerate(spec,2):
            row=normalize([invoice,stock,stock,q,datetime.fromisoformat('2011-'+day+'T12:00:00'),p,customer,'UK'],SHA,i)
            n.write(line(row.fields))
            for warning in row.warnings:w.write(line([row.fields[0],warning]))
    clean,stdout=run('J01',normalized,warnings=warnings,source_sha=SHA)
    orders,_=run('J02',clean/'clean')
    sales,_=run('J03',clean/'clean')
    product,_=run('J04',clean/'clean')
    base,_=run('J05base',orders/'order_fact')
    distribution,_=run('J05distribution',base/'rfm_base',customers=3)
    cache=','.join(str(p) for p in (distribution/'rfm_scores').glob('part-*'))
    rfm,_=run('J05',base/'rfm_base',cache=cache,customers=3,reference_date='2011-08-07')
    baskets,_=run('J06baskets',clean/'clean',orders=orders/'order_fact')
    interactions,_=run('J06interactions',clean/'clean')
    occurrences,pair_stdout=run('J07baskets',baskets/'baskets')
    pairs,_=run('J07',occurrences/'occurrences')
    cache=','.join(str(p) for p in (pairs/'item_frequency').glob('part-*'))
    similarity,_=run('J08',pairs/'item_pairs',cache=cache,baskets=6)
    return locals()


def test_t04_sales_orders(hand_calculation):
    x=hand_calculation
    assert 'RETAIL_COUNTER\tRAW_ROWS\t11' in x['stdout']
    assert 'RETAIL_COUNTER\tSALE_ROWS\t9' in x['stdout']
    assert len(dataset(x['clean'],'rejected'))==1
    orders=dataset(x['orders'],'order_fact')
    assert len(orders)==6 and len({r[3] for r in orders if r[3]})==3
    sales=dataset(x['sales'],'sales_cube')
    assert sum(int(r[3]) for r in sales)==2100
    assert sum(int(r[4]) for r in sales)==300
    assert sum(int(r[4]) for r in orders)==2100
    assert 2100//len(orders)==350
    assert {stock:sum(int(r[3]) for r in sales if r[2]==stock) for stock in ['10001','10002','10003','10004']}=={'10001':500,'10002':600,'10003':600,'10004':400}
    assert len([r for r in dataset(x['product'],'product_order_fact') if r[1]=='10001'])==4
    assert sum(int(r[3]) for r in sales if r[0]<='2011-08-02' and r[1]=='UK')==1000


def test_t05_rfm(hand_calculation):
    rows={r[0]:r[1:] for r in dataset(hand_calculation['rfm'],'rfm')}
    assert rows=={
      '1':['2011-08-02','5','2','1000','2','2','4','OTHER'],
      '2':['2011-08-06','1','2','600','5','2','2','NEW'],
      '3':['2011-08-06','1','1','100','5','1','1','NEW']}


def test_t06_pairs_empty_threshold(hand_calculation):
    x=hand_calculation
    assert len(dataset(x['baskets'],'baskets'))==9
    assert {r[0]:int(r[1]) for r in dataset(x['pairs'],'item_frequency')}=={'10001':4,'10002':2,'10003':2,'10004':1}
    assert {tuple(r[:2]):int(r[2]) for r in dataset(x['pairs'],'item_pairs')}=={('10001','10002'):2,('10001','10003'):1}
    assert dataset(x['similarity'],'similarity')==[]
    assert 'RETAIL_COUNTER\tELIGIBLE_BASKET_COUNT\t6' in x['pair_stdout']
    assert len(dataset(x['interactions'],'interactions'))==7


def test_t06_truncation_and_formula(java_runner):
    root,run=java_runner
    source=root/'large-baskets.tsv'
    # One basket of 102 distinct items, repeated small baskets, and a singleton.
    # Inputs here are J06's deduplicated basket contract.
    with source.open('w') as out:
        for stock in reversed(range(10001,10103)):
            out.write(line(['large','1','2011-08-01T12:00:00',str(stock)]))
        for invoice in ('small1','small2'):
            for stock in ('10001','10002'):out.write(line([invoice,None,'2011-08-02T12:00:00',stock]))
        out.write(line(['single',None,'2011-08-03T12:00:00','10001']))
    occurrences,stdout=run('J07baskets',source)
    assert 'RETAIL_COUNTER\tTRUNCATED_BASKETS\t1' in stdout
    assert 'RETAIL_COUNTER\tTRUNCATED_BASKET_ITEMS\t2' in stdout
    counts,_=run('J07',occurrences/'occurrences')
    frequency={r[0]:int(r[1]) for r in dataset(counts,'item_frequency')}
    assert len(frequency)==100 and frequency['10001']==4 and frequency['10002']==3
    assert '10101' not in frequency and '10102' not in frequency
    cache=','.join(str(p) for p in (counts/'item_frequency').glob('part-*'))
    similarity,_=run('J08',counts/'item_pairs',cache=cache,baskets=4)
    rows={tuple(r[:2]):r for r in dataset(similarity,'similarity')}
    assert set(rows)=={('10001','10002'),('10002','10001')}
    assert float(rows['10001','10002'][2])==pytest.approx(3/(12**0.5))
    assert float(rows['10001','10002'][4])==.75
    assert float(rows['10002','10001'][4])==1
    assert float(rows['10001','10002'][5])==1


def test_order_conflict_and_aggregate_overflow(java_runner):
    root,run=java_runner
    def clean(invoice,day,customer,amount):
        return line(['a'*64,2,invoice,'10001','description',1,1,day+'T12:00:00',day,amount,amount,customer,'UK','SALE','true'])
    conflict=root/'conflict.tsv'
    conflict.write_text(clean('100001','2011-08-01','1',100)+clean('100001','2011-08-02','1',100))
    run('J02',conflict,expect_failure=True)
    conflict.write_text(clean('100001','2011-08-01','1',100)+clean('100001','2011-08-01','2',100))
    run('J02',conflict,expect_failure=True)
    conflict.write_text(clean('100001','2011-08-01','1',2**63-1)+clean('100001','2011-08-01','1',1))
    run('J02',conflict,expect_failure=True)


def test_train_cutoff_precedes_basket_and_interaction_aggregation(java_runner):
    root, run = java_runner
    source = root / 'time-split-clean.tsv'
    source.write_text(''.join(line(['a'*64, n, invoice, item, 'description', 1, 1,
        day+'T12:00:00', day, 100, 100, 'u', 'UK', 'SALE', 'true'])
        for n, invoice, item, day in [(2,'a','10001','2011-08-31'),
                                      (3,'b','10002','2011-09-01'),
                                      (4,'c','10003','2011-11-01')]))
    orders, _ = run('J02', source)
    baskets, _ = run('J06baskets', source, orders=orders/'order_fact', train_end='2011-09-01')
    interactions, _ = run('J06interactions', source, train_end='2011-09-01')
    assert [r[3] for r in dataset(baskets, 'baskets')] == ['10001']
    assert dataset(interactions, 'interactions') == [['u', '10001', '1', '2011-08-31']]


def test_independent_audit_recomputes_all_results(hand_calculation,tmp_path):
    from etl.storage import FileStorage
    from orchestration.manifests import DATASETS,fingerprint
    from orchestration.pipeline_audit import audit
    x=hand_calculation;storage=FileStorage(x['root'])
    names={'clean':['clean','rejected','warnings'],'orders':['order_fact'],'sales':['sales_cube'],
        'product':['product_order_fact'],'rfm':['rfm'],'baskets':['baskets'],
        'interactions':['interactions','customer_seen'],'pairs':['item_pairs','item_frequency'],'similarity':['similarity']}
    datasets={}
    for variable,outputs in names.items():
        for name in outputs:
            refs=[]
            for path in (x[variable]/name).glob('part-*'):
                digest,_,rows=fingerprint(storage,str(path));refs.append({'path':str(path),'sha256':digest,'rows':rows})
            datasets[name]={'schema':DATASETS[name].split(','),'files':refs}
    report=audit(storage,{'batch_id':'isolated-hand-calculation','raw_rows':11,'sale_rows':9,'return_rows':1,'rejected_rows':1},datasets,tmp_path)
    assert (report['gross_pence'],report['return_pence'],report['order_count'],report['valid_customers'])==(2100,300,6,3)
    assert report['eligible_basket_count']==6 and report['status']=='PASS'
