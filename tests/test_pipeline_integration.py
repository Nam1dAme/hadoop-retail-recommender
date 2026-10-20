"""Whole orchestration on real isolated MariaDB + Java LocalJobRunner.

File/YARN adapters are explicit synthetic test infrastructure, never T-07 evidence.
"""
import json
import os
from pathlib import Path
import subprocess

import pytest
from etl.ingest import ingest
from etl.storage import FileStorage
from orchestration.pipeline import pipeline
from orchestration.settings import load_config,ConfigError
from test_normalization import workbook,BASE

ROOT=Path(__file__).resolve().parents[1]


class IsolatedStorage(FileStorage):
    def __init__(self,root,config):
        super().__init__(root)
        self.environment=os.environ|{'JAVA_HOME':config['runtime']['java_home'],'HADOOP_CONF_DIR':config['runtime']['hadoop_conf_dir']}
    def run(self,*args):
        assert args[:2]==('-ls','-R')
        directory=self.path(args[2]);rows=[]
        for p in directory.rglob('*'):
            if p.is_file() and not p.name.endswith('.crc'):rows.append('-rw-r----- 1 test test '+str(p.stat().st_size)+' 2026-10-20 00:00 '+str(p))
        return 0,('\n'.join(rows)+'\n').encode()


class LocalRunner:
    def __init__(self):self.apps={}
    def find(self,name):return next((app for app in self.apps.values() if app['name']==name),None)
    def app(self,identity):return self.apps[identity]
    def terminal(self,task):
        assert self.apps[task['application_id']]['name']=='retail-'+task['task_name']+'-'+task['run_id']
        return 'SUCCEEDED'
    def run(self,command,task,control,tick,directory,environment):
        command=[arg.replace('-Dmapreduce.framework.name=yarn','-Dmapreduce.framework.name=local') if arg.startswith('-Dmapreduce.framework.name=') else '-Dfs.defaultFS=file:///' if arg.startswith('-Dfs.defaultFS=') else arg for arg in command]
        with (directory/'driver.log').open('w') as log:
            result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,env=environment,timeout=90)
        assert result.returncode==0,(directory/'driver.log').read_text()[-5000:]
        tick(force=True)
        counters={}
        for line in (directory/'driver.log').read_text().splitlines():
            if line.startswith('RETAIL_COUNTER\t'):
                _,key,value=line.split('\t');counters[key]=int(value)
        identity='application_1234_'+str(len(self.apps)+1).zfill(4)
        app={'id':identity,'name':'retail-'+task['task_name']+'-'+task['run_id'],'state':'FINISHED','finalStatus':'SUCCEEDED'}
        self.apps[identity]=app;control.record_application(task['run_id'],identity)
        return app,counters


def test_whole_pipeline_reuse_and_final_quality(db,tmp_path,monkeypatch):
    config=load_config('config/development.toml');config['paths']['work_dir']=str(tmp_path)
    config['database']['name']=db[1];config['hdfs']['root']=str(tmp_path/'hdfs')
    storage=IsolatedStorage(config['hdfs']['root'],config);connection=db[0]['writer']
    source=workbook(tmp_path/'source.xlsx',[BASE.copy()])
    result=ingest(connection,config,source,'https://example.invalid/isolated-test',synthetic=True,storage=storage)
    runner=LocalRunner()
    with pytest.raises(ConfigError):pipeline(connection,config,result['batch_id'],storage=storage,yarn=runner)
    from orchestration.control import ControlStore
    from orchestration.manifests import ManifestError
    original=ControlStore.finish_task
    def fail_after_publish(self,run_id,*args,**kwargs):
        raise RuntimeError('isolated crash after bundle publication')
    monkeypatch.setattr(ControlStore,'finish_task',fail_after_publish)
    with pytest.raises(RuntimeError):pipeline(connection,config,result['batch_id'],reducers=1,synthetic=True,storage=storage,yarn=runner)
    monkeypatch.setattr(ControlStore,'finish_task',original)
    with connection.cursor() as cursor:
        cursor.execute("UPDATE task_run SET pid=999999999 WHERE task_name='J01'")
    connection.commit()
    first=pipeline(connection,config,result['batch_id'],reducers=1,synthetic=True,storage=storage,yarn=runner)
    assert first['status']=='SUCCEEDED' and len(first['run_ids'])==13 and len(runner.apps)==12
    quality=json.loads(Path(first['quality_report']).read_text())
    assert quality['status']=='PASS' and quality['metrics']['TRUNCATED_BASKETS']==0
    second=pipeline(connection,config,result['batch_id'],reducers=1,synthetic=True,storage=storage,yarn=runner)
    assert first['run_ids']==second['run_ids'] and len(runner.apps)==12
    with connection.cursor() as cursor:
        cursor.execute('SELECT status FROM data_batch');assert cursor.fetchone()['status']=='SUCCEEDED'
        cursor.execute('SELECT COUNT(*) AS n FROM task_run');assert cursor.fetchone()['n']==14
        cursor.execute('SELECT COUNT(*) AS n FROM data_quality WHERE metric_name IN (\'TRUNCATED_BASKETS\',\'TRUNCATED_BASKET_ITEMS\')');assert cursor.fetchone()['n']==2

    with pytest.raises(ManifestError):pipeline(connection,config,result['batch_id'],reducers=2,synthetic=True,storage=storage,yarn=runner)
    target=Path(first['datasets']['sales_cube']['files'][0]['path'])
    before=target.read_bytes();target.write_bytes(before+b'tampered\n')
    with pytest.raises(ManifestError):pipeline(connection,config,result['batch_id'],reducers=1,synthetic=True,storage=storage,yarn=runner)
    target.write_bytes(before)
