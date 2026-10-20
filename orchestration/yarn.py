"""Observe exact run-named YARN applications; observation failures are not terminal states."""
import json
from pathlib import Path
import re
import subprocess
import time
from urllib.request import Request,urlopen
from xml.etree import ElementTree
from orchestration.database import LockBusy
from etl.ingest import live_child

TERMINAL={'FINISHED','FAILED','KILLED'}


class Yarn:
    def __init__(self,config):
        values={p.findtext('name'):p.findtext('value') for p in ElementTree.parse(Path(config['runtime']['hadoop_conf_dir'])/'yarn-site.xml').findall('property')}
        self.url='http://'+values.get('yarn.resourcemanager.webapp.address',values.get('yarn.resourcemanager.hostname','localhost')+':8088')+'/ws/v1/cluster'

    def request(self,path,data=None):
        request=Request(self.url+path,data=json.dumps(data).encode() if data else None,
            headers={'Accept':'application/json','Content-Type':'application/json'},method='PUT' if data else 'GET')
        with urlopen(request,timeout=10) as response:return json.load(response)

    def app(self,application_id):
        if not re.fullmatch(r'application_[0-9]+_[0-9]+',application_id):raise ValueError('Invalid application identity')
        return self.request('/apps/'+application_id)['app']

    def find(self,name):
        apps=self.request('/apps').get('apps') or {}
        matches=[a for a in apps.get('app',[]) if a['name']==name]
        if len(matches)>1:raise LockBusy('Multiple applications share a run identity')
        return matches[0] if matches else None

    def terminal(self,task):
        app=self.app(task['application_id'])
        if app['name']!='retail-'+task['task_name']+'-'+task['run_id']:raise LockBusy('Application name differs from task')
        if app['state'] not in TERMINAL:raise LockBusy('Original application remains active')
        return 'SUCCEEDED' if app['finalStatus']=='SUCCEEDED' else ('CANCELLED' if app['finalStatus']=='KILLED' else 'FAILED')

    def run(self,command,task,control,tick,directory,environment):
        log=directory/'driver.log'
        application_id=None;offset=0;pending=''
        with log.open('wb') as output:
            process=subprocess.Popen(command,stdout=output,stderr=subprocess.STDOUT,env=environment)
            try:
                while True:
                    tick()
                    with log.open() as source:
                        source.seek(offset);new=source.read();offset=source.tell()
                    pending+=new
                    lines=pending.split('\n');pending=lines.pop()
                    for line in lines:
                        if line.startswith('RETAIL_APPLICATION\t'):
                            found=line.split('\t')[1]
                            if application_id not in (None,found):raise LockBusy('Driver changed application identity')
                            application_id=found;control.record_application(task['run_id'],found)
                    if process.poll() is not None:break
                    time.sleep(1)
            except BaseException:
                # This is our own child and exact run-named application. Stop submitting work
                # after losing lock ownership; leave DB RUNNING for authoritative recovery.
                if process.poll() is None:process.terminate();process.wait(timeout=20)
                app=self.app(application_id) if application_id else self.find('retail-'+task['task_name']+'-'+task['run_id'])
                if app and app['state'] not in TERMINAL:
                    self.request('/apps/'+app['id']+'/state',{'state':'KILLED'})
                raise
        if application_id is None:
            app=self.find('retail-'+task['task_name']+'-'+task['run_id'])
            if app:
                application_id=app['id'];control.record_application(task['run_id'],application_id)
            elif live_child(task['run_id']):raise LockBusy('Submission child still alive')
            else:raise RuntimeError('Java driver failed before submission; inspect private driver log')
        # RM observation can lag process exit. Poll the same identity; never resubmit.
        while True:
            tick();app=self.app(application_id)
            if app['state'] in TERMINAL:break
            time.sleep(2)
        if app['name']!='retail-'+task['task_name']+'-'+task['run_id']:raise LockBusy('Application association differs')
        counters={};job_counters={}
        for line in log.read_text().splitlines():
            if line.startswith('RETAIL_JOB_COUNTER\t'):
                _,name,value=line.split('\t');job_counters[name]=int(value)
            if line.startswith('RETAIL_RESOURCE\t'):
                _,cpu,gc=line.split('\t');job_counters.update(cpu_milliseconds=int(cpu),gc_milliseconds=int(gc))
            if line.startswith('RETAIL_COUNTER\t'):
                _,name,value=line.split('\t');counters[name]=int(value)
        if process.returncode or app['finalStatus']!='SUCCEEDED':raise RuntimeError('Business YARN application failed')
        app['job_counters']=job_counters
        return app,counters
