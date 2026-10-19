"""Initialize only the configured retail HDFS namespace; never format or overwrite data."""
import getpass
import os
from pathlib import Path
import subprocess
from orchestration.manifests import HdfsStore, ManifestError

AREAS=('raw','normalized','clean','features','results','models','releases')


def initialize_storage(config,dry_run=False):
    root=config['hdfs']['root']
    executable=str(Path(config['runtime']['hadoop_home'])/'bin/hdfs')
    store=HdfsStore(root,executable)
    environment=os.environ | {'JAVA_HOME':config['runtime']['java_home'],
        'HADOOP_CONF_DIR':config['runtime']['hadoop_conf_dir']}
    def run(*args,check=True):
        result=subprocess.run([executable,'dfs','-fs',config['hdfs']['uri'],*args],capture_output=True,text=True,env=environment,timeout=30)
        if check and result.returncode:
            raise ManifestError('HDFS namespace operation failed')
        return result
    run('-ls','-d','/')
    result=[]
    for path in [root,*(root+'/'+area for area in AREAS)]:
        store.check(path)
        exists=run('-test','-d',path,check=False).returncode==0
        if exists:
            fields=run('-ls','-d',path).stdout.strip().split()
            if len(fields)!=8 or fields[2]!=getpass.getuser() or fields[0][5]=='w' or fields[0][8]=='w':
                raise ManifestError('Existing HDFS namespace has unexpected ownership/write permissions')
        elif not dry_run:
            run('-mkdir','-p',path)
            run('-chmod','750',path)
        result.append({'path':path,'state':'EXISTS' if exists else ('PLANNED' if dry_run else 'CREATED')})
    return result
