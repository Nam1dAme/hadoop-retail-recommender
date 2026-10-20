"""Immutable directory publication under an explicit local/HDFS root."""
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import time
from orchestration.manifests import HdfsStore,LocalStore,ManifestError,fingerprint


class StorageError(RuntimeError):
    pass


class DependencyUnavailable(Exception):
    pass


class HdfsStorage(HdfsStore):
    def __init__(self,config):
        runtime=config['runtime']
        super().__init__(config['hdfs']['root'],str(Path(runtime['hadoop_home'])/'bin/hdfs'),
            config['hdfs']['uri'],os.environ|{'JAVA_HOME':runtime['java_home'],'HADOOP_CONF_DIR':runtime['hadoop_conf_dir']})
        self.heartbeat=lambda:None

    def run(self,*args,allow_absent=False):
        self.heartbeat()
        process=subprocess.Popen([*self.command,*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.environment)
        deadline=time.monotonic()+300
        try:
            while True:
                try:
                    stdout,stderr=process.communicate(timeout=2)
                    break
                except subprocess.TimeoutExpired:
                    self.heartbeat()
                    if time.monotonic()>deadline:raise StorageError('HDFS operation timed out')
            if process.returncode and not (allow_absent and process.returncode==1 and not stderr.strip()):
                raise StorageError('HDFS operation failed')
            return process.returncode,stdout
        finally:
            if process.poll() is None:
                process.terminate();process.wait(timeout=10)

    def ready(self):
        self.run('-ls','-d',str(self.root))

    def isdir(self,path):
        return self.run('-test','-d',self.check(path),allow_absent=True)[0]==0

    def mkdir(self,path):
        self.run('-mkdir','-p',self.check(path));self.run('-chmod','750',path)

    def put(self,local,path):
        self.run('-put',str(local),self.check(path));self.run('-chmod','640',path)

    def move(self,source,target):
        if self.isdir(target):raise ManifestError('Immutable destination already exists')
        self.run('-mv',self.check(source),self.check(target))


class FileStorage(LocalStore):
    """Isolated filesystem integration fixture; never a production HDFS substitute."""
    def __init__(self,root):
        Path(root).mkdir(parents=True,exist_ok=True)
        super().__init__(root)
        self.heartbeat=lambda:None

    def path(self,path):
        value=Path(path)
        if not value.is_absolute() or not value.resolve().is_relative_to(self.root):
            raise ManifestError('Artifact path escapes isolated root')
        return value

    def ready(self):pass
    def isdir(self,path):return self.path(path).is_dir()
    def mkdir(self,path):self.path(path).mkdir(parents=True,exist_ok=True)
    def put(self,local,path):
        target=self.path(path)
        with target.open('xb') as output,Path(local).open('rb') as source:shutil.copyfileobj(source,output)
    def move(self,source,target):
        source,target=self.path(source),self.path(target)
        if target.exists():raise ManifestError('Immutable destination already exists')
        source.rename(target)


def publish(storage,files,destination,run_id,heartbeat=lambda:None):
    """Upload, checksum every staged file, then atomically rename the complete directory."""
    if storage.isdir(destination):raise ManifestError('Refusing to overwrite an existing immutable directory')
    parent=str(PurePosixPath(destination).parent)
    temporary=parent+'/.'+PurePosixPath(destination).name+'.run_id='+run_id+'.tmp'
    if storage.isdir(temporary):raise ManifestError('Run staging directory already exists')
    storage.mkdir(temporary)
    for name,local in files.items():
        heartbeat()
        storage.put(local,temporary+'/'+name)
        source=fingerprint(LocalStore(Path(local).parent),str(Path(local).resolve()),heartbeat=heartbeat)
        target=fingerprint(storage,temporary+'/'+name,heartbeat=heartbeat)
        if source[:2]!=target[:2]:raise ManifestError('Staged file hash or byte count differs')
    storage.move(temporary,destination)
