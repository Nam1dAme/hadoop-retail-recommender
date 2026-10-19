"""Real MariaDB integration fixtures, isolated under var; never use the host service."""
import getpass
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from uuid import uuid4
import pytest
from orchestration.database import open_connection
from orchestration.provision import provision


@pytest.fixture(scope='session')
def mariadb_instance():
    if not shutil.which('mariadbd') or not shutil.which('mariadb-install-db'):
        pytest.skip('Real MariaDB binaries are required for W02 integration tests')
    root=Path(__file__).resolve().parents[1]/'var'
    root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='w02-tests-',dir=root) as directory:
        directory=Path(directory)
        socket=directory/'mysql.sock'
        subprocess.run(['mariadb-install-db','--no-defaults','--datadir='+str(directory/'data'),
            '--auth-root-authentication-method=socket','--skip-test-db'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,check=True,timeout=40)
        process=subprocess.Popen(['mariadbd','--no-defaults','--datadir='+str(directory/'data'),
            '--socket='+str(socket),'--pid-file='+str(directory/'mysql.pid'),'--skip-networking',
            '--log-error='+str(directory/'mysql.log')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            deadline=time.monotonic()+30
            while True:
                if process.poll() is not None:
                    pytest.fail('Isolated MariaDB stopped before startup')
                try:
                    connection=open_connection(user=getpass.getuser(),unix_socket=str(socket))
                    break
                except Exception:
                    if time.monotonic()>deadline:
                        raise
                    time.sleep(0.1)
            yield connection,str(socket),directory
            connection.close()
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)


@pytest.fixture
def db(mariadb_instance):
    admin,socket,directory=mariadb_instance
    token=uuid4().hex[:10]
    name='w02_'+token
    credentials=directory/token
    provision(admin,name,credentials,directory/'backups',unix_socket=socket,prefix=name)
    def role(role):
        values={}
        for line in (credentials/(role+'.env')).read_text().splitlines():
            key,value=line.removeprefix('export ').split('=',1)
            values[key]=value
        return open_connection(name=name,user=values['RETAIL_DB_USER'],password=values['RETAIL_DB_PASSWORD'],unix_socket=socket)
    connections={r:role(r) for r in ('migration','writer','reader')}
    yield connections,name,credentials,directory
    for connection in connections.values():
        connection.close()
