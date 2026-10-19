"""Create private, verifiable logical backups without credentials in argv."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from orchestration.database import utcnow


def backup_database(connection, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    def quote(value):
        return '"'+str(value).replace('\\','\\\\').replace('"','\\"').replace('\n','\\n').replace('\r','\\r')+'"'
    options = {'user': connection.user.decode() if isinstance(connection.user,bytes) else connection.user,
               'password': connection.password.decode() if isinstance(connection.password,bytes) else connection.password,
               'host': connection.host, 'port': connection.port, 'default-character-set': 'utf8mb4'}
    if connection.unix_socket:
        options.pop('host')
        options.pop('port')
        options['socket'] = connection.unix_socket
        options['protocol'] = 'socket'
    else:
        options['protocol'] = 'tcp'
    stamp = utcnow().strftime('%Y%m%dT%H%M%S%f')
    path = directory / (stamp+'.sql')
    with tempfile.NamedTemporaryFile(mode='w', dir=directory) as credentials:
        credentials.write('[client]\n'+''.join(k+'='+quote(v)+'\n' for k,v in options.items()))
        credentials.flush()
        with path.open('xb') as output:
            os.chmod(path,0o600)
            result = subprocess.run(['mariadb-dump', '--defaults-file='+credentials.name,
                '--single-transaction', '--skip-lock-tables', '--hex-blob',
                connection.db.decode() if isinstance(connection.db,bytes) else connection.db],
                stdout=output, stderr=subprocess.PIPE, timeout=120)
    if result.returncode or not path.stat().st_size:
        raise RuntimeError('Database backup failed; migration refused')
    evidence = {'path':str(path),'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    manifest = path.with_suffix('.json')
    with manifest.open('x') as stream:
        os.chmod(manifest,0o600)
        json.dump(evidence,stream)
    return evidence
