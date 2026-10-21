"""Checksummed release backups and non-destructive MariaDB/HDFS recovery drills."""
from contextlib import contextmanager
from copy import deepcopy
import getpass
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import shutil
import subprocess
import time
from uuid import uuid4

from etl.ingest import sha256, write_json
from etl.storage import HdfsStorage
from orchestration.control import ControlStore
from orchestration.database import named_lock, open_connection
from orchestration.db_backup import backup_database
from orchestration.manifests import ManifestError
from orchestration.publication import load_release, dependency_root, verify
from orchestration.releases import verify_local
from orchestration.runtime import Heartbeat
from orchestration.training import SnapshotStore, fetch, upload_bundle


def recovery_root(config, root):
    original = PurePosixPath(config['hdfs']['root'])
    target = PurePosixPath(root)
    if (not re.fullmatch(r'/retail/recovery/[A-Za-z0-9_-]+', root)
            or target.is_relative_to(original) or original.is_relative_to(target)):
        raise ManifestError('Recovery requires a new isolated /retail/recovery/<name> root')
    return str(target)


class RelocatedStorage(HdfsStorage):
    """Read original immutable paths from a new root without editing manifests."""
    def __init__(self, config, target):
        super().__init__(config)
        self.target = recovery_root(config, target)
        relocated = deepcopy(config)
        relocated['hdfs']['root'] = self.target
        self.restored = HdfsStorage(relocated)

    def mapped(self, path):
        self.check(path)
        return self.target+'/'+str(PurePosixPath(path).relative_to(self.root))

    def run(self, *args, **kwargs):
        return super().run(*(self.mapped(a) if isinstance(a, str) and
                            (a == str(self.root) or a.startswith(str(self.root)+'/')) else a for a in args), **kwargs)

    def chunks(self, path):
        yield from self.restored.chunks(self.mapped(path))

    def exists(self, path):
        return self.restored.exists(self.mapped(path))


def physical_backup_gate(directory):
    """A second partition on the WSL disk is not independent physical storage."""
    match = re.match(r'^/mnt/([a-z])(?:/|$)', str(Path(directory).resolve()))
    if not match:
        return {'passed': False, 'reason': 'Local WSL filesystem; independent physical storage not established'}
    powershell = '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
    command = "$p=@(Get-Partition | Where-Object DriveLetter | Select-Object DriveLetter,DiskNumber); $w=@(Get-ChildItem HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Lxss | Get-ItemProperty | Select-Object DistributionName,BasePath); @{partitions=$p;wsl=$w}|ConvertTo-Json -Depth 4"
    try:
        topology = json.loads(subprocess.check_output([powershell, '-NoProfile', '-Command', command], timeout=30))
        partitions = {p['DriveLetter'].upper(): p['DiskNumber'] for p in topology['partitions']}
        distro = next(w for w in topology['wsl'] if w['DistributionName'] == 'Ubuntu-24.04')
        source = distro['BasePath'][0].upper()
        target = match[1].upper()
        passed = source in partitions and target in partitions and partitions[source] != partitions[target]
        return {'passed': passed, 'wsl_drive': source, 'target_drive': target,
                'wsl_disk': partitions.get(source), 'target_disk': partitions.get(target),
                'reason': 'Distinct Windows physical disk' if passed else 'Same physical disk or unverified topology'}
    except (OSError, ValueError, StopIteration, subprocess.SubprocessError):
        return {'passed': False, 'reason': 'Independent physical storage could not be verified'}


def backup(connection, config, release_id, directory):
    directory = Path(directory).expanduser().resolve()
    if directory.exists():
        raise ManifestError('Backup destination must be a new directory')
    with named_lock(connection):
        tick = Heartbeat(connection, ControlStore(connection))
        manifest, local, digest = load_release(connection, config, release_id, tick=tick)
        comparison = verify_local(connection, release_id, manifest, local, digest, tick)
        directory.mkdir(parents=True, mode=0o700)
        os.chmod(directory, 0o700)
        hdfs = directory/'hdfs'
        hdfs.mkdir(mode=0o700)
        bundle_target = hdfs/'releases'/('release_id='+release_id)
        bundle_target.parent.mkdir()
        shutil.copytree(local, bundle_target, ignore=shutil.ignore_patterns('*.crc'))
        storage = HdfsStorage(config)
        storage.heartbeat = tick
        snapshots = SnapshotStore(storage)
        for ref in manifest['dependencies']:
            remote = ref['path']
            storage.check(remote)
            relative = PurePosixPath(remote).relative_to(storage.root)
            if snapshots.local(remote) is None:
                parent = dependency_root(remote, storage.root)
                snapshots.bind(parent, fetch(storage, parent, directory/'downloads'))
            source = snapshots.local(remote)
            if sha256(source) != ref['sha256'] or source.stat().st_size != ref['bytes']:
                raise ManifestError('Backup dependency differs from release')
            target = hdfs/str(relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            os.chmod(target, 0o600)
            tick()
        # Temporary downloads only hold redundant copies, not unique evidence.
        if (directory/'downloads').exists():
            shutil.rmtree(directory/'downloads')
        dump = backup_database(connection, directory/'database')
        files = [{'file': str(p.relative_to(directory)), 'sha256': sha256(p), 'bytes': p.stat().st_size}
                 for p in sorted(directory.rglob('*')) if p.is_file()]
        record = {'schema_version': '1.0', 'status': 'PASS', 'release_id': release_id,
                  'original_hdfs_root': str(storage.root), 'manifest_sha256': digest,
                  'database_dump': str(Path(dump['path']).relative_to(directory)),
                  'physical_storage_gate': physical_backup_gate(directory),
                  'verification': comparison, 'files': files}
        write_json(directory/'backup.json', record)
        os.chmod(directory/'backup.json', 0o600)
        return record | {'backup_directory': str(directory)}


def verify_backup(directory):
    directory = Path(directory).resolve(strict=True)
    record = json.loads((directory/'backup.json').read_text())
    if record.get('schema_version') != '1.0' or record.get('status') != 'PASS':
        raise ManifestError('Invalid backup record')
    for ref in record['files']:
        path = (directory/ref['file']).resolve(strict=True)
        if not path.is_relative_to(directory) or path.is_symlink() or not path.is_file():
            raise ManifestError('Backup file escapes archive')
        if sha256(path) != ref['sha256'] or path.stat().st_size != ref['bytes']:
            raise ManifestError('Backup file hash/size differs')
    dump = (directory/record['database_dump']).resolve(strict=True)
    if not dump.is_relative_to(directory) or str(dump.relative_to(directory)) not in {f['file'] for f in record['files']}:
        raise ManifestError('Unverified database dump')
    return record


@contextmanager
def isolated_database(directory):
    """Own a fresh socket-only server; never connect a restore to host MariaDB."""
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    socket = directory/'mysql.sock'
    subprocess.run(['mariadb-install-db', '--no-defaults', '--datadir='+str(directory/'data'),
                    '--auth-root-authentication-method=socket', '--skip-test-db'],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=40)
    process = subprocess.Popen(['mariadbd', '--no-defaults', '--datadir='+str(directory/'data'),
                                '--socket='+str(socket), '--pid-file='+str(directory/'mysql.pid'), '--skip-networking',
                                '--log-error='+str(directory/'mysql.log')],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    connection = None
    try:
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('Isolated MariaDB failed to start')
            try:
                connection = open_connection(user=getpass.getuser(), unix_socket=str(socket))
                break
            except Exception:
                time.sleep(.1)
        if connection is None:
            raise RuntimeError('Isolated MariaDB startup timed out')
        yield connection, socket
    finally:
        if connection:
            connection.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)


def restore(config, backup_directory, directory, target_root):
    """Restore full real data, verify through SELECT-only credentials, then stop."""
    started = time.monotonic()
    backup_directory = Path(backup_directory).expanduser().resolve(strict=True)
    record = verify_backup(backup_directory)
    if record['original_hdfs_root'] != config['hdfs']['root']:
        raise ManifestError('Backup HDFS origin differs from explicit configuration')
    target_root = recovery_root(config, target_root)
    directory = Path(directory).resolve()
    if directory.exists():
        raise ManifestError('Recovery work directory must be new')
    directory.mkdir(parents=True, mode=0o700)
    restored_config = deepcopy(config)
    restored_config['hdfs']['root'] = '/retail/recovery'
    storage = HdfsStorage(restored_config)
    if storage.isdir(target_root):
        raise ManifestError('Recovery HDFS target already exists')
    upload_bundle(storage, backup_directory/'hdfs', target_root, str(uuid4()))
    with isolated_database(directory/'mariadb') as (admin, socket):
        name = 'restore_'+uuid4().hex[:16]
        with admin.cursor() as cursor:
            cursor.execute('CREATE DATABASE `'+name+'` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        with (backup_directory/record['database_dump']).open('rb') as dump:
            subprocess.run(['mariadb', '--no-defaults', '--protocol=socket', '--socket='+str(socket),
                            '--user='+getpass.getuser(), name], stdin=dump, stdout=subprocess.DEVNULL,
                           stderr=subprocess.PIPE, check=True, timeout=300)
        password = secrets.token_urlsafe(36)
        with admin.cursor() as cursor:
            cursor.execute("CREATE USER 'restore_reader'@'localhost' IDENTIFIED BY %s", (password,))
            cursor.execute('GRANT SELECT ON `'+name+"`.* TO 'restore_reader'@'localhost'")
        reader = open_connection(name=name, user='restore_reader', password=password, unix_socket=str(socket))
        try:
            from orchestration.migrations import require_current
            require_current(reader)
            readonly_config = deepcopy(config)
            readonly_config['database']['name'] = name
            readonly_config['paths']['work_dir'] = str(directory)
            comparison = verify(reader, readonly_config, record['release_id'], restore_root=target_root)
            with reader.cursor() as cursor:
                cursor.execute('SELECT release_id FROM current_release WHERE singleton_id=1')
                restored_current = cursor.fetchone()['release_id']
                cursor.execute('SHOW GRANTS')
                grants = [next(iter(r.values())) for r in cursor.fetchall()]
            # Persist the test configuration, excluding passwords and user grants.
            write_json(directory/'reader-configuration.json', {'database': name, 'socket': str(socket),
                'hdfs_original_root': config['hdfs']['root'], 'hdfs_restore_root': target_root,
                'permissions': 'SELECT only', 'server_stops_after_drill': True})
            if comparison['tables'] != record['verification']['tables'] or comparison['manifest_sha256'] != record['manifest_sha256']:
                raise ManifestError('Restored business rows or manifest differ from backup')
            if not any('GRANT SELECT ON' in g for g in grants):
                raise ManifestError('Recovery reader is not SELECT-only')
        finally:
            reader.close()
    result = {'status': 'PASS', 'release_id': record['release_id'], 'database': name,
              'current_release_id': restored_current, 'hdfs_restore_root': target_root,
              'verification': comparison, 'backup_files_verified': len(record['files']),
              'physical_storage_gate': record['physical_storage_gate'],
              'elapsed_seconds': time.monotonic()-started, 'isolated_server_stopped': True}
    write_json(directory/'recovery.json', result)
    return result
