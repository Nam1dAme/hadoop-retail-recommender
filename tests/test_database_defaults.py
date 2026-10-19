"""Explicit MariaDB credentials and prepared role deployment on real isolated MariaDB."""
import os
import runpy
from pathlib import Path
import pytest
import pymysql
from orchestration.database import connect
from orchestration.migrations import require_current
from orchestration.provision import prepare_roles
from orchestration.settings import ConfigError


def make_defaults(db, path):
    connections, name, secrets, _ = db
    values = dict(line.removeprefix('export ').split('=', 1) for line in (secrets/'migration.env').read_text().splitlines())
    path.write_text('[client]\nuser='+values['RETAIL_DB_USER']+'\npassword='+values['RETAIL_DB_PASSWORD']+
                    '\ndatabase='+name+'\nprotocol=SOCKET\nsocket='+connections['migration'].unix_socket+'\n')
    path.chmod(0o600)
    return {'database': {'host':'127.0.0.1', 'port':3306, 'name':name}}


def test_explicit_client_file_and_permissions(db, tmp_path):
    path = tmp_path/'client.cnf'
    config = make_defaults(db, path)
    connection = connect(config, path)
    try:
        require_current(connection)
    finally:
        connection.close()
    path.chmod(0o644)
    with pytest.raises(ConfigError): connect(config, path)
    path.chmod(0o600)
    config['database']['name'] = 'unexpected_database'
    with pytest.raises(ConfigError): connect(config, path)
    path.write_text('[client]\npassword=private-value\nthis is not a valid configuration\n')
    with pytest.raises(ConfigError) as error: connect(config, path)
    assert 'private-value' not in str(error.value)


def test_prepare_existing_database_roles(db, mariadb_instance, tmp_path):
    migration = db[0]['migration']
    directory = tmp_path/'roles'
    plan = prepare_roles(migration, directory, dry_run=True)
    assert not directory.exists()
    result = prepare_roles(migration, directory)
    assert result['state'] == 'PREPARED_NOT_APPLIED'
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in directory.iterdir())
    with pytest.raises(ValueError): prepare_roles(migration, directory)
    admin = mariadb_instance[0]
    with admin.cursor() as cursor:
        for statement in Path(result['script']).read_text().split(';'):
            if statement.strip(): cursor.execute(statement)
    admin.commit()
    config = {'database': {'host':'127.0.0.1', 'port':3306, 'name':db[1]}}
    for role in ('writer', 'reader'):
        connection = connect(config, directory/(role+'.cnf'))
        try:
            require_current(connection)
            with connection.cursor() as cursor:
                cursor.execute('SELECT release_id FROM current_release')
                for sql in ('CREATE TABLE forbidden (id INT)', 'DELETE FROM schema_migration'):
                    with pytest.raises(pymysql.MySQLError): cursor.execute(sql)
                if role == 'reader':
                    with pytest.raises(pymysql.MySQLError): cursor.execute('UPDATE current_release SET release_id=NULL')
                else:
                    cursor.execute('UPDATE current_release SET updated_at=UTC_TIMESTAMP(6) WHERE singleton_id=1')
            connection.rollback()
        finally:
            connection.close()
    verifier = runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/verify-w02.py'))
    for role in ('writer', 'reader'):
        assert verifier['verify_role'](config, directory/(role+'.cnf'), role)['status'] == 'PASS'
