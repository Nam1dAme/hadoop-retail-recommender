#!/usr/bin/env python3
"""Verify a deployed W02 schema and three real accounts without retaining fixture rows."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pymysql
from orchestration.database import connect
from orchestration.migrations import require_current, catalog
from orchestration.provision import READ_TABLES, READ_COLUMNS
from orchestration.settings import load_config


def expected_grants(role):
    if role == 'writer':
        import re
        result = {(table, privilege) for table in re.findall(r'CREATE TABLE (\w+)', catalog()[0]['up'].read_text())
                  for privilege in ('SELECT', 'INSERT', 'UPDATE', 'DELETE')}
        result.add(('schema_migration', 'SELECT'))
        return result, set()
    return {(table, 'SELECT') for table in READ_TABLES}, {
        (table, column, 'SELECT') for table, columns in READ_COLUMNS.items() for column in columns.split(',')}


def verify_role(config, path, role):
    connection = connect(config, path)
    try:
        require_current(connection)
        with connection.cursor() as cursor:
            cursor.execute('SELECT CURRENT_USER() AS account, CURRENT_ROLE() AS active_role')
            identity = cursor.fetchone()
            if identity['active_role'] is not None:
                raise ValueError('Unexpected inherited role')
            user, host = identity['account'].rsplit('@', 1)
            grantee = "'" + user + "'@'" + host + "'"
            cursor.execute('SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES WHERE GRANTEE=%s', (grantee,))
            if {r['PRIVILEGE_TYPE'] for r in cursor.fetchall()} - {'USAGE'}:
                raise ValueError('Unexpected global privileges')
            cursor.execute('SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE=%s', (grantee,))
            if cursor.fetchall():
                raise ValueError('Expected table/column grants, found broad schema grants')
            cursor.execute('SELECT TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE FROM information_schema.TABLE_PRIVILEGES WHERE GRANTEE=%s AND TABLE_SCHEMA=%s', (grantee, config['database']['name']))
            rows = cursor.fetchall()
            if any(row['IS_GRANTABLE'] != 'NO' for row in rows):
                raise ValueError('Account can grant privileges')
            cursor.execute('SELECT TABLE_NAME,COLUMN_NAME,PRIVILEGE_TYPE,IS_GRANTABLE FROM information_schema.COLUMN_PRIVILEGES WHERE GRANTEE=%s AND TABLE_SCHEMA=%s', (grantee, config['database']['name']))
            columns = cursor.fetchall()
            expected_tables, expected_columns = expected_grants(role)
            if {(row['TABLE_NAME'],row['PRIVILEGE_TYPE']) for row in rows} != expected_tables or {
                (row['TABLE_NAME'],row['COLUMN_NAME'],row['PRIVILEGE_TYPE']) for row in columns} != expected_columns or any(row['IS_GRANTABLE'] != 'NO' for row in columns):
                raise ValueError('Account privileges differ from required minimum')
            cursor.execute('SELECT release_id FROM current_release WHERE singleton_id=1')
            cursor.fetchone()
            probes = {'create_denied': 'CREATE TABLE schema_migration (id INT)',
                      'migration_history_write_denied': 'DELETE FROM schema_migration WHERE 1=0'}
            if role == 'reader':
                probes.update(write_denied='UPDATE current_release SET updated_at=updated_at WHERE singleton_id=1',
                              private_source_denied='SELECT source_url FROM data_batch',
                              private_pid_denied='SELECT pid FROM task_run')
            passed = {}
            for name, sql in probes.items():
                try:
                    cursor.execute(sql)
                except pymysql.MySQLError as error:
                    passed[name] = error.args[0] in (1142, 1143)
                else:
                    passed[name] = False
            if role == 'writer':
                cursor.execute('UPDATE current_release SET updated_at=updated_at WHERE singleton_id=1')
                passed['control_write_allowed'] = True
            if not all(passed.values()):
                raise ValueError('Permission denial probe did not match expectations')
        return {'status': 'PASS', 'account': identity['account'], 'minimum_grants_match': True, 'probes': passed}
    finally:
        connection.rollback()
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--migration-defaults-file', required=True, type=Path)
    parser.add_argument('--writer-defaults-file', required=True, type=Path)
    parser.add_argument('--reader-defaults-file', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    report = {'baseline_version': 'HL-DOC-1.0.0', 'observed_at': datetime.now(timezone.utc).isoformat().replace('+00:00','Z'), 'checks': {}}
    try:
        config = load_config(args.config)
        connection = connect(config, args.migration_defaults_file)
        try:
            require_current(connection)
            with connection.cursor() as cursor:
                cursor.execute('SELECT VERSION() AS version,CURRENT_USER() AS account,DATABASE() AS db')
                identity = cursor.fetchone()
                if not identity['version'].startswith('10.11.') or 'MariaDB' not in identity['version']:
                    raise ValueError('Database server differs from required MariaDB 10.11')
                cursor.execute('SELECT version,checksum,applied_at FROM schema_migration ORDER BY version')
                history = cursor.fetchall()
                cursor.execute('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND ENGINE=\'InnoDB\'')
                count = cursor.fetchone()['n']
                if count != 23:
                    raise ValueError('Unexpected deployed table count or engine')
            report['checks']['migration'] = {'status': 'PASS', 'identity': identity, 'history': history, 'innodb_tables': count}
        finally:
            connection.close()
        for role, path in (('writer', args.writer_defaults_file), ('reader', args.reader_defaults_file)):
            try:
                report['checks'][role] = verify_role(config, path, role)
            except Exception as error:
                report['checks'][role] = {'status': 'FAIL', 'error_type': type(error).__name__}
        identities = [report['checks'][role].get('account') for role in ('writer', 'reader')]
        report['account_separation'] = all(identities) and len({identity['account'], *identities}) == 3
        report['database_gate_complete'] = report['account_separation'] and all(row['status'] == 'PASS' for row in report['checks'].values())
    except Exception as error:
        report['database_gate_complete'] = False
        report['error_type'] = type(error).__name__
    files = [*ROOT.glob('orchestration/*.py'), *ROOT.glob('migrations/*.sql'), ROOT/'scripts/verify-w02.py']
    report['source_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(files)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2)+'\n')
    print(json.dumps({'database_gate_complete': report['database_gate_complete'], 'evidence': str(args.output)}))
    return 0 if report['database_gate_complete'] else 3


if __name__ == '__main__':
    raise SystemExit(main())
