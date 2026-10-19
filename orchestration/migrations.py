"""Checksummed MariaDB migrations. DDL is not transactional; fail closed on partial DDL."""
from contextlib import contextmanager
import hashlib
from pathlib import Path
import re
from orchestration.database import named_lock, utcnow

ROOT = Path(__file__).resolve().parents[1] / 'migrations'
HISTORY_SQL = '''CREATE TABLE schema_migration (
 version VARCHAR(32) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
 description VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
 checksum CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 applied_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin'''


class MigrationError(ValueError):
    pass


def catalog(root=ROOT):
    result = []
    for up in sorted(root.glob('*.up.sql')):
        match = re.fullmatch(r'(\d{3})_([a-z_]+)\.up\.sql', up.name)
        if not match:
            raise MigrationError('Invalid migration filename')
        down = up.with_name(up.name.replace('.up.sql', '.down.sql'))
        if not down.is_file():
            raise MigrationError('Missing reverse migration')
        result.append(dict(version=match[1], description=match[2], up=up, down=down,
            checksum=hashlib.sha256(up.read_bytes()+b'\0'+down.read_bytes()).hexdigest()))
    if not result or len({m['version'] for m in result}) != len(result):
        raise MigrationError('Empty or duplicate migration catalog')
    return result


def statements(path):
    # This catalog deliberately contains plain DDL/DML, no routines or delimiters.
    text = re.sub(r'^\s*--.*$', '', path.read_text(), flags=re.M)
    return [s.strip() for s in text.split(';') if s.strip()]


def tables(connection):
    with connection.cursor() as cursor:
        cursor.execute('SHOW TABLES')
        return {next(iter(row.values())) for row in cursor.fetchall()}


def history(connection):
    if 'schema_migration' not in tables(connection):
        return []
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM schema_migration ORDER BY version')
        return list(cursor.fetchall())


def inspect(connection, root=ROOT):
    migrations = catalog(root)
    applied = history(connection)
    if len(applied) > len(migrations):
        raise MigrationError('Unknown applied migration')
    for actual, expected in zip(applied, migrations):
        if actual['version'] != expected['version'] or actual['checksum'] != expected['checksum']:
            raise MigrationError('Applied migration checksum/order differs')
    expected_tables = {'schema_migration'} if applied else set()
    for migration in migrations[:len(applied)]:
        expected_tables.update(re.findall(r'CREATE TABLE (\w+)', migration['up'].read_text()))
    observed = tables(connection)
    # Empty history table is safe after a pre-DDL crash. Any partial business DDL is not.
    if not applied:
        observed.discard('schema_migration')
    if observed != expected_tables:
        raise MigrationError('Schema differs from applied history; inspect partial DDL and restore backup')
    return migrations, applied


def migrate(connection, backup, direction='up', dry_run=False, root=ROOT):
    if direction not in ('up', 'down'):
        raise MigrationError('Unknown migration direction')
    # A dedicated migration connection must not reuse a stale read snapshot.
    connection.rollback()
    with named_lock(connection, 'retail-schema-migration'), named_lock(connection):
        migrations, applied = inspect(connection, root)
        pending = migrations[len(applied):] if direction == 'up' else migrations[len(applied)-1:len(applied)]
        if dry_run or not pending:
            return [m['version'] for m in pending]
        if direction == 'down':
            # Only an empty-schema rehearsal is authorized by this interface.
            with connection.cursor() as cursor:
                for table in tables(connection)-{'schema_migration'}:
                    cursor.execute(f'SELECT * FROM `{table}` LIMIT 1')
                    row = cursor.fetchone()
                    if row and not (table == 'current_release' and row['singleton_id'] == 1 and row['release_id'] is None):
                        raise MigrationError('Refusing rollback of populated schema')
        # Mandatory verified backup happens before even creating the history table.
        backup()
        with connection.cursor() as cursor:
            if 'schema_migration' not in tables(connection):
                cursor.execute(HISTORY_SQL)
            for migration in pending:
                for sql in statements(migration[direction]):
                    cursor.execute(sql)
                if direction == 'up':
                    cursor.execute('INSERT INTO schema_migration (version,description,checksum,applied_at) VALUES (%s,%s,%s,%s)',
                        (migration['version'],migration['description'],migration['checksum'],utcnow()))
                else:
                    cursor.execute('DELETE FROM schema_migration WHERE version=%s', (migration['version'],))
                connection.commit()
        return [m['version'] for m in pending]


def require_current(connection):
    migrations, applied = inspect(connection)
    if len(migrations) != len(applied):
        raise MigrationError('Database schema migration is not current')
