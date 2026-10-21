"""One short, consistent, read-only transaction per HTTP request."""
import re
import time

from orchestration.migrations import require_current
from service.errors import unavailable


def require_reader(connection):
    """Fail closed for writer/admin credentials, roles and global SELECT grants.

    Do not log SHOW GRANTS output: authentication clauses can contain hashes.
    Column-level SELECT is sufficient for the redacted control-table projection.
    """
    database = connection.db.decode() if isinstance(connection.db, bytes) else connection.db
    with connection.cursor() as cursor:
        cursor.execute('SHOW GRANTS')
        grants = [next(iter(r.values())) for r in cursor.fetchall()]
    has_select = False
    for grant in grants:
        if ' WITH GRANT OPTION' in grant:
            raise unavailable()
        match = re.match(r'^GRANT (.+?) ON (.+?) TO ', grant)
        if not match:
            raise unavailable()
        privileges, target = match.groups()
        if privileges == 'USAGE':
            continue
        if not re.fullmatch(r'SELECT(?:\s*\([^)]*\))?', privileges):
            raise unavailable()
        if not target.startswith('`' + database.replace('`', '``') + '`.') or target.endswith('.*'):
            raise unavailable()
        has_select = True
    if not has_select:
        raise unavailable()


class ReadSession:
    def __init__(self, connection_factory, deadline):
        self.factory = connection_factory
        self.deadline = deadline
        self.connection = None

    def __enter__(self):
        self.connection = self.factory()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute('SET SESSION max_statement_time=2')
                cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            require_reader(self.connection)
            self.connection.rollback()
            with self.connection.cursor() as cursor:
                cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
            require_current(self.connection)
            return self
        except BaseException:
            self.connection.close()
            raise

    def __exit__(self, *exc):
        # Closing rolls back the transaction and releases its read view, even on errors.
        self.connection.close()

    def rows(self, sql, args=()):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise unavailable()
        with self.connection.cursor() as cursor:
            cursor.execute('SET SESSION max_statement_time=%s', (min(2.0, remaining),))
            cursor.execute(sql, args)
            return list(cursor.fetchall())

    def one(self, sql, args=()):
        rows = self.rows(sql, args)
        return rows[0] if rows else None
