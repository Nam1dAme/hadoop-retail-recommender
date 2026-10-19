"""Explicit database connections and connection-scoped advisory locks."""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import os
import stat
import pymysql
from pymysql.optionfile import Parser
from orchestration.settings import ConfigError, database_credentials


class LockBusy(RuntimeError):
    pass


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def connect(config, defaults_file=None):
    if defaults_file is not None:
        return connect_defaults(config, defaults_file)
    user, password = database_credentials()
    return open_connection(**config['database'], user=user, password=password)


def connect_defaults(config, defaults_file):
    """Load only an explicitly supplied, owner-private MariaDB client file."""
    try:
        path = Path(defaults_file).expanduser()
        metadata = path.stat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
            raise ConfigError('Database client file must be owned by this user and private (0600)')
        options = Parser()
        with path.open() as stream:
            options.read_file(stream)
        def option(name, default=None):
            return options.get('client', name) if options.has_option('client', name) else default
        user, password = option('user'), option('password')
        if not user or user.lower() == 'root' or not password:
            raise ConfigError('Explicit non-root database user/password required in client file')
        database = config['database']
        if option('database') != database['name']:
            raise ConfigError('Client-file database must match explicit project configuration')
        protocol = option('protocol', 'SOCKET' if option('socket') else 'TCP').upper()
        if protocol not in ('TCP', 'SOCKET') or (protocol == 'SOCKET' and not option('socket')):
            raise ConfigError('Unsupported or incomplete client-file protocol')
        host = option('host', database['host'])
        port = int(option('port', str(database['port'])))
        if host not in ('127.0.0.1', 'localhost') or database['host'] not in ('127.0.0.1', 'localhost') or port != database['port']:
            raise ConfigError('Client endpoint must match the local project database')
        socket = option('socket') if protocol == 'SOCKET' else None
    except ConfigError:
        raise
    except Exception:
        # Parser errors may include credential lines; never propagate their text.
        raise ConfigError('Cannot read explicit database client configuration') from None
    return open_connection(host=host, port=port, name=database['name'], user=user,
                           password=password, unix_socket=socket)


def open_connection(host='127.0.0.1', port=3306, name=None, user=None, password=None, unix_socket=None):
    connection = pymysql.connect(host=host, port=port, database=name, user=user, password=password,
        unix_socket=unix_socket, charset='utf8mb4', autocommit=False, connect_timeout=5,
        read_timeout=10, write_timeout=10, cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cursor:
        cursor.execute("SET time_zone = '+00:00'")
        cursor.execute("SET SESSION sql_mode = 'STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE,NO_ZERO_IN_DATE,NO_ENGINE_SUBSTITUTION'")
    return connection


@contextmanager
def named_lock(connection, name='retail-heavy-job'):
    """Hold this connection for the entire job; reconnecting loses ownership."""
    with connection.cursor() as cursor:
        cursor.execute('SELECT GET_LOCK(%s,0) AS acquired', (name,))
        if cursor.fetchone()['acquired'] != 1:
            raise LockBusy('Database job lock is occupied')
    try:
        yield connection
    finally:
        # No reconnect: a lost connection must stop the caller, never reacquire silently.
        with connection.cursor() as cursor:
            cursor.execute('SELECT RELEASE_LOCK(%s)', (name,))
