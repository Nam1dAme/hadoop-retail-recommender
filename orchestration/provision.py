"""Explicit one-time local role provisioning. Never alters existing user passwords."""
import os
from pathlib import Path
import re
import secrets
from orchestration.db_backup import backup_database
from orchestration.database import open_connection
from orchestration.migrations import migrate, catalog
from orchestration.migrations import require_current

BUSINESS = ('product_dim','sales_cube','sale_order','sale_order_item','customer_rfm',
    'customer_seen_item','item_similarity','product_popularity','customer_recommendation')
READ_COLUMNS = {
 'data_batch':'batch_id,status,source_name,source_sha256,policy_version,raw_rows,created_at,updated_at,min_source_date,max_source_date,quality_report_path',
 'task_run':'run_id,batch_id,parent_run_id,task_name,attempt,status,created_at,started_at,finished_at,heartbeat_at,application_id,error_code,error_message,output_manifest_path,log_path',
 'experiment':'experiment_id,batch_id,status,split_policy_version,champion_algorithm,created_at,finished_at,test_sealed_at',
 'model_run':'model_run_id,batch_id,experiment_id,algorithm,purpose,device,status,created_at,finished_at',
}
READ_TABLES = ('schema_migration','data_quality','experiment_model','model_epoch','model_metric',
 'gpu_benchmark','data_release','release_model','current_release','release_event',*BUSINESS)


def role_grants(database, writer, reader):
    for identifier in (database, writer, reader):
        if not re.fullmatch('[a-z][a-z0-9_]{0,63}', identifier):
            raise ValueError('Invalid local database/account identifier')
    statements = []
    for table in re.findall(r'CREATE TABLE (\w+)', catalog()[0]['up'].read_text()):
        statements.append(f"GRANT SELECT,INSERT,UPDATE,DELETE ON `{database}`.`{table}` TO '{writer}'@'localhost'")
    statements.append(f"GRANT SELECT ON `{database}`.schema_migration TO '{writer}'@'localhost'")
    for table in READ_TABLES:
        statements.append(f"GRANT SELECT ON `{database}`.`{table}` TO '{reader}'@'localhost'")
    for table, columns in READ_COLUMNS.items():
        statements.append(f"GRANT SELECT ({columns}) ON `{database}`.`{table}` TO '{reader}'@'localhost'")
    return statements


def prepare_roles(connection, secrets_dir, dry_run=False):
    """Prepare a private admin script for an already migrated, restricted project DB."""
    require_current(connection)
    database = connection.db.decode() if isinstance(connection.db, bytes) else connection.db
    grants = role_grants(database, 'retail_writer', 'retail_reader')
    directory = Path(secrets_dir)
    script = directory / 'install-roles.sql'
    if dry_run:
        return {'plan': 'prepare writer/reader credentials and an administrator SQL file',
                'database': database, 'script': str(script)}
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.stat().st_mode & 0o077:
        raise ValueError('Credentials directory must have mode 0700')
    paths = [script, *(directory / (role + suffix) for role in ('writer', 'reader') for suffix in ('.env', '.cnf'))]
    if any(path.exists() or path.is_symlink() for path in paths):
        raise ValueError('Role setup artifacts already exist; verify and use them without regenerating credentials')
    passwords = {role: secrets.token_urlsafe(32) for role in ('writer', 'reader')}
    def private_file(path, content):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            stream.write(content)
    for role, password in passwords.items():
        user = 'retail_' + role
        private_file(directory / (role + '.env'), f'export RETAIL_DB_USER={user}\nexport RETAIL_DB_PASSWORD={password}\n')
        options = f'[client]\nuser={user}\npassword={password}\ndatabase={database}\n'
        if connection.unix_socket:
            socket = str(connection.unix_socket).replace('\\', '\\\\').replace('"', '\\"')
            options += f'protocol=SOCKET\nsocket="{socket}"\n'
        else:
            options += f'protocol=TCP\nhost={connection.host}\nport={connection.port}\n'
        private_file(directory / (role + '.cnf'), options)
    sql = '-- Run once as MariaDB administrator. Refuses existing users and never changes passwords.\n'
    for role, password in passwords.items():
        sql += f"CREATE USER 'retail_{role}'@'localhost' IDENTIFIED BY '{password}';\n"
    sql += '\n'.join(statement + ';' for statement in grants) + '\n'
    private_file(script, sql)
    return {'database': database, 'script': str(script), 'roles': ['retail_writer', 'retail_reader'],
            'state': 'PREPARED_NOT_APPLIED'}


def provision(admin, database, secrets_dir, backup_dir, host='127.0.0.1', port=3306, unix_socket=None, prefix='retail'):
    for value in (database,prefix):
        if not re.fullmatch('[a-z][a-z0-9_]{0,40}',value):
            raise ValueError('Invalid local database/account identifier')
    roles={r:prefix+'_'+r for r in ('migration','writer','reader')}
    directory=Path(secrets_dir)
    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    if directory.stat().st_mode & 0o077:
        raise ValueError('Credentials directory must have mode 0700')
    with admin.cursor() as cursor:
        for user in roles.values():
            cursor.execute('SELECT COUNT(*) AS n FROM mysql.user WHERE User=%s',(user,))
            if cursor.fetchone()['n']:
                raise ValueError('Role already exists; refusing credential changes')
        cursor.execute('SELECT COUNT(*) AS n FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s',(database,))
        if cursor.fetchone()['n']:
            raise ValueError('Database already exists; inspect and use migration command explicitly')
        if any((directory/(role+'.env')).exists() for role in roles):
            raise ValueError('Credential file already exists')
        cursor.execute(f'CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        passwords={}
        for role,user in roles.items():
            password=secrets.token_urlsafe(32)
            passwords[role]=password
            path=directory/(role+'.env')
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as stream:
                stream.write(f'export RETAIL_DB_USER={user}\nexport RETAIL_DB_PASSWORD={password}\n')
            # Localhost also covers authenticated loopback TCP on this local installation.
            cursor.execute('CREATE USER %s@%s IDENTIFIED BY %s',(user,'localhost',password))
        cursor.execute(f'GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,ALTER,DROP,INDEX,REFERENCES,SHOW VIEW,TRIGGER,EVENT ON `{database}`.* TO %s@%s',
            (roles['migration'],'localhost'))
    connection=open_connection(host=host,port=port,name=database,unix_socket=unix_socket,
        user=roles['migration'],password=passwords['migration'])
    try:
        migrate(connection,lambda:backup_database(connection,backup_dir))
    finally:
        connection.close()
    with admin.cursor() as cursor:
        for statement in role_grants(database, roles['writer'], roles['reader']):
            cursor.execute(statement)
    admin.commit()
    return {'database':database,'roles':roles,'credential_directory':str(directory),'migration':'001'}
