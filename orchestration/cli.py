"""W02 database/status commands; other retailctl commands arrive with their task packages."""
import argparse
import json
import os
from pathlib import Path
import sys
from orchestration.control import ControlStore, ControlError
from orchestration.database import connect, open_connection, LockBusy
from orchestration.db_backup import backup_database
from orchestration.migrations import migrate, inspect, MigrationError
from orchestration.settings import BASELINE, ConfigError, load_config


def main():
    parser=argparse.ArgumentParser(prog='retailctl')
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--baseline',required=True,choices=[BASELINE])
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--db-defaults-file',type=Path,help='Explicit private MariaDB [client] file')
    sub=parser.add_subparsers(dest='command',required=True)
    migration=sub.add_parser('db-migrate')
    migration.add_argument('--direction',choices=['up','down'],default='up')
    sub.add_parser('db-status')
    sub.add_parser('db-prepare-roles')
    sub.add_parser('storage-init')
    provision=sub.add_parser('db-provision')
    provision.add_argument('--admin-user',required=True)
    provision.add_argument('--admin-socket',type=Path)
    status=sub.add_parser('status')
    ids=status.add_mutually_exclusive_group(required=True)
    ids.add_argument('--run-id')
    ids.add_argument('--batch-id')
    args=parser.parse_args()
    try:
        config=load_config(args.config)
        work=Path(config['paths']['work_dir'])
        if args.command=='storage-init':
            from orchestration.storage import initialize_storage
            result={'directories':initialize_storage(config,args.dry_run),'dry_run':args.dry_run}
        elif args.command=='db-provision':
            from orchestration.provision import provision
            if args.dry_run:
                print(json.dumps({'plan':'create local roles, private credentials, database and migration 001','database':config['database']['name']}))
                return 0
            admin=open_connection(**(config['database'] | {'name':None}),user=args.admin_user,
                password=os.environ.get('RETAIL_DB_ADMIN_PASSWORD',''),unix_socket=str(args.admin_socket) if args.admin_socket else None)
            try:
                result=provision(admin,config['database']['name'],work/'secrets',work/'backups',
                    config['database']['host'],config['database']['port'])
            finally:
                admin.close()
        else:
            connection=connect(config,args.db_defaults_file)
            try:
                if args.command=='db-prepare-roles':
                    from orchestration.provision import prepare_roles
                    result=prepare_roles(connection,work/'secrets',args.dry_run)
                elif args.command=='db-migrate':
                    result={'versions':migrate(connection,lambda:backup_database(connection,work/'backups'),
                        direction=args.direction,dry_run=args.dry_run),'dry_run':args.dry_run}
                elif args.command=='db-status':
                    migrations,applied=inspect(connection)
                    result={'applied':[m['version'] for m in applied],'current':len(applied)==len(migrations)}
                elif args.run_id:
                    result=ControlStore(connection).task_status(args.run_id)
                else:
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT batch_id,status,created_at,updated_at FROM data_batch WHERE batch_id=%s',(args.batch_id,))
                        result=cursor.fetchone()
                        if not result:
                            raise ControlError('Unknown batch')
            finally:
                connection.close()
        print(json.dumps(result,default=str))
        return 0
    except (ConfigError,ControlError) as exc:
        print(str(exc),file=sys.stderr)
        return 2
    except LockBusy:
        print('Database lock is occupied',file=sys.stderr)
        return 5
    except MigrationError as exc:
        print(str(exc),file=sys.stderr)
        return 6
    except Exception as exc:
        # Do not expose server messages, SQL, usernames or filesystem credentials.
        print('Database operation failed: '+type(exc).__name__,file=sys.stderr)
        return 3


if __name__=='__main__':
    raise SystemExit(main())
