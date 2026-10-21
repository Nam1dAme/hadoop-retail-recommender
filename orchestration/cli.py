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
from orchestration.manifests import ManifestError


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
    ingestion=sub.add_parser('ingest')
    ingestion.add_argument('--source-file',required=True,type=Path)
    ingestion.add_argument('--source-url',required=True)
    ingestion.add_argument('--source-metadata',type=Path)
    ingestion.add_argument('--sheet')
    ingestion.add_argument('--source-date-format')
    pipeline=sub.add_parser('pipeline')
    pipeline.add_argument('--batch-id',required=True)
    pipeline.add_argument('--reducers',type=int,default=2)
    preparation=sub.add_parser('prepare-experiment')
    preparation.add_argument('--batch-id',required=True)
    preparation.add_argument('--experiment-id',required=True)
    evaluation=sub.add_parser('evaluate')
    evaluation.add_argument('--batch-id',required=True)
    evaluation.add_argument('--experiment-id',required=True)
    training=sub.add_parser('train')
    training.add_argument('--batch-id',required=True)
    training.add_argument('--experiment-id',required=True)
    training.add_argument('--algorithm',required=True,choices=['POP','ITEMCF','ALS','NCF'])
    training.add_argument('--device',required=True,choices=['cpu','cuda'])
    recommendation=sub.add_parser('recommend')
    recommendation.add_argument('--model-run-id',required=True)
    benchmark=sub.add_parser('benchmark')
    benchmark.add_argument('--experiment-id',required=True)
    benchmark.add_argument('--model-run-id',required=True)
    publication=sub.add_parser('publish')
    publication.add_argument('--batch-id',required=True)
    publication.add_argument('--experiment-id',required=True)
    publication.add_argument('--model-run-ids',nargs=4,required=True)
    rollback=sub.add_parser('rollback')
    rollback.add_argument('--release-id',required=True)
    rollback.add_argument('--reason',required=True)
    verification=sub.add_parser('verify')
    verification.add_argument('--release-id',required=True)
    verification.add_argument('--restore-root',help='Read an isolated HDFS restore while preserving original manifest hashes')
    backup=sub.add_parser('backup')
    backup.add_argument('--release-id',required=True)
    backup.add_argument('--output-dir',required=True,type=Path)
    restoration=sub.add_parser('restore')
    restoration.add_argument('--backup-dir',required=True,type=Path)
    restoration.add_argument('--output-dir',required=True,type=Path)
    restoration.add_argument('--hdfs-root',required=True)
    reporting=sub.add_parser('report')
    reporting.add_argument('--experiment-id',required=True)
    reporting.add_argument('--release-id',required=True)
    reporting.add_argument('--output-dir',required=True,type=Path)
    reporting.add_argument('--acceptance-file',type=Path)
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
        elif args.command=='restore':
            from orchestration.recovery import restore
            if args.dry_run:
                from orchestration.recovery import verify_backup,recovery_root
                record=verify_backup(args.backup_dir)
                result={'dry_run':True,'release_id':record['release_id'],'hdfs_root':recovery_root(config,args.hdfs_root)}
            else:
                # Use the host only for the shared job lock. SQL restoration is
                # confined to the fresh socket-only test server.
                from orchestration.database import named_lock
                connection=connect(config,args.db_defaults_file)
                try:
                    with named_lock(connection):
                        result=restore(config,args.backup_dir,args.output_dir,args.hdfs_root)
                finally:
                    connection.close()
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
                if args.command=='ingest':
                    from etl.ingest import ingest
                    result=ingest(connection,config,args.source_file,args.source_url,sheet=args.sheet,
                        date_format=args.source_date_format,source_metadata=args.source_metadata,dry_run=args.dry_run)
                    if not args.dry_run and not result['quality_gate_passed']:
                        print(json.dumps(result,default=str))
                        return 6
                elif args.command=='pipeline':
                    from orchestration.pipeline import pipeline
                    result=pipeline(connection,config,args.batch_id,reducers=args.reducers,dry_run=args.dry_run)
                elif args.command=='prepare-experiment':
                    from orchestration.experiment_data import prepare_experiment
                    result=prepare_experiment(connection,config,args.batch_id,args.experiment_id,dry_run=args.dry_run)
                elif args.command=='evaluate':
                    from orchestration.training import evaluate_experiment
                    result=evaluate_experiment(connection,config,args.batch_id,args.experiment_id,dry_run=args.dry_run)
                elif args.command=='train':
                    from orchestration.w07 import train_fullfit
                    result=train_fullfit(connection,config,args.batch_id,args.experiment_id,args.algorithm,args.device,dry_run=args.dry_run)
                elif args.command=='recommend':
                    from orchestration.w07 import recommend_fullfit
                    result=recommend_fullfit(connection,config,args.model_run_id,dry_run=args.dry_run)
                elif args.command=='benchmark':
                    from orchestration.w07 import benchmark as run_benchmark
                    result=run_benchmark(connection,config,args.experiment_id,args.model_run_id,dry_run=args.dry_run)
                elif args.command=='publish':
                    from orchestration.publication import publish
                    result=publish(connection,config,args.batch_id,args.experiment_id,args.model_run_ids,dry_run=args.dry_run)
                elif args.command=='rollback':
                    from orchestration.publication import rollback
                    result=rollback(connection,config,args.release_id,args.reason,dry_run=args.dry_run)
                elif args.command=='verify':
                    from orchestration.publication import verify
                    result=verify(connection,config,args.release_id,restore_root=args.restore_root)
                elif args.command=='backup':
                    from orchestration.recovery import backup
                    if args.dry_run:
                        result={'dry_run':True,'release_id':args.release_id,'output_directory':str(args.output_dir)}
                    else:
                        result=backup(connection,config,args.release_id,args.output_dir)
                elif args.command=='report':
                    from orchestration.delivery import report
                    result=report(connection,config,args.experiment_id,args.release_id,args.output_dir,
                        dry_run=args.dry_run,acceptance_file=args.acceptance_file)
                elif args.command=='db-prepare-roles':
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
    except (MigrationError,ManifestError) as exc:
        print(str(exc),file=sys.stderr)
        return 6
    except (OSError,RuntimeError) as exc:
        print('Offline operation failed: '+type(exc).__name__,file=sys.stderr)
        return 4
    except ValueError:
        print('Invalid source schema or data configuration',file=sys.stderr)
        return 6
    except Exception as exc:
        # Do not expose server messages, SQL, usernames or filesystem credentials.
        print('Database operation failed: '+type(exc).__name__,file=sys.stderr)
        return 3


if __name__=='__main__':
    raise SystemExit(main())
