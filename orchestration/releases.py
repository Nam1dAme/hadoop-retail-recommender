"""BUILDING imports, immutable READY releases and atomic audited pointer changes."""
import getpass
from pathlib import Path
import re
from uuid import uuid4

from etl.ingest import sha256
from ml.data import read_tsv
from orchestration.control import ControlStore, ALGORITHMS
from orchestration.database import utcnow, LockBusy
from orchestration.manifests import ManifestError
from orchestration.release_data import TABLES, normalize, compare_database
from orchestration.settings import BASELINE


def quoted(columns):
    return ','.join('`'+c+'`' for c in columns.split(','))


def current(connection):
    with connection.cursor() as cursor:
        cursor.execute('SELECT release_id FROM current_release WHERE singleton_id=1')
        value = cursor.fetchone()['release_id']
    connection.rollback()
    return value


def validate_bundle(manifest, directory):
    if (manifest.get('schema_version') != '1.0' or manifest.get('baseline_version') != BASELINE
            or manifest.get('synthetic') is not False or manifest.get('code_dirty') is not False
            or not re.fullmatch(r'R[0-9]{14}[0-9a-f]{8}', manifest.get('release_id', ''))
            or set(manifest.get('tables', {})) != set(TABLES)
            or set(manifest.get('models', {})) != set(ALGORITHMS)):
        raise ManifestError('Invalid release manifest identity or schema')
    for name, spec in TABLES.items():
        ref = manifest['tables'][name]
        if ref.get('file') != name+'.tsv' or ref.get('columns') != spec.columns.split(','):
            raise ManifestError('Invalid release export schema or path')
        path = Path(directory)/ref['file']
        if not path.is_file() or sha256(path) != ref['sha256'] or path.stat().st_size != ref['bytes']:
            raise ManifestError('Release export hash/size differs')
        count = 0
        for row in read_tsv(path, len(ref['columns'])):
            normalize(name, row)
            count += 1
        if count != ref['rows']:
            raise ManifestError('Release export row count differs')
    for ref in manifest.get('files', []):
        if Path(ref['file']).name != ref['file'] or ref['file'] in ('release.json', 'manifest.json'):
            raise ManifestError('Invalid release auxiliary path')
        path = Path(directory)/ref['file']
        if sha256(path) != ref['sha256'] or path.stat().st_size != ref['bytes']:
            raise ManifestError('Release code/config/environment hash differs')


def verify_bindings(connection, manifest):
    """Recheck live control rows; a manifest alone is not publication authority."""
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM data_batch WHERE batch_id=%s', (manifest['batch_id'],))
        batch = cursor.fetchone()
        if (not batch or batch['status'] != 'SUCCEEDED' or not batch['sale_rows']
                or batch['raw_rows'] != batch['sale_rows']+batch['return_rows']+batch['rejected_rows']
                or batch['rejected_rows']*20 > batch['raw_rows']
                or batch['source_sha256'] != manifest['source_sha256']):
            raise ManifestError('Release batch quality/source gate failed')
        cursor.execute('SELECT * FROM experiment WHERE experiment_id=%s', (manifest['experiment_id'],))
        experiment = cursor.fetchone()
        if (not experiment or experiment['status'] != 'SUCCEEDED' or not experiment['test_sealed_at']
                or experiment['batch_id'] != batch['batch_id']
                or experiment['champion_algorithm'] != manifest['champion_algorithm']):
            raise ManifestError('Release experiment/champion gate failed')
        for algorithm, mid in manifest['models'].items():
            cursor.execute('SELECT * FROM model_run WHERE model_run_id=%s', (mid,))
            model = cursor.fetchone()
            if (not model or model['algorithm'] != algorithm or model['purpose'] != 'FULLFIT'
                    or model['status'] != 'SUCCEEDED' or model['batch_id'] != batch['batch_id']
                    or model['experiment_id'] != manifest['experiment_id']
                    or model['mapping_sha256'] != manifest['mapping_sha256']
                    or model['device'] != ('CUDA' if algorithm == 'NCF' else 'CPU')):
                raise ManifestError('Release requires four matching successful FULLFIT models')
        cursor.execute('''SELECT m.model_run_id,m.algorithm,m.status,m.purpose,m.batch_id FROM experiment_model e
            JOIN model_run m USING(model_run_id) WHERE e.experiment_id=%s''', (manifest['experiment_id'],))
        selected = cursor.fetchall()
        if {r['algorithm'] for r in selected} != set(ALGORITHMS) or any(
                r['status'] != 'SUCCEEDED' or r['purpose'] != 'EVAL' or r['batch_id'] != batch['batch_id'] for r in selected):
            raise ManifestError('Incomplete selected EVAL models')
        for model in selected:
            cursor.execute('SELECT split_name,cohort FROM model_metric WHERE model_run_id=%s AND k=10', (model['model_run_id'],))
            if {(r['split_name'], r['cohort']) for r in cursor.fetchall()} != {
                    (s, c) for s in ('VALIDATION', 'TEST') for c in ('WARM', 'ALL_ELIGIBLE')}:
                raise ManifestError('Incomplete selected model metrics')
        ncf = next(r['model_run_id'] for r in selected if r['algorithm'] == 'NCF')
        cursor.execute('SELECT * FROM gpu_benchmark WHERE model_run_id=%s', (ncf,))
        repeats = cursor.fetchall()
        if (len(repeats) != 6 or {(r['device'], r['repeat_index']) for r in repeats} != {
                (d, n) for d in ('CPU', 'CUDA') for n in (1, 2, 3)}
                or len({(r['dataset_sha256'], r['config_sha256'], r['batch_size'], r['seed']) for r in repeats}) != 1
                or any(r['epochs'] != 5 or r['warmup_steps'] <= 0 or r['train_seconds'] <= 0 for r in repeats)):
            raise ManifestError('Incomplete or inconsistent CPU/CUDA benchmark gate')
    connection.rollback()


def verify_business(connection, manifest):
    """Check conservation and release-scoped relational constraints in MariaDB."""
    rid = manifest['release_id']
    audit = manifest['audit']
    with connection.cursor() as cursor:
        def scalar(sql, values=(rid,)):
            cursor.execute(sql, values)
            return next(iter(cursor.fetchone().values()))
        for table, column in (('sales_cube', 'gross_amount'), ('sale_order', 'gross_amount'), ('sale_order_item', 'gross_amount')):
            if scalar('SELECT COALESCE(SUM('+column+'),0)*100 FROM '+table+' WHERE release_id=%s') != audit['gross_pence']:
                raise ManifestError('Published gross amount is not conserved')
        if scalar('SELECT COALESCE(SUM(return_amount),0)*100 FROM sales_cube WHERE release_id=%s') != audit['return_pence']:
            raise ManifestError('Published return amount is not conserved')
        for column in ('sale_quantity', 'return_quantity'):
            if scalar('SELECT COALESCE(SUM('+column+'),0) FROM sales_cube WHERE release_id=%s') != audit[column]:
                raise ManifestError('Published quantity is not conserved')
        invalid = scalar('''SELECT COUNT(*) FROM customer_recommendation r JOIN product_dim p
            ON p.release_id=r.release_id AND p.stock_code=r.stock_code
            LEFT JOIN customer_seen_item s ON s.release_id=r.release_id AND s.customer_id=r.customer_id AND s.stock_code=r.stock_code
            WHERE r.release_id=%s AND (p.is_candidate<>1 OR s.stock_code IS NOT NULL
                OR r.reason_code<>CASE WHEN r.is_fallback=1 THEN 'FALLBACK_POPULAR'
                    WHEN r.algorithm='POP' THEN 'POPULAR' WHEN r.algorithm='ITEMCF' THEN 'CO_PURCHASE'
                    WHEN r.algorithm='ALS' THEN 'LATENT_FACTOR' ELSE 'NEURAL_CF' END
                OR (r.algorithm='POP' AND r.is_fallback<>0))''')
        if invalid:
            raise ManifestError('Published recommendations violate catalog/history/reason constraints')
        # Include customers with zero available candidates in this check.
        cursor.execute('''SELECT r.customer_id,a.algorithm,COALESCE(c.n,0) AS n,COALESCE(c.lo,0) AS lo,COALESCE(c.hi,0) AS hi,
                LEAST(10,p.n-COALESCE(s.n,0)) AS expected
            FROM customer_rfm r CROSS JOIN release_model a
            CROSS JOIN (SELECT COUNT(*) n FROM product_dim WHERE release_id=%s AND is_candidate=1) p
            LEFT JOIN (SELECT customer_id,COUNT(*) n FROM customer_seen_item WHERE release_id=%s GROUP BY customer_id) s
                ON s.customer_id=r.customer_id
            LEFT JOIN (SELECT algorithm,customer_id,COUNT(*) n,MIN(`rank`) lo,MAX(`rank`) hi
                FROM customer_recommendation WHERE release_id=%s GROUP BY algorithm,customer_id) c
                ON c.customer_id=r.customer_id AND c.algorithm=a.algorithm
            WHERE r.release_id=%s AND a.release_id=%s''', (rid,)*5)
        for row in cursor.fetchall():
            if row['n'] != row['expected'] or (row['n'] and (row['lo'] != 1 or row['hi'] != row['n'])):
                raise ManifestError('Published recommendation coverage/ranks are incomplete')
    connection.rollback()


def existing_release(connection, batch_id, experiment_id, models):
    with connection.cursor() as cursor:
        cursor.execute('''SELECT r.release_id,r.status,rm.algorithm,rm.model_run_id FROM data_release r
            JOIN release_model rm USING(release_id) WHERE r.batch_id=%s AND r.experiment_id=%s
            ORDER BY r.created_at DESC''', (batch_id, experiment_id))
        rows = cursor.fetchall()
    connection.rollback()
    releases = {}
    for row in rows:
        item = releases.setdefault(row['release_id'], {'status': row['status'], 'models': {}})
        item['models'][row['algorithm']] = row['model_run_id']
    return next(((rid, r['status']) for rid, r in releases.items()
                 if r['models'] == models and r['status'] in ('READY', 'BUILDING')), None)


def audit_event(cursor, action, before, after, reason, operator):
    if (not reason.strip() or len(reason) > 1024 or not operator.strip() or len(operator) > 64
            or any(ord(c) < 32 for c in reason+operator)):
        raise ManifestError('A bounded, nonempty audit reason and operator are required')
    cursor.execute('''INSERT INTO release_event
        (event_id,action,from_release_id,to_release_id,reason,operator,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)''',
        (str(uuid4()), action, before, after, reason, operator, utcnow()))


def import_bundle(connection, manifest, directory, manifest_path, manifest_sha256, expected_current,
                  *, tick=lambda: None, fault=lambda stage: None, operator=None):
    """Caller owns retail-heavy-job throughout. Fault hook is only for integration tests."""
    validate_bundle(manifest, directory)
    verify_bindings(connection, manifest)
    rid = manifest['release_id']
    control = ControlStore(connection)
    with control.transaction() as cursor:
        cursor.execute('''INSERT INTO data_release
            (release_id,batch_id,experiment_id,status,champion_algorithm,manifest_path,manifest_sha256,baseline_version,created_at)
            VALUES (%s,%s,%s,'BUILDING',%s,%s,%s,%s,%s)''',
            (rid, manifest['batch_id'], manifest['experiment_id'], manifest['champion_algorithm'],
             manifest_path, manifest_sha256, BASELINE, utcnow()))
        cursor.executemany('INSERT INTO release_model VALUES (%s,%s,%s)',
                           [(rid, a, m) for a, m in manifest['models'].items()])
    try:
        for name, spec in TABLES.items():
            pending = []
            sql = 'INSERT INTO '+name+' (release_id,'+quoted(spec.columns)+') VALUES ('+','.join(['%s']*(len(spec.columns.split(','))+1))+')'
            for row in read_tsv(Path(directory)/(name+'.tsv'), len(spec.columns.split(','))):
                pending.append((rid, *normalize(name, row)))
                if len(pending) >= 2000:
                    with control.transaction() as cursor:
                        cursor.executemany(sql, pending)
                    pending.clear()
                    tick()
                    fault('chunk:'+name)
            if pending:
                with control.transaction() as cursor:
                    cursor.executemany(sql, pending)
            tick()
            fault('table:'+name)
        comparison = compare_database(connection, rid, manifest['tables'], tick)
        connection.rollback()
        verify_business(connection, manifest)
        fault('verified')
        # Holding the single row lock makes READY + current + event indivisible.
        with control.transaction() as cursor:
            cursor.execute('SELECT release_id FROM current_release WHERE singleton_id=1 FOR UPDATE')
            before = cursor.fetchone()['release_id']
            if before != expected_current:
                raise LockBusy('Current release changed during publication; retry after inspection')
            cursor.execute("UPDATE data_release SET status='READY',published_at=%s WHERE release_id=%s AND status='BUILDING'", (utcnow(), rid))
            if cursor.rowcount != 1:
                raise ManifestError('Release is no longer BUILDING')
            cursor.execute('UPDATE current_release SET release_id=%s,updated_at=%s WHERE singleton_id=1', (rid, utcnow()))
            audit_event(cursor, 'PUBLISH', before, rid, 'Verified immutable release publication', operator or getpass.getuser())
            fault('switch')
        return {'status': 'PASS', 'release_id': rid, 'previous_release_id': expected_current,
                'reused': False, 'tables': comparison, 'manifest_sha256': manifest_sha256}
    except BaseException:
        connection.rollback()
        with control.transaction() as cursor:
            cursor.execute("UPDATE data_release SET status='FAILED',failure_reason=%s WHERE release_id=%s AND status='BUILDING'",
                           ('Publication interrupted or validation failed; inspect private execution log', rid))
        raise


def verify_local(connection, release_id, manifest, directory, digest, tick=lambda: None):
    validate_bundle(manifest, directory)
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM data_release WHERE release_id=%s', (release_id,))
        row = cursor.fetchone()
        cursor.execute('SELECT algorithm,model_run_id FROM release_model WHERE release_id=%s', (release_id,))
        bindings = {r['algorithm']: r['model_run_id'] for r in cursor.fetchall()}
    if (not row or row['status'] != 'READY' or manifest['release_id'] != release_id
            or row['manifest_sha256'] != digest or bindings != manifest['models']
            or any(row[k] != manifest[k] for k in ('batch_id', 'experiment_id', 'champion_algorithm', 'baseline_version'))):
        raise ManifestError('READY release manifest/control binding differs')
    connection.rollback()
    tables = compare_database(connection, release_id, manifest['tables'], tick)
    connection.rollback()
    verify_business(connection, manifest)
    return {'status': 'PASS', 'release_id': release_id, 'manifest_sha256': digest, 'tables': tables}


def rollback_local(connection, release_id, manifest, directory, digest, reason, *, expected_current,
                   operator=None, tick=lambda: None):
    verify_local(connection, release_id, manifest, directory, digest, tick)
    with ControlStore(connection).transaction() as cursor:
        cursor.execute('SELECT release_id FROM current_release WHERE singleton_id=1 FOR UPDATE')
        before = cursor.fetchone()['release_id']
        if before != expected_current:
            raise LockBusy('Current release changed during rollback')
        if before == release_id:
            return {'status': 'PASS', 'release_id': release_id, 'reused': True}
        cursor.execute("SELECT status FROM data_release WHERE release_id=%s FOR UPDATE", (release_id,))
        if cursor.fetchone()['status'] != 'READY':
            raise ManifestError('Rollback target is no longer READY')
        audit_event(cursor, 'ROLLBACK', before, release_id, reason, operator or getpass.getuser())
        cursor.execute('UPDATE current_release SET release_id=%s,updated_at=%s WHERE singleton_id=1', (release_id, utcnow()))
    return {'status': 'PASS', 'release_id': release_id, 'previous_release_id': before, 'reused': False}
