"""Canonical release exports and exact, bounded-memory database comparisons."""
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import math
from pathlib import Path

from etl.tsv import line
from ml.data import read_tsv, write_tsv
from orchestration.manifests import ManifestError


@dataclass(frozen=True)
class Table:
    columns: str
    key: str
    integers: str = ''
    money: str = ''
    floats: str = ''


# Only these internal identifiers can appear in generated SQL. release_id is
# always supplied as a bound value; every comparison fixes one release.
TABLES = {
    'product_dim': Table('stock_code,description,is_merchandise,is_candidate', 'stock_code', 'is_merchandise,is_candidate'),
    'sales_cube': Table('source_date,country,stock_code,gross_amount,return_amount,sale_quantity,return_quantity',
                        'source_date,country,stock_code', 'sale_quantity,return_quantity', 'gross_amount,return_amount'),
    'sale_order': Table('invoice_no,source_date,country,customer_id,gross_amount', 'invoice_no', money='gross_amount'),
    'sale_order_item': Table('invoice_no,stock_code,gross_amount,quantity', 'invoice_no,stock_code', 'quantity', 'gross_amount'),
    'customer_rfm': Table('customer_id,last_purchase,as_of_date,recency_days,frequency,monetary,r_score,f_score,m_score,segment',
                          'customer_id', 'recency_days,frequency,r_score,f_score,m_score', 'monetary'),
    'customer_seen_item': Table('customer_id,stock_code', 'customer_id,stock_code'),
    'item_similarity': Table('stock_code,related_stock_code,similarity,confidence,lift,co_count,rank',
                             'stock_code,related_stock_code', 'co_count,rank', floats='similarity,confidence,lift'),
    'product_popularity': Table('stock_code,order_count,rank', 'stock_code', 'order_count,rank'),
    'customer_recommendation': Table('algorithm,customer_id,stock_code,rank,score,reason_code,is_fallback',
                                    'algorithm,customer_id,stock_code', 'rank,is_fallback', floats='score'),
}


def normalize(table, row):
    """The same bytes represent TSV, MariaDB DECIMAL/DATE and restored rows."""
    spec = TABLES[table]
    result = []
    for column, value in zip(spec.columns.split(','), row, strict=True):
        if value is None:
            result.append(None)
        elif column in spec.integers.split(','):
            number = int(value)
            if isinstance(value, str) and str(number) != value:
                raise ManifestError('Noncanonical release integer')
            result.append(number)
        elif column in spec.money.split(','):
            number = Decimal(value)
            if not number.is_finite() or number < 0 or number != number.quantize(Decimal('.01')):
                raise ManifestError('Invalid release amount')
            result.append(format(number, '.2f'))
        elif column in spec.floats.split(','):
            number = float(value)
            if not math.isfinite(number):
                raise ManifestError('Nonfinite release score')
            result.append(format(number, '.17g'))
        else:
            result.append(value.isoformat() if isinstance(value, date) else str(value))
    return result


def export_table(db, table, query, directory):
    spec = TABLES[table]
    local = Path(directory)/(table+'.tsv')
    reference = write_tsv(local, (normalize(table, row) for row in db.execute(query)))
    return reference | {'file': local.name, 'columns': spec.columns.split(','), 'bytes': local.stat().st_size}


def build_exports(db, directory, recommendations, train, tick=lambda: None):
    """Consume the independently audited W04 SQLite tables, retaining pence."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    db.executescript('''
        CREATE TABLE catalog(stock_code TEXT PRIMARY KEY);
        CREATE TABLE popularity(stock_code TEXT PRIMARY KEY,order_count INTEGER,rank INTEGER UNIQUE);
        CREATE TABLE recommendations(algorithm TEXT,customer_id TEXT,stock_code TEXT,
            rank INTEGER,score TEXT,reason_code TEXT,is_fallback INTEGER,
            PRIMARY KEY(algorithm,customer_id,stock_code),UNIQUE(algorithm,customer_id,rank));
    ''')
    db.executemany('INSERT INTO catalog VALUES (?)', ((i,) for i in train.items))
    db.executemany('INSERT INTO popularity VALUES (?,?,?)',
                   ((item, count, rank) for rank, (item, count) in enumerate(train.popularity, 1)))
    db.execute('CREATE INDEX release_seen_user ON customer_seen(customer_id,stock_code)')
    catalog = set(train.items)
    users = {r[0] for r in db.execute('SELECT customer_id FROM rfm')}
    reasons = {'POP': 'POPULAR', 'ITEMCF': 'CO_PURCHASE', 'ALS': 'LATENT_FACTOR', 'NCF': 'NEURAL_CF'}
    for algorithm, (model_id, path) in recommendations.items():
        counts = {}
        for mid, user, item, rank, score, reason, fallback in read_tsv(path, 7):
            tick()
            rank, fallback = int(rank), int(fallback)
            if (mid != model_id or user not in users or item not in catalog
                    or item in train.history.get(user, {}) or not math.isfinite(float(score))
                    or fallback not in (0, 1) or algorithm == 'POP' and fallback
                    or reason != ('FALLBACK_POPULAR' if fallback else reasons[algorithm])):
                raise ManifestError('Invalid release recommendation reference, score or reason')
            db.execute('INSERT INTO recommendations VALUES (?,?,?,?,?,?,?)',
                       (algorithm, user, item, rank, score, reason, fallback))
            counts.setdefault(user, []).append(rank)
        for user in users:
            expected = min(10, len(catalog)-len(train.history.get(user, {})))
            if sorted(counts.get(user, [])) != list(range(1, expected+1)):
                raise ManifestError('Incomplete or noncontiguous release Top10')
    as_of = (date.fromisoformat(db.execute('SELECT MAX(source_date) FROM order_fact').fetchone()[0])
             + timedelta(days=1)).isoformat()
    # Product identity includes returns and nonmerchandise, while the display
    # description comes exclusively from the latest nonempty positive row.
    db.execute('CREATE INDEX release_clean_item ON clean(stock_code,record_class,invoice_at,source_row_number)')
    queries = {
        'product_dim': '''SELECT p.stock_code,(SELECT description FROM clean c WHERE c.stock_code=p.stock_code
            AND c.record_class='SALE' AND c.description IS NOT NULL ORDER BY invoice_at DESC,source_row_number DESC LIMIT 1),
            MAX(CASE WHEN p.record_class='SALE' AND p.is_merchandise='true' THEN 1 ELSE 0 END),
            EXISTS(SELECT 1 FROM catalog x WHERE x.stock_code=p.stock_code)
            FROM clean p GROUP BY p.stock_code ORDER BY p.stock_code''',
        'sales_cube': 'SELECT source_date,country,stock_code,gross_pence,return_pence,sale_quantity,return_quantity FROM sales_cube ORDER BY source_date,country,stock_code',
        'sale_order': 'SELECT invoice_no,source_date,country,customer_id,gross_pence FROM order_fact ORDER BY invoice_no',
        'sale_order_item': 'SELECT invoice_no,stock_code,gross_pence,quantity FROM product_order_fact ORDER BY invoice_no,stock_code',
        'customer_rfm': "SELECT customer_id,last_purchase,'"+as_of+"',recency_days,frequency,monetary_pence,r_score,f_score,m_score,segment FROM rfm ORDER BY customer_id",
        'customer_seen_item': 'SELECT customer_id,stock_code FROM customer_seen ORDER BY customer_id,stock_code',
        'item_similarity': 'SELECT stock_code,related_stock_code,similarity,confidence,lift,co_count,rank FROM similarity ORDER BY stock_code,related_stock_code',
        'product_popularity': 'SELECT * FROM popularity ORDER BY stock_code',
        'customer_recommendation': 'SELECT * FROM recommendations ORDER BY algorithm,customer_id,stock_code',
    }
    result = {}
    for name, query in queries.items():
        tick()
        # SQLite never converts financial integers through binary floating point.
        def converted_rows():
            columns = TABLES[name].columns.split(',')
            for row in db.execute(query):
                yield [Decimal(value)/100 if column in TABLES[name].money.split(',') else value
                       for column, value in zip(columns, row, strict=True)]
        local = directory/(name+'.tsv')
        ref = write_tsv(local, (normalize(name, row) for row in converted_rows()))
        result[name] = ref | {'file': local.name, 'columns': TABLES[name].columns.split(','), 'bytes': local.stat().st_size}
    return result


def compare_database(connection, release_id, tables, tick=lambda: None):
    """Hash each ordered row, including all amounts, ranks and DOUBLE values."""
    import pymysql.cursors
    result = {}
    for name, spec in TABLES.items():
        digest, count = hashlib.sha256(), 0
        with connection.cursor(pymysql.cursors.SSCursor) as cursor:
            columns = ','.join('`'+c+'`' for c in spec.columns.split(','))
            keys = ','.join('`'+c+'`' for c in spec.key.split(','))
            cursor.execute('SELECT '+columns+' FROM '+name+' WHERE release_id=%s ORDER BY '+keys, (release_id,))
            while rows := cursor.fetchmany(2000):
                for row in rows:
                    digest.update(line(normalize(name, row)).encode())
                    count += 1
                # A streaming cursor must be drained before heartbeat SQL.
        tick()
        actual = {'rows': count, 'sha256': digest.hexdigest()}
        if any(actual[k] != tables[name][k] for k in actual):
            raise ManifestError('Release database differs from immutable export: '+name)
        result[name] = actual
    return result
