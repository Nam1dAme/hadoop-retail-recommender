"""Frozen time-split-v1 datasets; bounded streaming input, disk-backed aggregation."""
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import sqlite3

from etl.tsv import line, parse_line
from etl.ingest import sha256, write_json

TRAIN_END = '2011-09-01'
VALIDATION_END = '2011-11-01'


def window(timestamp):
    day = timestamp[:10]
    # Reject malformed dates instead of accepting a lexical approximation.
    from datetime import datetime
    datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
    return 'TRAIN' if day < TRAIN_END else 'VALIDATION' if day < VALIDATION_END else 'TEST'


def read_tsv(path, columns):
    with Path(path).open(encoding='utf-8') as stream:
        for row in stream:
            yield parse_line(row, columns)


def write_tsv(path, rows):
    count = 0
    with Path(path).open('w', encoding='utf-8', newline='\n') as stream:
        for row in rows:
            stream.write(line(row))
            count += 1
    return {'sha256': sha256(path), 'rows': count}


def set_hash(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(line(row).encode())
    return digest.hexdigest()


def prepare(baskets, directory, tick=lambda: None, *, require_windows=True):
    """Split BEFORE interaction/popularity/pair aggregation. Anonymous baskets stay for POP.

    Input is J06's complete, untruncated basket rows, already joined to validated
    order identities. Every order has exactly one source date. No future-derived
    interaction, similarity or RFM table is consumed here.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    database = sqlite3.connect(directory / 'aggregation.sqlite')
    database.set_progress_handler(lambda: (tick() or 0), 10000)
    try:
        database.executescript('''
            PRAGMA journal_mode=OFF;
            CREATE TABLE baskets(invoice TEXT,customer TEXT,at TEXT,item TEXT,split TEXT,
                                 PRIMARY KEY(invoice,item)) WITHOUT ROWID;
            CREATE INDEX user_item ON baskets(customer,item);
        ''')
        minimum = maximum = None
        for number, (invoice, customer, at, item) in enumerate(baskets, 1):
            part = window(at)
            minimum = min(minimum or at, at)
            maximum = max(maximum or at, at)
            database.execute('INSERT INTO baskets VALUES (?,?,?,?,?)', (invoice, customer, at, item, part))
            if number % 4096 == 0:
                tick()
        if minimum is None:
            raise ValueError('No eligible baskets')
        if require_windows and not (minimum[:10] < TRAIN_END and maximum[:10] >= VALIDATION_END):
            raise ValueError('Source does not cover the frozen time windows')
        # This also prevents a single invoice being silently split across windows.
        conflict = database.execute('''SELECT invoice FROM baskets GROUP BY invoice
            HAVING COUNT(DISTINCT substr(at,1,10))>1 OR COUNT(DISTINCT customer)>1
                OR (COUNT(customer)>0 AND COUNT(customer)<COUNT(*)) LIMIT 1''').fetchone()
        if conflict:
            raise ValueError('Inconsistent order identity in basket input')
        summary = {'split_policy_version': 'time-split-v1', 'candidate_policy_version': 'full-catalog-v1',
                   'train_end_exclusive': TRAIN_END, 'validation_end_exclusive': VALIDATION_END,
                   'minimum': minimum, 'maximum': maximum, 'contexts': {}}
        for purpose in ('eval', 'fullfit'):
            destination = directory / purpose
            destination.mkdir()
            predicate = "split='TRAIN'" if purpose == 'eval' else '1=1'
            users = [r[0] for r in database.execute(f'''SELECT DISTINCT customer FROM baskets
                      WHERE {predicate} AND customer IS NOT NULL ORDER BY customer COLLATE BINARY''')]
            items = [r[0] for r in database.execute(f'''SELECT DISTINCT item FROM baskets
                      WHERE {predicate} AND customer IS NOT NULL ORDER BY item COLLATE BINARY''')]
            if not users or not items or max(len(users), len(items)) >= 2**31:
                raise ValueError('Empty or out-of-range training mapping')
            ui = {value: i for i, value in enumerate(users)}
            ii = {value: i for i, value in enumerate(items)}
            mapping = [('USER', value, i) for i, value in enumerate(users)]
            mapping += [('ITEM', value, i) for i, value in enumerate(items)]
            artifacts = {'mapping.tsv': write_tsv(destination / 'mapping.tsv', mapping)}
            query = f'''SELECT customer,item,COUNT(*),MAX(substr(at,1,10)) FROM baskets
                        WHERE {predicate} AND customer IS NOT NULL
                        GROUP BY customer,item ORDER BY customer COLLATE BINARY,item COLLATE BINARY'''
            artifacts['positive_interactions.tsv'] = write_tsv(destination / 'positive_interactions.tsv',
                ((ui[u], ii[i], count, at) for u, i, count, at in database.execute(query)))
            # Include anonymous orders in popularity, but never let them expand C.
            query = f'''SELECT item,COUNT(*) FROM baskets WHERE {predicate}
                        GROUP BY item ORDER BY COUNT(*) DESC,item COLLATE BINARY'''
            artifacts['popularity.tsv'] = write_tsv(destination / 'popularity.tsv',
                ((i, count) for i, count in database.execute(query) if i in ii))
            query = f'''SELECT invoice,customer,at,item FROM baskets WHERE {predicate}
                        ORDER BY invoice COLLATE BINARY,item COLLATE BINARY'''
            artifacts['baskets.tsv'] = write_tsv(destination / 'baskets.tsv', database.execute(query))
            summary['contexts'][purpose] = {'user_count': len(users), 'catalog_count': len(items),
                'catalog_sha256': set_hash((i,) for i in items),
                'users_sha256': set_hash((u,) for u in users), 'files': artifacts}
        train = TrainingData.load(directory / 'eval')
        for split in ('VALIDATION', 'TEST'):
            destination = directory / split.lower()
            destination.mkdir()
            purchases = defaultdict(set)
            for user, item in database.execute('''SELECT DISTINCT customer,item FROM baskets
                    WHERE split=? AND customer IS NOT NULL ORDER BY customer COLLATE BINARY,item COLLATE BINARY''', (split,)):
                purchases[user].add(item)
            truth, stats = cohort(train, purchases)
            truth_ref = write_tsv(destination / 'truth.tsv',
                ((u, i) for u in sorted(truth, key=lambda x: x.encode()) for i in sorted(truth[u], key=lambda x: x.encode())))
            stats['truth_sha256'] = truth_ref['sha256']
            stats['files'] = {'truth.tsv': truth_ref}
            write_json(destination / 'cohorts.json', stats)
            summary[split.lower()] = stats
        write_json(directory / 'summary.json', summary)
        return summary
    finally:
        database.close()


@dataclass
class TrainingData:
    users: list
    items: list
    history: dict
    popularity: list
    mapping_sha256: str

    @classmethod
    def load(cls, directory):
        directory = Path(directory)
        entities = {'USER': [], 'ITEM': []}
        for kind, value, index in read_tsv(directory / 'mapping.tsv', 3):
            if kind not in entities or int(index) != len(entities[kind]) or int(index) >= 2**31:
                raise ValueError('Invalid contiguous int32 mapping')
            entities[kind].append(value)
        for values in entities.values():
            if not values or values != sorted(set(values), key=lambda x: x.encode()):
                raise ValueError('Mapping must be nonempty and sorted by UTF-8 bytes')
        users, items = entities['USER'], entities['ITEM']
        history = {u: {} for u in users}
        for u, i, count, at in read_tsv(directory / 'positive_interactions.tsv', 4):
            u, i, count = int(u), int(i), int(count)
            if not 0 <= u < len(users) or not 0 <= i < len(items) or count <= 0 or items[i] in history[users[u]]:
                raise ValueError('Invalid or duplicate positive interaction')
            from datetime import date
            date.fromisoformat(at)
            if directory.name == 'eval' and at >= TRAIN_END:
                raise ValueError('EVAL interaction contains future purchase')
            history[users[u]][items[i]] = (count, at)
        if any(not h for h in history.values()):
            raise ValueError('Mapped user has no positive history')
        popularity = [(i, int(count)) for i, count in read_tsv(directory / 'popularity.tsv', 2)]
        if (set(i for i, _ in popularity) != set(items) or len(popularity) != len(items)
                or any(count <= 0 for _, count in popularity)
                or popularity != sorted(popularity, key=lambda r: (-r[1], r[0].encode()))):
            raise ValueError('Invalid popularity ordering or catalog')
        return cls(users, items, history, popularity, sha256(directory / 'mapping.tsv'))


def cohort(train, purchases):
    catalog, known = set(train.items), set(train.users)
    truth = {u: (set(items) & catalog) - train.history.get(u, {}).keys() for u, items in purchases.items()}
    excluded = sum(not values for values in truth.values())
    truth = {u: values for u, values in truth.items() if values}
    warm = sorted(known & truth.keys(), key=lambda x: x.encode())
    all_users = sorted(truth, key=lambda x: x.encode())
    stats = {'catalog_count': len(catalog), 'catalog_sha256': set_hash((i,) for i in train.items),
        'window_user_count': len(purchases), 'cold_user_count': len(purchases.keys() - known),
        'cold_item_count': len(set().union(*(set(v) - catalog for v in purchases.values()))),
        'cold_interaction_count': sum(len(set(v) - catalog) for v in purchases.values()),
        'excluded_user_count': excluded,
        'cohorts': {name: {'user_count': len(users), 'users_sha256': set_hash((u,) for u in users),
                          # C_u is represented exactly by C plus each user's exclusions.
                          # This canonical hash avoids materializing the Cartesian matrix.
                          'candidate_sha256': set_hash(
                              [('CATALOG', stats_catalog) for stats_catalog in train.items] +
                              [('USER', u) for u in users] +
                              [('EXCLUDE', u, i) for u in users for i in sorted(train.history.get(u, {}), key=lambda x: x.encode())])}
                    for name, users in (('WARM', warm), ('ALL_ELIGIBLE', all_users))}}
    if not catalog or not warm or not all_users:
        raise ValueError('Evaluation requires nonempty catalog and both cohorts')
    return truth, stats
