"""POP, ItemCF and bounded factor scoring; all scores use business-ID mappings."""
import json
import math
from pathlib import Path
import numpy as np

from etl.ingest import write_json
from ml.data import read_tsv


class Popularity:
    positive_only = False

    def __init__(self, train):
        self.counts = dict(train.popularity)

    def score(self, users, items):
        return np.tile([self.counts[i] for i in items], (len(users), 1))

    def save(self, directory):
        write_json(Path(directory) / 'rules.json', {'algorithm': 'POP', 'scores': self.counts})


class ItemCF:
    positive_only = True

    def __init__(self, train, neighbors):
        self.train = train
        self.neighbors = neighbors
        self.cache = {}
        for item, rows in neighbors.items():
            if (len(rows) > 10 or len({r[0] for r in rows}) != len(rows)
                    or any(j == item or not math.isfinite(sim) or not 0 < sim <= 1 for j, sim in rows)):
                raise ValueError('Invalid ItemCF neighbors')

    @classmethod
    def from_files(cls, train, files):
        neighbors = {}
        for path in files:
            for item, related, sim, count, confidence, lift, rank in read_tsv(path, 7):
                if int(count) < 3 or not 1 <= int(rank) <= 10:
                    raise ValueError('ItemCF feature threshold differs from baseline')
                neighbors.setdefault(item, []).append((related, float(sim)))
        return cls(train, neighbors)

    def score(self, users, items):
        for user in users:
            if user not in self.cache:
                history = self.train.history[user]
                # Timestamps are fixed ISO strings; descending time, ascending UTF-8 ID.
                seeds = sorted(history, key=lambda i: i.encode())
                seeds.sort(key=lambda i: history[i][1], reverse=True)
                scores = {}
                for seed in seeds[:20]:
                    weight = math.log1p(history[seed][0])
                    for item, similarity in self.neighbors.get(seed, []):
                        scores[item] = scores.get(item, 0.) + weight * similarity
                self.cache[user] = scores
        return np.asarray([[self.cache[u].get(i, 0.) for i in items] for u in users])

    def save(self, directory):
        write_json(Path(directory) / 'rules.json', {'algorithm': 'ITEMCF', 'neighbors': self.neighbors,
                                                  'max_seeds': 20, 'min_co_count': 3, 'max_neighbors': 10})


class Factors:
    positive_only = False

    def __init__(self, train, user_factors, item_factors):
        self.ui = {u: i for i, u in enumerate(train.users)}
        self.ii = {i: n for n, i in enumerate(train.items)}
        self.users = np.asarray(user_factors, dtype=np.float32)
        self.items = np.asarray(item_factors, dtype=np.float32)
        if (self.users.ndim != 2 or self.items.ndim != 2 or self.users.shape[0] != len(train.users)
                or self.items.shape[0] != len(train.items) or self.users.shape[1] != self.items.shape[1]
                or not np.isfinite(self.users).all() or not np.isfinite(self.items).all()
                or (self.users < 0).any() or (self.items < 0).any()):
            raise ValueError('Invalid nonnegative ALS factors')

    def score(self, users, items):
        return self.users[[self.ui[u] for u in users]] @ self.items[[self.ii[i] for i in items]].T


def fit_als(train, data_directory, directory, params):
    from pyspark.ml.recommendation import ALS, ALSModel
    from pyspark.sql import SparkSession, functions as F
    directory = Path(directory)
    spark = (SparkSession.builder.master('local[2]').appName('retail-als-' + directory.name)
             .config('spark.driver.memory', '2g').config('spark.sql.shuffle.partitions', '4')
             .config('spark.driver.bindAddress', '127.0.0.1')
             .config('spark.hadoop.fs.defaultFS', 'file:///')
             .config('spark.local.dir', str(directory / 'spark-tmp')).getOrCreate())
    try:
        spark.sparkContext.setLogLevel('WARN')
        if not spark.sparkContext._jvm.java.lang.System.getProperty('java.version').startswith('1.8.'):
            raise ValueError('ALS requires Java 8')
        frame = (spark.read.option('sep', '\t').schema('user int,item int,count long,last string')
                 .csv((Path(data_directory) / 'positive_interactions.tsv').resolve().as_uri())
                 .select('user', 'item', F.log1p('count').cast('float').alias('rating')))
        model = ALS(rank=params['rank'], regParam=params['reg_param'], maxIter=10, alpha=10.,
                    implicitPrefs=True, nonnegative=True, seed=42, numUserBlocks=2, numItemBlocks=2,
                    coldStartStrategy='drop').fit(frame)
        model_path = (directory / 'spark_model').resolve().as_uri()
        model.save(model_path)
        loaded = ALSModel.load(model_path)
        write_json(directory / 'als-runtime.json', {
            'spark_version': spark.version,
            'java_version': spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
            'master': spark.sparkContext.master,
            'driver_memory': spark.sparkContext.getConf().get('spark.driver.memory'),
            'shuffle_partitions': spark.conf.get('spark.sql.shuffle.partitions'),
            'parameters': {p.name: value for p,value in loaded.extractParamMap().items()}})

        def factors(value):
            users = sorted(value.userFactors.collect(), key=lambda r: r.id)
            items = sorted(value.itemFactors.collect(), key=lambda r: r.id)
            if [r.id for r in users] != list(range(len(train.users))) or [r.id for r in items] != list(range(len(train.items))):
                raise ValueError('Spark factor IDs differ from mapping')
            return Factors(train, [r.features for r in users], [r.features for r in items])

        before, after = factors(model), factors(loaded)
        if not np.array_equal(before.users, after.users) or not np.array_equal(before.items, after.items):
            raise ValueError('ALS factors changed after Spark save/load')
        np.savez(directory / 'factors.npz', users=after.users, items=after.items)
        return after
    finally:
        spark.stop()


def load_model(algorithm, train, directory, *, device='cuda'):
    directory = Path(directory)
    if algorithm == 'NCF':
        from ml.ncf import load
        return load(train, directory, device=device)
    if algorithm == 'ALS':
        with np.load(directory / 'factors.npz', allow_pickle=False) as factors:
            return Factors(train, factors['users'], factors['items'])
    rules = json.loads((directory / 'rules.json').read_text())
    if rules['algorithm'] != algorithm:
        raise ValueError('Rules algorithm differs')
    if algorithm == 'POP':
        model = Popularity(train)
        if model.counts != rules['scores']:
            raise ValueError('Saved popularity differs from training window')
        return model
    if algorithm == 'ITEMCF':
        return ItemCF(train, rules['neighbors'])
    raise ValueError('Unknown algorithm')
