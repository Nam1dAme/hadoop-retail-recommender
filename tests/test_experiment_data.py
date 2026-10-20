"""T-08/T-10: date boundaries, independent mappings, shared cohorts and hand metrics."""
import math
import json
import numpy as np
import pytest

from ml.data import prepare, window, TrainingData, read_tsv, cohort
from ml.evaluation import user_metrics, recommend, evaluate, choose


@pytest.fixture
def experiment_data(tmp_path):
    baskets = [
        ('1', 'u', '2011-08-01T12:00:00', '10001'),
        ('2', 'v', '2011-08-31T12:00:00', '10002'),
        ('3', 'w', '2011-08-01T12:00:00', '10003'),
        ('4', 'x', '2011-08-01T12:00:00', '10004'),
        ('5', None, '2011-08-01T12:00:00', '99999'),
        ('6', None, '2011-08-01T12:00:00', '10002'),
        ('7', 'u', '2011-09-01T12:00:00', '10002'),
        ('7', 'u', '2011-09-01T12:00:00', '10004'),
        ('8', 'cold', '2011-10-31T12:00:00', '10002'),
        ('8', 'cold', '2011-10-31T12:00:00', '99998'),
        ('9', 'v', '2011-09-02T12:00:00', '10002'),
        ('10', 'u', '2011-11-01T12:00:00', '10003'),
        ('11', 'v', '2011-12-09T12:00:00', '10004'),
        ('12', 'later', '2011-12-09T12:00:00', '10002'),
    ]
    directory = tmp_path / 'data'
    summary = prepare(iter(baskets), directory)
    return directory, summary, baskets


def test_boundaries():
    assert [window(d + 'T00:00:00') for d in ('2011-08-31', '2011-09-01', '2011-10-31', '2011-11-01')] == [
        'TRAIN', 'VALIDATION', 'VALIDATION', 'TEST']
    with pytest.raises(ValueError):
        window('2011-02-30T00:00:00')


def test_mapping_and_split_isolation(experiment_data):
    directory, summary, _ = experiment_data
    train = TrainingData.load(directory / 'eval')
    full = TrainingData.load(directory / 'fullfit')
    assert train.items == ['10001', '10002', '10003', '10004']
    assert '99999' not in full.items  # Anonymous-only purchases never expand C.
    assert '99998' in full.items and '99998' not in train.items
    assert 'cold' in full.users and 'cold' not in train.users
    assert train.mapping_sha256 != full.mapping_sha256
    assert train.history['u'] == {'10001': (1, '2011-08-01')}
    assert train.popularity[0] == ('10002', 2)  # Includes anonymous order.
    indices = list(read_tsv(directory / 'eval/mapping.tsv', 3))
    assert indices[0] == ['USER', 'u', '0']
    stats = summary['validation']
    assert {k: stats[k] for k in ('window_user_count', 'cold_user_count', 'cold_item_count',
                                  'cold_interaction_count', 'excluded_user_count')} == {
        'window_user_count': 3, 'cold_user_count': 1, 'cold_item_count': 1,
        'cold_interaction_count': 1, 'excluded_user_count': 1}
    assert stats['cohorts']['WARM']['user_count'] == 1
    assert stats['cohorts']['ALL_ELIGIBLE']['user_count'] == 2


def test_order_cannot_cross_split(experiment_data, tmp_path):
    _, _, baskets = experiment_data
    baskets += [('1', 'u', '2011-09-01T12:00:00', '10003')]
    with pytest.raises(ValueError, match='order identity'):
        prepare(iter(baskets), tmp_path / 'bad')


def test_empty_evaluation_fails(tmp_path):
    with pytest.raises(ValueError, match='cover'):
        prepare(iter([('1', 'u', '2011-08-01T12:00:00', '10001')]), tmp_path / 'empty')


def test_hand_ranking_metrics():
    precision, recall, ndcg = user_metrics(['B', 'C', 'D'], {'B', 'D'})
    assert precision == .2 and recall == 1
    assert ndcg == pytest.approx(1.5 / (1 + 1 / math.log2(3)), abs=1e-6)
    with pytest.raises(ValueError):
        user_metrics([], set())
    with pytest.raises(ValueError):
        user_metrics(['B', 'B'], {'B'})


class SparseModel:
    positive_only = True

    def score(self, users, items):
        return np.asarray([[.01 if i == '10004' else 0 for i in items] for _ in users])


def test_common_candidates_fallback_and_blocking(experiment_data):
    directory, summary, _ = experiment_data
    train = TrainingData.load(directory / 'eval')
    truth = {'u': {'10002', '10004'}, 'cold': {'10002'}}
    users = ['cold', 'u']
    a = recommend(train, users, SparseModel(), user_block=1, item_block=1)
    b = recommend(train, users, SparseModel(), user_block=32, item_block=512)
    assert a == b
    assert a['u'][0] == ('10004', .01, False)
    assert [r[0] for r in a['u']] == ['10004', '10002', '10003']
    assert a['cold'][0] == ('10002', 2., True)
    rows = evaluate(train, truth, summary['validation'], a, 'VALIDATION')
    assert rows[0]['user_count'] == 1 and rows[1]['user_count'] == 2
    assert rows[0]['precision_at_k'] == .2 and rows[0]['recall_at_k'] == 1
    assert rows[0]['coverage'] == .75
    corrupted = json.loads(json.dumps(summary['validation']))
    corrupted['cohorts']['WARM']['users_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='cohort'):
        evaluate(train, truth, corrupted, a, 'VALIDATION')
    a['u'][0] = ('10001', .1, False)
    with pytest.raises(ValueError, match='seen'):
        evaluate(train, truth, summary['validation'], a, 'VALIDATION')


def test_selection_uses_validation_tolerance_recall_and_size():
    candidates = [
        {'warm': {'ndcg_at_k': .2, 'recall_at_k': .3}, 'parameter_count': 30, 'grid_index': 0},
        {'warm': {'ndcg_at_k': .2000005, 'recall_at_k': .4}, 'parameter_count': 40, 'grid_index': 1},
        {'warm': {'ndcg_at_k': .2, 'recall_at_k': .4}, 'parameter_count': 20, 'grid_index': 2},
    ]
    assert choose(candidates) is candidates[2]
