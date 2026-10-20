"""T-09/T-11: deterministic negatives, persistence, seeds and unified recommendation rules."""
import numpy as np
import pytest
from ml.data import TrainingData
from ml.evaluation import recommend
from ml.models import Popularity, ItemCF, load_model
from ml.sampling import sample_epoch, save_samples, load_samples


@pytest.fixture
def training():
    return TrainingData(['u', 'v', 'w'], ['10001', '10002', '10003'],
        {'u': {'10001': (2, '2011-08-01')}, 'v': {'10002': (1, '2011-08-01')},
         'w': {'10003': (1, '2011-08-01')}},
        [('10001', 3), ('10002', 2), ('10003', 1)], 'a'*64)


def test_negative_samples_are_reproducible_and_train_only(training, tmp_path):
    samples, excluded = sample_epoch(training, 1)
    assert excluded == 0 and samples.shape == (15, 3)
    assert np.array_equal(samples, sample_epoch(training, 1)[0])
    assert not np.array_equal(samples, sample_epoch(training, 2)[0])
    for u, i, label in samples:
        assert bool(label) == (training.items[i] in training.history[training.users[u]])
    assert sum(label == 1 for _, _, label in samples) == 3
    # Future labels never enter the sampling API. A future purchase remains legal.
    assert any(u == 0 and i == 1 and label == 0 for u, i, label in samples)
    path = tmp_path / 'samples.tsv'
    save_samples(path, samples, 1)
    assert np.array_equal(samples, load_samples(path, training, 1))
    with pytest.raises(ValueError):
        load_samples(path, training, 2)


def test_no_legal_negatives_are_excluded(training):
    training.history['u'] = {i: (1, '2011-08-01') for i in training.items}
    samples, excluded = sample_epoch(training, 1)
    assert excluded == 1 and not any(row[0] == 0 for row in samples)
    training.history = {u: dict(training.history['u']) for u in training.users}
    with pytest.raises(ValueError, match='no legal'):
        sample_epoch(training, 1)


def test_pop_itemcf_persistence_and_recommendations(training, tmp_path):
    pop = Popularity(training)
    pop.save(tmp_path)
    reloaded = load_model('POP', training, tmp_path)
    assert recommend(training, ['u', 'unknown'], pop) == recommend(training, ['u', 'unknown'], reloaded)
    assert [r[0] for r in recommend(training, ['u'], pop)['u']] == ['10002', '10003']
    cf = ItemCF(training, {'10001': [('10003', .5)]})
    cf.save(tmp_path)
    reloaded = load_model('ITEMCF', training, tmp_path)
    assert cf.score(['u'], ['10003'])[0, 0] == pytest.approx(np.log(3)*.5)
    assert recommend(training, ['u'], reloaded)['u'] == [
        ('10003', pytest.approx(np.log(3)*.5), False), ('10002', 2., True)]
    training.history['u'] = {i: (1, '2011-08-01') for i in training.items}
    assert recommend(training, ['u'], pop)['u'] == []


def test_itemcf_recent_twenty_seed_limit():
    items = [str(10000+i) for i in range(23)]
    history = {i: (1, '2011-08-01') for i in items[:-1]}
    train = TrainingData(['u'], items, {'u': history}, [(i, 1) for i in items], 'a'*64)
    model = ItemCF(train, {i: [(items[-1], .5)] for i in history})
    assert model.score(['u'], [items[-1]])[0, 0] == pytest.approx(20 * np.log(2) * .5)
