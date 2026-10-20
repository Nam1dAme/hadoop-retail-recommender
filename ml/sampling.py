"""Reproducible seed+epoch training samples, excluding TRAIN purchases only."""
from pathlib import Path
import numpy as np
from ml.data import write_tsv, read_tsv


def sample_epoch(train, epoch, *, seed=42):
    if epoch < 1:
        raise ValueError('Epoch starts at one')
    rng = np.random.default_rng(seed + epoch)
    ii = {value: n for n, value in enumerate(train.items)}
    rows = []
    excluded = 0
    for u, user in enumerate(train.users):
        history = train.history[user]
        negative = np.asarray([i for i, value in enumerate(train.items) if value not in history], dtype=np.int32)
        if not len(negative):
            excluded += 1
            continue
        for item in sorted(history, key=lambda i: i.encode()):
            rows.append((u, ii[item], 1))
            rows.extend((u, int(i), 0) for i in rng.choice(negative, size=4, replace=True))
    if not rows:
        raise ValueError('NCF has no legal negative training samples')
    samples = np.asarray(rows, dtype=np.int32)
    samples = samples[rng.permutation(len(samples))]
    return samples, excluded


def save_samples(path, samples, epoch, seed=42):
    return write_tsv(path, ((int(u), int(i), int(label), epoch, seed + epoch) for u, i, label in samples))


def load_samples(path, train, epoch, seed=42):
    rows = []
    for user, item, label, actual_epoch, actual_seed in read_tsv(path, 5):
        u, i, label = int(user), int(item), int(label)
        if (int(actual_epoch) != epoch or int(actual_seed) != seed + epoch or label not in (0, 1)
                or not 0 <= u < len(train.users) or not 0 <= i < len(train.items)
                or bool(label) != (train.items[i] in train.history[train.users[u]])):
            raise ValueError('Invalid training sample or TRAIN-history leakage')
        rows.append((u, i, label))
    return np.asarray(rows, dtype=np.int32)
