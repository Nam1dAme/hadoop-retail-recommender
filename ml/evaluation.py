"""Shared full-catalog ranking and macro metrics. K remains ten for short lists."""
import math
import numpy as np

K = 10


def recommend(train, users, model, *, user_block=32, item_block=512, tick=lambda: None):
    if user_block < 1 or item_block < 1:
        raise ValueError('Invalid score block size')
    result = {}
    catalog = train.items
    for start in range(0, len(users), user_block):
        group = users[start:start + user_block]
        best = {u: [] for u in group}
        known = [u for u in group if u in train.history]
        for offset in range(0, len(catalog), item_block):
            tick()
            items = catalog[offset:offset + item_block]
            if not known:
                break
            scores = np.asarray(model.score(known, items), dtype=np.float64)
            if scores.shape != (len(known), len(items)) or not np.isfinite(scores).all():
                raise ValueError('Model returned invalid full-catalog scores')
            for user, row in zip(known, scores):
                candidates = [(i, float(s), False) for i, s in zip(items, row)
                              if i not in train.history[user] and (not model.positive_only or s > 0)]
                best[user] = sorted(best[user] + candidates, key=lambda r: (-r[1], r[0].encode()))[:K]
        for user in group:
            seen = set(train.history.get(user, {})) | {r[0] for r in best[user]}
            for item, score in train.popularity:
                if len(best[user]) == K:
                    break
                if item not in seen:
                    best[user].append((item, float(score), True))
                    seen.add(item)
            result[user] = best[user]
    return result


def user_metrics(recommendations, truth):
    if not truth:
        raise ValueError('Empty relevance set')
    if len(recommendations) > K or len(set(recommendations)) != len(recommendations):
        raise ValueError('Duplicate or excessive recommendations')
    hits = sum(i in truth for i in recommendations)
    dcg = sum(1 / math.log2(rank + 1) for rank, item in enumerate(recommendations, 1) if item in truth)
    idcg = sum(1 / math.log2(rank + 1) for rank in range(1, min(K, len(truth)) + 1))
    return hits / K, hits / len(truth), dcg / idcg


def evaluate(train, truth, stats, recommendations, split):
    if split not in ('VALIDATION', 'TEST'):
        raise ValueError('Unknown evaluation split')
    from ml.data import cohort, set_hash
    if stats['catalog_sha256'] != set_hash((i,) for i in train.items):
        raise ValueError('Evaluation catalog differs from training')
    catalog = set(train.items)
    common = {key: stats[key] for key in ('catalog_count', 'window_user_count', 'cold_user_count',
                                         'cold_item_count', 'cold_interaction_count', 'excluded_user_count')}
    results = []
    for name in ('WARM', 'ALL_ELIGIBLE'):
        users = sorted((u for u in truth if name == 'ALL_ELIGIBLE' or u in train.history), key=lambda u: u.encode())
        if (not users or stats['catalog_count'] != len(catalog)
                or stats['cohorts'][name]['users_sha256'] != set_hash((u,) for u in users)
                or stats['cohorts'][name]['user_count'] != len(users)):
            raise ValueError('Empty or inconsistent shared cohort')
        totals = np.zeros(3, dtype=np.float64)
        covered = set()
        for user in users:
            if not truth[user] or not truth[user] <= catalog - train.history.get(user, {}).keys():
                raise ValueError('Relevance contains seen or out-of-catalog item')
            rows = recommendations[user]
            items = [r[0] for r in rows]
            if not set(items) <= catalog - train.history.get(user, {}).keys():
                raise ValueError('Recommendation contains seen or out-of-catalog item')
            totals += user_metrics(items, truth[user])
            covered.update(items)
        p, r, n = (totals / len(users)).tolist()
        results.append(common | {'split_name': split, 'cohort': name, 'user_count': len(users),
            'precision_at_k': p, 'recall_at_k': r, 'ndcg_at_k': n, 'coverage': len(covered) / len(catalog)})
    return results


def choose(candidates):
    """Each entry carries validation WARM metrics, parameter_count and grid_index."""
    if not candidates:
        raise ValueError('No validation candidates')
    best = candidates[0]
    for candidate in candidates[1:]:
        n, b = candidate['warm'], best['warm']
        if n['ndcg_at_k'] > b['ndcg_at_k'] + 1e-6:
            best = candidate
        elif abs(n['ndcg_at_k'] - b['ndcg_at_k']) <= 1e-6:
            if (-n['recall_at_k'], candidate['parameter_count'], candidate['grid_index']) < (
                    -b['recall_at_k'], best['parameter_count'], best['grid_index']):
                best = candidate
    return best
