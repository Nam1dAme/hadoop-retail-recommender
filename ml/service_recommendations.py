"""Partition-friendly service Top10 and explicit coverage, including empty users."""
from collections import Counter
from pathlib import Path
import math

from ml.data import write_tsv, set_hash
from ml.evaluation import recommend

REASONS = {'POP':'POPULAR', 'ITEMCF':'CO_PURCHASE', 'ALS':'LATENT_FACTOR', 'NCF':'NEURAL_CF'}


def generate(train, users, model, algorithm, model_id, directory):
    if algorithm not in REASONS or len(users) != len(set(users)) or not set(train.users) <= set(users):
        raise ValueError('Service target must contain every full-history model user exactly once')
    users = sorted(users, key=lambda u:u.encode())
    catalog = set(train.items)
    coverage, counts, covered = [], Counter(), set()

    def rows():
        for offset in range(0, len(users), 32):
            group = users[offset:offset+32]
            predictions = recommend(train, group, model)
            for user in group:
                history = train.history.get(user, {})
                available = len(catalog)-len(history)
                values = predictions[user]
                if len(values) != min(10, available):
                    raise ValueError('Service recommendation count does not cover available candidates')
                seen, fallback_count = set(), 0
                for rank, (item, score, fallback) in enumerate(values, 1):
                    fallback = fallback and algorithm != 'POP'
                    if item not in catalog or item in history or item in seen or not math.isfinite(score):
                        raise ValueError('Invalid full-history service recommendation')
                    seen.add(item)
                    covered.add(item)
                    fallback_count += int(fallback)
                    reason = 'FALLBACK_POPULAR' if fallback else REASONS[algorithm]
                    counts['recommendation_count'] += 1
                    counts['fallback_item_count'] += int(fallback)
                    yield model_id, user, item, rank, score, reason, int(fallback)
                coverage.append((user, available, len(values), fallback_count, int(user in train.history)))
                counts['recommended_user_count'] += int(bool(values))
                counts['no_candidate_user_count'] += int(available == 0)
                counts['short_list_user_count'] += int(len(values) < 10)
                counts['fallback_user_count'] += int(fallback_count > 0)

    output = write_tsv(Path(directory)/'recommendations.tsv', rows())
    coverage_ref = write_tsv(Path(directory)/'coverage.tsv', coverage)
    return {'expected_user_count': len(users), 'known_history_user_count': len(train.users),
            'no_history_user_count': len(set(users)-set(train.users)),
            'users_sha256': set_hash((u,) for u in users),
            'catalog_count': len(catalog), 'covered_item_count': len(covered),
            'catalog_sha256': set_hash((i,) for i in train.items),
            **{k:counts[k] for k in ('recommendation_count','fallback_item_count','recommended_user_count',
                                  'no_candidate_user_count','short_list_user_count','fallback_user_count')},
            'recommendations': output, 'coverage': coverage_ref,
            'coverage_schema': ['customer_id','eligible_candidates','returned_count','fallback_count','known_history']}
