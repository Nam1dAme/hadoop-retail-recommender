"""Isolated serial FULLFIT, service scoring and benchmark children; no DB access."""
import argparse
import json
from pathlib import Path
import time
import numpy as np

from etl.ingest import write_json
from ml.data import TrainingData, read_tsv
from ml.models import Popularity, ItemCF, fit_als, load_model
from ml.worker import hardware, labels


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', required=True, type=Path)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--algorithm', required=True, choices=['POP','ITEMCF','ALS','NCF'])
    parser.add_argument('--mode', required=True, choices=['fullfit','recommend','benchmark'])
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    worker_started = time.perf_counter()
    directory, data = args.directory, args.data
    params = json.loads((directory/'config.json').read_text())
    train = TrainingData.load(data/('eval' if args.mode == 'benchmark' else 'fullfit'))
    if train.mapping_sha256 != params['mapping_sha256']:
        raise ValueError('W07 worker input mapping differs')
    started = time.perf_counter()
    if args.mode == 'benchmark':
        if args.algorithm != 'NCF':
            raise ValueError('CPU/CUDA comparison requires NCF')
        from ml.benchmark import run
        truth, stats = labels(data/'validation')
        result = run(train, data, directory, params, truth, stats)
        result['hardware'] = hardware(params['device'] == 'CUDA')
        result['benchmark_body_seconds'] = result['end_to_end_seconds']
        result['end_to_end_seconds'] = time.perf_counter()-worker_started
        result['end_to_end_scope'] = ('worker config/data/sample reads, warmup, reset, timed training, '
                                      'validation, checkpoint and hardware collection; common TRAIN sample generation excluded')
    elif args.mode == 'fullfit':
        details = {}
        if args.algorithm == 'POP':
            model = Popularity(train)
            model.save(directory)
        elif args.algorithm == 'ITEMCF':
            model = ItemCF.from_files(train, sorted((data/'fullfit-similarity').glob('*.tsv')))
            model.save(directory)
        elif args.algorithm == 'ALS':
            model = fit_als(train, data/'fullfit', directory, params['hyperparameters'])
        else:
            from ml.fullfit import fit
            model, details = fit(train, directory, params['hyperparameters'])
        reloaded = load_model(args.algorithm, train, directory)
        before, after = model.score(train.users[:2],train.items[:5]), reloaded.score(train.users[:2],train.items[:5])
        if not np.allclose(before, after, rtol=0, atol=1e-6):
            raise ValueError('FULLFIT scores changed after save/load')
        details['reload_max_error'] = float(np.max(np.abs(before-after)))
        result = {'details': details, 'hardware': hardware(args.algorithm == 'NCF'),
                  'elapsed_seconds': time.perf_counter()-started, 'purpose': 'FULLFIT'}
        write_json(directory/'hardware.json',result['hardware'])
    else:
        from ml.service_recommendations import generate
        model = load_model(args.algorithm, train, data/'models'/params['model_run_id'])
        users = [u for (u,) in read_tsv(data/'service-users.tsv',1)]
        result = {'coverage': generate(train,users,model,args.algorithm,params['model_run_id'],directory),
                  'elapsed_seconds': time.perf_counter()-started,
                  'hardware': hardware(args.algorithm == 'NCF')}
    result |= {'status': 'PASS', 'algorithm': args.algorithm, 'run_id': args.run_id}
    write_json(directory/(args.mode+'-result.json'),result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
