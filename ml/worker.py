"""One serial training child. Parent owns the database lock and ten-second heartbeat."""
import os
os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
os.environ.setdefault('PYSPARK_SUBMIT_ARGS', '--driver-memory 2g pyspark-shell')
os.environ.setdefault('OPENBLAS_NUM_THREADS', '2')
from collections import defaultdict
import argparse
import json
from pathlib import Path
import resource
import shutil
import subprocess
import time
import numpy as np

from etl.ingest import write_json
from ml.data import TrainingData, read_tsv
from ml.evaluation import recommend, evaluate
from ml.models import Popularity, ItemCF, fit_als, load_model


def labels(directory):
    directory = Path(directory)
    truth = defaultdict(set)
    for user, item in read_tsv(directory / 'truth.tsv', 2):
        truth[user].add(item)
    return dict(truth), json.loads((directory / 'cohorts.json').read_text())


def hardware(cuda=False):
    import platform
    import torch
    driver = toolkit = None
    smi = shutil.which('nvidia-smi')
    if smi:
        result = subprocess.run([smi, '--query-gpu=name,driver_version,memory.total', '--format=csv,noheader'],
                                capture_output=True, text=True, timeout=20)
        if result.returncode == 0:
            driver = result.stdout.strip()
    nvcc = shutil.which('nvcc')
    if nvcc:
        result = subprocess.run([nvcc, '--version'], capture_output=True, text=True, timeout=20)
        toolkit = result.stdout.strip() if result.returncode == 0 else None
    cpu_model = next((row.partition(':')[2].strip() for row in Path('/proc/cpuinfo').read_text().splitlines()
                      if row.startswith('model name')), platform.processor())
    return {'device': 'CUDA' if cuda else 'CPU', 'platform': platform.platform(), 'cpu_model': cpu_model,
            'torch_cpu_threads': torch.get_num_threads(), 'torch_version': torch.__version__,
            'cuda_runtime': torch.version.cuda, 'toolkit': toolkit,
            'nvidia_smi': driver, 'cuda_available': torch.cuda.is_available(),
            'gpu_name': torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
            'gpu_memory_bytes': torch.cuda.get_device_properties(0).total_memory if torch.cuda.is_available() else None,
            'tf32': torch.backends.cuda.matmul.allow_tf32, 'amp': torch.is_autocast_enabled(),
            'seed': 42, 'deterministic_algorithms': torch.are_deterministic_algorithms_enabled(),
            'num_workers': 0, 'cpu_peak_rss_mib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', required=True, type=Path)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--algorithm', required=True, choices=['POP','ITEMCF','ALS','NCF'])
    parser.add_argument('--mode', choices=['train','test'], default='train')
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    directory = args.directory
    params = json.loads((directory / 'config.json').read_text())
    train = TrainingData.load(args.data / 'eval')
    started = time.perf_counter()
    if train.mapping_sha256 != params['mapping_sha256']:
        raise ValueError('Worker input mapping hash differs')
    details = {}
    if args.mode == 'test':
        model = load_model(args.algorithm, train, directory)
        truth, stats = labels(args.data / 'test')
        split = 'TEST'
    else:
        truth, stats = labels(args.data / 'validation')
        split = 'VALIDATION'
        if args.algorithm == 'POP':
            model = Popularity(train)
            model.save(directory)
            details['parameter_count'] = len(train.items)
        elif args.algorithm == 'ITEMCF':
            model = ItemCF.from_files(train, sorted((args.data / 'similarity').glob('*.tsv')))
            model.save(directory)
            details['parameter_count'] = sum(len(v) for v in model.neighbors.values())
        elif args.algorithm == 'ALS':
            model = fit_als(train, args.data / 'eval', directory, params['hyperparameters'])
            details['parameter_count'] = params['hyperparameters']['rank'] * (len(train.users) + len(train.items))
        else:
            from ml.ncf import fit
            model, details = fit(train, directory, params['hyperparameters'], truth, stats)
        loaded = load_model(args.algorithm, train, directory)
        before = model.score(train.users[:2], train.items[:5])
        after = loaded.score(train.users[:2], train.items[:5])
        if not np.allclose(before, after, rtol=0, atol=1e-6):
            raise ValueError('Saved model scoring differs from in-memory model')
        details['reload_max_error'] = float(np.max(np.abs(before-after)))
        model = loaded
    recommendations = recommend(train, sorted(truth, key=lambda u: u.encode()), model)
    metrics = evaluate(train, truth, stats, recommendations, split)
    # TEST evidence is a new experiment attachment. Never rewrite successful model bundles.
    write_json(directory / ('test-result.json' if args.mode == 'test' else 'worker-result.json'), {
        'status': 'PASS', 'algorithm': args.algorithm, 'metrics': metrics, 'details': details,
        'elapsed_seconds': time.perf_counter()-started, 'hardware': hardware(args.algorithm == 'NCF'),
        'catalog_sha256': stats['catalog_sha256'], 'cohorts': stats['cohorts'], 'truth_sha256': stats['truth_sha256']})
    if args.mode == 'train':
        write_json(directory / 'metrics.json', metrics)
        write_json(directory / 'hardware.json', hardware(args.algorithm == 'NCF'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
