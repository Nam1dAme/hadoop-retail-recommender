"""One fresh-process repeat of the frozen five-epoch CPU/CUDA experiment."""
import gc
import hashlib
import itertools
import json
from pathlib import Path
import resource
import time
import torch

from etl.ingest import sha256
from ml.evaluation import recommend, evaluate
from ml.neural_training import initialize, batches, train_epoch, step, state_hash
from ml.ncf import NeuralScorer
from ml.sampling import load_samples


def run(train, data, directory, params, truth, stats):
    end_to_end_start = time.perf_counter()
    device = params['device'].lower()
    if params['epochs'] != 5 or params['warmup_steps'] != 20 or params['batch_size'] != 1024:
        raise ValueError('Benchmark protocol differs from the frozen baseline')
    samples, refs = [], []
    for epoch in range(1, 6):
        path = Path(data) / 'benchmark-samples' / f'epoch-{epoch:03d}.tsv'
        digest = sha256(path)
        if digest != params['sample_files'][epoch-1]['sha256']:
            raise ValueError('Benchmark sample order/hash differs')
        values = load_samples(path, train, epoch)
        samples.append(batches(values))
        refs.append({'epoch': epoch, 'sha256': digest, 'rows': len(values)})
    model, optimizer = initialize(train, params['hyperparameters']['learning_rate'], device)
    initial_hash = state_hash(model)
    initial_random_state = hashlib.sha256(torch.get_rng_state().numpy().tobytes()).hexdigest()
    indices, labels = samples[0]
    batch_offsets = range(0, len(labels), 1024)
    warmup_audit = None
    for number, offset in enumerate(itertools.islice(itertools.cycle(batch_offsets), 20)):
        _, evidence = step(model, optimizer, indices[offset:offset+1024], labels[offset:offset+1024],
                           device, audit=number == 0)
        warmup_audit = evidence or warmup_audit
    if device == 'cuda':
        torch.cuda.synchronize()
    warmed_hash = state_hash(model)
    del optimizer, model
    gc.collect()
    model, optimizer = initialize(train, params['hyperparameters']['learning_rate'], device)
    reset_hash = state_hash(model)
    reset_random_state = hashlib.sha256(torch.get_rng_state().numpy().tobytes()).hexdigest()
    if (initial_hash != reset_hash or initial_hash == warmed_hash or len(optimizer.state)
            or initial_random_state != reset_random_state):
        raise ValueError('Warmup did not reset model/optimizer/random state')
    if device == 'cuda':
        torch.cuda.synchronize()
        torch.cuda.reset_peak_memory_stats()
    epoch_rows, audit = [], None
    started = time.perf_counter()
    for epoch, tensors in enumerate(samples, 1):
        epoch_started = time.perf_counter()
        loss, evidence = train_epoch(model, optimizer, tensors, device, audit=epoch == 1)
        if device == 'cuda':
            torch.cuda.synchronize()
        epoch_rows.append({'epoch': epoch, 'train_loss': loss,
                           'elapsed_seconds': time.perf_counter()-epoch_started})
        audit = evidence or audit
    if device == 'cuda':
        torch.cuda.synchronize()
    elapsed = time.perf_counter()-started
    peak = torch.cuda.max_memory_allocated()/1024**2 if device == 'cuda' else None
    cpu_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024
    if epoch_rows[-1]['train_loss'] >= epoch_rows[0]['train_loss']:
        raise ValueError('Benchmark training loss did not decrease overall')
    scorer = NeuralScorer(train, model, device)
    predictions = recommend(train, sorted(truth, key=lambda u:u.encode()), scorer)
    metrics = evaluate(train, truth, stats, predictions, 'VALIDATION')
    # Independent copies, never the selected EVAL model or its database metrics.
    torch.save({k:v.detach().cpu() for k,v in model.state_dict().items()}, Path(directory)/'benchmark-model.pt')
    total = sum(ref['rows'] for ref in refs)
    return {'device': params['device'], 'repeat_index': params['repeat_index'],
            'seed': 42, 'batch_size': 1024, 'epochs': 5, 'warmup_steps': 20,
            'train_seconds': elapsed, 'samples_per_second': total/elapsed, 'samples_processed': total,
            'peak_memory_mib': peak, 'cpu_peak_rss_mib': cpu_peak,
            'cpu_memory_scope': 'training Python process RSS high-water mark through timed training',
            'gpu_memory_scope': 'torch max_memory_allocated after reset, timed training only',
            'end_to_end_seconds': time.perf_counter()-end_to_end_start,
            'timing_scope': 'CPU batches already loaded; includes transfers, forward, backward, optimizer, epoch loss reduction and first-batch audit; excludes warmup, sample loading/generation and validation',
            'sample_files': refs, 'initial_state_sha256': initial_hash,
            'initial_torch_rng_sha256': initial_random_state, 'reset_torch_rng_sha256': reset_random_state,
            'warmed_state_sha256': warmed_hash, 'reset_state_sha256': reset_hash,
            'final_state_sha256': state_hash(model),
            'optimizer_reset': True, 'warmup_device_audit': warmup_audit,
            'first_batch_device_audit': audit, 'history': epoch_rows, 'validation_metrics': metrics,
            'validation_cohorts': stats['cohorts'], 'truth_sha256': stats['truth_sha256'],
            'catalog_sha256': stats['catalog_sha256']}
