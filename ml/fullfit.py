"""Full-history CUDA training with a validation-selected fixed epoch count."""
from pathlib import Path
import time
import numpy as np
import torch

from etl.ingest import write_json
from ml.ncf import NeuralScorer, load
from ml.neural_training import initialize, batches, train_epoch, state_hash
from ml.sampling import sample_epoch, save_samples


def fit(train, directory, params):
    epochs = params['epochs']
    if type(epochs) is not int or not 1 <= epochs <= 20:
        raise ValueError('FULLFIT requires the selected EVAL epoch count')
    directory = Path(directory)
    model, optimizer = initialize(train, params['learning_rate'], 'cuda')
    initial_hash = state_hash(model)
    history, audit = [], None
    (directory / 'samples').mkdir()
    with (directory / 'training_history.tsv').open('w') as output:
        for epoch in range(1, epochs + 1):
            started = time.perf_counter()
            samples, excluded = sample_epoch(train, epoch)
            ref = save_samples(directory / 'samples' / f'epoch-{epoch:03d}.tsv', samples, epoch)
            tensors = batches(samples)
            torch.cuda.synchronize()
            torch.cuda.reset_peak_memory_stats()
            loss, evidence = train_epoch(model, optimizer, tensors, 'cuda', audit=epoch == 1)
            torch.cuda.synchronize()
            audit = evidence or audit
            row = {'epoch': epoch, 'train_loss': loss, 'validation_ndcg': None,
                   'elapsed_seconds': time.perf_counter()-started,
                   'peak_gpu_mib': torch.cuda.max_memory_allocated()/1024**2,
                   'sample_count': len(samples), 'sample_sha256': ref['sha256'],
                   'excluded_no_negative_users': excluded}
            history.append(row)
            output.write(f"{epoch}\t{loss:.17g}\t\\N\t{row['elapsed_seconds']:.17g}\t{row['peak_gpu_mib']:.17g}\n")
            output.flush()
            write_json(directory / 'progress.json', {'epochs': history})
    if history[-1]['train_loss'] >= history[0]['train_loss']:
        raise ValueError('FULLFIT loss did not decrease overall')
    scorer = NeuralScorer(train, model, 'cuda')
    before = scorer.score(train.users[:2], train.items[:5])
    torch.save({k:v.detach().cpu() for k,v in model.state_dict().items()}, directory / 'model.pt')
    reloaded = load(train, directory)
    after = reloaded.score(train.users[:2], train.items[:5])
    if not np.allclose(before, after, rtol=0, atol=1e-6):
        raise ValueError('FULLFIT checkpoint reload changed scores')
    return reloaded, {'epochs': history, 'fixed_epochs': epochs, 'early_stopping': False,
                      'initial_state_sha256': initial_hash, 'first_batch_device_audit': audit,
                      'parameter_count': sum(p.numel() for p in model.parameters()),
                      'checkpoint_reload_max_error': float(np.max(np.abs(before-after)))}
