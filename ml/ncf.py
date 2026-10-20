"""Frozen NeuMF architecture and genuine CUDA training; no device fallback."""
import os
os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')
import json
from pathlib import Path
import random
import time
import numpy as np
import torch
from torch import nn

from etl.ingest import write_json
from ml.sampling import sample_epoch, save_samples
from ml.evaluation import recommend, evaluate


def seed_everything(seed=42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True


class NeuMF(nn.Module):
    def __init__(self, users, items):
        super().__init__()
        self.gmf_user = nn.Embedding(users, 16)
        self.gmf_item = nn.Embedding(items, 16)
        self.mlp_user = nn.Embedding(users, 16)
        self.mlp_item = nn.Embedding(items, 16)
        self.mlp = nn.Sequential(nn.Linear(32, 64), nn.ReLU(), nn.Linear(64, 32), nn.ReLU(),
                                 nn.Linear(32, 16), nn.ReLU())
        self.output = nn.Linear(32, 1)
        for module in self.modules():
            if isinstance(module, nn.Embedding):
                nn.init.normal_(module.weight, mean=0., std=.01)
            elif isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                nn.init.zeros_(module.bias)

    def forward(self, users, items):
        gmf = self.gmf_user(users) * self.gmf_item(items)
        mlp = self.mlp(torch.cat((self.mlp_user(users), self.mlp_item(items)), dim=-1))
        return self.output(torch.cat((gmf, mlp), dim=-1)).squeeze(-1)


class NeuralScorer:
    positive_only = False

    def __init__(self, train, model, device):
        self.ui = {u: n for n, u in enumerate(train.users)}
        self.ii = {i: n for n, i in enumerate(train.items)}
        self.model = model
        self.device = torch.device(device)

    def score(self, users, items):
        self.model.eval()
        with torch.inference_mode():
            u = torch.tensor([self.ui[x] for x in users], device=self.device, dtype=torch.long)
            i = torch.tensor([self.ii[x] for x in items], device=self.device, dtype=torch.long)
            values = self.model(u.repeat_interleave(len(items)), i.repeat(len(users)))
            if self.device.type == 'cuda' and (not values.is_cuda or not next(self.model.parameters()).is_cuda):
                raise ValueError('NCF scoring tensors left CUDA')
            return values.reshape(len(users), len(items)).cpu().numpy()


def load(train, directory, *, device='cuda'):
    directory = Path(directory)
    seed_everything()
    metadata = json.loads((directory / 'config.json').read_text())
    if metadata['mapping_sha256'] != train.mapping_sha256:
        raise ValueError('NCF mapping differs from checkpoint configuration')
    model = NeuMF(len(train.users), len(train.items)).to(device)
    # Only project-generated, manifest-verified state dictionaries are accepted.
    model.load_state_dict(torch.load(directory / 'model.pt', map_location=device, weights_only=True))
    return NeuralScorer(train, model, device)


def fit(train, directory, params, truth, stats):
    if torch.__version__ != '2.6.0+cu124' or torch.version.cuda != '12.4':
        raise RuntimeError('Formal NCF requires the frozen PyTorch 2.6.0 cu124 runtime')
    if not torch.cuda.is_available():
        raise RuntimeError('Formal NCF requires a real CUDA device')
    seed_everything()
    directory = Path(directory)
    model = NeuMF(len(train.users), len(train.items)).to('cuda')
    assert all(p.is_cuda and p.dtype == torch.float32 for p in model.parameters())
    optimizer = torch.optim.Adam(model.parameters(), lr=params['learning_rate'], weight_decay=1e-5)
    criterion = nn.BCEWithLogitsLoss(reduction='mean')
    scorer = NeuralScorer(train, model, 'cuda')
    users = sorted(truth, key=lambda u: u.encode())
    history, best_state = [], None
    best_ndcg, best_epoch, stale = -1., 0, 0
    first_batch_audit = None
    (directory / 'samples').mkdir()
    with (directory / 'training_history.tsv').open('w') as output:
        for epoch in range(1, 21):
            started = time.perf_counter()
            samples, excluded = sample_epoch(train, epoch)
            save_samples(directory / 'samples' / f'epoch-{epoch:03d}.tsv', samples, epoch)
            model.train()
            torch.cuda.reset_peak_memory_stats()
            total_loss = 0.
            for offset in range(0, len(samples), 1024):
                batch = samples[offset:offset+1024]
                indices = torch.from_numpy(batch[:, :2].astype(np.int64)).to('cuda')
                labels = torch.from_numpy(batch[:, 2].astype(np.float32)).to('cuda')
                optimizer.zero_grad(set_to_none=True)
                logits = model(indices[:, 0], indices[:, 1])
                loss = criterion(logits, labels)
                if not logits.is_cuda or not loss.is_cuda or not torch.isfinite(loss):
                    raise ValueError('Invalid CUDA training loss/device')
                loss.backward()
                if first_batch_audit is None:
                    gradients = [p.grad for p in model.parameters() if p.grad is not None]
                    first_batch_audit = {'model_cuda': all(p.is_cuda for p in model.parameters()),
                        'input_cuda': indices.is_cuda and labels.is_cuda, 'output_cuda': logits.is_cuda,
                        'loss_cuda': loss.is_cuda, 'gradients_cuda': all(g.is_cuda for g in gradients),
                        'gradients_finite': all(bool(torch.isfinite(g).all()) for g in gradients),
                        'nonzero_gradient': any(bool((g != 0).any()) for g in gradients)}
                    if not all(first_batch_audit.values()):
                        raise ValueError('CUDA forward/backward audit failed')
                optimizer.step()
                total_loss += float(loss.detach()) * len(batch)
            torch.cuda.synchronize()
            loss_value = total_loss / len(samples)
            recommendations = recommend(train, users, scorer)
            metrics = evaluate(train, truth, stats, recommendations, 'VALIDATION')
            ndcg = metrics[0]['ndcg_at_k']
            torch.cuda.synchronize()
            elapsed = time.perf_counter() - started
            peak = torch.cuda.max_memory_allocated() / 1024**2
            row = {'epoch': epoch, 'train_loss': loss_value, 'validation_ndcg': ndcg,
                   'elapsed_seconds': elapsed, 'peak_gpu_mib': peak, 'sample_count': len(samples),
                   'excluded_no_negative_users': excluded}
            history.append(row)
            output.write(f'{epoch}\t{loss_value:.17g}\t{ndcg:.17g}\t{elapsed:.17g}\t{peak:.17g}\n')
            output.flush()
            write_json(directory / 'progress.json', {'epochs': history})
            if ndcg > best_ndcg + 1e-6:
                best_ndcg, best_epoch, stale = ndcg, epoch, 0
                best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            else:
                stale += 1
            if stale >= 3:
                break
    if best_state is None or not all(np.isfinite(r['train_loss']) for r in history) or history[-1]['train_loss'] >= history[0]['train_loss']:
        raise ValueError('NCF training did not produce a finite decreasing loss history')
    model.load_state_dict(best_state)
    before = scorer.score(train.users[:2], train.items[:5])
    torch.save(best_state, directory / 'model.pt')
    reloaded = load(train, directory, device='cuda')
    after = reloaded.score(train.users[:2], train.items[:5])
    if not np.allclose(before, after, rtol=0, atol=1e-6):
        raise ValueError('NCF checkpoint scores changed after reload')
    return reloaded, {'best_epoch': best_epoch, 'best_validation_ndcg': best_ndcg,
                      'epochs': history, 'device': str(next(model.parameters()).device),
                      'parameter_count': sum(p.numel() for p in model.parameters()),
                      'first_batch_cuda_audit': first_batch_audit,
                      'checkpoint_reload_max_error': float(np.max(np.abs(before-after)))}
