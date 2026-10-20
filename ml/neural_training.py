"""FP32 NeuMF steps shared by FULLFIT and the isolated CPU/CUDA comparison."""
import hashlib
import math
import numpy as np
import torch
from torch import nn

from ml.ncf import NeuMF, seed_everything


def initialize(train, learning_rate, device):
    if torch.__version__ != '2.6.0+cu124' or torch.version.cuda != '12.4':
        raise RuntimeError('NCF requires the frozen PyTorch 2.6.0 cu124 runtime')
    if device not in ('cpu', 'cuda') or (device == 'cuda' and not torch.cuda.is_available()):
        raise RuntimeError('Requested training device is unavailable')
    seed_everything(42)
    model = NeuMF(len(train.users), len(train.items)).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate, weight_decay=1e-5)
    return model, optimizer


def state_hash(model):
    digest = hashlib.sha256()
    for name, value in model.state_dict().items():
        digest.update(name.encode())
        digest.update(value.detach().cpu().contiguous().numpy().tobytes())
    return digest.hexdigest()


def batches(samples):
    if samples.ndim != 2 or samples.shape[1] != 3 or not len(samples):
        raise ValueError('Empty or invalid training samples')
    return (torch.from_numpy(samples[:, :2].astype(np.int64)),
            torch.from_numpy(samples[:, 2].astype(np.float32)))


def step(model, optimizer, indices, labels, device, *, audit=False):
    indices, labels = indices.to(device), labels.to(device)
    optimizer.zero_grad(set_to_none=True)
    logits = model(indices[:, 0], indices[:, 1])
    loss = nn.functional.binary_cross_entropy_with_logits(logits, labels, reduction='mean')
    loss.backward()
    evidence = None
    if audit:
        gradients = [p.grad for p in model.parameters()]
        evidence = {
            'model_device': str(next(model.parameters()).device),
            'input_device': str(indices.device), 'labels_device': str(labels.device),
            'output_device': str(logits.device), 'loss_device': str(loss.device),
            'gradients_on_device': all(g is not None and g.device == loss.device for g in gradients),
            'gradients_finite': all(g is not None and bool(torch.isfinite(g).all()) for g in gradients),
            'nonzero_gradient': any(g is not None and bool((g != 0).any()) for g in gradients),
            'fp32': all(p.dtype == torch.float32 for p in model.parameters()) and logits.dtype == torch.float32,
        }
        if (not all(evidence[k] for k in ('gradients_on_device','gradients_finite','nonzero_gradient','fp32'))
                or any(not evidence[k].startswith(device) for k in (
                    'model_device','input_device','labels_device','output_device','loss_device'))):
            raise ValueError('Actual forward/backward device audit failed')
    optimizer.step()
    return loss.detach(), evidence


def train_epoch(model, optimizer, tensors, device, *, audit=False):
    model.train()
    indices, labels = tensors
    total = torch.zeros((), device=device)
    first_audit = None
    for offset in range(0, len(labels), 1024):
        loss, evidence = step(model, optimizer, indices[offset:offset+1024], labels[offset:offset+1024],
                              device, audit=audit and offset == 0)
        total += loss * len(labels[offset:offset+1024])
        if evidence is not None:
            first_audit = evidence
    value = float(total) / len(labels)
    if not math.isfinite(value):
        raise ValueError('Nonfinite training loss')
    return value, first_audit
