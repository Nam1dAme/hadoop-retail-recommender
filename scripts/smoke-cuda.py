#!/usr/bin/env python3
"""G-ENV CUDA smoke only; does not replace NCF training or benchmarking."""

import json
import sys


def main():
    try:
        import torch
    except ImportError:
        print("PyTorch is not installed in this interpreter.", file=sys.stderr)
        return 3
    if torch.__version__ != "2.6.0+cu124" or torch.version.cuda != "12.4" or not torch.cuda.is_available():
        print(json.dumps({"torch": torch.__version__, "cuda_runtime": torch.version.cuda, "available": torch.cuda.is_available()}))
        return 3
    torch.manual_seed(42)
    torch.cuda.manual_seed_all(42)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    x = torch.randn(32, 16, device="cuda", requires_grad=True)
    weight = torch.randn(16, 8, device="cuda", requires_grad=True)
    loss = (x @ weight).square().mean()
    loss.backward()
    torch.cuda.synchronize()
    assert x.is_cuda and weight.is_cuda and loss.is_cuda
    assert torch.isfinite(loss) and torch.isfinite(x.grad).all() and torch.isfinite(weight.grad).all()
    props = torch.cuda.get_device_properties(0)
    print(json.dumps({"status": "PASS", "torch": torch.__version__, "cuda_runtime": torch.version.cuda, "device": props.name, "memory_bytes": props.total_memory, "compute_capability": list(torch.cuda.get_device_capability(0)), "loss": loss.item(), "matmul_and_backward": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
