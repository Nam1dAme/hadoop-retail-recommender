"""CPU unit tests of structure/checkpoints; formal CUDA evidence comes from real EVAL."""
import json
import numpy as np
import pytest
import torch
from ml.data import TrainingData
from ml.ncf import NeuMF, NeuralScorer, seed_everything, load


def test_neumf_structure_forward_backward_and_seed():
    seed_everything()
    model = NeuMF(3, 4)
    assert model.gmf_user.weight.shape == (3, 16)
    assert model.mlp_item.weight.shape == (4, 16)
    assert [(m.in_features,m.out_features) for m in model.mlp if isinstance(m,torch.nn.Linear)] == [(32,64),(64,32),(32,16)]
    assert model.output.in_features == 32 and model.output.out_features == 1
    state = {k:v.detach().clone() for k,v in model.state_dict().items()}
    users,items = torch.tensor([0,1,2]),torch.tensor([1,2,3])
    logits = model(users,items)
    assert logits.shape == (3,)
    loss = torch.nn.BCEWithLogitsLoss()(logits,torch.tensor([1.,0.,1.]))
    loss.backward()
    assert torch.isfinite(loss) and all(p.grad is not None and torch.isfinite(p.grad).all() for p in model.parameters())
    seed_everything()
    repeated = NeuMF(3,4)
    assert all(torch.equal(state[k],v) for k,v in repeated.state_dict().items())


def test_trusted_state_dict_reload_and_mapping_guard(tmp_path):
    train = TrainingData(['u','v'],['10001','10002'],
        {'u':{'10001':(1,'2011-08-01')},'v':{'10002':(1,'2011-08-01')}},
        [('10001',1),('10002',1)],'a'*64)
    seed_everything()
    model = NeuMF(2,2)
    torch.save(model.state_dict(),tmp_path/'model.pt')
    (tmp_path/'config.json').write_text(json.dumps({'mapping_sha256':train.mapping_sha256}))
    before = NeuralScorer(train,model,'cpu').score(train.users,train.items)
    loaded = load(train,tmp_path,device='cpu')
    assert np.array_equal(before,loaded.score(train.users,train.items))
    train.mapping_sha256 = 'b'*64
    with pytest.raises(ValueError,match='mapping'):
        load(train,tmp_path,device='cpu')
