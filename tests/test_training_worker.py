"""Actual subprocess/isolated DB checks of heartbeat ownership and model-worker handoff."""
import json
from pathlib import Path
import pytest

from ml.data import prepare, TrainingData
from orchestration.control import ControlStore
from orchestration.database import named_lock
from orchestration.runtime import Heartbeat
from orchestration.training import run_worker, grids
from test_database import batch, H, C


def test_all_frozen_validation_grids_present():
    candidates=grids()
    assert {k:len(v) for k,v in candidates.items()} == {'POP':1,'ITEMCF':1,'ALS':4,'NCF':2}
    assert {(p['rank'],p['reg_param']) for p in candidates['ALS']} == {(16,.01),(16,.1),(32,.01),(32,.1)}
    assert {p['learning_rate'] for p in candidates['NCF']} == {.001,.0005}


def test_actual_pop_worker_under_owner_heartbeat(db,tmp_path):
    data=tmp_path/'data'
    prepare(iter([
        ('1','u','2011-08-01T12:00:00','10001'),
        ('2','v','2011-08-01T12:00:00','10002'),
        ('3','u','2011-09-01T12:00:00','10002'),
        ('4','v','2011-11-01T12:00:00','10001'),
    ]),data)
    train=TrainingData.load(data/'eval')
    destination=tmp_path/'worker'
    destination.mkdir()
    (destination/'config.json').write_text(json.dumps({'mapping_sha256':train.mapping_sha256,'hyperparameters':{}}))
    connection=db[0]['writer']
    control=ControlStore(connection)
    bid=batch(control)
    control.transition_batch(bid,'PROCESSING')
    run=control.create_task(bid,'isolated_POP',H,C)
    control.start_task(run)
    from orchestration.pipeline import task_row
    from orchestration.settings import load_config
    with named_lock(connection):
        tick=Heartbeat(connection,control)
        tick.run_id=run
        result=run_worker(load_config('config/development.toml'),control,tick,task_row(connection,run),'POP',data,destination)
    assert result['status']=='PASS' and result['algorithm']=='POP'
    assert len(result['metrics'])==2 and result['metrics'][0]['recall_at_k']==1
    assert result['details']['reload_max_error']==0
    assert (destination/'train-driver.log').is_file()
    assert control.task_status(run)['heartbeat_at'] is not None
