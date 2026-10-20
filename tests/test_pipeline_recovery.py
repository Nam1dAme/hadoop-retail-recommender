"""Fault injection uses isolated real MariaDB; YARN final acceptance is separate."""
from datetime import datetime
import os
import pytest
from etl.storage import FileStorage
from orchestration.control import ControlStore
from orchestration.database import LockBusy
from orchestration.pipeline import recover_task,task_row
from orchestration.runtime import process_start
from test_database import batch,H,C


@pytest.fixture
def abandoned(db,tmp_path):
    connection=db[0]['writer'];control=ControlStore(connection)
    bid=batch(control);control.transition_batch(bid,'PROCESSING')
    run=control.create_task(bid,'J01',H,C)
    control.start_task(run,pid=999999999,process_start_utc=datetime(2026,1,1))
    return connection,control,run,FileStorage(tmp_path)


class ObservedYarn:
    def __init__(self,row,state='FINISHED',final='SUCCEEDED'):
        self.observed={'id':'application_1234_0001','name':'retail-J01-'+row['run_id'],'state':state,'finalStatus':final}
    def find(self,name):
        assert name==self.observed['name'];return self.observed
    def app(self,identity):
        assert identity==self.observed['id'];return self.observed


def test_active_application_never_retried(abandoned):
    conn,control,rid,storage=abandoned;row=task_row(conn,rid)
    with pytest.raises(LockBusy):recover_task(control,ObservedYarn(row,'RUNNING','UNDEFINED'),row,storage,lambda:None)
    assert task_row(conn,rid)['status']=='RUNNING'


def test_submission_gap_terminal_without_bundle_requires_new_attempt(abandoned):
    conn,control,rid,storage=abandoned;row=task_row(conn,rid)
    recover_task(control,ObservedYarn(row),row,storage,lambda:None)
    previous=task_row(conn,rid)
    assert previous['application_id']=='application_1234_0001' and previous['status']=='FAILED'
    new=control.create_task(row['batch_id'],'J01',H,C)
    assert task_row(conn,new)['parent_run_id']==rid and task_row(conn,new)['attempt']==2


def test_observation_failure_is_not_failure(abandoned):
    conn,control,rid,storage=abandoned;row=task_row(conn,rid)
    class Unavailable:
        def find(self,name):raise OSError('test-only RM unavailable')
    with pytest.raises(OSError):recover_task(control,Unavailable(),row,storage,lambda:None)
    assert task_row(conn,rid)['status']=='RUNNING'


def test_live_owner_blocks_even_terminal_application(abandoned):
    conn,control,rid,storage=abandoned
    with control.transaction() as cursor:
        cursor.execute('UPDATE task_run SET pid=%s,process_start_utc=%s WHERE run_id=%s',(os.getpid(),process_start(os.getpid()),rid))
    row=task_row(conn,rid)
    with pytest.raises(LockBusy):recover_task(control,ObservedYarn(row),row,storage,lambda:None)
    assert task_row(conn,rid)['status']=='RUNNING'
