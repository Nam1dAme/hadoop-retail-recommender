"""A long SQL statement must keep checking its owner's lock before returning."""
import pytest
from orchestration.database import LockBusy
from orchestration.pipeline_audit import audit_database

QUERY='WITH RECURSIVE numbers(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM numbers WHERE x<200000) SELECT SUM(x) FROM numbers'


def test_heartbeat_runs_inside_sql_statement():
    callbacks=[]
    with audit_database(':memory:',lambda:callbacks.append(True)) as connection:
        assert connection.execute(QUERY).fetchone()[0]==20000100000
        assert len(callbacks)>10


def test_lost_lock_interrupts_sql_and_preserves_original_exception():
    checks=[]
    def heartbeat():
        checks.append(True)
        if len(checks)==3:raise LockBusy('isolated lock loss during SQL')
    with pytest.raises(LockBusy,match='isolated lock loss'):
        with audit_database(':memory:',heartbeat) as connection:
            connection.execute(QUERY).fetchone()
    assert len(checks)==3
