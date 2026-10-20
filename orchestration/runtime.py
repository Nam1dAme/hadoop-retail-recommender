"""Linux process identity and lock-owning ten-second task heartbeat."""
from datetime import datetime,timezone
import os
from pathlib import Path
import time
from orchestration.database import LockBusy


def process_start(pid):
    try:
        # comm may contain spaces or parentheses. The remaining fields start at state (field 3).
        fields=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
        ticks=int(fields[19])
        boot=int(next(line.split()[1] for line in Path('/proc/stat').read_text().splitlines() if line.startswith('btime ')))
        return datetime.fromtimestamp(boot+ticks/os.sysconf('SC_CLK_TCK'),timezone.utc).replace(tzinfo=None)
    except (OSError,StopIteration,ValueError,IndexError):
        return None


def process_alive(row):
    if row['pid'] is None or row['process_start_utc'] is None:return None
    started=process_start(row['pid'])
    return started is not None and abs((started-row['process_start_utc']).total_seconds())<0.001


class Heartbeat:
    def __init__(self,connection,control):
        self.connection=connection;self.control=control;self.run_id=None
        self.last_check=self.last_heartbeat=0

    def __call__(self,force=False):
        now=time.monotonic()
        if not force and now-self.last_check<2:return
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT IS_USED_LOCK('retail-heavy-job')=CONNECTION_ID() AS owned")
            if cursor.fetchone()['owned']!=1:raise LockBusy('Heavy job lock ownership was lost')
        self.last_check=now
        if self.run_id and (force or now-self.last_heartbeat>=10):
            self.control.heartbeat(self.run_id)
            self.last_heartbeat=now
