#!/usr/bin/env python3
"""Serial, isolated J03 YARN scaling experiment: real / 5x / 10x, three runs each.

Copies retain their original keys: this measures aggregation throughput, not new
customers, new orders, recommendation quality, or all twelve pipeline stages.
"""
import argparse
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from uuid import uuid4

from etl.ingest import sha256
from etl.storage import HdfsStorage
from etl.tsv import parse_line
from orchestration.database import connect, named_lock
from orchestration.manifests import fingerprint
from orchestration.settings import load_config
from orchestration.yarn import Yarn, TERMINAL
from orchestration.runtime import process_start


ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str)+'\n')
    temporary.replace(path)


def storage_budget(source_bytes, available_bytes):
    # All inputs remain for reproducibility. Includes local + HDFS copies,
    # repeated outputs, downloads, map spills and intermediate shuffle headroom.
    estimated_peak = source_bytes * 16 * 12
    required = estimated_peak*2 + 5*1024**3
    return dict(estimated_temporary_peak_bytes=estimated_peak, required_free_bytes=required,
                available_free_bytes=available_bytes, passed=available_bytes >= required,
                formula='source_clean_bytes * (1+5+10) * 12 peak allowance; required = peak*2 + 5GiB')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--db-defaults-file', required=True, type=Path)
    p.add_argument('--batch-id', required=True)
    p.add_argument('--output-dir', required=True, type=Path)
    a = p.parse_args()
    config = load_config(a.config)
    directory = a.output_dir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    connection = connect(config, a.db_defaults_file)
    storage = HdfsStorage(config)
    yarn = Yarn(config)
    evidence = dict(schema_version='1.0', baseline='HL-DOC-1.0.0', test_id='T-18', status='RUNNING',
                    started_at=datetime.now(timezone.utc).isoformat(), batch_id=a.batch_id,
                    code_commit=subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip(),
                    source_sha256=sha256(Path(__file__)), environment_sha256=sha256(ROOT/'uv.lock'),
                    hadoop_configuration_sha256={p.name:sha256(p) for p in Path(config['runtime']['hadoop_conf_dir']).glob('*.xml')},
                    stage='J03', reducers=2, seed=42, results=[],
                    generator='Byte-for-byte repetition of verified J01 clean TSVs, preserving order and keys; no random sampling (seed metadata 42)',
                    cache_policy='OS/HDFS caches not flushed; first run may be colder, repeats may reuse cache. No cold-cache speedup claimed.',
                    scope='Sales aggregation only, same production JAR/config; synthetic never ingested, trained, evaluated, or published')
    jar = ROOT/'mapreduce/target/retail-mapreduce-0.1.0-SNAPSHOT.jar'
    evidence['jar_sha256'] = sha256(jar)
    last_tick = [0.]
    resource_samples = []
    evidence['resource_samples'] = resource_samples

    def tick():
        if time.monotonic()-last_tick[0] >= 5:
            connection.ping(reconnect=False)
            last_tick[0] = time.monotonic()
            memory = {line.split(':',1)[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if len(line.split())==3}
            resource_samples.append(dict(utc=datetime.now(timezone.utc).isoformat(),
                                         wsl_used_excluding_available_kib=memory['MemTotal']-memory['MemAvailable'],
                                         swap_used_kib=memory['SwapTotal']-memory['SwapFree']))

    try:
        with named_lock(connection):
            with connection.cursor() as q:
                q.execute("SELECT run_id FROM task_run WHERE status IN ('PENDING','RUNNING')")
                if q.fetchone():
                    raise RuntimeError('Unfinished project tasks require handle verification')
                q.execute("SELECT output_manifest_path FROM task_run WHERE batch_id=%s AND task_name='J01' AND status='SUCCEEDED' ORDER BY attempt DESC LIMIT 1", (a.batch_id,))
                row = q.fetchone()
                q.execute('SELECT source_sha256 FROM data_batch WHERE batch_id=%s AND status=%s', (a.batch_id, 'SUCCEEDED'))
                evidence['data_sha256'] = q.fetchone()['source_sha256']
            connection.rollback()
            apps = (yarn.request('/apps').get('apps') or {}).get('app', [])
            if any(app['state'] not in TERMINAL for app in apps):
                raise RuntimeError('A YARN application is still active; observe it before starting')
            manifest = json.loads(b''.join(storage.chunks(row['output_manifest_path'])))
            if manifest.get('synthetic') or manifest['status'] != 'SUCCEEDED':
                raise ValueError('Verified real clean source required')
            refs = manifest['datasets']['clean']['files']
            # Fingerprint remote sources before generating anything synthetic.
            for ref in refs:
                digest, size, rows = fingerprint(storage, ref['path'], heartbeat=tick)
                if digest != ref['sha256'] or rows != ref['rows']:
                    raise ValueError('Clean source differs from manifest')
                ref['bytes'] = size
            source_size = sum(r['bytes'] for r in refs)
            output = subprocess.check_output([
                '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe', '-NoProfile',
                '-ExecutionPolicy', 'Bypass', '-File', str(ROOT/'scripts/check-host-storage.ps1')], timeout=30)
            volumes = json.loads(output.decode('utf-8-sig'))
            distro = next(v for v in volumes if v['distribution']=='Ubuntu-24.04')
            budget = storage_budget(source_size, distro['available_free_bytes'])
            evidence['host_storage'] = distro
            evidence['disk_budget'] = budget
            save(directory/'mr-performance.json', evidence)
            if not distro['vhdx_exists'] or not distro['gate_passed'] or not budget['passed']:
                raise RuntimeError('Host VHDX disk budget failed; no synthetic data generated')
            source = directory/'clean.tsv'
            with source.open('wb') as target:
                for ref in refs:
                    for chunk in storage.chunks(ref['path']):
                        target.write(chunk)
                        tick()
            expected = defaultdict(lambda: [0,0,0,0])
            source_rows = 0
            with source.open() as stream:
                for line in stream:
                    f = parse_line(line, 15)
                    totals = expected[(f[8], f[12], f[3])]
                    if f[13] == 'SALE':
                        totals[0] += int(f[10]); totals[2] += int(f[6])
                    else:
                        totals[1] -= int(f[10]); totals[3] += int(f[6])
                    source_rows += 1
            if source_rows != sum(r['rows'] for r in refs):
                raise ValueError('Downloaded source row count differs')
            evidence['source_files'] = refs
            evidence['base_rows'] = source_rows
            evidence['source_clean_sha256'] = sha256(source)
            identity = 'w11-'+uuid4().hex
            for factor in (1,5,10):
                scope = 'synthetic' if factor > 1 else 'real'
                remote = config['hdfs']['root']+'/benchmarks/'+scope+'/'+identity+'/x'+str(factor)
                storage.mkdir(remote+'/input')
                local = directory/('x'+str(factor)+'.tsv')
                with local.open('wb') as target:
                    for _ in range(factor):
                        with source.open('rb') as stream:
                            shutil.copyfileobj(stream, target)
                        tick()
                storage.put(local, remote+'/input/part-00000')
                digest, size, rows = fingerprint(storage, remote+'/input/part-00000', heartbeat=tick)
                if digest != sha256(local) or size != local.stat().st_size or rows != source_rows*factor:
                    raise ValueError('Expanded input differs from local generator')
                input_record = dict(path=remote+'/input/part-00000', sha256=digest, bytes=size, rows=rows,
                                    synthetic=factor>1, factor=factor, seed=42, generator=evidence['generator'])
                save(directory/('x'+str(factor)+'-input.json'), input_record)
                storage.put(directory/('x'+str(factor)+'-input.json'), remote+'/input-manifest.json')
                for repeat in (1,2,3):
                    run_id = str(uuid4())
                    task = dict(run_id=run_id, task_name='w11J03')
                    run_dir = directory/('x'+str(factor)+'-repeat'+str(repeat))
                    run_dir.mkdir()
                    task.update(pid=os.getpid(), process_start_utc=process_start(os.getpid()), application_id=None,
                                status='SUBMITTING', hdfs_output=remote+'/repeat'+str(repeat))
                    save(run_dir/'handle.json', task)

                    class LocalControl:
                        def record_application(self, rid, aid):
                            if rid != run_id:
                                raise ValueError('Wrong run association')
                            task.update(application_id=aid, status='RUNNING')
                            save(run_dir/'handle.json', task)

                    command = [str(Path(config['runtime']['hadoop_home'])/'bin/hadoop'), 'jar', str(jar),
                               'cn.hadooplab.retail.RetailJobs', '-Dmapreduce.framework.name=yarn',
                               '-Dfs.defaultFS='+config['hdfs']['uri'], '--stage','J03','--task-name','w11J03',
                               '--run-id',run_id,'--output',task['hdfs_output'],'--reducers','2', '--input',remote+'/input']
                    begin = time.perf_counter()
                    app, counters = yarn.run(command, task, LocalControl(), tick, run_dir,
                                             storage.environment | {'HADOOP_MAPRED_HOME':config['runtime']['hadoop_home']})
                    wall_seconds = time.perf_counter()-begin
                    if storage.run('-test','-e',task['hdfs_output']+'/_SUCCESS',allow_absent=True)[0]:
                        raise ValueError('Missing MR success marker')
                    _, listing = storage.run('-ls',task['hdfs_output']+'/sales_cube')
                    seen = set()
                    output_files = []
                    for entry in listing.decode().splitlines():
                        if not entry.startswith('-'):
                            continue
                        path = entry.split(None,7)[7]
                        target = run_dir/Path(path).name
                        with target.open('wb') as stream:
                            for chunk in storage.chunks(path):
                                stream.write(chunk); tick()
                        output_files.append(dict(path=path, bytes=target.stat().st_size, sha256=sha256(target)))
                        with target.open() as stream:
                            for line in stream:
                                f = parse_line(line,7)
                                key = tuple(f[:3])
                                if key in seen or key not in expected or list(map(int,f[3:])) != [n*factor for n in expected[key]]:
                                    raise ValueError('Independent integer aggregation check failed')
                                seen.add(key)
                    if len(seen) != len(expected) or app['job_counters']['MAP_INPUT_RECORDS'] != rows:
                        raise ValueError('MR coverage/input counters differ')
                    result = dict(factor=factor, repeat=repeat, synthetic=factor>1, run_id=run_id,
                                  status='PASS', input=input_record, wall_seconds=wall_seconds,
                                  application={k:v for k,v in app.items() if k in ('id','name','state','finalStatus','startedTime','finishedTime','elapsedTime','memorySeconds','vcoreSeconds','job_counters')},
                                  output_rows=len(seen), exact_integer_values_verified=True, output_files=output_files,
                                  reducers=2, input_splits=app['job_counters']['INPUT_SPLITS'],
                                  samples_per_second=rows/wall_seconds)
                    task['status'] = 'SUCCEEDED'
                    save(run_dir/'handle.json',task)
                    save(run_dir/'result.json',result)
                    evidence['results'].append(result)
                    save(directory/'mr-performance.json',evidence)
                    print(json.dumps(dict(factor=factor,repeat=repeat,seconds=wall_seconds,application=app['id'],status='PASS')),flush=True)
            if sha256(jar) != evidence['jar_sha256']:
                raise ValueError('JAR changed during the experiment')
            evidence.update(status='PASS',finished_at=datetime.now(timezone.utc).isoformat())
            save(directory/'mr-performance.json',evidence)
    except BaseException as exc:
        evidence.update(status='INCOMPLETE', error=type(exc).__name__)
        save(directory/'mr-performance.json',evidence)
        raise
    finally:
        connection.close()


if __name__ == '__main__':
    main()
