#!/usr/bin/env bash
# Ordinary WSL: start a stopped local cluster, preserving existing HDFS data.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$retail_project_root"
source scripts/dev-env.sh
export HADOOP_CONF_DIR="$retail_project_root/config/hadoop"
export HADOOP_LOG_DIR="$retail_project_root/var/logs/hadoop"
export HADOOP_PID_DIR="$retail_project_root/var/tmp/hadoop-pids"
retail_run_dir="$retail_project_root/var/artifacts/hadoop-host-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$retail_run_dir" "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
exec > >(tee "$retail_run_dir/run.log") 2>&1
printf 'Evidence directory: %s\n' "$retail_run_dir"
jps -l > "$retail_run_dir/jps-before.txt"
uv run --locked --no-sync python - "$retail_run_dir" <<'PY'
from pathlib import Path
import getpass, re, socket, sys, tarfile
import xml.etree.ElementTree as ET

run = Path(sys.argv[1])
assert getpass.getuser() == 'hadoop', 'Use the existing hadoop account.'
with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
    pass
processes = (run / 'jps-before.txt').read_text()
assert not re.search(r'org\.apache\.hadoop\..*\.(NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager)(?:\s|$)', processes), 'Hadoop daemons already exist; inspect them instead of starting duplicates.'
for port in (9000, 9870, 8088):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=1):
            raise SystemExit(f'Port {port} is already in use; inspect it before starting.')
    except ConnectionRefusedError:
        pass
tree = ET.parse('config/hadoop/hdfs-site.xml')
properties = {p.findtext('name'): p.findtext('value') for p in tree.findall('property')}
assert properties['dfs.namenode.name.dir'] == 'file:/usr/local/hadoop/tmp/dfs/name'
assert properties['dfs.datanode.data.dir'] == 'file:/usr/local/hadoop/tmp/dfs/data'
base = Path('/usr/local/hadoop/tmp/dfs')
versions = [(base / part / 'current/VERSION').read_text() for part in ('name', 'data')]
cluster_ids = [re.search(r'^clusterID=(.+)$', text, re.M).group(1) for text in versions]
assert cluster_ids[0] == cluster_ids[1], 'NameNode/DataNode cluster IDs differ.'
with tarfile.open(run / 'hdfs-metadata-before.tar.gz', 'w:gz') as archive:
    archive.add(base / 'name', arcname='name')
    archive.add(base / 'data/current/VERSION', arcname='data-VERSION')
    if (base / 'namesecondary').exists():
        archive.add(base / 'namesecondary', arcname='namesecondary')
print('Stopped-cluster checks and metadata backup passed; existing data retained.')
PY
for retail_daemon in namenode datanode secondarynamenode; do
    hdfs --daemon start "$retail_daemon"
done
for retail_daemon in resourcemanager nodemanager; do
    yarn --daemon start "$retail_daemon"
done
uv run --locked --no-sync python - "$retail_run_dir" <<'PY'
from pathlib import Path
import re, subprocess, sys, time

run = Path(sys.argv[1])
deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    results = []
    for filename, command in [('hdfs-report.txt', ['hdfs', 'dfsadmin', '-report']), ('yarn-nodes.txt', ['yarn', 'node', '-list'])]:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=8)
            output = result.stdout + result.stderr
            passed = result.returncode == 0
        except subprocess.TimeoutExpired:
            output, passed = 'Timed out waiting for the service.\n', False
        (run / filename).write_text(output)
        results.append((passed, output))
    if (results[0][0] and re.search(r'Live datanodes \([1-9]\d*\)', results[0][1])
            and results[1][0] and re.search(r'Total Nodes:\s*[1-9]\d*', results[1][1])
            and 'RUNNING' in results[1][1]):
        print('Live DataNode and RUNNING NodeManager confirmed.')
        break
    time.sleep(2)
else:
    raise SystemExit(f'Services not ready; inspect {run} and var/logs/hadoop. Do not repeat startup while daemons are running.')
PY
timeout 90s hdfs dfsadmin -safemode wait > "$retail_run_dir/safemode.txt" 2>&1
retail_hdfs_smoke="/user/hadoop/environment-smoke-$(basename -- "$retail_run_dir")"
printf 'hadoop uv\nhadoop spark\n' > "$retail_run_dir/wordcount-input.txt"
hdfs dfs -mkdir -p "$retail_hdfs_smoke/input"
hdfs dfs -put "$retail_run_dir/wordcount-input.txt" "$retail_hdfs_smoke/input/fixture.txt"
timeout 180s hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar" \
    wordcount -Dmapreduce.job.reduces=1 "$retail_hdfs_smoke/input" "$retail_hdfs_smoke/output" \
    2>&1 | tee "$retail_run_dir/wordcount.log"
hdfs dfs -test -e "$retail_hdfs_smoke/output/_SUCCESS"
hdfs dfs -cat "$retail_hdfs_smoke/output/part-r-*" > "$retail_run_dir/wordcount-output.tsv"
uv run --locked --no-sync python - "$retail_run_dir" "$retail_hdfs_smoke" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json, re, subprocess, sys

run = Path(sys.argv[1])
counts = dict(line.split('\t') for line in (run / 'wordcount-output.tsv').read_text().splitlines())
assert counts == {'hadoop': '2', 'spark': '1', 'uv': '1'}, counts
applications = sorted(set(re.findall(r'application_\d+_\d+', (run / 'wordcount.log').read_text())))
assert len(applications) == 1, applications
status = subprocess.run(['yarn', 'application', '-status', applications[0]], capture_output=True, text=True, timeout=15)
output = status.stdout + status.stderr
(run / 'application-status.txt').write_text(output)
assert status.returncode == 0 and re.search(r'Final-State\s*:\s*SUCCEEDED', output), output
record = {'status': 'PASS', 'observed_at': datetime.now(timezone.utc).isoformat(), 'fixture': True,
          'hdfs_root': sys.argv[2], 'application_id': applications[0], 'final_state': 'SUCCEEDED',
          'word_counts': counts, 'scope': 'Environment WordCount only; business MapReduce unimplemented.'}
(run / 'completed.json').write_text(json.dumps(record, indent=2)+'\n')
print(json.dumps(record, indent=2))
PY
printf 'Hadoop environment smoke completed. Services remain running. Evidence: %s\n' "$retail_run_dir"
