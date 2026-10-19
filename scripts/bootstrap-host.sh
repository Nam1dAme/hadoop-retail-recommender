#!/usr/bin/env bash
# Ordinary WSL only. Checks actual VHDX host storage before downloading packages.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$retail_project_root"
printf '[1/5] Checking uv, Python 3.12 and network socket access\n'
command -v uv >/dev/null || { echo 'Install uv before running this script.' >&2; exit 3; }
export UV_CACHE_DIR="$retail_project_root/var/cache/uv"
export UV_PROJECT_ENVIRONMENT="$retail_project_root/.venv"
if [[ ! -f .venv/bin/activate ]]; then
    uv venv --python 3.12 --no-python-downloads
fi
source scripts/dev-env.sh
uv run --no-project --python 3.12 python - <<'PY'
import socket, sys
try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
        pass
except OSError as exc:
    print('Network socket access is unavailable. Run this command in ordinary WSL, not the restricted agent session.', file=sys.stderr)
    raise SystemExit(3) from exc
PY
retail_run_dir="$retail_project_root/var/artifacts/host-bootstrap-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$retail_run_dir"
exec > >(tee "$retail_run_dir/bootstrap.log") 2>&1
printf 'Evidence directory: %s\n' "$retail_run_dir"
printf '[2/5] Checking the VHDX host volume and 15 GiB storage gate\n'
retail_powershell=/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe
if [[ ! -x "$retail_powershell" ]]; then
    echo 'Windows PowerShell unavailable; host storage gate cannot be verified.' >&2
    exit 3
fi
"$retail_powershell" -NoLogo -NoProfile -NonInteractive -Command - \
    < <(
        cat scripts/check-host-storage.ps1
        printf '\n'
    ) > "$retail_run_dir/host-storage.json"
uv run --no-project --python 3.12 python - "$retail_run_dir/host-storage.json" <<'PY'
import json, os, sys
with open(sys.argv[1], encoding='utf-8-sig') as stream:
    rows = json.load(stream)
requested = os.environ.get('RETAIL_WSL_DISTRIBUTION') or os.environ.get('WSL_DISTRO_NAME')
if requested:
    matches = [row for row in rows if row.get('distribution') == requested]
else:
    matches = [row for row in rows if row.get('wsl_version') == 2]
if len(matches) != 1:
    raise SystemExit('Could not uniquely identify the current WSL2 distribution. Rerun with RETAIL_WSL_DISTRIBUTION set to its exact registered name.')
row = matches[0]
print(json.dumps(row, ensure_ascii=False, indent=2))
if not row.get('gate_passed') or row.get('available_free_bytes', 0) < 15 * 1024**3:
    raise SystemExit('Host storage gate failed: require a confirmed WSL2 VHDX and at least 15 GiB free on its host volume. No package download started.')
PY
printf '[3/5] Resolving uv.lock and syncing the project environment\n'
./scripts/lock-dependencies.sh
cp uv.lock pyproject.toml var/artifacts/uv-environment.json var/artifacts/uv-packages.json "$retail_run_dir/"
printf '[4/5] Running CUDA, ALS and Maven checks sequentially\n'
uv run --locked --no-sync python scripts/smoke-cuda.py > "$retail_run_dir/cuda-smoke.json"
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
    uv run --locked --no-sync python scripts/smoke-als.py > "$retail_run_dir/als-smoke.json"
mvn -Dmaven.repo.local="$retail_project_root/var/cache/maven" -f mapreduce/pom.xml package
printf '[5/5] Collecting environment evidence and remaining gates\n'
uv run --locked --no-sync python scripts/check-environment.py --config config/development.toml \
    --output "$retail_run_dir/environment.json" || {
        retail_check_exit=$?
        if [[ "$retail_check_exit" -ne 3 ]]; then
            exit "$retail_check_exit"
        fi
        echo 'Environment inventory collected; manual/service gates remain. See environment.json.'
    }
uv run --locked --no-sync python - "$retail_run_dir" <<'PY'
from datetime import datetime, timezone
import json, pathlib, sys
directory = pathlib.Path(sys.argv[1])
(directory / 'completed.json').write_text(json.dumps({
    'finished_at': datetime.now(timezone.utc).isoformat(),
    'dependency_install_cuda_als_maven': 'PASS',
    'g_env_complete': False,
    'note': 'Database privileges/migrations, HDFS/YARN live nodes/WordCount and manual gates need separate acceptance.'
}, indent=2) + '\n')
PY
printf 'Dependency installation, CUDA/ALS smoke and Maven package finished. Evidence: %s\n' "$retail_run_dir"
