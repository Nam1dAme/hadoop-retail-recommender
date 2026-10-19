#!/usr/bin/env bash
# Ordinary WSL after host storage verification. uv is the sole environment manager.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$retail_project_root"
command -v uv >/dev/null || { echo 'uv is required.' >&2; exit 3; }
mkdir -p var/cache/uv var/artifacts
export UV_CACHE_DIR="$retail_project_root/var/cache/uv"
export UV_PROJECT_ENVIRONMENT="$retail_project_root/.venv"
uv run --no-project --python 3.12 python - <<'PY'
import platform, sys
if not (sys.version_info[:2] == (3, 12) and sys.platform == 'linux' and platform.machine() == 'x86_64' and platform.python_implementation() == 'CPython'):
    raise SystemExit('CPython 3.12 on Linux x86_64 is required.')
PY
uv lock
uv sync --locked
uv pip check --python "$UV_PROJECT_ENVIRONMENT/bin/python"
uv pip list --python "$UV_PROJECT_ENVIRONMENT/bin/python" --format json > var/artifacts/uv-packages.json
uv pip freeze --python "$UV_PROJECT_ENVIRONMENT/bin/python" > var/artifacts/uv-freeze.txt
uv run --locked --no-sync python - <<'PY'
from datetime import datetime, timezone
import hashlib, json, pathlib, platform, subprocess, sys
root = pathlib.Path.cwd()
record = {
    'observed_at': datetime.now(timezone.utc).isoformat(),
    'manager': subprocess.check_output(['uv', '--version'], text=True).strip(),
    'python': sys.version,
    'platform': platform.platform(),
    'machine': platform.machine(),
    'lock_sha256': hashlib.sha256((root / 'uv.lock').read_bytes()).hexdigest(),
    'pyproject_sha256': hashlib.sha256((root / 'pyproject.toml').read_bytes()).hexdigest(),
    'python_pin': (root / '.python-version').read_text().strip(),
    'code_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'code_dirty': bool(subprocess.check_output(['git', '--no-optional-locks', 'status', '--porcelain'], text=True).strip()),
}
(root / 'var/artifacts/uv-environment.json').write_text(json.dumps(record, indent=2) + '\n')
PY
