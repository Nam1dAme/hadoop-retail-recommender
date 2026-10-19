#!/usr/bin/env bash
# Ordinary WSL: recheck network/GPU, commit this cleanup, then push main.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$retail_project_root"
retail_remote_url="${1:?Usage: bash scripts/push-host.sh https://github.com/OWNER/REPOSITORY.git}"
case "$retail_remote_url" in
    https://github.com/* | git@github.com:*) ;;
    *) echo 'Use a GitHub HTTPS URL without embedded credentials, or a git@github.com SSH URL.' >&2; exit 2 ;;
esac
source scripts/dev-env.sh
retail_run_dir="$retail_project_root/var/artifacts/push-host-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$retail_run_dir"
exec > >(tee "$retail_run_dir/run.log") 2>&1
printf 'Evidence directory: %s\n' "$retail_run_dir"
[[ "$(git branch --show-current)" == main ]] || { echo 'Expected the main branch.' >&2; exit 3; }
git diff --cached --quiet || { echo 'Existing staged changes found; review them before publishing.' >&2; exit 3; }
uv run --locked --no-sync python scripts/diagnose-environment.py \
    --output "$retail_run_dir/diagnostics-host.json" > /dev/null
uv run --locked --no-sync python - "$retail_run_dir/diagnostics-host.json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
assert report['socket_creation']['ipv4']['status'] == 'CREATED', 'Use ordinary WSL with network access.'
assert all(row['status'] == 'HTTP_REACHED' and 200 <= row['http_status'] < 400 for row in report['package_sources'].values()), 'One or more package sources could not be reached; inspect diagnostics-host.json.'
assert report['gpu']['dev_dxg'] and report['gpu']['nvidia_smi']['exit_code'] == 0, 'GPU is unavailable; inspect diagnostics-host.json.'
print('Host network sources and NVIDIA GPU visibility passed.')
PY
uv run --locked --no-sync python scripts/smoke-cuda.py > "$retail_run_dir/cuda-smoke.json"
uv pip check --python "$VIRTUAL_ENV/bin/python"
uv sync --locked --offline --check
uv run --locked --no-sync python scripts/check-environment.py --config config/development.toml \
    --output "$retail_run_dir/environment.json" || {
        retail_check_exit=$?
        [[ "$retail_check_exit" -eq 3 ]] || exit "$retail_check_exit"
    }
uv run --locked --no-sync python - "$retail_run_dir/environment.json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
failures = [row['check'] for row in report['checks'] if row['status'] == 'FAIL']
assert not failures, f'Environment checks failed: {failures}'
PY
cp "$retail_run_dir/diagnostics-host.json" docs/execution/diagnostics-host-2026-10-19-cleanup.json
cp "$retail_run_dir/cuda-smoke.json" docs/execution/cuda-smoke-2026-10-19-cleanup.json
cp "$retail_run_dir/environment.json" docs/execution/environment-2026-10-19-cleanup.json
if retail_existing_remote="$(git remote get-url origin 2>/dev/null)"; then
    [[ "${retail_existing_remote%.git}" == "${retail_remote_url%.git}" ]] || {
        echo 'Existing origin points elsewhere; no remote configuration changed.' >&2
        exit 3
    }
else
    git remote add origin "$retail_remote_url"
fi
git ls-remote --heads --tags origin > "$retail_run_dir/remote-before.txt"
printf 'GitHub remote reachable. Preparing the cleanup and verification commit.\n'
git add -- README.md scripts/push-host.sh \
    docs/execution/2026-10-19-cleanup.md \
    docs/execution/2026-10-19-uv-migration.md \
    docs/execution/cleanup-2026-10-19.json \
    docs/execution/diagnostics-session-2026-10-19-cleanup.json \
    docs/execution/diagnostics-host-2026-10-19-cleanup.json \
    docs/execution/cuda-smoke-2026-10-19-cleanup.json \
    docs/execution/environment-2026-10-19-cleanup.json
git diff --cached --check
if ! git diff --cached --quiet; then
    git commit -m "Record cleanup and host verification before GitHub push"
fi
git diff --quiet && [[ -z "$(git ls-files --others --exclude-standard)" ]] || {
    echo 'Other uncommitted changes remain; review them before pushing.' >&2
    exit 3
}
git push --set-upstream origin main
git ls-remote origin refs/heads/main > "$retail_run_dir/remote-after.txt"
uv run --locked --no-sync python - "$retail_run_dir" "$retail_remote_url" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import json, subprocess, sys
run = Path(sys.argv[1])
commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
remote = (run / 'remote-after.txt').read_text().split()
assert len(remote) == 2 and remote == [commit, 'refs/heads/main'], remote
assert not subprocess.check_output(['git', 'status', '--porcelain'])
receipt = {'status': 'PASS', 'observed_at': datetime.now(timezone.utc).isoformat(),
           'remote_url': sys.argv[2], 'branch': 'main', 'local_commit': commit,
           'remote_commit': remote[0], 'worktree_clean': True}
(run / 'completed.json').write_text(json.dumps(receipt, indent=2)+'\n')
print(json.dumps(receipt, indent=2))
PY
printf 'Push verified. Receipt: %s/completed.json\n' "$retail_run_dir"
