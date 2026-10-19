#!/usr/bin/env bash
# Run in ordinary WSL after verifying the VHDX host volume has >=15 GiB free.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$retail_project_root"
mkdir -p var/cache/pip var/artifacts
if ! .venv/bin/python -c 'import sys, platform; assert sys.version_info[:2] == (3, 12) and sys.platform == "linux" and platform.machine() == "x86_64" and platform.python_implementation() == "CPython"'; then
    echo 'CPython 3.12 on Linux x86_64 is required.' >&2
    exit 3
fi
export PIP_CACHE_DIR="$retail_project_root/var/cache/pip"
export PIP_TOOLS_CACHE_DIR="$PIP_CACHE_DIR"
export PIP_CONFIG_FILE=/dev/null
unset PIP_NO_INDEX PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_FIND_LINKS PIP_TRUSTED_HOST
.venv/bin/python -m pip install --index-url https://pypi.org/simple 'pip-tools==7.4.1' 'setuptools>=75,<76'
.venv/bin/pip-compile --verbose --resolver=backtracking --generate-hashes --allow-unsafe --strip-extras \
    --index-url https://pypi.org/simple \
    --output-file config/requirements-linux-py312.lock config/requirements-linux-py312.in
.venv/bin/python -m pip install --require-hashes -r config/requirements-linux-py312.lock
.venv/bin/python -m pip check
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m pip inspect > var/artifacts/pip-inspect.json
.venv/bin/python -m pip freeze --all > var/artifacts/pip-freeze.txt
