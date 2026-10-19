#!/usr/bin/env bash
# Explicit fallback for sessions where the empty .git mount is read-only.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "$retail_project_root/.git/HEAD" || -f "$retail_project_root/.git" ]]; then
    exec git -C "$retail_project_root" "$@"
fi
if [[ ! -f "$retail_project_root/.git-local/HEAD" ]]; then
    echo 'No repository found in .git or .git-local.' >&2
    exit 3
fi
exec git --git-dir="$retail_project_root/.git-local" --work-tree="$retail_project_root" "$@"
