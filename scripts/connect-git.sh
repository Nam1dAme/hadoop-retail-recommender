#!/usr/bin/env bash
# Connect the existing metadata using Git's standard gitdir pointer file.
set -euo pipefail
retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
retail_git_dir="$retail_project_root/.git-local"
if [[ ! -f "$retail_git_dir/HEAD" ]]; then
    echo 'Existing .git-local repository not found.' >&2
    exit 3
fi
if [[ -e "$retail_project_root/.git" || -L "$retail_project_root/.git" ]]; then
    retail_existing_dir="$(git -C "$retail_project_root" rev-parse --absolute-git-dir 2>/dev/null || true)"
    if [[ "$retail_existing_dir" == "$retail_git_dir" ]]; then
        echo 'Standard Git entry already points to the existing repository.'
        exit 0
    fi
    echo '.git already exists and is not the expected pointer; leaving it untouched. Run in ordinary WSL if this is the protected mount.' >&2
    exit 3
fi
(
    set -o noclobber
    printf '%s\n' 'gitdir: .git-local' > "$retail_project_root/.git"
)
git -C "$retail_project_root" rev-parse --absolute-git-dir
git -C "$retail_project_root" status --short --branch
