#!/usr/bin/env bash
# Source explicitly: source scripts/dev-env.sh
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo 'Use: source scripts/dev-env.sh' >&2
    exit 2
fi
_retail_project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -f "$_retail_project_root/.venv/bin/activate" ]]; then
    echo 'Missing .venv; run python3.12 -m venv .venv in the project root.' >&2
    unset _retail_project_root
    return 3
fi
if ! mkdir -p "$_retail_project_root/var/cache/pip" "$_retail_project_root/var/artifacts" \
    "$_retail_project_root/var/tmp" "$_retail_project_root/var/logs"; then
    echo 'Cannot initialize project runtime directories.' >&2
    unset _retail_project_root
    return 3
fi
source "$_retail_project_root/.venv/bin/activate"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$_retail_project_root/config/hadoop}"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export CUDA_HOME="${CUDA_HOME:-/usr/local/cuda}"
export CUDA_PATH="$CUDA_HOME"
export RETAIL_WORK_DIR="${RETAIL_WORK_DIR:-$_retail_project_root/var}"
export PIP_CACHE_DIR="$_retail_project_root/var/cache/pip"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$CUDA_HOME/bin:/usr/lib/wsl/lib:$PATH"
unset _retail_project_root
