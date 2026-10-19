#!/usr/bin/env python3
"""Read-only environment audit; missing evidence never counts as PASS."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from orchestration.settings import BASELINE, ConfigError, load_config


def command(args: list[str]) -> dict:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=15)
        return {"exit_code": result.returncode, "output": (result.stdout + result.stderr).strip()}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"exit_code": None, "output": type(exc).__name__}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="Explicit path to write JSON evidence")
    args = parser.parse_args()
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    checks = []

    def add(name, status, evidence):
        checks.append({"check": name, "status": status, "evidence": evidence})

    entries = re.findall(r"\[([^]]+\.md)\]\([^)]*\) \| `([a-f0-9]{64})`", (ROOT / "docs/BASELINE-MANIFEST.md").read_text())
    verified = {name: hashlib.sha256((ROOT / "docs" / name).read_bytes()).hexdigest() == digest for name, digest in entries}
    add("frozen_baseline", "PASS" if len(verified) == 18 and all(verified.values()) else "FAIL", verified)
    venv_file = Path(sys.prefix) / "pyvenv.cfg"
    isolated = venv_file.exists() and not bool(re.search(r"include-system-site-packages\s*=\s*true", venv_file.read_text()))
    add("python_venv", "PASS" if sys.version_info[:2] == (3, 12) and sys.prefix != sys.base_prefix and isolated else "FAIL", {"version": platform.python_version(), "executable": sys.executable, "isolated": isolated})
    runtime = config["runtime"]
    commands = {
        "java": ([str(Path(runtime["java_home"]) / "bin/java"), "-version"], "1.8.0_504"),
        "maven": (["mvn", "-version"], "Apache Maven 3.8.7"),
        "hadoop": ([str(Path(runtime["hadoop_home"]) / "bin/hadoop"), "version"], "Hadoop 3.3.5"),
        "cuda_toolkit": ([str(Path(runtime["cuda_home"]) / "bin/nvcc"), "--version"], "V12.4.99"),
        "mariadb_client": (["mariadb", "--version"], "10.11."),
    }
    for name, (argv, version) in commands.items():
        result = command(argv)
        add(name, "PASS" if result["exit_code"] == 0 and version in result["output"] else "FAIL", result)
    gpu = command(["/usr/lib/wsl/lib/nvidia-smi"])
    add("gpu_visibility", "PASS" if gpu["exit_code"] == 0 and Path("/dev/dxg").exists() else "NOT_VERIFIED", {"dev_dxg": Path("/dev/dxg").exists(), **gpu})
    baseline_packages = {
        "torch": ("2.6.0+cu124", True), "pyspark": ("3.5.7", True),
        "numpy": ("1.26.4", True), "pandas": ("2.2.3", True), "openpyxl": ("3.1.5", True),
        "fastapi": ("0.115.", False), "pydantic": ("2.", False), "uvicorn": ("0.34.", False),
        "streamlit": ("1.40.", False), "PyMySQL": ("1.1.", False), "plotly": ("5.24.", False),
        "PyYAML": ("6.", False), "pytest": ("8.", False), "httpx": ("0.28.", False),
    }
    for name, (expected, exact) in baseline_packages.items():
        try:
            actual = importlib.metadata.version(name)
            matches = actual == expected if exact else actual.startswith(expected)
        except importlib.metadata.PackageNotFoundError:
            actual, matches = None, False
        add(f"package:{name}", "PASS" if matches else "FAIL", {"installed": actual, "expected": expected})
    conf = Path(runtime["hadoop_conf_dir"])
    expected_properties = {
        "core-site.xml": {"fs.defaultFS": config["hdfs"]["uri"]},
        "hdfs-site.xml": {"dfs.replication": "1"},
        "mapred-site.xml": {"mapreduce.framework.name": "yarn"},
        "yarn-site.xml": {"yarn.nodemanager.aux-services": "mapreduce_shuffle"},
    }
    for filename, required in expected_properties.items():
        try:
            tree = ET.parse(conf / filename)
            properties = {node.findtext("name"): node.findtext("value") for node in tree.findall("property")}
            observed = {name: properties.get(name) for name in required}
            add(filename, "PASS" if observed == required else "FAIL", {"observed": observed, "required": required})
        except (OSError, ET.ParseError) as exc:
            add(filename, "FAIL", type(exc).__name__)
    try:
        env_text = (conf / "hadoop-env.sh").read_text()
    except OSError:
        env_text = ""
    add("hadoop_env_java_home", "PASS" if re.search(r"^\s*export\s+JAVA_HOME=" + re.escape(runtime["java_home"]) + r"\s*$", env_text, re.M) else "FAIL", "hadoop-env.sh must explicitly export the configured JAVA_HOME")
    for label, host, port in (("hdfs_rpc", "127.0.0.1", 9000), ("namenode_ui", "127.0.0.1", 9870), ("yarn_ui", "127.0.0.1", 8088), ("database", config["database"]["host"], config["database"]["port"])):
        try:
            with socket.create_connection((host, port), timeout=2):
                add(f"tcp:{label}", "PASS", {"host": host, "port": port, "scope": "TCP only, not service readiness"})
        except OSError as exc:
            add(f"tcp:{label}", "NOT_VERIFIED", {"error": type(exc).__name__, "errno": exc.errno})
    for name, argv in {"hdfs_datanodes": [str(Path(runtime["hadoop_home"]) / "bin/hdfs"), "dfsadmin", "-report"], "yarn_nodes": [str(Path(runtime["hadoop_home"]) / "bin/yarn"), "node", "-list"]}.items():
        result = command(argv)
        # Even exit 0 needs human confirmation of live DataNode/NodeManager counts.
        add(name, "NOT_VERIFIED", result)
    lock = ROOT / "config/requirements-linux-py312.lock"
    add("dependency_lock", "PASS" if lock.exists() and "--hash=sha256:" in lock.read_text() else "FAIL", {"exists": lock.exists(), "path": str(lock.relative_to(ROOT))})
    result = {
        "baseline_version": BASELINE,
        "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "platform": platform.platform(),
        "config_sha256": hashlib.sha256(args.config.read_bytes()).hexdigest(),
        "effective_config": config,
        "effective_config_sha256": hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "checks": checks,
        "manual_gates": ["VHDX host volume and >=15 GiB free", "database server version/roles/migrations (W02)", "real CUDA matmul and backward", "PySpark ALS fit/save/load", "HDFS live DataNode and YARN NodeManager/WordCount"],
        "g_env_complete": False,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    for item in checks:
        print(f"{item['status']:12} {item['check']}")
    return 3 if any(item["status"] != "PASS" for item in checks) or result["manual_gates"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
