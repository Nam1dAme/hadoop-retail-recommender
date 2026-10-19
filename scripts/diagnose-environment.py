#!/usr/bin/env python3
"""Read-only transport, GPU and Git diagnostics; never start or reconfigure services."""

from datetime import datetime, timezone
import argparse
import errno
import importlib.metadata
import json
from pathlib import Path
import socket
import subprocess
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def run(argv):
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=8)
        return {"exit_code": result.returncode, "output": (result.stdout + result.stderr).strip()[:6000]}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"exit_code": None, "error": type(exc).__name__, "errno": getattr(exc, "errno", None)}


def error_result(exc):
    number = getattr(exc, "errno", None)
    return {"status": "PERMISSION_DENIED" if number in (errno.EPERM, errno.EACCES) else "UNVERIFIED", "error": type(exc).__name__, "errno": number}


def socket_probes():
    result = {}
    for label, family in (("ipv4", socket.AF_INET), ("ipv6", socket.AF_INET6), ("unix", socket.AF_UNIX)):
        try:
            with socket.socket(family, socket.SOCK_STREAM):
                result[label] = {"status": "CREATED"}
        except OSError as exc:
            result[label] = error_result(exc)
    return result


def database_handshake():
    """Read only the server greeting; send no credentials or SQL."""
    path = "/run/mysqld/mysqld.sock"
    result = {"path": path, "exists": Path(path).exists(), "authenticated": False}
    if not result["exists"]:
        return {**result, "status": "SOCKET_NOT_FOUND"}
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(3)
            connection.connect(path)
            stream = connection.makefile("rb")
            with stream:
                header = stream.read(4)
                if len(header) != 4:
                    return {**result, "status": "INCOMPLETE_GREETING"}
                size = int.from_bytes(header[:3], "little")
                if not 1 <= size <= 4096:
                    return {**result, "status": "INVALID_GREETING_LENGTH"}
                payload = stream.read(size)
                if len(payload) != size or payload[0] != 10 or b"\0" not in payload[1:]:
                    return {**result, "status": "UNRECOGNIZED_GREETING"}
                version = payload[1:].split(b"\0", 1)[0].decode("ascii", "replace")
                return {**result, "status": "GREETING_RECEIVED", "server_version": version[:128]}
    except OSError as exc:
        return {**result, **error_result(exc)}


def package_sources(probes):
    sources = ("https://pypi.org/simple/pip-tools/", "https://download.pytorch.org/whl/cu124/", "https://repo.maven.apache.org/maven2/")
    result = {}
    if all(probes[label]["status"] == "PERMISSION_DENIED" for label in ("ipv4", "ipv6")):
        return {url: {"status": "SKIPPED_SOCKET_PERMISSION_DENIED"} for url in sources}
    for url in sources:
        try:
            request = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(request, timeout=3) as response:
                result[url] = {"status": "HTTP_REACHED", "http_status": response.status}
        except urllib.error.HTTPError as exc:
            result[url] = {"status": "HTTP_REACHED", "http_status": exc.code}
        except (urllib.error.URLError, OSError) as exc:
            result[url] = error_result(getattr(exc, "reason", exc))
    return result


def collect():
    probes = socket_probes()
    process = {}
    status = Path("/proc/self/status")
    if status.exists():
        for line in status.read_text().splitlines():
            key, _, value = line.partition(":")
            if key in {"NoNewPrivs", "Seccomp", "Seccomp_filters"}:
                process[key] = value.strip()
    mount = run(["findmnt", "--json", "--target", str(ROOT / ".git"), "--output", "TARGET,FSTYPE,OPTIONS"])
    if mount.get("exit_code") == 0:
        mount = json.loads(mount["output"])
    packages = {}
    for name in ("pip", "pip-tools", "setuptools", "torch", "pyspark"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    identity = run([str(ROOT / "scripts/git.sh"), "var", "GIT_AUTHOR_IDENT"])
    commit = run([str(ROOT / "scripts/git.sh"), "rev-parse", "--verify", "HEAD"])
    return {
        "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "project_root": str(ROOT),
        "python": {"executable": sys.executable, "version": sys.version.split()[0], "venv": sys.prefix != sys.base_prefix},
        "process_isolation": process,
        "socket_creation": probes,
        "package_sources": package_sources(probes),
        "database_unix_greeting": database_handshake(),
        "gpu": {"dev_dxg": Path("/dev/dxg").exists(), "nvidia_smi": run(["/usr/lib/wsl/lib/nvidia-smi"])},
        "git": {"author_configured": identity.get("exit_code") == 0, "head": commit.get("output") if commit.get("exit_code") == 0 else None, "standard_metadata_mount": mount},
        "packages": packages,
        "boundaries": [
            "Socket creation or connect permission errors occur before service authentication.",
            "Seccomp presence alone does not identify an individual filter rule.",
            "A database greeting proves transport/server version, not account privileges or migrations.",
            "HTTP reachability does not prove package resolution or installation.",
            "GPU visibility here does not establish Windows or ordinary WSL GPU availability.",
            "VHDX host volume and >=15 GiB free still require host verification.",
        ],
        "g_env_complete": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = collect()
    encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")
    # Exit 0 means the diagnostic completed; it never claims G-ENV acceptance.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
