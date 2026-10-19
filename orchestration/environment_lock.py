"""Inspect native uv lock evidence without treating a file as installed packages."""

from pathlib import Path
import hashlib
import re
import tomllib


def inspect_uv_lock(path: Path) -> tuple[bool, dict]:
    evidence = {"exists": path.exists(), "path": path.name}
    if not path.exists():
        return False, evidence
    try:
        data = tomllib.loads(path.read_text())
        packages = data.get("package", [])
        if not isinstance(packages, list) or not packages or not all(isinstance(p, dict) for p in packages):
            raise ValueError("Invalid package list")
        torch = [p for p in packages if p.get("name") == "torch"]
        source_ok = len(torch) == 1 and torch[0].get("version") == "2.6.0+cu124" and torch[0].get("source", {}).get("registry", "").rstrip("/") == "https://download.pytorch.org/whl/cu124"
        artifacts = []
        all_registry_packages_have_artifacts = True
        for package in packages:
            files = [*package.get("wheels", []), *([package["sdist"]] if "sdist" in package else [])]
            if "registry" in package.get("source", {}) and not files:
                all_registry_packages_have_artifacts = False
            artifacts.extend(files)
        hashes = bool(artifacts) and all_registry_packages_have_artifacts and all(re.fullmatch(r"sha256:[0-9a-f]{64}", a.get("hash", "")) for a in artifacts)
        evidence.update({"packages": len(packages), "artifact_hashes_present": hashes, "torch_source_matches": source_ok, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        return bool(hashes and source_ok), evidence
    except (OSError, tomllib.TOMLDecodeError, TypeError, KeyError, ValueError, AttributeError) as exc:
        evidence["error"] = type(exc).__name__
        return False, evidence
