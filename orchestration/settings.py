"""Load a named TOML file; never discover config or credentials from cwd."""

from __future__ import annotations

import os
from pathlib import Path
import tomllib
from urllib.parse import urlsplit

BASELINE = "HL-DOC-1.0.0"
_SCHEMA = {
    "database": {"host": str, "port": int, "name": str},
    "hdfs": {"uri": str, "root": str},
    "paths": {"work_dir": str},
    "api": {"host": str, "port": int, "url": str},
    "ui": {"host": str, "port": int},
    "runtime": {"java_home": str, "hadoop_home": str, "hadoop_conf_dir": str, "cuda_home": str},
}
_OVERRIDES = {
    "database": {"host": "RETAIL_DB_HOST", "port": "RETAIL_DB_PORT", "name": "RETAIL_DB_NAME"},
    "hdfs": {"uri": "RETAIL_HDFS_URI", "root": "RETAIL_HDFS_ROOT"},
    "paths": {"work_dir": "RETAIL_WORK_DIR"},
    "api": {"host": "RETAIL_API_HOST", "port": "RETAIL_API_PORT", "url": "RETAIL_API_URL"},
    "ui": {"host": "RETAIL_UI_HOST", "port": "RETAIL_UI_PORT"},
    "runtime": {"java_home": "JAVA_HOME", "hadoop_home": "HADOOP_HOME", "hadoop_conf_dir": "HADOOP_CONF_DIR", "cuda_home": "CUDA_HOME"},
}


class ConfigError(ValueError):
    """A missing or invalid setting (messages exclude supplied values)."""


def load_config(path: str | Path) -> dict:
    try:
        with Path(path).open("rb") as stream:
            config = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError("Cannot read the explicit TOML configuration") from exc
    if set(config) != {"baseline_version", *_SCHEMA} or config["baseline_version"] != BASELINE:
        raise ConfigError("Unknown/missing sections or baseline_version mismatch")
    for section, fields in _SCHEMA.items():
        if not isinstance(config[section], dict) or set(config[section]) != set(fields):
            raise ConfigError(f"Unknown/missing fields in {section}")
        for name, kind in fields.items():
            variable = _OVERRIDES[section][name]
            value = os.environ.get(variable, config[section][name])
            if variable in os.environ and kind is int:
                try:
                    value = int(value)
                except ValueError as exc:
                    raise ConfigError(f"Invalid integer: {variable}") from exc
            if type(value) is not kind or (kind is str and (not value.strip() or any(ord(c) < 32 for c in value))):
                raise ConfigError(f"Invalid {section}.{name}")
            if name == "port" and not 1 <= value <= 65535:
                raise ConfigError(f"Invalid port in {section}")
            config[section][name] = value
    for section in ("api", "ui"):
        if config[section]["host"] != "127.0.0.1":
            raise ConfigError(f"{section} must listen on 127.0.0.1")
    for name in _SCHEMA["runtime"]:
        if not Path(config["runtime"][name]).is_absolute():
            raise ConfigError(f"runtime.{name} must be absolute")
    work = Path(config["paths"]["work_dir"])
    if not work.is_absolute():
        raise ConfigError("paths.work_dir must be absolute")
    root = config["hdfs"]["root"]
    if not root.startswith("/") or ".." in root.split("/"):
        raise ConfigError("hdfs.root must be an absolute HDFS path without traversal")
    for section, name, scheme in (("hdfs", "uri", "hdfs"), ("api", "url", "http")):
        try:
            url = urlsplit(config[section][name])
            valid_port = url.port
        except ValueError as exc:
            raise ConfigError(f"Invalid {section}.{name}") from exc
        if url.scheme != scheme or not url.hostname or url.username is not None or url.password is not None or url.query or url.fragment:
            raise ConfigError(f"Invalid {section}.{name}")
        if section == "api" and (url.hostname != "127.0.0.1" or url.path != "/api/v1" or valid_port != config["api"]["port"]):
            raise ConfigError("api.url must match the loopback API port and /api/v1")
    return config


def database_credentials() -> tuple[str, str]:
    """Call only from a database consumer, never from the UI."""
    user = os.environ.get("RETAIL_DB_USER", "")
    password = os.environ.get("RETAIL_DB_PASSWORD", "")
    if not user.strip() or not password.strip() or user.lower() == "root":
        raise ConfigError("Explicit non-root RETAIL_DB_USER and RETAIL_DB_PASSWORD are required")
    return user, password
