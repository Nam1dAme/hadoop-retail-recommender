"""Check fail-fast config and credential boundaries without external services."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from orchestration.settings import ConfigError, database_credentials, load_config

CONFIG = Path(__file__).resolve().parents[1] / "config/development.toml"


class SettingsTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_baseline_config(self):
        config = load_config(CONFIG)
        self.assertEqual(config["database"]["name"], "retail_analytics")
        self.assertEqual(config["api"]["host"], "127.0.0.1")

    def test_explicit_missing_path_fails(self):
        with self.assertRaises(ConfigError):
            load_config(CONFIG.parent / "missing.toml")

    def test_no_default_credentials(self):
        with self.assertRaises(ConfigError):
            database_credentials()

    def test_root_rejected(self):
        with patch.dict(os.environ, {"RETAIL_DB_USER": "root", "RETAIL_DB_PASSWORD": "test-only"}):
            with self.assertRaises(ConfigError):
                database_credentials()

    def test_credentials_are_not_in_config(self):
        with patch.dict(os.environ, {"RETAIL_DB_USER": "test_reader", "RETAIL_DB_PASSWORD": "test-only"}):
            self.assertEqual(database_credentials(), ("test_reader", "test-only"))
            self.assertNotIn("password", repr(load_config(CONFIG)))

    def test_public_bind_and_invalid_ports_fail(self):
        for variable, value in (("RETAIL_API_HOST", "0.0.0.0"), ("RETAIL_UI_HOST", "0.0.0.0"), ("RETAIL_DB_PORT", "0"), ("RETAIL_DB_PORT", "65536"), ("RETAIL_DB_PORT", "password-like-invalid-value")):
            with self.subTest(variable=variable), patch.dict(os.environ, {variable: value}):
                with self.assertRaises(ConfigError) as error:
                    load_config(CONFIG)
                self.assertNotIn(value, str(error.exception))

    def test_api_url_must_match_port_and_have_no_credentials(self):
        for url in ("http://user:secret@127.0.0.1:8000/api/v1", "http://127.0.0.1:9000/api/v1", "http://127.0.0.1:8000/api/v1?secret=x"):
            with self.subTest(url=url), patch.dict(os.environ, {"RETAIL_API_URL": url}):
                with self.assertRaises(ConfigError):
                    load_config(CONFIG)

    def test_unknown_keys_and_baseline_drift_fail(self):
        text = CONFIG.read_text()
        for modified in (text.replace('[database]', '[database]\npassword = "forbidden"'), text.replace('HL-DOC-1.0.0', 'HL-DOC-2.0.0')):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "config.toml"
                path.write_text(modified)
                with self.assertRaises(ConfigError):
                    load_config(path)

    def test_path_traversal_and_relative_workdir_fail(self):
        for variable, value in (("RETAIL_HDFS_ROOT", "/retail/../other"), ("RETAIL_WORK_DIR", "relative")):
            with self.subTest(variable=variable), patch.dict(os.environ, {variable: value}):
                with self.assertRaises(ConfigError):
                    load_config(CONFIG)


if __name__ == "__main__":
    unittest.main()
