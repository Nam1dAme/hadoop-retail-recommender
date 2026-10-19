"""Ensure permission failures are not reported as broken services or passwords."""

import importlib.util
import io
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
import urllib.error

spec = importlib.util.spec_from_file_location("environment_diagnostics", Path(__file__).resolve().parents[1] / "scripts/diagnose-environment.py")
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class DiagnosticsTests(unittest.TestCase):
    def test_denied_sockets_skip_http_without_implying_source_failure(self):
        probes = {name: {"status": "PERMISSION_DENIED"} for name in ("ipv4", "ipv6")}
        with patch.object(diagnostics.urllib.request, "urlopen") as request:
            results = diagnostics.package_sources(probes)
        request.assert_not_called()
        self.assertEqual(len(results), 3)
        self.assertTrue(all(row["status"] == "SKIPPED_SOCKET_PERMISSION_DENIED" for row in results.values()))

    def test_http_errors_still_prove_transport_reached_server(self):
        probes = {name: {"status": "CREATED"} for name in ("ipv4", "ipv6")}
        error = urllib.error.HTTPError("https://pypi.org/", 403, "Forbidden", {}, None)
        with patch.object(diagnostics.urllib.request, "urlopen", side_effect=error):
            results = diagnostics.package_sources(probes)
        self.assertTrue(all(row == {"status": "HTTP_REACHED", "http_status": 403} for row in results.values()))

    def test_unix_connect_denial_precedes_authentication(self):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.connect.side_effect = PermissionError(1, "Operation not permitted")
        with patch.object(diagnostics.Path, "exists", return_value=True), patch.object(diagnostics.socket, "socket", return_value=connection):
            result = diagnostics.database_handshake()
        self.assertEqual(result["status"], "PERMISSION_DENIED")
        self.assertFalse(result["authenticated"])
        connection.makefile.assert_not_called()
        connection.send.assert_not_called()
        connection.sendall.assert_not_called()

    def test_server_greeting_does_not_authenticate_or_send_sql(self):
        payload = b"\x0a5.5.5-10.11.14-MariaDB\0unused"
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.makefile.return_value = io.BytesIO(len(payload).to_bytes(3, "little") + b"\0" + payload)
        with patch.object(diagnostics.Path, "exists", return_value=True), patch.object(diagnostics.socket, "socket", return_value=connection):
            result = diagnostics.database_handshake()
        self.assertEqual(result["status"], "GREETING_RECEIVED")
        self.assertEqual(result["server_version"], "5.5.5-10.11.14-MariaDB")
        self.assertFalse(result["authenticated"])
        connection.send.assert_not_called()
        connection.sendall.assert_not_called()


if __name__ == "__main__":
    unittest.main()
