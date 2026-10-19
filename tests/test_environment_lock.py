"""Reject missing artifact hashes and incorrect CUDA source/version evidence."""

from pathlib import Path
import tempfile
import unittest

from orchestration.environment_lock import inspect_uv_lock

# Isolated parser fixture only, never an installation or environment evidence file.
FIXTURE = '''version = 1
[[package]]
name = "torch"
version = "2.6.0+cu124"
source = {registry = "https://download.pytorch.org/whl/cu124"}
wheels = [{url = "https://fixture.invalid/torch.whl", hash = "sha256:HASH"}]
'''.replace('HASH', '0' * 64)


class EnvironmentLockTests(unittest.TestCase):
    def inspect(self, content):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "uv.lock"
            path.write_text(content)
            return inspect_uv_lock(path)

    def test_cuda_source_and_hash_presence(self):
        valid, result = self.inspect(FIXTURE)
        self.assertTrue(valid)
        self.assertTrue(result["torch_source_matches"])
        self.assertTrue(result["artifact_hashes_present"])

    def test_cpu_or_wrong_registry_rejected(self):
        for content in (FIXTURE.replace('2.6.0+cu124', '2.6.0+cpu'), FIXTURE.replace('https://download.pytorch.org/whl/cu124', 'https://pypi.org/simple')):
            with self.subTest(content=content):
                self.assertFalse(self.inspect(content)[0])

    def test_missing_or_malformed_hash_rejected(self):
        for content in (FIXTURE.replace('sha256:' + '0' * 64, 'sha256:bad'), FIXTURE.split('wheels =')[0]):
            self.assertFalse(self.inspect(content)[0])

    def test_missing_or_malformed_file_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(inspect_uv_lock(Path(directory) / "missing.lock")[0])
        self.assertFalse(self.inspect('[[package]')[0])


if __name__ == "__main__":
    unittest.main()
