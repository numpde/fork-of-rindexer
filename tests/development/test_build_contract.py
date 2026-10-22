"""The prepared GraphQL boundary never invokes Node or fetches dependencies."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class PreparedGraphqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.binary = cls.root / 'build-script'
        subprocess.run(['rustc', '--edition=2021', 'core/build.rs', '-o', str(cls.binary)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def invoke(self, supplied):
        output = self.root / 'out'
        output.mkdir(exist_ok=True)
        env = dict(os.environ, OUT_DIR=str(output), RINDEXER_GRAPHQL_PREBUILT=str(supplied), PATH='/nonexistent', CARGO_MANIFEST_DIR=str(Path('core').resolve()), CARGO_CFG_TARGET_OS='linux', CARGO_CFG_TARGET_ARCH='x86_64')
        return subprocess.run([str(self.binary)], env=env, cwd=self.root, capture_output=True, text=True)

    def test_prepared_bytes_are_embedded_without_node_or_npm(self):
        source = self.root / 'prepared'
        source.write_bytes(b'fixture binary bytes\x00\xff')
        result = self.invoke(source)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(source.read_bytes(), (self.root / 'out/graphql_binary').read_bytes())
        self.assertIn('cargo:rerun-if-env-changed=RINDEXER_GRAPHQL_PREBUILT', result.stdout)

    def test_missing_prepared_binary_fails_without_fallback(self):
        result = self.invoke(self.root / 'missing')
        self.assertNotEqual(0, result.returncode)
        self.assertIn('RINDEXER_GRAPHQL_PREBUILT must name a regular file', result.stderr)

    def test_directory_is_not_accepted_as_a_binary(self):
        result = self.invoke(self.root)
        self.assertNotEqual(0, result.returncode)
        self.assertIn('RINDEXER_GRAPHQL_PREBUILT must name a regular file', result.stderr)
