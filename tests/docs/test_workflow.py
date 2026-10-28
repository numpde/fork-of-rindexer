"""The supported docs commands keep Node and npm inside bounded containers."""
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]


class ContainerEntrypoints(unittest.TestCase):
    def test_docs_commands_use_containers_and_prepare_dependencies_explicitly(self):
        for goal in ('docs/deps/install', 'docs/build', 'docs/dev', 'docs/preview'):
            with self.subTest(goal=goal):
                result = subprocess.run(['make', '-n', goal], cwd=ROOT,
                                        capture_output=True, text=True, check=False)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertIn('docker ', result.stdout)
                self.assertNotRegex(result.stdout, r'(?m)^\s*(npm|npx|node)\s')
                if goal != 'docs/deps/install':
                    self.assertNotIn('docker pull ', result.stdout)
                    self.assertNotIn('docker build ', result.stdout)
