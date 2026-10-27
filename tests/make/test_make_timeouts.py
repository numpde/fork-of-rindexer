"""Make entrypoints bound complete recipes, including cleanup and nested Make."""
import os
from pathlib import Path
import shlex
import signal
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class MakeTimeoutTests(unittest.TestCase):
    def run_recipe(self, makefile, recipe, limit='1s', guard=5):
        command = ['make', '--no-print-directory', '-C', str(ROOT / makefile.parent),
                   '-f', makefile.name, f'MAKE_TIMEOUT={limit}',
                   '--eval', '__deadline_probe:\n' + ''.join('\t@' + line + '\n' for line in recipe.splitlines()),
                   '__deadline_probe']
        environment = dict(os.environ)
        environment.pop('RINDEXER_MAKE_TIMEOUT_ACTIVE', None)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True, env=environment)
        try:
            stdout, stderr = process.communicate(timeout=guard)
            return process.returncode, stdout, stderr
        except subprocess.TimeoutExpired:
            self.fail(f'{makefile} did not enforce MAKE_TIMEOUT={limit}')
        finally:
            # The red implementation leaves the probe running. Own its entire
            # process group so a failed assertion cannot leak a sleeping child.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate(timeout=2)

    def test_all_three_makefiles_enforce_the_recipe_deadline(self):
        for makefile in [Path('Makefile'), Path('cli/Makefile'), Path('e2e-tests/Makefile')]:
            with self.subTest(makefile=makefile):
                code, _, error = self.run_recipe(makefile, 'sleep 30')
                self.assertNotEqual(code, 0)
                self.assertIn('124', error, error)

    def test_timeout_runs_the_recipe_cleanup_trap(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'cleaned'
            recipe = f"trap 'touch {shlex.quote(str(marker))}; exit 143' TERM\nsleep 30 &\nwait"
            code, _, error = self.run_recipe(Path('Makefile'), recipe)
            self.assertNotEqual(code, 0)
            self.assertIn('124', error, error)
            self.assertTrue(marker.exists(), 'the timeout must allow owned cleanup')

    def test_timeout_stops_a_spawned_child(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / 'pid'
            script = f'import os,time;open({str(pid_file)!r},"w").write(str(os.getpid()));time.sleep(30)'
            code, _, error = self.run_recipe(Path('Makefile'), 'python3 -c ' + shlex.quote(script))
            self.assertNotEqual(code, 0)
            self.assertIn('124', error, error)
            status = Path(f'/proc/{pid_file.read_text()}/stat')
            if status.exists():
                self.assertEqual('Z', status.read_text().split()[2], 'child must no longer execute')

    def test_zero_and_invalid_limits_do_not_run_the_recipe(self):
        for limit in ['0', '0s', 'invalid', '-1s']:
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as directory:
                marker = Path(directory) / 'started'
                code, _, error = self.run_recipe(Path('Makefile'), 'touch ' + shlex.quote(str(marker)), limit)
                self.assertNotEqual(code, 0)
                self.assertIn('MAKE_TIMEOUT', error)
                self.assertFalse(marker.exists())

    def test_success_and_command_failure_keep_their_status(self):
        code, output, _ = self.run_recipe(Path('Makefile'), "printf 'finished'", '10s')
        self.assertEqual(code, 0)
        self.assertEqual(output, 'finished')
        code, _, error = self.run_recipe(Path('Makefile'), 'exit 7', '10s')
        self.assertNotEqual(code, 0)
        self.assertIn('Error 7', error)


if __name__ == '__main__':
    unittest.main()
