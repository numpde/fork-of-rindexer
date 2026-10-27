"""Drive the real launcher and Compose parser without granting fake effects Docker access."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = 'unix:///fixture/rootless.sock'


class LauncherContextTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.log = self.directory / 'docker.jsonl'
        self.env_file = self.directory / 'runtime.env'
        self.bin = self.directory / 'bin'
        self.bin.mkdir()
        docker = shutil.which('docker')
        self.assertIsNotNone(docker, 'the offline Compose renderer must be installed')
        fake = self.bin / 'docker'
        fake.write_text(f'#!{sys.executable}\n' + '''import json,os,subprocess,sys
from pathlib import Path
args=sys.argv[1:]
with open(os.environ['LAUNCHER_DOCKER_LOG'],'a') as log:
    log.write(json.dumps(args)+'\\n')
selector=None
if args and args[0] in ['--context','--host']:
    selector,args=args[:2],args[2:]
endpoints={'default':'unix:///fixture/default.sock','rootless':'unix:///fixture/rootless.sock','remote':'ssh://fixture.invalid'}
if args[:2]==['context','inspect']:
    names=[arg for arg in args[2:] if arg in endpoints]
    if not names:
        print('fixture: Docker context does not exist',file=sys.stderr);sys.exit(17)
    value=endpoints[names[0]]
    fmt=args[args.index('--format')+1]
    print(json.dumps(value) if 'json' in fmt else value)
    Path(os.environ['LAUNCHER_INSPECTED']).touch()
    sys.exit(0)
if args[0]=='compose' and args[-3:]==['config','--format','json']:
    # Real parsing owns interpolation and dotenv. This command never contacts a daemon.
    sys.exit(subprocess.run([os.environ['LAUNCHER_REAL_DOCKER'],*args]).returncode)
if args[0]!='compose':
    raise SystemExit('unexpected Docker operation')
if selector and selector[0]=='--host':
    endpoint=selector[1]
else:
    name=selector[1] if selector else 'ambient'
    endpoint=endpoints.get(name,'unexpected')
    if os.environ.get('LAUNCHER_SWITCH_CONTEXT')=='1' and Path(os.environ['LAUNCHER_INSPECTED']).exists():
        endpoint='unix:///fixture/changed.sock'
if endpoint!=os.environ['LAUNCHER_EXPECTED_ENDPOINT']:
    print('fixture: engine request used the wrong endpoint',file=sys.stderr);sys.exit(42)
if os.environ.get('LAUNCHER_FAIL_ENGINE')=='1':
    print('fixture: selected engine is unavailable',file=sys.stderr);sys.exit(43)
''')
        fake.chmod(0o700)
        self.environment = dict(os.environ,
            PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
            LAUNCHER_REAL_DOCKER=docker,
            LAUNCHER_DOCKER_LOG=str(self.log),
            LAUNCHER_INSPECTED=str(self.directory / 'inspected'),
            LAUNCHER_EXPECTED_ENDPOINT=ENDPOINT,
            DOCKER_HOST='unix:///ambient/wrong.sock', DOCKER_CONTEXT='ambient-wrong',
            RINDEXER_DOCKER_CONTEXT='ambient-wrong', COMPOSE_FILE='/ambient/wrong.yml')
        self.write_env('rootless')

    def write_env(self, context):
        lines = [f'RINDEXER_IMAGE=sha256:{"a" * 64}',
                 f'RINDEXER_PROJECT_DIR={ROOT / "containers/runtime/project"}',
                 f'POSTGRES_PASSWORD={"a" * 64}', 'RPC_HTTP_URL=http://fixture.invalid',
                 'CHAIN_ID=1', f'CONTRACT_ADDRESS=0x{"1" * 40}', 'START_BLOCK=0']
        if context is not None:
            lines.append(f'RINDEXER_DOCKER_CONTEXT={context}')
        self.env_file.write_text('\n'.join(lines) + '\n')
        self.env_file.chmod(0o600)

    def run_launcher(self, operation):
        return subprocess.run(['bash', str(ROOT / 'containers/runtime/deploy.sh'), operation,
                               str(self.env_file)], env=self.environment, capture_output=True,
                              text=True, timeout=15)

    def effects(self):
        requests = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [args for args in requests if 'compose' in args and 'config' not in args]

    def test_start_status_and_stop_use_the_configured_rootless_engine(self):
        for operation in ['start', 'status', 'stop']:
            with self.subTest(operation=operation):
                result = self.run_launcher(operation)
                self.assertEqual(0, result.returncode, result.stderr)
        requests = self.effects()
        self.assertEqual(6, len(requests))
        self.assertTrue(all(args[:2] == ['--host', ENDPOINT] for args in requests), requests)
        self.assertEqual(['stop', 'up', 'up', 'ps', 'stop', 'stop'],
                         [next(arg for arg in args if arg in ['stop', 'up', 'ps']) for args in requests])
        self.assertEqual(['indexer', 'postgres', 'indexer'], [args[-1] for args in requests[:3]])

    def test_context_changes_cannot_redirect_an_admitted_invocation(self):
        self.environment['LAUNCHER_SWITCH_CONTEXT'] = '1'
        result = self.run_launcher('start')
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(all(args[:2] == ['--host', ENDPOINT] for args in self.effects()))

    def test_legacy_env_file_keeps_default_context(self):
        self.write_env(None)
        self.environment['LAUNCHER_EXPECTED_ENDPOINT'] = 'unix:///fixture/default.sock'
        result = self.run_launcher('status')
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual('ps', self.effects()[0][-2])

    def test_remote_and_missing_contexts_fail_before_engine_effects(self):
        for context in ['remote', 'missing']:
            with self.subTest(context=context):
                self.write_env(context)
                self.log.write_text('')
                result = self.run_launcher('start')
                self.assertNotEqual(0, result.returncode)
                self.assertEqual([], self.effects(), result.stderr)

    def test_unavailable_selected_engine_never_falls_back(self):
        self.environment['LAUNCHER_FAIL_ENGINE'] = '1'
        result = self.run_launcher('start')
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(1, len(self.effects()))
        self.assertEqual(['--host', ENDPOINT], self.effects()[0][:2])


if __name__ == '__main__':
    unittest.main()
