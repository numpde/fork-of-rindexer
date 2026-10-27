"""Exercise the actual Compose model and its admission checks with non-secret inputs."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parents[2]
environment = {key: value for key, value in os.environ.items()
               if not key.startswith(('DOCKER_', 'COMPOSE_', 'RINDEXER_'))}
environment.update(COMPOSE_DISABLE_ENV_FILE='1',
                   RINDEXER_IMAGE='sha256:' + 'a' * 64,
                   RINDEXER_PROJECT_DIR=str(root / 'containers/runtime/project'),
                   POSTGRES_IMAGE='postgres@sha256:' + 'b' * 64,
                   POSTGRES_PASSWORD='a' * 64, RPC_HTTP_URL='http://fixture.invalid:8545',
                   CHAIN_ID='1', CONTRACT_ADDRESS='0x' + '1' * 40, START_BLOCK='0')
rendered = subprocess.run(['docker', '--context', 'default', 'compose', '--env-file', '/dev/null',
                          '--project-name', 'rindexer-postgres-check', '--project-directory', str(root),
                          '-f', str(root / 'compose/postgres-runtime.yml'), 'config', '--format', 'json'],
                         check=True, capture_output=True, text=True, env=environment)
config = json.loads(rendered.stdout)
indexer = config['services']['indexer']
assert indexer['ports'] == [{'mode': 'ingress', 'host_ip': '127.0.0.1',
                            'target': 8080, 'published': '8080', 'protocol': 'tcp'}]
assert indexer['security_opt'] == ['no-new-privileges:true']
assert indexer['restart'] == 'no'
assert set(indexer['networks']) == {'database', 'rpc'}
assert config['services']['postgres']['security_opt'] == ['no-new-privileges:true']
with tempfile.TemporaryDirectory() as directory:
    input_path = Path(directory) / 'rendered.json'
    def check(model, succeeds):
        input_path.write_text(json.dumps(model))
        result = subprocess.run([sys.executable, '-B', str(root / 'containers/runtime/check.py'),
                                 str(input_path)], capture_output=True, text=True)
        assert (result.returncode == 0) == succeeds, result.stderr
    check(config, True)
    public_db = copy.deepcopy(config)
    public_db['services']['postgres']['ports'] = [{'published': '5432', 'target': 5432}]
    check(public_db, False)
    writable_project = copy.deepcopy(config)
    writable_project['services']['indexer']['volumes'][0]['read_only'] = False
    check(writable_project, False)
    mutable_image = copy.deepcopy(config)
    mutable_image['services']['indexer']['image'] = 'rindexer-postgres:local'
    check(mutable_image, False)
    root_user = copy.deepcopy(config)
    root_user['services']['indexer']['user'] = '0:0'
    check(root_user, False)
    extra_service = copy.deepcopy(config)
    extra_service['services']['graphql'] = {'image': 'unexpected'}
    check(extra_service, False)
print('Runtime Compose checks passed: private database, read-only project, immutable image and non-root users.')
