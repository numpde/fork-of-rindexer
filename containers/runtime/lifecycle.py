"""Check an explicitly selected runtime image across backfill, restarts and reorg.

The host owns Docker control and disposable fixture cleanup; containers receive no
engine socket or checkout. A local emitter supplies Funded logs, not payment logic.
Run through make runtime/lifecycle/checks so its deadline includes teardown.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / 'containers/runtime/fixtures/lifecycle'
ABI = FIXTURE / 'abis/funded.json'


def run(args, *, env=None, timeout=90):
    """Bound each host-side engine operation and retain output for failure evidence."""
    return subprocess.run(args, cwd=ROOT, env=env, check=True, capture_output=True,
                          text=True, timeout=timeout).stdout.strip()


def interrupt(signum, _frame):
    raise SystemExit(128 + signum)


for signum in [signal.SIGINT, signal.SIGTERM]:
    signal.signal(signum, interrupt)

image = os.environ.get('RINDEXER_IMAGE', '')
if not re.fullmatch(r'sha256:[a-f0-9]{64}', image):
    raise SystemExit('Set RINDEXER_IMAGE to an existing immutable local image ID (sha256:...); build it separately with make runtime/image/build.')
context = os.environ.get('RINDEXER_DOCKER_CONTEXT', 'rootless')
if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', context):
    raise SystemExit('RINDEXER_DOCKER_CONTEXT must name a local rootless Docker context.')
if os.getuid() == 0:
    raise SystemExit('Run the lifecycle check as the non-root Docker operator.')
engine_env = dict(os.environ)
for key in list(engine_env):
    if key.startswith(('DOCKER_', 'COMPOSE_', 'RINDEXER_')):
        del engine_env[key]
endpoint = json.loads(run(['docker', 'context', 'inspect', context, '--format', '{{json .Endpoints.docker.Host}}'], env=engine_env))
if not isinstance(endpoint, str) or not endpoint.startswith('unix:///') or any(ord(c) < 32 for c in endpoint):
    raise SystemExit('The lifecycle check requires a local Unix-socket Docker context; no fixture services were started.')
docker = ['docker', '--host', endpoint]
security = json.loads(run(docker + ['info', '--format', '{{json .SecurityOptions}}'], env=engine_env))
if not isinstance(security, list) or 'name=rootless' not in security:
    raise SystemExit('The selected engine is not rootless; no fixture services were started.')
info = json.loads(run(docker + ['image', 'inspect', image], env=engine_env))[0]
assert info['Id'] == image
# Historical images are valid inputs: the same check must reproduce old failures.
revision = (info['Config'].get('Labels') or {}).get('org.opencontainers.image.revision')
assert info['Config']['User'] == '65532:65532'
assert info['Architecture'] == 'amd64'

pins = dict(line.split('=', 1) for line in (ROOT/'containers/development/images.env').read_text().splitlines()
            if line and not line.startswith('#') and '=' in line)
anvil_image = (ROOT/'core/src/indexer/fixtures/anvil-image.txt').read_text().strip()
for pin in [pins['POSTGRES_IMAGE'], anvil_image]:
    assert re.search(r'@sha256:[a-f0-9]{64}$', pin), pin
    run(docker + ['image', 'inspect', pin], env=engine_env)

abi_bytes = ABI.read_bytes()
abi = json.loads(abi_bytes)
abi = abi.get('abi', abi) if isinstance(abi, dict) else abi
funded = [item for item in abi if item.get('type') == 'event' and item.get('name') == 'Funded']
assert len(funded) == 1
assert [(item['name'], item['type'], item['indexed']) for item in funded[0]['inputs']] == [
    ('customer', 'address', True), ('channelId', 'uint256', True), ('amount', 'uint256', False)]

scratch = Path(tempfile.mkdtemp(prefix='rindexer-runtime-lifecycle-'))
project = 'rindexer-lifecycle-' + secrets.token_hex(8)
project_dir = scratch / 'project'
project_dir.mkdir()
(project_dir / 'abis').mkdir()
for relative in ['rindexer.yaml', 'abis/funded.json']:
    shutil.copyfile(FIXTURE / relative, project_dir / relative)

env = dict(engine_env, COMPOSE_DISABLE_ENV_FILE='1', POSTGRES_IMAGE=pins['POSTGRES_IMAGE'],
           ANVIL_IMAGE=anvil_image, RINDEXER_IMAGE=image, RINDEXER_PROJECT_DIR=str(project_dir),
           POSTGRES_PASSWORD='a'*64, RPC_HTTP_URL='http://anvil:8545', CHAIN_ID='31337',
           CONTRACT_ADDRESS='0x'+'1'*40, START_BLOCK='0', LIFECYCLE_INDEXER=project+'-indexer',
           LIFECYCLE_OWNER=project, RINDEXER_DOCKER_CONTEXT=context)
# Generated values contain only fixed literals, hashes and private temporary paths.
# Keep them available if teardown needs an operator retry; no deployment secrets enter.
fixture_env = scratch / 'fixture.env'
fixture_keys = ['POSTGRES_IMAGE', 'ANVIL_IMAGE', 'RINDEXER_IMAGE', 'RINDEXER_PROJECT_DIR',
                'POSTGRES_PASSWORD', 'RPC_HTTP_URL', 'CHAIN_ID', 'CONTRACT_ADDRESS',
                'START_BLOCK', 'LIFECYCLE_INDEXER', 'LIFECYCLE_OWNER', 'RINDEXER_DOCKER_CONTEXT']
fixture_env.write_text('\n'.join(f'{key}={env[key]}' for key in fixture_keys) + '\n')
fixture_env.chmod(0o600)
compose = docker + ['compose', '--env-file', str(fixture_env), '--project-name', project,
                    '--project-directory', str(ROOT), '-f', 'compose/postgres-runtime.yml',
                    '-f', 'compose/runtime-lifecycle.yml']


def compose_run(*args, timeout=90):
    return run(compose + list(args), env=env, timeout=timeout)


def require_empty_project():
    """Refuse reuse before startup and verify all owned resources disappear after teardown."""
    for resource, flags in [('container', ['--all']), ('network', []), ('volume', [])]:
        occupied = run(docker + [resource, 'ls', *flags, '--quiet', '--filter',
                                 'label=com.docker.compose.project=' + project], env=env)
        assert not occupied, f'Fixture project {project} still has {resource} state: {occupied}'


def service_id(service):
    """Admit the exact container owned by this invocation before using docker exec."""
    value = compose_run('ps', '--all', '--quiet', service)
    assert re.fullmatch(r'[a-f0-9]{64}', value), (service, value)
    metadata = json.loads(run(docker + ['inspect', value], env=env))[0]
    assert metadata['Config']['Labels']['io.rindexer.lifecycle'] == project
    assert metadata['Config']['Labels']['com.docker.compose.project'] == project
    assert metadata['HostConfig']['ReadonlyRootfs'] is True
    assert 0 < metadata['HostConfig']['NanoCpus'] <= 600000000
    assert 0 < metadata['HostConfig']['Memory'] <= 1073741824
    assert 'ALL' in metadata['HostConfig']['CapDrop']
    assert 'no-new-privileges:true' in metadata['HostConfig']['SecurityOpt']
    return value


def rpc(method, params):
    raw = run(docker + ['exec', anvil_id, '/usr/local/bin/cast', 'rpc', '--rpc-url',
                       'http://127.0.0.1:8545', '--raw', method, json.dumps(params)], env=env, timeout=10)
    return json.loads(raw)


def send(amount):
    """Mine one fixture emission for the same customer/channel, requiring a successful EVM receipt."""
    encoded = customer[2:].rjust(64, '0') + format(7, '064x') + format(amount, '064x')
    tx = rpc('eth_sendTransaction', [{'from': customer, 'to': env['CONTRACT_ADDRESS'],
                                    'data': '0x'+encoded, 'gas': '0x186a0'}])
    for _ in range(30):
        receipt = rpc('eth_getTransactionReceipt', [tx])
        if receipt is not None:
            assert int(receipt['status'], 16) == 1, receipt
            assert len(receipt['logs']) == 1, receipt
            return tx, int(receipt['blockNumber'], 16)
        time.sleep(0.2)
    raise AssertionError('Fixture transaction never received a receipt')


schema = 'lifecycle_funding'
orphan = '0x'+'0'*64


def database_state():
    """Read raw rows, aggregate provenance and application receipts through the real database."""
    sql = f'''SELECT json_build_object(
      'raw', (SELECT count(*) FROM {schema}.funded),
      'rows', (SELECT count(*) FROM {schema}.funding_totals),
      'funded_wei', (SELECT coalesce(sum(funded_wei),0)::text FROM {schema}.funding_totals),
      'receipts', (SELECT count(*) FROM rindexer_internal.event_applications),
      'blocks', (SELECT json_agg(block_number ORDER BY block_number) FROM {schema}.funded),
      'metadata_block', (SELECT max(rindexer_block_number) FROM {schema}.funding_totals),
      'orphan', (SELECT count(*) FROM {schema}.funded WHERE tx_hash='{orphan}')
    )::text'''
    raw = run(docker + ['exec', postgres_id, 'psql', '-X', '-A', '-t', '-U', 'rindexer',
                       '-d', 'rindexer', '-v', 'ON_ERROR_STOP=1', '-c', sql], env=env, timeout=10)
    return json.loads(raw)


health_url = None
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def health_endpoint():
    binding = compose_run('port','indexer','8080')
    match = re.fullmatch(r'127\.0\.0\.1:([0-9]+)', binding)
    assert match and 0 < int(match.group(1)) < 65536, binding
    return 'http://'+binding+'/health'


def await_state(label, blocks, total, orphan_count):
    """Require database and health agreement; a healthy process alone cannot certify recovery."""
    last = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            state = database_state()
            with opener.open(health_url, timeout=2) as response:
                health = json.load(response)
            subscriptions = health['indexing']['subscriptions']
            expected = {'raw':len(blocks), 'rows':1, 'funded_wei':str(total),
                        'receipts':len(blocks), 'blocks':blocks,
                        'metadata_block':max(blocks), 'orphan':orphan_count}
            last = {'database':state, 'subscriptions':subscriptions}
            if state == expected and len(subscriptions) == 1 and subscriptions[0]['status'] == 'live' \
                    and subscriptions[0]['last_synced_block'] == max(blocks) \
                    and subscriptions[0]['observed_head'] == max(blocks) \
                    and subscriptions[0]['lag_blocks'] == 0:
                proof['phases'].append({'phase':label, **last})
                print(json.dumps({'phase':label, **last}), flush=True)
                return
        except (subprocess.CalledProcessError, urllib.error.URLError, KeyError, ValueError) as error:
            last = str(error)
        time.sleep(0.5)
    raise AssertionError(f'{label} did not reach expected durable state: {last}')


proof = {'revision':revision, 'image':image, 'fixture_project':project,
         'fixture_abi_sha256':hashlib.sha256(abi_bytes).hexdigest(),
         'fixture':'Funded event emitter; no payment business logic', 'phases':[]}
started = False
try:
    rendered = json.loads(compose_run('config', '--format', 'json'))
    assert set(rendered['services']) == {'indexer','postgres','anvil'}
    assert rendered['networks']['database']['internal'] is True
    assert not rendered['services']['postgres'].get('ports')
    assert not rendered['services']['anvil'].get('ports')
    assert rendered['services']['indexer']['image'] == image
    assert set(rendered['volumes']) == {'database'}
    assert not rendered['volumes']['database'].get('external', False)
    assert not rendered['networks']['database'].get('external', False)
    assert not rendered['networks']['rpc'].get('external', False)
    assert rendered['services']['indexer']['ports'][0]['host_ip'] == '127.0.0.1'
    require_empty_project()
    started = True
    compose_run('up','-d','--no-build','--pull','never','--wait','--wait-timeout','90','postgres','anvil',timeout=100)
    postgres_id = service_id('postgres')
    anvil_id = service_id('anvil')
    customer = rpc('eth_accounts',[])[0].lower()
    topic = rpc('web3_sha3',['0x'+b'Funded(address,uint256,uint256)'.hex()])
    assert re.fullmatch(r'0x[a-f0-9]{64}',topic), topic
    # The emitter reads three calldata words, puts amount in log data, and uses
    # customer/channel as indexed topics. It holds no payment state and transfers no funds.
    code = '0x6040356000526020356000357f'+topic[2:]+'60206000a300'
    rpc('anvil_setCode',[env['CONTRACT_ADDRESS'],code])
    first = 10**20+7
    replacement = 3*10**20+13
    _, first_block = send(first)
    compose_run('up','-d','--no-build','--pull','never','indexer')
    indexer_id = service_id('indexer')
    live = json.loads(run(docker + ['inspect',indexer_id],env=env))[0]
    assert live['Image'] == image
    assert live['Config']['User'] == '65532:65532'
    assert all(m['RW'] is False for m in live['Mounts'] if m['Destination']=='/project')
    health_url = health_endpoint()
    await_state('historical_backfill',[first_block],first,0)
    compose_run('stop','--timeout','10','indexer',timeout=25)
    orphan, orphan_block = send(2*10**20+11)
    compose_run('start','indexer')
    health_url = health_endpoint()
    await_state('downtime_restart',[first_block,orphan_block],first+2*10**20+11,1)
    rpc('anvil_reorg',[1,[]])
    _, replacement_block = send(replacement)
    await_state('controlled_reorg',[first_block,replacement_block],first+replacement,0)
    compose_run('stop','--timeout','10','indexer',timeout=25)
    compose_run('start','indexer')
    health_url = health_endpoint()
    await_state('stable_restart',[first_block,replacement_block],first+replacement,0)
finally:
    failed = sys.exc_info()[0] is not None
    for signum in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(signum, signal.SIG_IGN)
    if started:
        if failed:
            try:
                print(compose_run('logs','--no-color','--tail','80','indexer',timeout=5),file=sys.stderr)
            except Exception as error:
                print('Could not retrieve fixture logs:',error,file=sys.stderr)
        try:
            # The unique project was admitted empty and is exclusively owned by this invocation.
            compose_run('down','--volumes','--timeout','10',timeout=35)
            require_empty_project()
        except Exception as error:
            print(f'Fixture cleanup failed; retain {scratch} for recovery: {error}',file=sys.stderr)
            # Recovery must use the same file and socket, without ambient overrides.
            recovery = ['env', '-i', 'PATH=' + os.environ.get('PATH', os.defpath),
                        'HOME=' + os.environ['HOME'], 'COMPOSE_DISABLE_ENV_FILE=1',
                        *compose, 'down', '--volumes', '--timeout', '10']
            print('Recovery: ' + shlex.join(recovery), file=sys.stderr)
            if not failed:
                raise
        else:
            shutil.rmtree(scratch)
    else:
        shutil.rmtree(scratch)

# Success includes teardown, rather than merely a completed scenario.
print(json.dumps({'result': 'passed', **proof}), flush=True)
