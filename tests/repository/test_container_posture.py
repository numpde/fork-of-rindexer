"""Check Docker Compose's rendered configuration, not duplicated YAML text."""
import copy
import json
from pathlib import Path
import unittest


def validate(config):
    services = config['services']
    if set(services) != {'offline', 'postgres-tests', 'postgres'}:
        raise ValueError('unexpected service topology')
    for name, service in services.items():
        if not service.get('read_only') or service.get('privileged'):
            raise ValueError('writable or privileged root filesystem')
        if service.get('cap_add') or service.get('cap_drop') != ['ALL']:
            raise ValueError('unexpected capabilities')
        if 'no-new-privileges:true' not in service.get('security_opt', []):
            raise ValueError('privilege escalation is not prohibited')
        user = str(service.get('user', '0')).split(':')[0]
        if not user.isdecimal() or int(user) == 0:
            raise ValueError('runtime must use a non-root numeric user')
        if service.get('ports') or service.get('devices'):
            raise ValueError('published port or device')
        if any(service.get(key) == 'host' for key in ('pid', 'ipc', 'network_mode')):
            raise ValueError('host namespace')
        if not (0 < int(service.get('pids_limit', 0)) <= 512):
            raise ValueError('unbounded PID count')
        if not (0 < int(service.get('mem_limit', 0)) <= 6 * 1024**3):
            raise ValueError('unbounded memory')
        if not (0 < float(service.get('cpus', 0)) <= 2):
            raise ValueError('unbounded CPU')
        if service.get('pull_policy') != 'never':
            raise ValueError('execution can implicitly pull an image')
        for mount in service.get('volumes', []):
            if mount['type'] == 'bind':
                if not mount.get('read_only') or mount.get('target') not in {'/input/source.tar', '/input/run.sh'}:
                    raise ValueError('unexpected host mount')
                if 'docker.sock' in mount.get('source', '') or mount.get('bind', {}).get('create_host_path', True):
                    raise ValueError('Docker access or implicit host path creation')
            elif mount['type'] != 'volume' or mount.get('target') != '/build' or mount.get('source') != 'build':
                raise ValueError('unexpected writable storage')
        allowed_tmpfs = {'/work', '/tmp'} if name != 'postgres' else {
            '/var/lib/postgresql/data', '/var/run/postgresql', '/tmp'
        }
        for mount in service.get('tmpfs', []):
            destination, separator, options = mount.partition(':')
            if destination not in allowed_tmpfs or not separator:
                raise ValueError('unexpected tmpfs mount')
            options = set(options.split(','))
            if (
                not {'nosuid', 'nodev', 'noexec'} <= options
                or 'exec' in options
                or not any(option.startswith('size=') for option in options)
            ):
                raise ValueError('unbounded or unsafe tmpfs')
    if services['offline'].get('network_mode') != 'none' or services['offline'].get('networks'):
        raise ValueError('offline compiler has network access')
    if set(config['networks']) != {'fixtures'} or not config['networks']['fixtures'].get('internal'):
        raise ValueError('fixtures have external egress')
    for name in ('postgres-tests', 'postgres'):
        if set(services[name].get('networks', {})) != {'fixtures'}:
            raise ValueError('fixture service has an unexpected network')
    if '@sha256:' not in services['postgres']['image']:
        raise ValueError('PostgreSQL image is not digest-pinned')
    if set(config['volumes']) != {'build'} or not config['volumes']['build'].get('external'):
        raise ValueError('unexpected persistent volume lifecycle')


class ContainerPostureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = json.loads(Path('/input/compose.json').read_text())

    def test_rendered_configuration_is_least_privilege(self):
        validate(self.config)

    def test_unsafe_service_variants_are_rejected(self):
        variants = [
            ('privileged', True), ('read_only', False), ('user', '0:0'),
            ('cap_add', ['SYS_ADMIN']), ('pids_limit', 0), ('mem_limit', 0),
            ('cpus', 0), ('pull_policy', 'always'), ('network_mode', 'host'),
            ('ports', [{'target': 5432, 'published': '5432'}]),
            ('tmpfs', ['/etc:rw,size=1m']),
            ('tmpfs', ['/tmp:rw,nosuid,nodev,size=1m']),
            ('tmpfs', ['/tmp:rw,nosuid,nodev,noexec,exec,size=1m']),
            ('volumes', [{'type': 'bind', 'source': '/var/run/docker.sock', 'target': '/var/run/docker.sock', 'read_only': True}]),
        ]
        for field, value in variants:
            with self.subTest(field=field):
                config = copy.deepcopy(self.config)
                config['services']['offline'][field] = value
                with self.assertRaises(ValueError):
                    validate(config)

    def test_fixture_egress_is_rejected(self):
        config = copy.deepcopy(self.config)
        config['networks']['fixtures']['internal'] = False
        with self.assertRaises(ValueError):
            validate(config)
