"""The supported docs commands keep Node and npm inside bounded containers."""
import copy
import json
import os
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


def validate(config):
    services = config['services']
    if set(services) != {'build', 'dev', 'preview'}:
        raise ValueError('unexpected docs services')
    expected = {'/work/vocs.config.tsx', '/work/tsconfig.json',
                '/work/docs/pages', '/work/docs/public'}
    for service in services.values():
        if (not service.get('read_only') or service.get('privileged')
                or service.get('user') != '65532:65532'
                or service.get('cap_drop') != ['ALL'] or service.get('cap_add')
                or 'no-new-privileges:true' not in service.get('security_opt', [])
                or service.get('pull_policy') != 'never'
                or service.get('devices') or service.get('pid') == 'host'
                or service.get('ipc') == 'host'):
            raise ValueError('unexpected docs privilege')
        if (not 0 < float(service.get('cpus', 0)) <= 1
                or not 0 < int(service.get('mem_limit', 0)) <= 4 * 1024**3
                or not 0 < int(service.get('pids_limit', 0)) <= 256):
            raise ValueError('unbounded docs resources')
        mounts = service.get('volumes', [])
        if {mount['target'] for mount in mounts} != expected:
            raise ValueError('unexpected docs inputs')
        if any(mount['type'] != 'bind' or not mount.get('read_only')
               or mount.get('bind', {}).get('create_host_path', True)
               for mount in mounts):
            raise ValueError('docs source must be read-only')
        scratch = {entry.split(':', 1)[0]: set(entry.split(':', 1)[1].split(','))
                   for entry in service.get('tmpfs', [])}
        if set(scratch) != {'/work', '/work/docs', '/tmp'}:
            raise ValueError('unexpected docs scratch storage')
        if not {'uid=65532', 'gid=65532', 'mode=0700', 'noexec'} <= scratch['/work/docs']:
            raise ValueError('the non-root app must own its output parent')
    build = services['build']
    if build.get('network_mode') != 'none' or build.get('ports') or build.get('networks'):
        raise ValueError('docs build must be offline and publish no ports')
    if (set(config.get('networks', {})) != {'docs'}
            or config['networks']['docs'].get('internal')
            or config['networks']['docs'].get('driver') != 'bridge'
            or config['networks']['docs'].get('external')):
        raise ValueError('docs server needs its explicit browser-facing bridge')
    for name, target in (('dev', 5173), ('preview', 4173)):
        server = services[name]
        if set(server.get('networks', {})) != {'docs'} or server.get('network_mode'):
            raise ValueError('unexpected docs server network')
        ports = server.get('ports', [])
        if (len(ports) != 1 or ports[0].get('host_ip') != '127.0.0.1'
                or ports[0].get('target') != target
                or server.get('command') != [name]
                or not 1 <= int(ports[0].get('published', 0)) <= 65535):
            raise ValueError('docs server command and loopback port must match Vocs')


class RenderedDocs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = json.loads(Path(os.environ['DOCS_COMPOSE_JSON']).read_text())

    def test_actual_composition(self):
        validate(self.config)

    def test_public_port_egress_host_writes_and_extra_mounts_are_rejected(self):
        for change in ('public_port', 'egress', 'writes', 'socket', 'output_parent', 'output_owner'):
            with self.subTest(change=change):
                config = copy.deepcopy(self.config)
                if change == 'public_port':
                    config['services']['dev']['ports'][0]['host_ip'] = '0.0.0.0'
                elif change == 'egress':
                    config['services']['build']['network_mode'] = 'bridge'
                elif change == 'writes':
                    config['services']['build']['volumes'][0]['read_only'] = False
                elif change == 'output_owner':
                    config['services']['build']['tmpfs'] = [entry.replace('uid=65532,gid=65532,', '')
                                                           for entry in config['services']['build']['tmpfs']]
                elif change == 'output_parent':
                    config['services']['build']['tmpfs'] = [entry for entry in config['services']['build']['tmpfs']
                                                           if not entry.startswith('/work/docs:')]
                else:
                    config['services']['build']['volumes'].append({
                        'type': 'bind', 'source': '/var/run/docker.sock',
                        'target': '/var/run/docker.sock', 'read_only': True})
                with self.assertRaises(ValueError):
                    validate(config)
