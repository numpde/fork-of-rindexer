"""Exercise rendered chart behavior using Helm and the Python standard library."""
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

helm = sys.argv.pop(1) if len(sys.argv) > 1 else 'helm'
chart = Path(__file__).resolve().parents[1]
digest = 'sha256:' + 'a' * 64

# Helm parses the actual rendered resources and emits them as JSON in a temporary
# chart, so these checks need no additional Python YAML package in CI.
inspection_template = '''\
{{- $items := list -}}
{{- range $name := list "deployment.yaml" "service.yaml" "configmap.yaml" "configmap-abis.yaml" -}}
{{- $items = append $items (include (print $.Template.BasePath "/" $name) $ | fromYaml) -}}
{{- end -}}
{{ dict "apiVersion" "v1" "kind" "List" "items" $items | toJson }}
'''


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.chart = Path(self.temporary.name) / 'rindexer'
        shutil.copytree(chart, self.chart)
        (self.chart / 'templates' / 'test-inspection.yaml').write_text(inspection_template)
        self.args = ['--set-string', 'image.digest=' + digest]

    def render(self, *extra, template='deployment.yaml'):
        return subprocess.run(
            [helm, 'template', 'readiness', str(self.chart), '--show-only',
             'templates/' + template, *extra],
            capture_output=True, text=True, timeout=30,
        )

    def resources(self, *extra):
        result = self.render(*self.args, *extra, template='test-inspection.yaml')
        self.assertEqual(result.returncode, 0, result.stderr)
        document = '\n'.join(
            line for line in result.stdout.splitlines()
            if line and not line.startswith(('#', '---'))
        )
        return {
            (item['kind'], item['metadata']['name']): item
            for item in json.loads(document)['items']
        }

    def deployment(self, resources):
        return resources['Deployment', 'rindexer-readiness']

    def test_admits_only_immutable_images(self):
        self.assertNotEqual(self.render().returncode, 0,
                            'an unspecified production image must be refused')
        self.assertNotEqual(self.render('--set', 'image.digest=latest').returncode, 0)
        deployment = self.deployment(self.resources())
        self.assertEqual(deployment['spec']['template']['spec']['containers'][0]['image'],
                         'ghcr.io/numpde/fork-of-rindexer@' + digest)

    def test_replacement_stops_previous_writer(self):
        deployment = self.deployment(self.resources())
        self.assertEqual(deployment['spec']['replicas'], 1)
        self.assertEqual(deployment['spec']['strategy'], {'type': 'Recreate'})

    def test_rejects_multiple_or_invalid_replicas(self):
        for replicas in ['0', '2', '-1', '1.5', 'true']:
            with self.subTest(replicas=replicas):
                self.assertNotEqual(
                    self.render(*self.args, '--set-json', 'replicaCount=' + replicas).returncode,
                    0,
                )

    def test_config_and_abi_changes_roll_out_pods(self):
        original = self.resources()
        original_template = self.deployment(original)['spec']['template']
        self.assertEqual(original_template,
                         self.deployment(self.resources())['spec']['template'],
                         'unchanged chart input must not cause another rollout')
        config_path = self.chart / 'templates' / 'configmap.yaml'
        abi_path = self.chart / 'files' / 'abis' / 'RocketTokenRETH.abi.json'
        config = config_path.read_text()
        abi = abi_path.read_text()
        updated_abi = json.loads(abi) + [
            {'anonymous': False, 'inputs': [], 'name': 'NewEvent', 'type': 'event'},
        ]
        changes = [
            (config_path, config, config.replace("start_block: '18900000'",
                                               "start_block: '18900001'"), 'config'),
            (abi_path, abi, json.dumps(updated_abi, separators=(',', ':')), 'abis'),
        ]
        for path, previous, updated, suffix in changes:
            with self.subTest(resource=suffix):
                try:
                    path.write_text(updated)
                    rendered = self.resources()
                    resource = ('ConfigMap', 'rindexer-readiness-' + suffix)
                    self.assertNotEqual(original[resource]['data'], rendered[resource]['data'])
                    self.assertNotEqual(
                        original_template, self.deployment(rendered)['spec']['template'],
                        suffix + ' changes must change the Pod template even at the same image digest',
                    )
                finally:
                    path.write_text(previous)

    def test_default_security_restricts_container(self):
        pod = self.deployment(self.resources())['spec']['template']['spec']
        with self.subTest(scope='pod'):
            self.assertEqual(pod.get('securityContext'), {
                'runAsUser': 1000,
                'runAsGroup': 3000,
                'fsGroup': 2000,
                'runAsNonRoot': True,
                'seccompProfile': {'type': 'RuntimeDefault'},
            })
        with self.subTest(scope='container'):
            self.assertEqual(pod['containers'][0].get('securityContext'), {
                'allowPrivilegeEscalation': False,
                'capabilities': {'drop': ['ALL']},
            })

    def test_security_overrides_reach_correct_scopes(self):
        rendered = self.resources(
            '--set-json', 'securityContext.runAsUser=1100',
            '--set-json', 'securityContext.runAsGroup=3100',
            '--set-json', 'securityContext.fsGroup=2100',
            '--set-json', 'securityContext.runAsNonRoot=false',
            '--set-json', 'securityContext.allowPrivilegeEscalation=true',
            '--set-json', 'securityContext.dropCapabilities=["NET_RAW","SYS_ADMIN"]',
            '--set-json', 'securityContext.seccompProfile={"type":"Localhost","localhostProfile":"profiles/rindexer.json"}',
        )
        pod = self.deployment(rendered)['spec']['template']['spec']
        with self.subTest(scope='pod'):
            self.assertEqual(pod.get('securityContext'), {
                'runAsUser': 1100,
                'runAsGroup': 3100,
                'fsGroup': 2100,
                'runAsNonRoot': False,
                'seccompProfile': {
                    'type': 'Localhost', 'localhostProfile': 'profiles/rindexer.json',
                },
            })
        with self.subTest(scope='container'):
            self.assertEqual(pod['containers'][0].get('securityContext'), {
                'allowPrivilegeEscalation': True,
                'capabilities': {'drop': ['NET_RAW', 'SYS_ADMIN']},
            })

    def test_security_can_be_disabled(self):
        rendered = self.resources('--set-json', 'securityContext.enabled=false')
        pod = self.deployment(rendered)['spec']['template']['spec']
        self.assertNotIn('securityContext', pod)
        self.assertNotIn('securityContext', pod['containers'][0])



if __name__ == '__main__':
    unittest.main()
