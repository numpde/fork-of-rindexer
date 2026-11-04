"""Exercise rendered chart admission and replacement using the Helm executable."""
import subprocess
import sys
from pathlib import Path

helm = sys.argv[1] if len(sys.argv) > 1 else 'helm'
chart = Path(__file__).resolve().parents[1]
digest = 'sha256:' + 'a' * 64

def render(*extra):
    return subprocess.run([helm, 'template', 'readiness', str(chart), '--show-only', 'templates/deployment.yaml', *extra], capture_output=True, text=True, timeout=30)

assert render().returncode != 0, 'an unspecified production image must be refused'
args = ['--set', 'image.digest=' + digest]
result = render(*args)
assert result.returncode == 0, result.stderr
assert 'ghcr.io/numpde/fork-of-rindexer@' + digest in result.stdout
assert '\n  replicas: 1\n' in result.stdout
assert 'strategy:\n    type: Recreate' in result.stdout, 'replacement must stop the previous writer'
for replicas in ['0', '2', '-1', '1.5', 'true']:
    assert render(*args, '--set-json', 'replicaCount=' + replicas).returncode != 0, replicas
assert render('--set', 'image.digest=latest').returncode != 0
print('Helm deployment admission and replacement checks passed')
