"""Validate the rendered single-host deployment without printing its credentials."""
import json
from pathlib import Path
import re
import sys

config = json.loads(Path(sys.argv[1]).read_text())
services = config['services']
if set(services) != {'indexer', 'postgres'}:
    raise SystemExit('Runtime must contain only the indexer and PostgreSQL services')
indexer, postgres = services['indexer'], services['postgres']
if not re.fullmatch(r'sha256:[a-f0-9]{64}', indexer['image']):
    raise SystemExit('RINDEXER_IMAGE must be an immutable local image ID (sha256:...)')
if indexer.get('container_name') != 'rindexer-postgres-indexer':
    raise SystemExit('Runtime requires its fixed single-writer container name')
password = postgres['environment']['POSTGRES_PASSWORD']
if not re.fullmatch(r'[a-fA-F0-9]{32,}', password):
    raise SystemExit('POSTGRES_PASSWORD must contain at least 32 hexadecimal characters; URL encoding is otherwise required')
mounts = indexer['volumes']
if len(mounts) != 1 or mounts[0]['target'] != '/project' or not mounts[0]['read_only']:
    raise SystemExit('Runtime requires exactly one read-only project bind')
project = Path(mounts[0]['source'])
if not project.is_dir() or not (project / 'rindexer.yaml').is_file():
    raise SystemExit('RINDEXER_PROJECT_DIR must be a pre-existing project with rindexer.yaml')
if (project / '.env').exists():
    raise SystemExit('Keep credentials in the private runtime env file; the read-only project must not contain .env')
if postgres.get('ports') or set(postgres['networks']) != {'database'}:
    raise SystemExit('PostgreSQL must have no published port and only the private database network')
if not config['networks']['database'].get('internal'):
    raise SystemExit('The database network must remain internal')
for name, service in services.items():
    if service.get('user') != ('65532:65532' if name == 'indexer' else '999:999') or not service.get('read_only'):
        raise SystemExit(f'{name} requires its non-root user and a read-only root filesystem')
    if service.get('cap_drop') != ['ALL'] or service.get('privileged', False):
        raise SystemExit(f'{name} must drop all capabilities and forbid privileged mode')
