"""Prepare locked dependencies from manifests only; verify before offline use."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib

ROOT = Path('/opt/rindexer')


def command(*args, **kwargs):
    subprocess.run(args, check=True, **kwargs)


def inventory(root):
    result = {}
    for path in sorted(root.rglob('*')):
        key = str(path.relative_to(root))
        if path.is_symlink():
            result[key] = ['link', os.readlink(path)]
        elif path.is_file():
            with path.open('rb') as stream:
                result[key] = ['sha256', hashlib.file_digest(stream, 'sha256').hexdigest()]
    return result


def acquire(update):
    if update not in ('0', '1'):
        raise SystemExit('UPDATE_LOCKS must be 0 or 1')
    work = Path('/input')
    manifests = sorted(work.rglob('Cargo.toml'))
    # Cargo resolves the real manifests, but receives no application source.
    for manifest in manifests:
        spec = tomllib.loads(manifest.read_text())
        if 'package' not in spec:
            continue
        package = manifest.parent
        for default in ('src/lib.rs', 'src/main.rs', 'build.rs'):
            file = package / default
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text('fn main() {}\n')
        targets = [spec.get('lib', {})]
        for kind in ('bin', 'test', 'bench', 'example'):
            targets.extend(spec.get(kind, []))
        for target in targets:
            if 'path' in target:
                file = package / target['path']
                if not file.resolve().is_relative_to(work):
                    raise SystemExit('target path escapes dependency input')
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text('fn main() {}\n')
    if update == '1':
        command('cargo', 'metadata', '--format-version=1', cwd=work, stdout=subprocess.DEVNULL)
        command('npm', 'install', '--package-lock-only', '--ignore-scripts', '--no-audit', '--no-fund', cwd=work / 'graphql')
    with (ROOT / 'cargo-config.toml').open('w') as output:
        command('cargo', 'vendor', '--locked', '--versioned-dirs', str(ROOT / 'vendor'), cwd=work, stdout=output)
    command('npm', 'ci', '--ignore-scripts', '--no-audit', '--no-fund', cwd=work / 'graphql')
    command('node', '-e', "const p=require('path'); const base=p.dirname(require.resolve('@yao-pkg/pkg/package.json')); const f=require(require.resolve('@yao-pkg/pkg-fetch',{paths:[base]})); f.need({nodeRange:'node22',platform:'linux',arch:'x64',forceFetch:true}).then(p=>console.log(p)).catch(e=>{console.error(e);process.exit(1)});", cwd=work / 'graphql')
    (ROOT / 'graphql').mkdir()
    shutil.move(str(work / 'graphql/node_modules'), ROOT / 'graphql/node_modules')
    for file in [*manifests, work / 'Cargo.lock', work / 'rust-toolchain.toml', work / 'graphql/package.json', work / 'graphql/package-lock.json']:
        destination = ROOT / 'inputs' / file.relative_to(work)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, destination)
    # Image layers are read-only during execution. Also check their content,
    # including symlink targets, rather than treating a cache hit as evidence.
    snapshot = {name: inventory(ROOT / name) for name in ('vendor', 'graphql', 'pkg-cache', 'inputs')}
    (ROOT / 'prepared.json').write_text(json.dumps(snapshot, sort_keys=True))


def verify():
    expected = json.loads((ROOT / 'prepared.json').read_text())
    for name, contents in expected.items():
        if inventory(ROOT / name) != contents:
            raise SystemExit(f'prepared dependency content changed: {name}')
    for name, entry in expected['inputs'].items():
        file = Path('/work') / name
        if entry[0] != 'sha256' or not file.is_file():
            raise SystemExit(f'missing prepared input: {name}; run make deps/install')
        with file.open('rb') as stream:
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
        if actual != entry[1]:
            raise SystemExit(f'dependency input changed: {name}; run make deps/install')
    print('Verified locked dependency inputs and installed contents.', flush=True)


if __name__ == '__main__':
    if sys.argv[1:] == ['verify']:
        verify()
    elif len(sys.argv) == 3 and sys.argv[1] == 'acquire':
        acquire(sys.argv[2])
    else:
        raise SystemExit('usage: prepare.py acquire 0|1 | verify')
