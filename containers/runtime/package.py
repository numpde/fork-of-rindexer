"""Export the compiled no-feature CLI and its runtime dependencies as build context."""
import io
from pathlib import Path
import re
import subprocess
import sys
import tarfile

binary = Path('/build/target/release/rindexer_cli')
linked = subprocess.run(['ldd', str(binary)], check=True, capture_output=True, text=True).stdout
if 'not found' in linked:
    raise SystemExit('Cannot package runtime: a linked library is unavailable')
# ldd names the actual ELF loader and resolved shared objects with absolute paths.
# Preserve their original locations; dereference symlinks into regular image files.
libraries = {Path(path) for path in re.findall(r'(/[^\s()]+)', linked)}
if not libraries:
    raise SystemExit('Cannot package runtime: ldd returned no resolved libraries')
files = {'rootfs/usr/local/bin/rindexer': binary,
         'rootfs/etc/ssl/certs/ca-certificates.crt': Path('/etc/ssl/certs/ca-certificates.crt'),
         'rootfs/etc/nsswitch.conf': Path('/etc/nsswitch.conf'),
         'Dockerfile': Path('containers/runtime/Dockerfile')}
files.update({f'rootfs{path}': path for path in libraries})
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|') as archive:
    for name, path in sorted(files.items()):
        contents = path.read_bytes()
        entry = tarfile.TarInfo(name)
        entry.size = len(contents)
        entry.mode = 0o755 if name.endswith('/rindexer') or path in libraries else 0o644
        archive.addfile(entry, io.BytesIO(contents))
    for directory in ['rootfs/project', 'rootfs/tmp']:
        entry = tarfile.TarInfo(directory)
        entry.type = tarfile.DIRTYPE
        entry.mode = 0o1777 if directory.endswith('/tmp') else 0o755
        archive.addfile(entry)
