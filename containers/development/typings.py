"""Generate on copied source, then check or apply only workspace example typings."""
import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib

MAX_BYTES = 64 * 1024 * 1024
MAX_FILES = 10_000


def output_roots(checkout):
    with (checkout / "Cargo.toml").open("rb") as source:
        members = tomllib.load(source)["workspace"]["members"]
    roots = []
    for member in members:
        if not isinstance(member, str):
            raise ValueError("Cargo workspace members must be path strings")
        if not member.startswith("examples/"):
            continue
        path = PurePosixPath(member)
        if path.is_absolute() or ".." in path.parts or any(c in member for c in ",*?[]") or any(ord(c) < 32 for c in member):
            raise ValueError(f"Workspace example path cannot be mounted unambiguously: {member!r}")
        roots.append(path / "src/rindexer_lib/typings")
    if not roots or len(set(roots)) != len(roots):
        raise ValueError("Cargo.toml must declare distinct workspace examples to refresh")
    return sorted(roots)


def check_path(checkout, relative, directory=False):
    current = checkout
    for index, part in enumerate(relative.parts):
        current /= part
        if current.is_symlink():
            raise ValueError(f"Generated output path contains a symlink: {str(relative)!r}")
        if index < len(relative.parts) - 1 and current.exists() and not current.is_dir():
            raise ValueError(f"Generated output parent is not a directory: {str(relative)!r}")
    if directory and not current.is_dir():
        raise ValueError(f"Generated output directory must already exist: {str(relative)!r}")
    if not directory and current.exists() and not current.is_file():
        raise ValueError(f"Generated output is not a regular file: {str(relative)!r}")
    return current


def current_files(checkout, roots):
    files = {}
    for root in roots:
        directory = check_path(checkout, root, directory=True)
        for path in directory.rglob("*"):
            relative = PurePosixPath(path.relative_to(checkout).as_posix())
            if path.is_symlink():
                raise ValueError(f"Symlink in generated typings: {str(relative)!r}")
            if path.is_dir():
                continue
            if not path.is_file() or path.suffix != ".rs":
                raise ValueError(f"Unexpected non-Rust output in typings directory: {str(relative)!r}")
            files[relative] = path.read_bytes()
    return files


def generated_files(archive, roots):
    files = {}
    total = 0
    with tarfile.open(archive, "r:") as source:
        for entry in source:
            path = PurePosixPath(entry.name)
            if (not entry.isfile() or path.is_absolute() or ".." in path.parts
                    or path.suffix != ".rs" or not any(path.is_relative_to(root) for root in roots)):
                raise ValueError(f"Archive entry is outside generated Rust typings: {entry.name!r}")
            if path in files:
                raise ValueError(f"Duplicate generated output: {entry.name!r}")
            total += entry.size
            if len(files) >= MAX_FILES or total > MAX_BYTES:
                raise ValueError("Generated typings exceed the file or byte budget")
            contents = source.extractfile(entry).read()
            if len(contents) != entry.size:
                raise ValueError(f"Truncated generated output: {entry.name!r}")
            files[path] = contents
    for root in roots:
        if root / "mod.rs" not in files:
            raise ValueError(f"Archive omits the generated module root: {str(root)!r}")
    for path in files:
        if any(parent in files for parent in path.parents):
            raise ValueError(f"Generated file is also an output parent: {str(path)!r}")
    return files


def refresh(checkout, archive, write):
    roots = output_roots(checkout)
    original = current_files(checkout, roots)
    generated = generated_files(archive, roots)
    # Admit the complete candidate before changing any file on the checkout.
    for path in generated:
        check_path(checkout, path)
    changed = sorted(path for path in original.keys() | generated.keys()
                     if original.get(path) != generated.get(path))
    if write:
        for path in changed:
            target = check_path(checkout, path)
            if path not in generated:
                target.unlink()
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".typings-", delete=False) as output:
                    temporary = Path(output.name)
                    output.write(generated[path])
                temporary.chmod(0o644)
                os.replace(temporary, target)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
    return changed


def build_cli(checkout):
    manifest = checkout / "cli/Cargo.toml"
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--frozen", "--no-deps", "--format-version", "1",
        "--manifest-path", str(manifest),
    ]))
    package = next(package for package in metadata["packages"]
                   if Path(package["manifest_path"]) == manifest)
    binaries = [target for target in package["targets"] if target["kind"] == ["bin"]]
    if len(binaries) != 1:
        raise ValueError("The CLI package must have exactly one binary for typing generation")
    subprocess.run([
        "cargo", "build", "--frozen", "--manifest-path", str(manifest),
        "--no-default-features", "--bin", binaries[0]["name"],
    ], check=True, stdout=sys.stderr)
    binary = Path(metadata["target_directory"]) / "debug" / binaries[0]["name"]
    return binary


def generate(checkout):
    roots = output_roots(checkout)
    current_files(checkout, roots)
    binary = build_cli(checkout)
    for root in roots:
        project = checkout / root.parents[2]
        # Remove stale bindings only in the disposable source copy.
        shutil.rmtree(checkout / root)
        (checkout / root).mkdir()
        subprocess.run([str(binary), "codegen", "--path", str(project), "typings"],
                       check=True, stdout=sys.stderr)
    files = current_files(checkout, roots)
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|", dereference=True) as archive:
        for path in sorted(files):
            archive.add(checkout / path, arcname=str(path), recursive=False)


def check_traces(checkout):
    # This existing Rust project is a compilation fixture. Enable traces only in
    # the disposable copy, so generated trace code is exercised without changing
    # the checked-in example's indexing behavior or introducing a second lockfile.
    project = checkout / "examples/rindexer_rust_playground"
    manifest = project / "rindexer.yaml"
    original = manifest.read_text()
    if any(line.startswith("native_transfers:") for line in original.splitlines()):
        raise ValueError("The trace compilation fixture must start without native_transfers")
    binary = build_cli(checkout)
    manifest.write_text(original + "\nnative_transfers: true\n")
    subprocess.run([str(binary), "codegen", "--path", str(project), "typings"],
                   check=True, stdout=sys.stderr)
    with (project / "Cargo.toml").open("rb") as source:
        package = tomllib.load(source)["package"]["name"]
    subprocess.run(["cargo", "clippy", "--frozen", "-p", package, "--all-targets",
                    "--", "-D", "warnings"], check=True, stdout=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["roots", "generate", "check", "update", "check-traces"])
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--archive", type=Path)
    arguments = parser.parse_args()
    checkout = arguments.root.resolve(strict=True)
    try:
        if arguments.action == "roots":
            for root in output_roots(checkout):
                print(root)
            return 0
        if arguments.action == "generate":
            generate(checkout)
            return 0
        if arguments.action == "check-traces":
            check_traces(checkout)
            return 0
        if arguments.archive is None:
            parser.error("check and update require --archive")
        changed = refresh(checkout, arguments.archive, arguments.action == "update")
        for path in changed:
            print(f"Generated typings differ: {str(path)!r}")
        return int(bool(changed) and arguments.action == "check")
    except (ValueError, OSError, tarfile.TarError, subprocess.CalledProcessError) as error:
        print(f"Cannot {arguments.action} example typings: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
