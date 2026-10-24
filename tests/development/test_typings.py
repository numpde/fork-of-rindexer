"""Typing refresh admits the whole archive before writing owned output paths."""
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("typings", "containers/development/typings.py")
typings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(typings)


class TypingRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["core", "examples/demo"]\n')
        self.output = Path("examples/demo/src/rindexer_lib/typings")
        (self.root / self.output).mkdir(parents=True)
        (self.root / self.output / "mod.rs").write_bytes(b"old module")
        self.archive = self.root / "candidate.tar"

    def candidate(self, entries):
        with tarfile.open(self.archive, "w") as archive:
            for name, contents, kind in entries:
                entry = tarfile.TarInfo(name)
                entry.type = kind
                entry.size = len(contents) if kind == tarfile.REGTYPE else 0
                entry.linkname = "/outside"
                archive.addfile(entry, io.BytesIO(contents) if entry.isfile() else None)

    def module(self, contents=b"new module"):
        return (str(self.output / "mod.rs"), contents, tarfile.REGTYPE)

    def test_check_reports_drift_without_writing_and_update_removes_stale_files(self):
        (self.root / self.output / "stale.rs").write_bytes(b"stale")
        self.candidate([self.module(), (str(self.output / "event.rs"), b"event", tarfile.REGTYPE)])
        before = typings.current_files(self.root, typings.output_roots(self.root))
        self.assertEqual(3, len(typings.refresh(self.root, self.archive, False)))
        self.assertEqual(before, typings.current_files(self.root, typings.output_roots(self.root)))
        self.assertEqual(3, len(typings.refresh(self.root, self.archive, True)))
        self.assertFalse((self.root / self.output / "stale.rs").exists())
        self.assertEqual(b"new module", (self.root / self.output / "mod.rs").read_bytes())
        self.assertEqual([], typings.refresh(self.root, self.archive, False))

    def test_unsafe_archives_fail_before_any_write(self):
        invalid = [
            ("core/src/lib.rs", b"outside", tarfile.REGTYPE),
            (str(self.output / "../escape.rs"), b"escape", tarfile.REGTYPE),
            ("/absolute.rs", b"absolute", tarfile.REGTYPE),
            (str(self.output / "link.rs"), b"", tarfile.SYMTYPE),
            (str(self.output / "hard.rs"), b"", tarfile.LNKTYPE),
            (str(self.output / "not-rust.txt"), b"text", tarfile.REGTYPE),
            self.module(b"duplicate"),
            (str(self.output / "mod.rs/child.rs"), b"child", tarfile.REGTYPE),
        ]
        for entry in invalid:
            with self.subTest(entry=entry[0]):
                self.candidate([self.module(), entry])
                with self.assertRaises(ValueError):
                    typings.refresh(self.root, self.archive, True)
                self.assertEqual(b"old module", (self.root / self.output / "mod.rs").read_bytes())

    def test_missing_module_and_byte_budget_are_rejected(self):
        self.candidate([(str(self.output / "event.rs"), b"event", tarfile.REGTYPE)])
        with self.assertRaisesRegex(ValueError, "module root"):
            typings.refresh(self.root, self.archive, True)
        self.candidate([self.module()])
        original_limit = typings.MAX_BYTES
        try:
            typings.MAX_BYTES = 1
            with self.assertRaisesRegex(ValueError, "budget"):
                typings.refresh(self.root, self.archive, True)
        finally:
            typings.MAX_BYTES = original_limit
        self.assertEqual(b"old module", (self.root / self.output / "mod.rs").read_bytes())

    def test_host_output_symlink_is_rejected_without_writing_target(self):
        outside = self.root / "outside.rs"
        outside.write_bytes(b"outside")
        (self.root / self.output / "event.rs").symlink_to(outside)
        self.candidate([self.module(), (str(self.output / "event.rs"), b"event", tarfile.REGTYPE)])
        with self.assertRaisesRegex(ValueError, "Symlink"):
            typings.refresh(self.root, self.archive, True)
        self.assertEqual(b"outside", outside.read_bytes())
        self.assertEqual(b"old module", (self.root / self.output / "mod.rs").read_bytes())

    def test_workspace_inventory_rejects_ambiguous_mount_paths(self):
        for member in ["examples/../escape", "examples/a,b", "examples/*", "examples/demo\nother"]:
            with self.subTest(member=member):
                import json
                (self.root / "Cargo.toml").write_text('[workspace]\nmembers = [' + json.dumps(member) + ']\n')
                with self.assertRaises(ValueError):
                    typings.output_roots(self.root)
