import copy
import io
import tarfile
import unittest
from unittest.mock import patch

import source_review as sr

OLD = "a" * 40
NEW = "b" * 40
IMAGE = "ghcr.io/example/database@sha256:" + "c" * 64


def inventory(files):
    return {
        p: {
            "path": p,
            "type": "blob",
            "mode": "100644",
            "sha": sr.git_blob(v),
            "size": len(v),
        }
        for p, v in files.items()
    }


class SourceAuditTests(unittest.TestCase):
    def assess(self, path, before, after, status="modified"):
        old = {path: before.encode()}
        new = {path: after.encode()} if status != "removed" else {}
        return sr.analyze(
            "example/database",
            OLD,
            NEW,
            IMAGE,
            [{"filename": path, "status": status}],
            (inventory(old), inventory(new)),
            (old, new),
            sr.DEFAULT_LIMITS,
        )

    def test_runtime_code_is_assessed_without_manual_source_pair(self):
        report = self.assess(
            "store/read.go",
            "func read() int { return 1 }",
            "func read() int { return 2 }",
        )
        self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))
        self.assertEqual("approval-required", report["decision"])
        self.assertTrue(
            any(f["code"] == "persistence-logic" for f in report["findings"])
        )
        self.assertTrue(report["files"][0]["review_excerpt"])

    def test_known_wire_and_persisted_format_breaks_are_blocked(self):
        for path, old, new in [
            (
                "proto/kv.proto",
                "message KV {\n string key=1;\n}\n",
                "message KV {\n bytes key=1;\n}\n",
            ),
            (
                "store/snapshot.go",
                "const snapshotFormatVersion uint32 = 1",
                "const snapshotFormatVersion uint32 = 2",
            ),
            (
                "store/codec.go",
                'const (\n diskMagic = "old"\n)',
                'const (\n diskMagic = "new"\n)',
            ),
            (
                "main.go",
                'flag.Bool("commitTSFence", false, "fence")',
                'flag.Bool("commitTSFence", true, "fence")',
            ),
        ]:
            with self.subTest(path=path):
                report = self.assess(path, old, new)
                self.assertFalse(sr.eligible(report, OLD, NEW, IMAGE))
                self.assertEqual("blocked", report["decision"])

    def test_actual_server_flag_registration_defaults_are_blocked(self):
        registrations = [
            (
                "internal/encryptionwiring/kek.go",
                'fs.BoolVar(&f.Enabled, "encryption-enabled", false, "enable")',
                "false",
                "true",
            ),
            (
                "internal/server/commit_ts_fence.go",
                'fs.BoolVar(&f.commitTSFence, "commitTSFence", false, "fence")',
                "false",
                "true",
            ),
            (
                "internal/tsowiring/tso.go",
                'fs.BoolVar(&f.PhaseDEnabled, "tsoPhaseDEnabled", false, "phase")',
                "false",
                "true",
            ),
            (
                "internal/tsowiring/tso.go",
                'fs.BoolVar(&f.Enabled, "tsoEnabled", false, "enable")',
                "false",
                "true",
            ),
            (
                "internal/tsowiring/tso.go",
                'fs.StringVar(\n &f.ModeFile,\n "tsoModeFile", "", "mode")',
                '""',
                '"cutover"',
            ),
            (
                "main.go",
                'fs.String("tsoMode", "legacy", "mode")',
                '"legacy"',
                '"phase-d"',
            ),
        ]
        for path, registration, old, new in registrations:
            with self.subTest(registration=registration):
                report = self.assess(path, registration, registration.replace(old, new))
                self.assertEqual("blocked", report["decision"])
                self.assertTrue(
                    any(
                        f["code"] == "feature-default-activation"
                        for f in report["findings"]
                    )
                )
                self.assertEqual(
                    "approval-required",
                    self.assess(path, registration, registration)["decision"],
                )

    def test_reader_removal_is_blocked(self):
        self.assertEqual(
            "blocked",
            self.assess("store/legacy.go", "func legacy() {}", "", "removed")[
                "decision"
            ],
        )

    def test_additive_wire_change_retains_existing_declarations(self):
        report = self.assess(
            "proto/kv.proto",
            "message KV {\n string key=1;\n}\n",
            "message KV {\n string key=1;\n string value=2;\n}\n",
        )
        self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))

    def test_digest_and_identity_do_not_follow_a_changed_image_or_reviewer(self):
        report = self.assess("adapter/redis.go", "old", "new")
        self.assertFalse(sr.eligible(report, OLD, NEW, "other"))
        changed = copy.deepcopy(report)
        changed["reviewer_sha256"] = "stale"
        self.assertFalse(sr.eligible(changed, OLD, NEW, IMAGE))
        changed = copy.deepcopy(report)
        changed["findings"].append({"severity": "blocker"})
        self.assertNotEqual(sr.digest(report), sr.digest(changed))


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.old = {"store/read.go": b"old", "README.md": b"same"}
        self.new = {"store/read.go": b"new", "README.md": b"same"}
        self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
        self.comparison = {
            "status": "ahead",
            "files": [{"filename": "store/read.go", "status": "modified"}],
        }

    def fetch(self, path):
        sha = path.split("/")[2].split("?")[0]
        return {"truncated": False, "tree": list(self.trees[sha].values())}

    def archive(self, repo, sha, needed, limits, tick):
        contents = self.old if sha == OLD else self.new
        return {p: contents[p] for p in needed}

    def collect(self, **kw):
        return sr.collect(
            "example/database",
            OLD,
            NEW,
            IMAGE,
            self.comparison,
            self.fetch,
            archive=self.archive,
            **kw,
        )

    def test_full_source_inventory_is_covered(self):
        self.assertTrue(self.collect()["complete"])

    def test_omitted_changed_files_cannot_produce_approval(self):
        self.new["README.md"] = b"changed"
        self.trees[NEW] = inventory(self.new)
        with self.assertRaises(ValueError):
            self.collect()

    def test_aggregate_source_memory_budget_fails_closed(self):
        with self.assertRaises(ValueError):
            self.collect(limits={"max_total_file_bytes": 1})

    def test_truncated_tree_comparison_and_invalid_limits_fail_closed(self):
        with patch.object(self, "fetch", return_value={"truncated": True, "tree": []}):
            with self.assertRaises(ValueError):
                self.collect()
        with self.assertRaises(ValueError):
            self.collect(limits={"max_files": 0})
        with self.assertRaises(ValueError):
            self.collect(limits={"max_files": 10000})
        self.comparison["files"] = self.comparison["files"] * 300
        with self.assertRaises(ValueError):
            self.collect()


class ArchiveTests(unittest.TestCase):
    def archive(self, path="root/store/read.go", data=b"hello", symlink=False):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz") as tf:
            member = tarfile.TarInfo(path)
            member.size = len(data)
            if symlink:
                member.type = tarfile.SYMTYPE
                member.linkname = "/etc/passwd"
                member.size = 0
            tf.addfile(member, io.BytesIO(data) if not symlink else None)
        return stream.getvalue()

    def load(self, archive, sha=None, **limits):
        needed = inventory({"store/read.go": b"hello"})
        if sha:
            needed["store/read.go"]["sha"] = sha
        with patch.object(
            sr.urllib.request, "urlopen", return_value=io.BytesIO(archive)
        ):
            return sr.download_archive(
                "example/database", OLD, needed, {**sr.DEFAULT_LIMITS, **limits}
            )

    def test_archive_blobs_are_verified_without_extracting_files(self):
        self.assertEqual({"store/read.go": b"hello"}, self.load(self.archive()))

    def test_tampered_missing_or_symlink_entries_are_refused(self):
        for archive, sha in [
            (self.archive(data=b"evil"), None),
            (self.archive(symlink=True), None),
            (self.archive(path="root/other.go"), None),
            (self.archive(), "wrong"),
        ]:
            with self.subTest(sha=sha), self.assertRaises(ValueError):
                self.load(archive, sha)

    def test_traversal_and_size_budgets_are_enforced(self):
        with self.assertRaises(ValueError):
            self.load(self.archive(path="root/../store/read.go"))
        with self.assertRaises(ValueError):
            self.load(self.archive(), max_archive_bytes=10)
        with self.assertRaises(ValueError):
            self.load(self.archive(), max_expanded_bytes=10)
        with self.assertRaises(ValueError):
            self.load(self.archive(), max_file_bytes=2)


if __name__ == "__main__":
    unittest.main()
