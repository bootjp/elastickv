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

    def test_actual_persisted_marker_expressions_are_blocked(self):
        declarations = [
            (
                "store/mvcc_store.go",
                "mvccSnapshotVersionV8 = uint32(8)",
                "uint32(8)",
                "uint32(9)",
            ),
            (
                "store/snapshot_pebble.go",
                "var pebbleSnapshotMagic = [8]byte{'E', 'K', 'V', 'P', 'B', 'B', 'L', '1'}",
                "'1'",
                "'2'",
            ),
            (
                "kv/snapshot.go",
                "var hlcSnapshotMagicPrefix = [4]byte{\n'E', 'K', 'V', 'H',\n}",
                "'H'",
                "'X'",
            ),
            ("kv/snapshot.go", "const hlcSnapshotVersion = uint32(\n 8\n)", "8", "9"),
        ]
        declarations += [
            (
                "kv/snapshot.go",
                "var hlcSnapshotMagic = [8]byte{'E', 'K', 'V', 'T', 'H', 'L', 'C', '1'}",
                "'1'",
                "'2'",
            ),
            (
                "kv/snapshot.go",
                'var hlcSnapshotMagicPrefix = []byte("EKVTHLC")',
                "EKVTHLC",
                "EKVTHLX",
            ),
        ]
        for path, declaration, old, new in declarations:
            with self.subTest(path=path, declaration=declaration):
                self.assertEqual(
                    "blocked",
                    self.assess(path, declaration, declaration.replace(old, new))[
                        "decision"
                    ],
                )
                report = self.assess(
                    path, declaration, declaration + " // explanatory comment"
                )
                self.assertFalse(
                    any(
                        f["code"] == "persistent-version-change"
                        for f in report["findings"]
                    )
                )

    def test_complete_compound_persisted_marker_expression_is_compared(self):
        expressions = [
            "uint32(8) + 1",
            "uint32(8) +\n 1",
            "(uint32(8) +\n 1)",
            "[2]byte{1, 2}[1]",
        ]
        for value in expressions:
            old = "const snapshotFormatVersion = " + value
            new = old.rsplit("1", 1)[0] + "2" + old.rsplit("1", 1)[1]
            with self.subTest(expression=value):
                self.assertEqual(
                    "blocked", self.assess("store/snapshot.go", old, new)["decision"]
                )

    def test_incomplete_marker_expression_cannot_be_eligible(self):
        report = self.assess(
            "store/snapshot.go",
            "const snapshotFormatVersion = uint32(8)",
            "const snapshotFormatVersion = uint32(8) + (",
        )
        self.assertEqual("blocked", report["decision"])

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

    def test_large_report_keeps_every_file_and_blocker_when_excerpts_are_omitted(self):
        old = {
            f"adapter/{n:04}_{'x' * 100}.go": ("old" * 40 + "\n").encode() * 14
            for n in range(999)
        }
        new = {p: value.replace(b"old", b"new") for p, value in old.items()}
        old["store/snapshot.go"] = b"const snapshotVersion = 1"
        new["store/snapshot.go"] = b"const snapshotVersion = 2"
        report = sr.analyze(
            "example/database",
            OLD,
            NEW,
            IMAGE,
            [{"filename": p, "status": "modified"} for p in old],
            (inventory(old), inventory(new)),
            (old, new),
            sr.DEFAULT_LIMITS,
        )
        self.assertEqual(1000, len(report["files"]))
        self.assertTrue(report["review_excerpts_omitted_for_budget"])
        self.assertLessEqual(len(sr.canonical(report)), 900 * 1024)
        self.assertEqual("blocked", report["decision"])


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

    def large_inventory(self):
        self.old = {f"adapter/file_{n:04}.go": b"old" for n in range(340)}
        self.new = {p: b"new" for p in self.old}
        self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
        self.comparison["files"] = [
            {"filename": p, "status": "modified"} for p in sorted(self.old)[:300]
        ]

    def test_full_trees_cover_changes_beyond_the_comparison_api_cap(self):
        self.large_inventory()
        with self.assertRaises(ValueError):
            self.collect()  # The default 200-file profile remains bounded.
        report = self.collect(limits={"max_files": 1000})
        self.assertEqual(340, len(report["files"]))
        self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))

    def test_hidden_format_edit_and_reader_removal_remain_blockers(self):
        for removed in (False, True):
            with self.subTest(removed=removed):
                self.large_inventory()
                self.old["store/snapshot.go"] = b"const snapshotVersion = 1"
                if not removed:
                    self.new["store/snapshot.go"] = b"const snapshotVersion = 2"
                self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
                report = self.collect(limits={"max_files": 1000})
                self.assertEqual(341, len(report["files"]))
                self.assertEqual("blocked", report["decision"])
                self.assertTrue(
                    any(f["severity"] == "blocker" for f in report["findings"])
                )

    def test_renames_into_persistence_keep_their_blocker_at_the_api_cap(self):
        for count in (1, 299, 340):
            for changed in (False, True):
                with self.subTest(count=count, changed=changed):
                    self.old = {f"adapter/file_{n:04}.go": b"old" for n in range(count)}
                    self.new = {p: b"new" for p in self.old}
                    regular = [
                        {"filename": p, "status": "modified"} for p in sorted(self.old)
                    ]
                    self.old["adapter/legacy.go"] = b"func legacyReader() {}"
                    self.new["store/legacy.go"] = (
                        b"func legacyReader() { /* moved */ }"
                        if changed
                        else self.old["adapter/legacy.go"]
                    )
                    self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
                    self.comparison["files"] = regular[:299] + [
                        {
                            "filename": "store/legacy.go",
                            "previous_filename": "adapter/legacy.go",
                            "status": "renamed",
                        }
                    ]
                    report = self.collect(limits={"max_files": 1000})
                    self.assertEqual("blocked", report["decision"])
                    self.assertFalse(sr.eligible(report, OLD, NEW, IMAGE))
                    moved = next(
                        r for r in report["files"] if r["path"] == "store/legacy.go"
                    )
                    self.assertEqual("renamed", moved["status"])
                    self.assertEqual("adapter/legacy.go", moved["previous_path"])
                    self.assertEqual(count + 1, len(report["files"]))

    def test_capped_rename_hints_must_match_disjoint_removed_and_added_paths(self):
        for previous in ("missing.go", "store/legacy.go", "adapter/file_0000.go"):
            self.large_inventory()
            self.new["store/legacy.go"] = b"reader"
            self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
            self.comparison["files"][-1] = {
                "filename": "store/legacy.go",
                "previous_filename": previous,
                "status": "renamed",
            }
            with self.subTest(previous=previous), self.assertRaises(ValueError):
                self.collect(limits={"max_files": 1000})

    def test_incomplete_small_comparison_and_foreign_prefix_fail_closed(self):
        self.large_inventory()
        self.comparison["files"].pop()
        with self.assertRaises(ValueError):
            self.collect(limits={"max_files": 1000})
        self.large_inventory()
        self.comparison["files"][0]["filename"] = "not-in-either-tree.go"
        with self.assertRaises(ValueError):
            self.collect(limits={"max_files": 1000})

    def test_large_inventory_still_honors_file_byte_and_blob_budgets(self):
        self.large_inventory()
        for limits in (
            {"max_files": 320},
            {"max_files": 1000, "max_total_file_bytes": 1},
        ):
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                self.collect(limits=limits)
        with patch.object(self, "archive", return_value={}):
            with self.assertRaises(ValueError):
                self.collect(limits={"max_files": 1000})

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
