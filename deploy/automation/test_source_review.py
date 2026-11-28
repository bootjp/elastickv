import copy
import gzip
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
        return self.assess_bytes(path, before.encode(), after.encode(), status)

    def assess_bytes(self, path, before, after, status="modified"):
        old = {path: before} if status != "added" else {}
        new = {path: after} if status != "removed" else {}
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

    def test_diagnostic_gzip_artifacts_remain_in_the_verified_report(self):
        for suffix in (".pprof", ".cpu", ".block", ".mutex", ".log.gz", ".tar.gz"):
            for status in ("added", "modified", "removed"):
                with self.subTest(suffix=suffix, status=status):
                    path = "docs/evidence/trial/profile" + suffix
                    old, new = (
                        gzip.compress(b"old evidence"),
                        gzip.compress(b"new evidence"),
                    )
                    with patch.object(
                        sr.gzip,
                        "decompress",
                        side_effect=AssertionError("Do not expand opaque evidence"),
                    ):
                        report = self.assess_bytes(path, old, new, status)
                    self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))
                    self.assertEqual(path, report["files"][0]["path"])
                    self.assertEqual(
                        sr.git_blob(old) if status != "added" else None,
                        report["files"][0]["old_blob"],
                    )
                    self.assertEqual(
                        sr.git_blob(new) if status != "removed" else None,
                        report["files"][0]["new_blob"],
                    )
                    self.assertTrue(
                        any(
                            f["code"] == "diagnostic-evidence"
                            for f in report["findings"]
                        )
                    )

    def test_evidence_classification_does_not_allow_other_binary_source(self):
        for path, data in [
            ("store/snapshot.tar.gz", gzip.compress(b"snapshot")),
            ("proto/schema.cpu", gzip.compress(b"wire")),
            ("adapter/profile.cpu", gzip.compress(b"runtime")),
            ("docs/other/profile.cpu", gzip.compress(b"unknown")),
            ("docs/evidence/profile.cpu", b"\x7fELF\xff"),
            ("docs/evidence/helper.py", b"\x00\xff"),
            ("docs/evidence/unknown.bin", gzip.compress(b"unknown")),
            ("docs/evidence/../runtime/profile.cpu", gzip.compress(b"alias")),
        ]:
            with self.subTest(path=path):
                self.assertEqual(
                    "blocked", self.assess_bytes(path, b"", data, "added")["decision"]
                )

    def test_evidence_classification_keeps_code_and_wire_analysis(self):
        for path, old, new in [
            (
                "docs/evidence/example.go",
                'flag.Bool("commitTSFence", false, "")',
                'flag.Bool("commitTSFence", true, "")',
            ),
            (
                "docs/evidence/example.proto",
                "message KV {\nstring key=1;\n}\n",
                "message KV {\nbytes key=1;\n}\n",
            ),
        ]:
            with self.subTest(path=path):
                self.assertEqual("blocked", self.assess(path, old, new)["decision"])

    def test_evidence_executable_and_symlink_modes_are_not_exempt(self):
        path = "docs/evidence/trial/profile.cpu"
        data = {path: gzip.compress(b"profile")}
        for mode in ("100755", "120000", "160000"):
            with self.subTest(mode=mode):
                tree = inventory(data)
                tree[path]["mode"] = mode
                report = sr.analyze(
                    "example/database",
                    OLD,
                    NEW,
                    IMAGE,
                    [{"filename": path, "status": "added"}],
                    ({}, tree),
                    ({}, data),
                    sr.DEFAULT_LIMITS,
                )
                self.assertEqual("blocked", report["decision"])

    def test_evidence_still_uses_complete_collection_hashes_and_budgets(self):
        f = InventoryTests()
        f.setUp()
        path = "docs/evidence/trial/profile.cpu"
        f.new[path] = gzip.compress(b"profile")
        f.trees[NEW] = inventory(f.new)
        f.comparison["files"].append({"filename": path, "status": "added"})
        report = f.collect()
        self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))
        self.assertEqual(2, len(report["files"]))
        for limit in (
            {"max_files": 1},
            {"max_file_bytes": 8},
            {"max_total_file_bytes": 10},
        ):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                f.collect(limits=limit)
        f.new[path] = gzip.compress(b"different profile")
        with self.assertRaisesRegex(ValueError, "immutable blobs"):
            f.collect()

    def test_binary_rename_into_evidence_does_not_hide_reader_or_unknown_binary(self):
        for prior, path in [
            ("store/reader.go", "docs/evidence/reader.cpu"),
            ("adapter/reader.bin", "docs/evidence/reader.cpu"),
            ("docs/evidence/reader.cpu", "adapter/reader.bin"),
        ]:
            with self.subTest(prior=prior, path=path):
                old = {prior: gzip.compress(b"old")}
                new = {path: gzip.compress(b"new")}
                report = sr.analyze(
                    "example/database",
                    OLD,
                    NEW,
                    IMAGE,
                    [
                        {
                            "filename": path,
                            "previous_filename": prior,
                            "status": "renamed",
                        }
                    ],
                    (inventory(old), inventory(new)),
                    (old, new),
                    sr.DEFAULT_LIMITS,
                )
                self.assertEqual("blocked", report["decision"])

    def test_export_only_marker_rename_preserves_value_and_reports_attention(self):
        for old, new in [
            (
                "const forwardDecisionSchemaVersion = 1",
                "const ForwardDecisionSchemaVersion uint32 = 1",
            ),
            (
                "const snapshotFormatVersion uint32 = 1",
                "const SnapshotFormatVersion uint32 = 1",
            ),
            (
                "const SnapshotFormatVersion uint32 = 1",
                "const snapshotFormatVersion uint32 = 1",
            ),
            ('const snapshotMagic = "EKV"', 'const SnapshotMagic = "EKV"'),
        ]:
            with self.subTest(old=old, new=new):
                report = self.assess("kv/format.go", old, new)
                self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))
                self.assertTrue(
                    any(
                        f["code"] == "persistent-marker-rename"
                        for f in report["findings"]
                    )
                )

    def test_marker_rename_never_hides_value_type_or_kind_change(self):
        cases = [
            ("var snapshotVersion = 1", "var SnapshotVersion = 1"),
            ("const snapshotVersion = 1", "const SnapshotVersion uint32 = 2"),
            ("const snapshotVersion uint32 = 1", "const SnapshotVersion uint64 = 1"),
            ("const snapshotVersion = 1", "var SnapshotVersion uint32 = 1"),
            ("const snapshotVersion = 1", "const SnapshotVersion int = 1"),
            ("const snapshotVersion = 256", "const SnapshotVersion uint8 = 256"),
            ("const snapshotVersion = -1", "const SnapshotVersion uint32 = -1"),
            (
                "const snapshotVersion = currentVersion",
                "const SnapshotVersion uint32 = currentVersion",
            ),
            ("const snapshotVersion = 1", "const SnapshotVersion uint32 = 1 + ("),
            ('var snapshotMagic = []byte("EKV")', 'var SnapshotMagic = []byte("BAD")'),
            ("const snapshotVersion = 1", "const otherSchemaVersion = 1"),
            ("const snapshotVersion = 1", ""),
            (
                "const snapshotVersion = 1\nconst SnapshotVersion = 2",
                "const SnapshotVersion = 1",
            ),
            (
                "const snapshotVersion = 1",
                "const SnapshotVersion = 1\nconst SnapshotVersion = 1",
            ),
            (
                "const snapshotVersion = 1",
                "const snapshotVersion = 2\nconst SnapshotVersion = 1",
            ),
        ]
        for old, new in cases:
            with self.subTest(old=old, new=new):
                self.assertEqual(
                    "blocked", self.assess("store/format.go", old, new)["decision"]
                )

    def test_explicit_marker_type_change_remains_blocked_without_rename(self):
        self.assertEqual(
            "blocked",
            self.assess(
                "store/format.go",
                "const snapshotVersion uint32 = 1",
                "const snapshotVersion uint64 = 1",
            )["decision"],
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

    def test_omitted_implementation_moves_into_persistence_are_blocked(self):
        for changed in (False, True):
            self.large_inventory()
            self.old["adapter/legacy.go"] = b"func legacyReader() {}"
            self.new["store/legacy.go"] = (
                b"func legacyReader() { /* moved */ }"
                if changed
                else self.old["adapter/legacy.go"]
            )
            self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
            with self.subTest(changed=changed):
                report = self.collect(limits={"max_files": 1000})
                self.assertEqual(342, len(report["files"]))
                self.assertEqual("blocked", report["decision"])
                self.assertFalse(sr.eligible(report, OLD, NEW, IMAGE))
                self.assertTrue(
                    any(
                        f["code"] == "persistent-move-unresolved"
                        for f in report["findings"]
                    )
                )

    def test_document_or_test_deletion_does_not_imply_an_implementation_move(self):
        for removed in ("docs/legacy.md", "adapter/legacy_test.go"):
            self.large_inventory()
            self.old[removed] = b"old"
            self.new["store/new.go"] = b"func newReader() {}"
            self.trees = {OLD: inventory(self.old), NEW: inventory(self.new)}
            with self.subTest(removed=removed):
                report = self.collect(limits={"max_files": 1000})
                self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))

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


class RelocationProfileTests(unittest.TestCase):
    def fixture(self):
        f = InventoryTests()
        f.setUp()
        f.large_inventory()
        f.old["assembly.go"] = b"func oldReader() {}"
        f.new["internal/server/assembly.go"] = b"func newReader() {}"
        f.new["store/add.go"] = b"func added() {}"
        f.trees = {OLD: inventory(f.old), NEW: inventory(f.new)}
        profile = {
            "version": 1,
            "repository": "example/database",
            "from_source": OLD,
            "to_source": NEW,
            "image": IMAGE,
            "relocations": [
                {
                    "old_path": "assembly.go",
                    "old_blob": sr.git_blob(f.old["assembly.go"]),
                    "targets": [
                        {
                            "path": "internal/server/assembly.go",
                            "blob": sr.git_blob(f.new["internal/server/assembly.go"]),
                        }
                    ],
                    "review": "Reviewed assembly relocation; persistence formats are still assessed separately.",
                }
            ],
        }
        return f, profile

    def test_exact_trusted_profile_resolves_only_its_nonpersistent_removals(self):
        f, profile = self.fixture()
        self.assertEqual("blocked", f.collect(limits={"max_files": 1000})["decision"])
        report = f.collect(limits={"max_files": 1000}, relocation_profile=profile)
        self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))
        self.assertEqual(sr.digest(profile), report["relocation_profile_sha256"])
        self.assertEqual(["assembly.go"], report["resolved_nonpersistent_removals"])

    def test_pair_image_repository_and_blob_changes_are_rejected(self):
        f, profile = self.fixture()
        for field, value in [
            ("from_source", "c" * 40),
            ("to_source", "d" * 40),
            ("repository", "other/database"),
            ("image", IMAGE[:-1] + "d"),
        ]:
            bad = copy.deepcopy(profile)
            bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                f.collect(limits={"max_files": 1000}, relocation_profile=bad)
        for field in ("old_blob", "target_blob"):
            bad = copy.deepcopy(profile)
            if field == "old_blob":
                bad["relocations"][0]["old_blob"] = "f" * 40
            else:
                bad["relocations"][0]["targets"][0]["blob"] = "f" * 40
            with self.subTest(field=field), self.assertRaises(ValueError):
                f.collect(limits={"max_files": 1000}, relocation_profile=bad)

    def test_missing_duplicate_or_unreviewed_targets_fail_closed(self):
        for failure in ("missing", "duplicate", "review", "target", "test_target"):
            f, profile = self.fixture()
            row = profile["relocations"][0]
            if failure == "missing":
                row["targets"][0]["path"] = "missing.go"
            if failure == "duplicate":
                profile["relocations"].append(copy.deepcopy(row))
            if failure == "review":
                row["review"] = ""
            if failure == "target":
                row["targets"] = []
            if failure == "test_target":
                row["targets"][0]["path"] = "internal/server/assembly_test.go"
            with self.subTest(failure=failure), self.assertRaises(ValueError):
                f.collect(limits={"max_files": 1000}, relocation_profile=profile)

    def test_additional_deletions_and_format_changes_are_not_overridden(self):
        for kind in ("unregistered", "format"):
            f, profile = self.fixture()
            if kind == "unregistered":
                f.old["another.go"] = b"func another() {}"
            else:
                f.old["store/format.go"] = b"const snapshotVersion = 1"
                f.new["store/format.go"] = b"const snapshotVersion = 2"
            f.trees = {OLD: inventory(f.old), NEW: inventory(f.new)}
            with self.subTest(kind=kind):
                report = f.collect(
                    limits={"max_files": 1000}, relocation_profile=profile
                )
                self.assertEqual("blocked", report["decision"])

    def test_reviewed_move_into_persistence_never_waives_another_reader_removal(self):
        f, profile = self.fixture()
        target = profile["relocations"][0]["targets"][0]
        f.new["kv/client.go"] = f.new.pop(target["path"])
        target["path"] = "kv/client.go"
        f.trees[NEW] = inventory(f.new)
        report = f.collect(limits={"max_files": 1000}, relocation_profile=profile)
        self.assertTrue(sr.eligible(report, OLD, NEW, IMAGE))
        f.old["store/reader.go"] = b"func readExistingRecord() {}"
        f.trees[OLD] = inventory(f.old)
        report = f.collect(limits={"max_files": 1000}, relocation_profile=profile)
        self.assertEqual("blocked", report["decision"])
        self.assertTrue(
            any(r["code"] == "persistent-contract-removal" for r in report["findings"])
        )

    def test_persistent_reader_removal_cannot_be_classified_by_profile(self):
        f, profile = self.fixture()
        f.old["store/reader.go"] = f.old.pop("assembly.go")
        f.trees[OLD] = inventory(f.old)
        profile["relocations"][0]["old_path"] = "store/reader.go"
        with self.assertRaises(ValueError):
            f.collect(limits={"max_files": 1000}, relocation_profile=profile)

    def test_bootstrap_target_is_exact_and_retires_after_baseline_advances(self):
        _, profile = self.fixture()
        config = {"relocation_profile": profile}
        head = "c" * 40
        self.assertEqual(NEW, sr.candidate_source(config, OLD, head))
        self.assertEqual(head, sr.candidate_source(config, NEW, head))
        self.assertIsNone(sr.active_profile(config, NEW))


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
