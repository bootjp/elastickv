import copy
import gzip
import hashlib
import json
import pathlib
import tempfile
import unittest

import check_elastickv_compatibility as check


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        source = "b" * 40
        path = "docs/operations/elastickv-compatibility/" + source + "/summary.json"
        self.path = self.root / path
        self.path.parent.mkdir(parents=True)
        driver = b"# Synthetic test fixture, not executed\n"
        (self.path.parent / "isolated_driver.py").write_bytes(driver)
        cases = {
            n: {
                "minimum_ready": 2,
                "observation_errors": 0,
                "data": {
                    "redis_keys_verified_on_each_of_three_members": 128,
                    "dynamodb_consistent_read": True,
                    "s3_bytes": 262144,
                    "sqs_message_bodies_preserved": 3,
                },
            }
            for n in ["upgrade", "downgrade", "reupgrade", "snapshot-restart"]
        }
        samples = {n: [{"ready": 2}, {"ready": 3}] for n in cases}
        (self.path.parent / "observations.json.gz").write_bytes(
            gzip.compress(json.dumps(samples).encode())
        )
        self.report = {
            "passed": True,
            "from_source": "a" * 40,
            "to_source": source,
            "from_image": "ghcr.io/example/elastickv@sha256:" + "a" * 64,
            "to_image": "ghcr.io/example/elastickv@sha256:" + "b" * 64,
            "scope_kind": "isolated-synthetic-kubernetes",
            "production_data_used": False,
            "new_features": {
                "commit_ts_fence": False,
                "timestamp_binding": False,
                "phase_d": False,
            },
            "pvc_uids": {"a": "u1", "b": "u2", "c": "u3"},
            "cases": cases,
            "snapshots_before_restart": {
                n: {"last_snapshot_index": "64"} for n in ["a", "b", "c"]
            },
            "driver_sha256": hashlib.sha256(driver).hexdigest(),
        }
        self.before = {"approved_transitions": [], "safe_paths": ["docs/*"]}
        self.after = {
            **self.before,
            "approved_transitions": [
                {
                    "from": self.report["from_source"],
                    "to": self.report["to_source"],
                    "readiness": True,
                    "evidence": path,
                    "image": self.report["to_image"],
                }
            ],
        }
        self.baseline = {
            "last_good": {
                "source": self.report["from_source"],
                "image": self.report["from_image"],
            },
            "transaction": None,
        }

    def run_check(self):
        self.path.write_text(json.dumps(self.report))
        return check.validate(self.before, self.after, self.baseline, self.root)

    def test_complete_isolated_evidence_is_accepted(self):
        self.assertEqual(self.report["to_source"], self.run_check()["to"])

    def test_failed_or_missing_scenarios_and_unsafe_features_are_refused(self):
        original = copy.deepcopy(self.report)
        for mutation in [
            lambda r: r.update(passed=False),
            lambda r: r.update(production_data_used=True),
            lambda r: r["cases"].pop("downgrade"),
            lambda r: r["cases"]["upgrade"].update(minimum_ready=1),
            lambda r: r["new_features"].update(commit_ts_fence=True),
        ]:
            with self.subTest(mutation=mutation):
                self.report = copy.deepcopy(original)
                mutation(self.report)
                with self.assertRaises(ValueError):
                    self.run_check()

    def test_policy_weakening_and_changed_baseline_are_refused(self):
        self.after["safe_paths"] = ["*"]
        with self.assertRaises(ValueError):
            self.run_check()
        self.after["safe_paths"] = self.before["safe_paths"]
        self.baseline["last_good"]["source"] = "c" * 40
        with self.assertRaises(ValueError):
            self.run_check()


class StrengtheningTests(unittest.TestCase):
    def test_enable_without_permission_and_reject_disabling(self):
        before = {"approved_transitions": []}
        after = {**before, "require_qualification": True}
        self.assertIsNone(check.validate(before, after, {}))
        with self.assertRaises(ValueError):
            check.validate(after, before, {})


if __name__ == "__main__":
    unittest.main()
