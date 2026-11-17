import copy
import unittest
from unittest.mock import patch

import compatibility_pr as pr

HEAD = "b" * 40
MAIN = "a" * 40
MERGED = "d" * 40


class MergeTests(unittest.TestCase):
    def setUp(self):
        self.pull = {
            "number": 1,
            "head": {
                "sha": HEAD,
                "ref": "feature/elastickv-validation-aaaaaaaa-bbbbbbbb-"
                + "c" * 12
                + "-"
                + "d" * 12,
                "repo": {"full_name": pr.REPO},
            },
            "base": {"ref": "main", "repo": {"full_name": pr.REPO}},
            "state": "open",
            "draft": False,
            "user": {"login": "github-actions[bot]"},
        }
        self.result = {
            "eligible": True,
            "policy": {
                "require_human_approval": True,
                "promotion_reviewers": ["reviewer"],
            },
            "proposed_policy": {"approved_transitions": [{"from": MAIN, "to": HEAD}]},
            "base": MAIN,
            "changes": [
                {"filename": pr.POLICY, "sha": "policy-blob"},
                {"filename": "docs/record", "sha": "proof-blob"},
            ],
            "report": {"to_source": HEAD},
        }
        self.consent = [
            {
                "id": 7,
                "state": "APPROVED",
                "commit_id": HEAD,
                "user": {"login": "reviewer", "type": "User"},
                "author_association": "OWNER",
            }
        ]
        self.calls = []
        self.ci = "success"
        self.current = MAIN

    def api(self, path, method="GET", data=None):
        self.calls.append((path, method, data))
        if path.startswith("pulls/1/reviews?"):
            return self.consent
        if path == "pulls/1/merge":
            if self.pull["state"] != "open" or self.pull["head"]["sha"] != data["sha"]:
                raise ValueError("GitHub rejected closed/changed PR")
            self.pull.update(state="closed", merged=True, merge_commit_sha=MERGED)
            self.current = MERGED
            return {"merged": True, "sha": MERGED}
        if path == "git/commits/" + MERGED:
            return {
                "parents": [{"sha": MAIN}, {"sha": HEAD}],
                "tree": {"sha": "server-tree"},
            }
        if path == "git/blobs":
            return {"sha": "human-policy-blob"}
        if path == "pulls/1":
            return copy.deepcopy(self.pull)
        if "/check-runs?" in path:
            return {
                "check_runs": [
                    {
                        "name": "compatibility-review",
                        "app": {"slug": "github-actions"},
                        "id": 1,
                        "conclusion": self.ci,
                        "status": "completed",
                    }
                ]
            }
        if path == "git/commits/" + MAIN:
            return {"tree": {"sha": "base-tree"}}
        if path == "git/trees":
            return {"sha": "merged-tree"}
        if path == "git/commits":
            return {"sha": "c" * 40}
        if path == "git/ref/heads/main":
            return {"object": {"sha": self.current}}
        if path == "git/refs/heads/main":
            return {}
        self.fail("Unexpected API operation " + path)

    def run_merge(self):
        with (
            patch.object(pr, "api", side_effect=self.api),
            patch.object(pr, "review", return_value=self.result),
            patch.object(pr, "read", return_value=self.result["proposed_policy"]),
        ):
            pr.merge_pr(1, HEAD)

    def test_only_reviewed_machine_pr_merges_with_configured_identity_and_nonforce_cas(
        self,
    ):
        self.run_merge()
        commit = [
            data
            for path, method, data in self.calls
            if path == "git/commits" and method == "POST"
        ][0]
        self.assertEqual([MERGED], commit["parents"])
        server_merge = next(
            data for path, method, data in self.calls if path == "pulls/1/merge"
        )
        self.assertEqual(HEAD, server_merge["sha"])
        self.assertEqual("merge", server_merge["merge_method"])
        tree = next(data for path, method, data in self.calls if path == "git/trees")
        self.assertEqual("server-tree", tree["base_tree"])
        self.assertEqual([pr.POLICY], [e["path"] for e in tree["tree"]])
        self.assertEqual(
            {"name": "Release Operator", "email": "operator@example.com"},
            commit["author"],
        )
        self.assertEqual(commit["author"], commit["committer"])
        update = [
            data for path, method, data in self.calls if path == "git/refs/heads/main"
        ][0]
        self.assertIs(update["force"], False)
        self.assertIn(
            "Signed-off-by: Release Operator <operator@example.com>", commit["message"]
        )

    def test_failed_ci_manual_source_and_main_drift_never_update_main(self):
        for failure in ["ci", "source", "drift", "author", "fork"]:
            with self.subTest(failure=failure):
                self.setUp()
                if failure == "ci":
                    self.ci = "failure"
                if failure == "source":
                    self.result["eligible"] = False
                if failure == "drift":
                    self.current = "d" * 40
                if failure == "author":
                    self.pull["user"]["login"] = "other"
                if failure == "fork":
                    self.pull["head"]["repo"]["full_name"] = "other/repo"
                with self.assertRaises(ValueError):
                    self.run_merge()
                self.assertFalse(
                    any(
                        path == "git/refs/heads/main"
                        for path, method, data in self.calls
                    )
                )

    def test_without_human_approval_no_git_writes_occur(self):
        self.consent = []
        self.run_merge()
        self.assertFalse(any(method != "GET" for path, method, data in self.calls))

    def test_revoke_or_head_change_before_cas_never_advances_main(self):
        original_api = self.api

        def mutate(path, method="GET", data=None):
            if path == "git/commits" and method == "POST":
                self.consent = []
            return original_api(path, method, data)

        with (
            patch.object(pr, "api", side_effect=mutate),
            patch.object(pr, "review", return_value=self.result),
            patch.object(pr, "read", return_value=self.result["proposed_policy"]),
        ):
            with self.assertRaises(ValueError):
                pr.merge_pr(1, HEAD)
        self.assertFalse(
            any(path == "git/refs/heads/main" for path, method, data in self.calls)
        )

    def test_close_or_head_change_during_review_fetch_is_rejected_by_server(self):
        for mutation in ("close", "head"):
            self.setUp()
            reads = 0
            original = self.api

            def race(path, method="GET", data=None):
                nonlocal reads
                if path.startswith("pulls/1/reviews?"):
                    reads += 1
                    if reads == 2:
                        if mutation == "close":
                            self.pull["state"] = "closed"
                        else:
                            self.pull["head"]["sha"] = "e" * 40
                return original(path, method, data)

            with (
                self.subTest(mutation=mutation),
                patch.object(pr, "api", side_effect=race),
                patch.object(pr, "review", return_value=self.result),
            ):
                with self.assertRaises(ValueError):
                    pr.merge_pr(1, HEAD)
            self.assertFalse(
                any(path == "git/refs/heads/main" for path, _, _ in self.calls)
            )

    def test_branch_protection_rejection_never_falls_back_to_ref_merge(self):
        original = self.api

        def protected(path, method="GET", data=None):
            if path == "pulls/1/merge":
                raise ValueError("Protected branch rejected merge")
            return original(path, method, data)

        with (
            patch.object(pr, "api", side_effect=protected),
            patch.object(pr, "review", return_value=self.result),
        ):
            with self.assertRaises(ValueError):
                pr.merge_pr(1, HEAD)
        self.assertFalse(any(method != "GET" for _, method, _ in self.calls))

    def test_failed_attestation_can_resume_from_verified_server_merge(self):
        self.pull.update(state="closed", merged=True, merge_commit_sha=MERGED)
        self.current = MERGED
        self.run_merge()
        self.assertFalse(any(path == "pulls/1/merge" for path, _, _ in self.calls))
        self.assertTrue(any(path == "git/refs/heads/main" for path, _, _ in self.calls))

    def test_base_change_during_server_merge_never_records_permission(self):
        original = self.api

        def changed_base(path, method="GET", data=None):
            result = original(path, method, data)
            if path == "git/commits/" + MERGED:
                result["parents"][0]["sha"] = "e" * 40
            return result

        with (
            patch.object(pr, "api", side_effect=changed_base),
            patch.object(pr, "review", return_value=self.result),
        ):
            with self.assertRaises(ValueError):
                pr.merge_pr(1, HEAD)
        self.assertFalse(
            any(path == "git/refs/heads/main" for path, _, _ in self.calls)
        )

    def test_record_write_failure_leaves_server_merge_unstamped(self):
        original = self.api

        def reject_record(path, method="GET", data=None):
            if path == "git/refs/heads/main":
                raise ValueError("Operational policy write forbidden")
            return original(path, method, data)

        with (
            patch.object(pr, "api", side_effect=reject_record),
            patch.object(pr, "review", return_value=self.result),
            patch.object(pr, "read", return_value=self.result["proposed_policy"]),
        ):
            with self.assertRaises(ValueError):
                pr.merge_pr(1, HEAD)
        self.assertTrue(self.pull["merged"])
        self.assertNotIn(
            "human_approval", self.result["proposed_policy"]["approved_transitions"][-1]
        )
        self.assertEqual(MERGED, self.current)

    def test_merged_state_without_consent_does_not_grant_permission(self):
        self.pull.update(state="closed", merged=True, merge_commit_sha=MERGED)
        self.current = MERGED
        self.consent = []
        self.run_merge()
        self.assertFalse(any(method != "GET" for _, method, _ in self.calls))

    def test_unrelated_dispatch_branch_is_rejected_before_api(self):
        with patch.object(pr, "api") as api:
            with self.assertRaises(ValueError):
                pr.review("main", HEAD)
            with self.assertRaises(ValueError):
                pr.review(
                    "feature/elastickv-validation-aaaaaaaa-bbbbbbbb-"
                    + "c" * 12
                    + "-"
                    + "d" * 12,
                    "not-a-sha",
                )
            api.assert_not_called()


if __name__ == "__main__":
    unittest.main()


class ReviewEventTests(unittest.TestCase):
    def setUp(self):
        fixture = MergeTests()
        fixture.setUp()
        self.pr = fixture.pull
        self.reviews = fixture.consent
        self.policy = fixture.result["policy"]
        self.calls = []

    def api(self, path, method="GET", data=None):
        self.calls.append((path, method, data))
        if path == "pulls/1":
            return self.pr
        if path.startswith("pulls/1/reviews?"):
            return self.reviews
        if path == "git/ref/heads/main":
            return {"object": {"sha": MAIN}}
        if path.endswith("/dispatches"):
            return {}
        if path == "check-runs":
            return {"id": 99}
        if path == "check-runs/99":
            return {}
        self.fail("Unexpected " + path)

    def test_human_review_dispatches_main_workflow_with_proposal_sha(self):
        with (
            patch.object(pr, "api", side_effect=self.api),
            patch.object(pr, "read", return_value=self.policy),
        ):
            pr.dispatch_approved(1)
        writes = [(path, data) for path, method, data in self.calls if method != "GET"]
        self.assertEqual(1, len(writes))
        self.assertEqual("main", writes[0][1]["ref"])
        self.assertEqual(HEAD, writes[0][1]["inputs"]["head_sha"])
        self.assertEqual("true", writes[0][1]["inputs"]["promote"])

    def test_run_permission_without_review_cannot_dispatch_promotion(self):
        self.reviews = []
        with (
            patch.object(pr, "api", side_effect=self.api),
            patch.object(pr, "read", return_value=self.policy),
        ):
            pr.dispatch_approved(1)
        self.assertFalse(any(method != "GET" for path, method, data in self.calls))

    def test_check_is_published_on_proposal_not_main_and_fails_on_blocker(self):
        with (
            patch.object(pr, "api", side_effect=self.api),
            patch.object(pr, "review", return_value={"eligible": True}),
        ):
            pr.record_check(1, HEAD)
        create = next(data for path, method, data in self.calls if path == "check-runs")
        self.assertEqual(HEAD, create["head_sha"])
        self.assertNotEqual(MAIN, create["head_sha"])
        self.calls = []
        with (
            patch.object(pr, "api", side_effect=self.api),
            patch.object(pr, "review", return_value={"eligible": False}),
        ):
            with self.assertRaises(ValueError):
                pr.record_check(1, HEAD)
        self.assertEqual("failure", self.calls[-1][2]["conclusion"])
        self.assertFalse(
            any(
                path.startswith("git/") and method != "GET"
                for path, method, data in self.calls
            )
        )
