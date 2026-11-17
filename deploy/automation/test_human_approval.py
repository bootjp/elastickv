import unittest

import agent
import human_approval as human

HEAD = "b" * 40
POLICY = {"require_human_approval": True, "promotion_reviewers": ["maintainer"]}


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.pr = {"number": 1, "state": "open", "draft": False, "head": {"sha": HEAD}}
        self.review = {
            "id": 7,
            "state": "APPROVED",
            "commit_id": HEAD,
            "author_association": "OWNER",
            "user": {"login": "maintainer", "type": "User"},
        }

    def decision(self, *reviews):
        return human.approval(self.pr, HEAD, list(reviews), POLICY)

    def test_exact_human_review_is_required_and_stamped(self):
        self.assertIsNone(self.decision())
        value = self.decision(self.review)
        self.assertEqual("maintainer", value["reviewer"])
        self.assertTrue(human.valid_attestation(POLICY, {"human_approval": value}))

    def test_wrong_head_other_person_bot_draft_and_closed_pr_never_approve(self):
        for change in [
            {"commit_id": "a" * 40},
            {"user": {"login": "other", "type": "User"}},
            {"user": {"login": "maintainer", "type": "Bot"}},
        ]:
            with self.subTest(change=change):
                self.assertIsNone(self.decision({**self.review, **change}))
        self.pr["draft"] = True
        self.assertIsNone(self.decision(self.review))
        self.pr["draft"] = False
        self.pr["state"] = "closed"
        self.assertIsNone(self.decision(self.review))
        self.pr["state"] = "open"
        self.pr["head"]["sha"] = "c" * 40
        self.assertIsNone(self.decision(self.review))

    def test_allowlisted_name_without_repository_association_cannot_approve(self):
        self.assertIsNone(self.decision({**self.review, "author_association": "NONE"}))

    def test_dismissal_or_change_request_revokes_but_comments_do_not(self):
        for state in ["DISMISSED", "CHANGES_REQUESTED"]:
            self.assertIsNone(
                self.decision(self.review, {**self.review, "id": 8, "state": state})
            )
        self.assertIsNotNone(
            self.decision(self.review, {**self.review, "id": 8, "state": "COMMENTED"})
        )

    def test_another_authorized_change_request_blocks_promotion(self):
        other = {
            **self.review,
            "id": 8,
            "state": "CHANGES_REQUESTED",
            "user": {"login": "other-maintainer", "type": "User"},
        }
        self.assertIsNone(self.decision(self.review, other))

    def test_release_engine_refuses_an_unstamped_permission(self):
        previous = {"source": "a" * 40, "readiness": True}
        entry = {"from": previous["source"], "to": HEAD, "readiness": True}
        policy = {**POLICY, "approved_transitions": [entry]}
        self.assertIsNone(agent.compatible(policy, previous, HEAD, {}))
        entry["human_approval"] = self.decision(self.review)
        self.assertTrue(agent.compatible(policy, previous, HEAD, {}))

    def test_bots_and_empty_reviewers_cannot_be_policy(self):
        for reviewers in [[], ["github-actions[bot]"], ["x", "x"]]:
            with self.assertRaises(ValueError):
                human.validate_policy({**POLICY, "promotion_reviewers": reviewers})


if __name__ == "__main__":
    unittest.main()
