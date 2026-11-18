"""End-to-end decision flow with immutable fake repositories; no live writes."""

import base64
import copy
import hashlib
import json
import pathlib
import unittest
import urllib.parse
from unittest.mock import patch

import agent
import compatibility_pr as frontend
import compatibility_profile as profile
import settings
import source_review
import test_check_elastickv_compatibility as fixtures

OLD = "a" * 40
NEW = "b" * 40
HEAD = "e" * 40
IMAGE = "ghcr.io/example/elastickv@sha256:" + "b" * 64


class PromotionFlowTests(unittest.TestCase):
    def test_audited_proposal_waits_for_exact_consent_then_merges_and_permits_release(
        self,
    ):
        self.exercise()

    def test_exact_relocation_profile_still_requires_consent_and_trusted_revalidation(
        self,
    ):
        self.exercise(with_relocations=True)

    def exercise(self, with_relocations=False):
        fixture = fixtures.ApprovalTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.reviews = []
        self.writes = []
        self.blobs = {}
        self.merged = None
        self.current = OLD
        self.fail_consent_record = True
        self.state = {
            "last_good": {
                "source": OLD,
                "image": "ghcr.io/example/elastickv@sha256:" + "a" * 64,
                "readiness": True,
            },
            "transaction": None,
        }
        policy = {
            "repository": settings.CONFIG["source_repository"],
            "gitops_repository": settings.CONFIG["gitops_repository"],
            "image": settings.CONFIG["application_image"],
            "namespace": settings.CONFIG["production_namespace"],
            "statefulset": "elastickv",
            "members": ["elastickv-" + str(i) for i in range(3)],
            "required_workflows": ["Test"],
            "approved_transitions": [],
            "require_qualification": True,
            "require_human_approval": True,
            "promotion_reviewers": ["reviewer"],
            "controller_sha256": hashlib.sha256(
                pathlib.Path(agent.__file__).read_bytes()
            ).hexdigest(),
        }
        config = {
            "version": 2,
            "agent_sha256": policy["controller_sha256"],
            "qualification_source_sha256": hashlib.sha256(
                (settings.PACKAGE / "compatibility.py").read_bytes()
            ).hexdigest(),
            "source_reviewer_sha256": source_review.fingerprint(),
            "source_review_limits": source_review.DEFAULT_LIMITS,
            "controller_image": "ghcr.io/example/controller@sha256:" + "c" * 64,
        }
        source = {
            "old": {"store/read.go": b"func read() int { return 1 }"},
            "new": {"store/read.go": b"func read() int { return 2 }"},
        }
        if with_relocations:
            source["old"]["assembly.go"] = b"func oldAssembly() {}"
            source["new"]["internal/server/assembly.go"] = b"func newAssembly() {}"
            config["relocation_profile"] = {
                "version": 1,
                "repository": policy["repository"],
                "from_source": OLD,
                "to_source": NEW,
                "image": IMAGE,
                "relocations": [
                    {
                        "old_path": "assembly.go",
                        "old_blob": source_review.git_blob(
                            source["old"]["assembly.go"]
                        ),
                        "targets": [
                            {
                                "path": "internal/server/assembly.go",
                                "blob": source_review.git_blob(
                                    source["new"]["internal/server/assembly.go"]
                                ),
                            }
                        ],
                        "review": "Reviewed exact assembly split; no contract blocker is overridden.",
                    }
                ],
            }
        inventories = {
            sha: {
                p: {
                    "path": p,
                    "mode": "100644",
                    "type": "blob",
                    "sha": source_review.git_blob(data),
                    "size": len(data),
                }
                for p, data in source[kind].items()
            }
            for sha, kind in [(OLD, "old"), (NEW, "new")]
        }
        comparison = {
            "status": "ahead",
            "files": [
                {
                    "filename": p,
                    "status": "added"
                    if p not in source["old"]
                    else "removed"
                    if p not in source["new"]
                    else "modified",
                }
                for p in sorted(source["old"].keys() | source["new"].keys())
            ],
        }

        def source_fetch(path):
            sha = path.split("/")[2].split("?")[0]
            return {"truncated": False, "tree": list(inventories[sha].values())}

        def archive(repo, sha, needed, limits, tick):
            return {p: source["old" if sha == OLD else "new"][p] for p in needed}

        source_report = source_review.collect(
            policy["repository"],
            OLD,
            NEW,
            IMAGE,
            comparison,
            source_fetch,
            archive=archive,
            relocation_profile=config.get("relocation_profile"),
        )
        spec = (
            "spec:\n  template:\n    spec:\n      containers:\n      - name: elastickv\n        image: "
            + self.state["last_good"]["image"]
            + "\n"
        )
        runtime_profile = profile.configuration_hash({agent.SPEC: spec})
        bindings = {
            "claims": {
                "data-elastickv-" + str(i): "production-" + str(i) for i in range(3)
            },
            "pods": {
                "elastickv-" + str(i): {
                    "uid": "pod-" + str(i),
                    "restarts": {"elastickv": 0},
                }
                for i in range(3)
            },
            "live_runtime_sha256": "d" * 64,
        }
        bindings["claims"]["elastickv-backup"] = "protected-backup"
        before_bindings = {**bindings, "unchanged": True}
        with patch.dict(settings.CONFIG, {"promotion_reviewers": ["reviewer"]}):
            identity = profile.qualification_identity(
                config,
                {},
                self.state["last_good"],
                {"source": NEW, "image": IMAGE, "readiness": True},
                runtime_profile,
                before_bindings,
                True,
                "automated-source-review",
                source_report,
            )
            self.branch = (
                "feature/elastickv-validation-"
                + OLD[:8]
                + "-"
                + NEW[:8]
                + "-"
                + runtime_profile[:12]
                + "-"
                + identity[:12]
            )
            folder = (
                settings.CONFIG["evidence_path"]
                + "/"
                + NEW
                + "/"
                + runtime_profile[:12]
                + "-12345678"
            )
            report = copy.deepcopy(fixture.report)
            report.update(
                scope=settings.CONFIG["validation_namespace"],
                architecture="x86_64",
                from_source=OLD,
                to_source=NEW,
                from_image=self.state["last_good"]["image"],
                to_image=IMAGE,
                controller_image=config["controller_image"],
                agent_source_sha256=config["agent_sha256"],
                profile_sha256=runtime_profile,
                qualification_sha256=identity,
                automatic_eligible=True,
                eligibility_reason="automated-source-review",
                source_review_sha256=source_review.digest(source_report),
                production_protection=before_bindings,
            )
            driver = (settings.PACKAGE / "compatibility_validation.py").read_bytes()
            helper = (settings.PACKAGE / "compatibility_runtime.py").read_bytes()
            report["driver_sha256"] = hashlib.sha256(driver).hexdigest()
            report["runtime_sha256"] = hashlib.sha256(helper).hexdigest()
            self.main = {
                agent.SPEC: spec.encode(),
                agent.STATE: json.dumps(self.state).encode(),
                frontend.POLICY: json.dumps(policy).encode(),
                frontend.CONFIG: json.dumps(config).encode(),
                "automation/deployment.json": json.dumps(settings.CONFIG).encode(),
                settings.CONFIG["release_path"] + "/controller.yaml": (
                    "kind: CronJob\nspec:\n  jobTemplate:\n    spec:\n      template:\n        spec:\n          containers:\n          - image: "
                    + config["controller_image"]
                    + "\n"
                ).encode(),
            }
            for path in settings.PACKAGE.glob("*.py"):
                self.main["automation/" + path.name] = path.read_bytes()
            after = copy.deepcopy(policy)
            after["approved_transitions"].append(
                {
                    "from": OLD,
                    "to": NEW,
                    "image": IMAGE,
                    "readiness": True,
                    "profile_sha256": runtime_profile,
                    "production_bindings": bindings,
                    "evidence": folder + "/summary.json",
                }
            )
            self.proposed = {
                **self.main,
                frontend.POLICY: json.dumps(after).encode(),
                folder + "/source-review.json": source_review.canonical(source_report),
                folder + "/summary.json": json.dumps(report).encode(),
                folder + "/isolated_driver.py": driver,
                folder + "/runtime_checks.py": helper,
                folder + "/observations.json.gz": (
                    fixture.path.parent / "observations.json.gz"
                ).read_bytes(),
            }
            self.diff = [
                {
                    "filename": path,
                    "status": "modified" if path == frontend.POLICY else "added",
                    "sha": source_review.git_blob(data),
                }
                for path, data in self.proposed.items()
                if data != self.main.get(path)
            ]
            self.blobs.update(
                {source_review.git_blob(data): data for data in self.proposed.values()}
            )
            self.pull = {
                "number": 1,
                "state": "open",
                "draft": False,
                "user": {"login": "github-actions[bot]"},
                "head": {
                    "ref": self.branch,
                    "sha": HEAD,
                    "repo": {"full_name": frontend.REPO},
                },
                "base": {"ref": "main", "repo": {"full_name": frontend.REPO}},
            }

            def public(url, *args, **kwargs):
                if "/git/trees/" in url:
                    return source_fetch(
                        url.split("/repos/" + policy["repository"] + "/")[1]
                    )
                if "/compare/" in url:
                    return comparison
                if "/commits/main" in url:
                    return {"sha": NEW}
                if "/actions/runs?" in url:
                    return {
                        "workflow_runs": [
                            {
                                "id": 1,
                                "name": "Test",
                                "head_sha": NEW,
                                "head_branch": "main",
                                "event": "push",
                                "status": "completed",
                                "conclusion": "success",
                            }
                        ]
                    }
                self.fail("Unexpected public read " + url)

            # Do not replace review(): exercise source audit, profile, evidence,
            # source CI/digest and actual merge implementation together.
            original_collect = source_review.collect

            def collect(*args, **kwargs):
                return original_collect(*args, **{**kwargs, "archive": archive})

            with (
                patch.object(frontend, "api", side_effect=self.api),
                patch.object(agent, "http_json", side_effect=public),
                patch.object(agent, "registry_image", return_value=IMAGE),
                patch.object(source_review, "collect", side_effect=collect),
            ):
                frontend.merge_pr(1, HEAD)
                self.assertEqual([], self.writes)
                self.reviews = [
                    {
                        "id": 7,
                        "state": "APPROVED",
                        "commit_id": HEAD,
                        "author_association": "OWNER",
                        "user": {"login": "reviewer", "type": "User"},
                    }
                ]
                with self.assertRaisesRegex(RuntimeError, "Consent record unavailable"):
                    frontend.merge_pr(1, HEAD)
                self.assertEqual("d" * 40, self.current)
                self.assertTrue(self.pull["merged"])
                self.fail_consent_record = False
            # Recovery must use immutable commits even after GitHub auto-deletes the branch.
            original_api = self.api

            def no_branch(path, method="GET", data=None):
                if path == "git/ref/heads/" + self.branch:
                    self.fail("Merged recovery must not require a live branch ref")
                return original_api(path, method, data)

            with (
                patch.object(frontend, "api", side_effect=no_branch),
                patch.object(agent, "http_json", side_effect=public),
                patch.object(agent, "registry_image", return_value=IMAGE),
                patch.object(source_review, "collect", side_effect=collect),
            ):
                frontend.record_check(1, HEAD)
                frontend.merge_pr(1, HEAD)
            self.assertEqual("f" * 40, self.current)
            self.assertIsNotNone(self.merged)
            entry = self.merged["approved_transitions"][-1]
            self.assertEqual(HEAD, entry["human_approval"]["commit"])
            self.assertEqual("reviewer", entry["human_approval"]["reviewer"])
            self.assertTrue(
                agent.compatible(self.merged, self.state["last_good"], NEW, comparison)
            )
            entry.pop("human_approval")
            self.assertIsNone(
                agent.compatible(self.merged, self.state["last_good"], NEW, comparison)
            )
            self.assertEqual(
                "protected-backup",
                entry["production_bindings"]["claims"]["elastickv-backup"],
            )

    def api(self, path, method="GET", data=None):
        if method != "GET":
            self.writes.append((path, method, data))
        if path == "pulls/1":
            return self.pull
        if path.startswith("pulls/1/reviews?"):
            return self.reviews
        if path == "git/ref/heads/" + self.branch:
            return {"object": {"sha": HEAD}}
        if path == "git/ref/heads/main":
            return {"object": {"sha": self.current}}
        if path == "git/commits/" + HEAD:
            return {
                "parents": [{"sha": OLD}],
                "author": agent.AUTHOR,
                "committer": agent.AUTHOR,
            }
        if path == "pulls/1/merge":
            self.assertEqual(HEAD, data["sha"])
            self.assertEqual("merge", data["merge_method"])
            self.pull.update(state="closed", merged=True, merge_commit_sha="d" * 40)
            self.current = "d" * 40
            return {"merged": True, "sha": self.current}
        if path == "git/commits/" + "d" * 40:
            return {
                "parents": [{"sha": OLD}, {"sha": HEAD}],
                "tree": {"sha": "server-tree"},
            }
        if path == "git/commits/" + OLD:
            return {"tree": {"sha": "base-tree"}}
        if path == "compare/" + OLD + "..." + OLD:
            return {"status": "identical"}
        if path == "compare/" + OLD + "..." + HEAD:
            return {"files": self.diff}
        if path.startswith("contents/"):
            filename, query = path[len("contents/") :].split("?", 1)
            ref = urllib.parse.parse_qs(query)["ref"][0]
            return {
                "content": base64.b64encode(
                    (self.main if ref == OLD else self.proposed)[filename]
                ).decode()
            }
        if path == "git/trees/" + OLD + "?recursive=1":
            return {
                "truncated": False,
                "tree": [{"path": name, "type": "blob"} for name in self.main],
            }
        if path.startswith("commits/") and "/check-runs?" in path:
            return {
                "check_runs": [
                    {
                        "id": 1,
                        "name": "compatibility-review",
                        "status": "completed",
                        "conclusion": "success",
                        "app": {"slug": "github-actions"},
                    }
                ]
            }
        if path == "check-runs":
            return {"id": 99}
        if path == "check-runs/99":
            return {}
        if path == "git/blobs":
            raw = base64.b64decode(data["content"])
            sha = source_review.git_blob(raw)
            self.blobs[sha] = raw
            return {"sha": sha}
        if path == "git/trees":
            self.merged = json.loads(
                self.blobs[
                    next(e["sha"] for e in data["tree"] if e["path"] == frontend.POLICY)
                ]
            )
            return {"sha": "merge-tree"}
        if path == "git/commits":
            return {"sha": "f" * 40}
        if path == "git/refs/heads/main":
            self.assertIs(data["force"], False)
            if self.fail_consent_record:
                raise RuntimeError("Consent record unavailable")
            self.current = data["sha"]
            return {}
        self.fail("Unexpected API operation " + path)


if __name__ == "__main__":
    unittest.main()
