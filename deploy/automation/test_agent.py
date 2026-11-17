import copy
import unittest
from unittest.mock import Mock

import agent

OLD = {"source": "a" * 40, "image": "old@sha256:1", "readiness": False}
NEW = {"source": "b" * 40, "image": "new@sha256:2", "readiness": True}


class PolicyTests(unittest.TestCase):
    def test_requires_exact_main_push_and_all_successful_latest_runs(self):
        run = dict(
            id=1,
            name="Test",
            head_sha=NEW["source"],
            head_branch="main",
            event="push",
            status="completed",
            conclusion="success",
        )
        self.assertTrue(agent.ci_ready([run], NEW["source"], ["Test"]))
        for field, value in [
            ("head_sha", OLD["source"]),
            ("head_branch", "feature/x"),
            ("event", "pull_request"),
            ("conclusion", "failure"),
            ("status", "in_progress"),
        ]:
            self.assertFalse(
                agent.ci_ready([{**run, field: value}], NEW["source"], ["Test"])
            )
        self.assertFalse(
            agent.ci_ready(
                [run, {**run, "id": 2, "conclusion": "failure"}],
                NEW["source"],
                ["Test"],
            )
        )
        self.assertFalse(agent.ci_ready([run], NEW["source"], ["Test", "Jepsen Test"]))

    def test_default_codeql_requires_dynamic_analysis_of_exact_main_commit(self):
        run = dict(
            id=1,
            name="CodeQL",
            head_sha=NEW["source"],
            head_branch="main",
            event="dynamic",
            status="completed",
            conclusion="success",
        )
        self.assertTrue(agent.ci_ready([run], NEW["source"], ["CodeQL"]))
        self.assertFalse(
            agent.ci_ready(
                [{**run, "head_branch": "refs/pull/1/head"}], NEW["source"], ["CodeQL"]
            )
        )
        self.assertFalse(
            agent.ci_ready(
                [{**run, "head_sha": OLD["source"]}], NEW["source"], ["CodeQL"]
            )
        )

    def test_default_codeql_run_display_name_is_not_workflow_name(self):
        run = dict(
            id=1,
            workflow_id=12345,
            name="Push on main",
            head_sha=NEW["source"],
            head_branch="main",
            event="dynamic",
            status="completed",
            conclusion="success",
        )
        self.assertTrue(agent.ci_ready([run], NEW["source"], ["CodeQL"], 12345))
        self.assertFalse(
            agent.ci_ready(
                [{**run, "workflow_id": 1}], NEW["source"], ["CodeQL"], 12345
            )
        )
        self.assertFalse(
            agent.ci_ready(
                [{**run, "workflow_id": 1, "name": "CodeQL"}],
                NEW["source"],
                ["CodeQL"],
                12345,
            )
        )
        self.assertFalse(
            agent.ci_ready([{**run, "event": "push"}], NEW["source"], ["CodeQL"], 12345)
        )

    def test_unknown_code_changes_fail_closed_and_renames_check_both_paths(self):
        policy = {"safe_paths": ["docs/*"], "approved_transitions": []}
        comparison = {"status": "ahead", "files": [{"filename": "docs/a.md"}]}
        self.assertIs(agent.compatible(policy, OLD, NEW["source"], comparison), False)
        for files in (
            [{"filename": "store/main.go"}],
            [],
            [{"filename": "docs/a.md", "previous_filename": "store/main.go"}],
            [{"filename": "docs/a.md"}] * 300,
        ):
            self.assertIsNone(
                agent.compatible(
                    policy, OLD, NEW["source"], {**comparison, "files": files}
                )
            )
        self.assertIsNone(
            agent.compatible(
                policy, OLD, NEW["source"], {**comparison, "status": "diverged"}
            )
        )
        policy["approved_transitions"] = [
            {"from": OLD["source"], "to": NEW["source"], "readiness": True}
        ]
        self.assertTrue(agent.compatible(policy, OLD, NEW["source"], {}))


class ReadinessContractTests(unittest.TestCase):
    def test_qualified_update_rejects_legacy_readiness_baseline(self):
        policy = {
            "require_qualification": True,
            "approved_transitions": [
                {"from": OLD["source"], "to": NEW["source"], "readiness": True}
            ],
        }
        self.assertIsNone(agent.compatible(policy, OLD, NEW["source"], {}))


class FakeStore:
    def __init__(self):
        self.state = {
            "last_good": copy.deepcopy(OLD),
            "transaction": None,
            "quarantined": [],
        }
        self.commits = []

    def commit(self, release, message, guard):
        guard()
        self.commits.append((copy.deepcopy(release), copy.deepcopy(self.state)))


class ManagerTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.runtime, self.lease = Mock(), Mock()
        self.runtime.pvcs.return_value = {"data-0": "uid0"}
        self.manager = agent.Manager(self.store, self.runtime, self.lease)

    def test_persists_transaction_before_roll_and_accepts_after_verification(self):
        def rolling(release, pvcs):
            self.assertEqual(
                "forward", self.store.commits[0][1]["transaction"]["phase"]
            )
            self.assertEqual({"data-0": "uid0"}, pvcs)

        self.runtime.roll.side_effect = rolling
        self.manager.execute(NEW)
        self.assertEqual(NEW, self.store.state["last_good"])
        self.assertIsNone(self.store.state["transaction"])
        self.runtime.verify.assert_any_call(OLD)
        self.runtime.accept.assert_called_with(NEW)

    def test_failed_update_commits_old_probe_and_image_before_rollback(self):
        def rolling(release, pvcs):
            if release == NEW:
                raise agent.ReleaseError("unready")
            self.assertEqual(OLD, self.store.commits[-1][0])
            self.assertEqual(
                "rollback", self.store.commits[-1][1]["transaction"]["phase"]
            )

        self.runtime.roll.side_effect = rolling
        self.manager.execute(NEW)
        self.assertEqual(OLD, self.store.state["last_good"])
        self.assertIn(NEW["image"], self.store.state["quarantined"])
        self.assertIsNone(self.store.state["transaction"])

    def test_failed_rollback_remains_recoverable_and_does_not_promote(self):
        self.runtime.roll.side_effect = agent.ReleaseError("unready")
        with self.assertRaises(agent.ReleaseError):
            self.manager.execute(NEW)
        self.assertEqual("rollback", self.store.state["transaction"]["phase"])
        self.assertEqual(OLD, self.store.state["last_good"])
        self.runtime.roll.side_effect = None
        self.manager.execute()
        self.runtime.roll.assert_called_with(OLD, {"data-0": "uid0"})
        self.assertIsNone(self.store.state["transaction"])

    def test_lost_lease_never_triggers_rollback_write(self):
        self.runtime.roll.side_effect = agent.LostLease("another holder")
        with self.assertRaises(agent.LostLease):
            self.manager.execute(NEW)
        self.assertEqual(1, len(self.store.commits))
        self.assertEqual("forward", self.store.state["transaction"]["phase"])

    def test_manual_drift_stops_without_rollback(self):
        self.runtime.roll.side_effect = agent.UnsafeState("manual image drift")
        with self.assertRaises(agent.UnsafeState):
            self.manager.execute(NEW)
        self.assertEqual(1, len(self.store.commits))
        self.assertEqual("forward", self.store.state["transaction"]["phase"])

    def test_preflight_failure_never_changes_desired_image(self):
        self.runtime.verify.side_effect = agent.ReleaseError("no quorum")
        with self.assertRaises(agent.ReleaseError):
            self.manager.execute(NEW)
        self.assertEqual([], self.store.commits)
        self.assertIsNone(self.store.state["transaction"])


class GitGuardTests(unittest.TestCase):
    def setUp(self):
        self.github = Mock()
        self.git = agent.GitState.__new__(agent.GitState)
        self.git.github = self.github
        self.git.shas = {"a": "blob-a"}
        self.git.inputs = {}
        self.git.config_inputs = Mock(return_value={})
        self.github.request.return_value = {"object": {"sha": "head"}}
        self.git.read_files = Mock(return_value=({}, dict(self.git.shas)))

    def test_unrelated_commit_is_allowed_but_controlled_drift_stops(self):
        self.assertEqual("head", self.git.guard())
        self.git.read_files.return_value = ({}, {"a": "other"})
        with self.assertRaises(agent.UnsafeState):
            self.git.guard()

    def test_rendered_input_drift_stops_even_if_image_file_is_unchanged(self):
        self.git.config_inputs.return_value = {
            "apps/elastickv/endpoint-patch.json": "changed"
        }
        with self.assertRaises(agent.UnsafeState):
            self.git.guard()

    def test_bootstrap_and_rollback_change_image_and_probe_together(self):
        import yaml

        self.git.files = {
            agent.SPEC: "spec:\n  template:\n    spec:\n      containers:\n      - name: elastickv\n        image: ghcr.io/example/elastickv@sha256:old\n",
            agent.KUSTOM: "resources: [upstream]\npatches: [{path: statefulset.yaml}]\n",
            agent.STATE: "{}",
        }
        self.git.state = {"last_good": OLD, "transaction": {"phase": "forward"}}
        self.git.guard = Mock(return_value="head")

        def request(path, method="GET", data=None):
            if path.startswith("git/commits/") and method == "GET":
                return {"tree": {"sha": "base"}}
            return {"sha": "new-commit"}

        self.github.request.side_effect = request
        target = {**NEW, "image": "ghcr.io/example/elastickv@sha256:new"}
        self.git.commit(target, "update", Mock())
        self.assertIn(target["image"], self.git.files[agent.SPEC])
        self.assertIn(
            agent.READINESS_PATCH,
            [
                p["path"]
                for p in yaml.safe_load(self.git.files[agent.KUSTOM])["patches"]
            ],
        )
        self.git.commit(
            {**OLD, "image": "ghcr.io/example/elastickv@sha256:old"}, "rollback", Mock()
        )
        self.assertNotIn(
            agent.READINESS_PATCH,
            [
                p["path"]
                for p in yaml.safe_load(self.git.files[agent.KUSTOM])["patches"]
            ],
        )
        patches = [
            c
            for c in self.github.request.call_args_list
            if c.args[0] == "git/refs/heads/main"
        ]
        self.assertEqual(2, len(patches))
        self.assertTrue(all(c.args[2]["force"] is False for c in patches))


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.policy = {
            "namespace": "elastickv",
            "statefulset": "elastickv",
            "members": ["n1", "n2", "n3"],
            "healthy_window": 0,
        }
        self.runtime = agent.Runtime(self.policy, Mock(), Mock(), Mock())

    def test_no_disruption_without_two_ready_voters(self):
        with self.assertRaises(agent.ReleaseError):
            self.runtime.quorum({"n1": {"metadata": {}, "status": {}}})
        self.runtime.kube.request.assert_not_called()

    def test_terminating_or_unbound_pvc_stops_before_disruption(self):
        for metadata, phase in [
            ({"uid": "uid", "deletionTimestamp": "now"}, "Bound"),
            ({"uid": "uid"}, "Pending"),
        ]:
            self.runtime.kube.request.return_value = {
                "metadata": metadata,
                "status": {"phase": phase},
            }
            with self.assertRaises(agent.UnsafeState):
                self.runtime.pvcs()
        self.runtime.kube.request.return_value = {
            "metadata": {"uid": "uid"},
            "status": {"phase": "Bound"},
        }
        self.assertEqual({"n1": "uid", "n2": "uid", "n3": "uid"}, self.runtime.pvcs())

    def test_stable_health_window_precedes_protocol_canary(self):
        self.runtime.verify = Mock()
        self.runtime.canary = Mock()
        self.runtime.accept(NEW)
        self.runtime.verify.assert_called_with(NEW, canary=False)
        self.runtime.canary.assert_called_once()

    def test_lost_lease_and_manual_drift_do_not_retry(self):
        predicate = Mock(side_effect=agent.LostLease("lost"))
        with self.assertRaises(agent.LostLease):
            self.runtime.wait(predicate, 1)
        self.assertEqual(1, predicate.call_count)
        predicate = Mock(side_effect=agent.UnsafeState("drift"))
        with self.assertRaises(agent.UnsafeState):
            self.runtime.wait(predicate, 1)
        self.assertEqual(1, predicate.call_count)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        patcher = unittest.mock.patch.object(agent, "verify_image_provenance")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_single_manifest_rejects_wrong_platform_and_accepts_digest_pin(self):
        import hashlib
        import io
        import json

        config = json.dumps({"os": "linux", "architecture": "arm64"}).encode()
        config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
        manifest = json.dumps({"config": {"digest": config_digest}}).encode()
        digest = "sha256:" + hashlib.sha256(manifest).hexdigest()

        class Response(io.BytesIO):
            headers = {"Docker-Content-Digest": digest}

        with (
            unittest.mock.patch.object(
                agent, "http_json", return_value={"token": "unused"}
            ),
            unittest.mock.patch.object(
                agent.urllib.request,
                "urlopen",
                side_effect=[Response(manifest), Response(config)],
            ),
        ):
            with self.assertRaises(agent.ReleaseError):
                agent.registry_image("ghcr.io/o/r", "source")
        config = json.dumps({"os": "linux", "architecture": "amd64"}).encode()
        config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
        manifest = json.dumps({"config": {"digest": config_digest}}).encode()
        digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
        Response.headers = {"Docker-Content-Digest": digest}
        with (
            unittest.mock.patch.object(
                agent, "http_json", return_value={"token": "unused"}
            ),
            unittest.mock.patch.object(
                agent.urllib.request,
                "urlopen",
                side_effect=[Response(manifest), Response(config)],
            ),
        ):
            self.assertEqual(
                "ghcr.io/o/r@" + digest, agent.registry_image("ghcr.io/o/r", "source")
            )


class RolloutTests(unittest.TestCase):
    def runtime(self):
        policy = {
            "namespace": "elastickv",
            "statefulset": "elastickv",
            "members": ["n1", "n2", "n3"],
            "startup_timeout": 1,
        }
        kube, lease, git = Mock(), Mock(), Mock()
        git.state = {"transaction": {"from": OLD, "to": NEW}}
        runtime = agent.Runtime(policy, kube, lease, git)
        pods = {
            n: {
                "metadata": {
                    "name": n,
                    "uid": n + "-old",
                    "labels": {"controller-revision-hash": "old"},
                },
                "spec": {"containers": [{"name": "elastickv", "image": OLD["image"]}]},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
            for n in policy["members"]
        }
        template = {
            "metadata": {"generation": 2},
            "spec": {
                "replicas": 3,
                "updateStrategy": {"type": "OnDelete"},
                "template": {
                    "spec": {
                        "containers": [{"name": "elastickv", "image": NEW["image"]}]
                    }
                },
            },
            "status": {"observedGeneration": 2, "updateRevision": "new"},
        }
        runtime.pods = Mock(side_effect=lambda: copy.deepcopy(pods))
        runtime.pvcs = Mock(return_value={"data": "retained"})
        runtime.wait = lambda predicate, timeout: predicate() or (_ for _ in ()).throw(
            agent.ReleaseError("predicate false")
        )
        runtime._verified = Mock(return_value=True)
        leader = ["n1"]
        actions = []
        runtime.quorum = Mock(side_effect=lambda p: leader[0])

        def rpc(name, command, *args):
            actions.append(("transfer", name, args[0]))
            leader[0] = args[0]

        runtime.rpc = Mock(side_effect=rpc)

        def request(path, method="GET", data=None):
            if path == runtime.sts_path:
                return copy.deepcopy(template)
            name = path.rsplit("/", 1)[-1]
            if method == "DELETE":
                self.assertEqual({"preconditions": {"uid": name + "-old"}}, data)
                self.assertNotEqual(name, leader[0])
                actions.append(("delete", name))
                pods[name]["metadata"]["uid"] = name + "-new"
                pods[name]["metadata"]["labels"]["controller-revision-hash"] = "new"
                pods[name]["spec"]["containers"][0]["image"] = NEW["image"]
            return copy.deepcopy(pods[name])

        kube.request.side_effect = request
        return runtime, pods, template, actions

    def test_serial_follower_first_replacement_and_transfer_before_leader(self):
        runtime, pods, template, actions = self.runtime()
        runtime.roll(NEW, {"data": "retained"})
        self.assertEqual(
            [
                ("delete", "n2"),
                ("delete", "n3"),
                ("transfer", "n1", "n2"),
                ("delete", "n1"),
            ],
            actions,
        )
        self.assertEqual(3, runtime._verified.call_count)
        self.assertTrue(
            all(
                p["spec"]["containers"][0]["image"] == NEW["image"]
                for p in pods.values()
            )
        )

    def test_sidecar_before_application_does_not_block_template_wait(self):
        runtime, pods, template, actions = self.runtime()
        template["spec"]["template"]["spec"]["containers"].insert(
            0, {"name": "observer", "image": "other"}
        )
        runtime.roll(NEW, {"data": "retained"})
        self.assertEqual(3, sum(a[0] == "delete" for a in actions))

    def test_resumed_forward_checks_existing_candidate_before_any_deletion(self):
        runtime, pods, template, actions = self.runtime()
        runtime.git.state["transaction"]["phase"] = "forward"
        pods["n2"]["metadata"]["labels"]["controller-revision-hash"] = "new"
        pods["n2"]["spec"]["containers"][0]["image"] = NEW["image"]
        runtime.verify = Mock(side_effect=agent.ReleaseError("candidate canary failed"))
        with self.assertRaises(agent.ReleaseError):
            runtime.roll(NEW, {"data": "retained"})
        runtime.verify.assert_called_once_with(NEW)
        self.assertEqual([], actions)

    def test_pvc_or_unexpected_image_drift_stops_without_deletion(self):
        runtime, pods, template, actions = self.runtime()
        runtime.pvcs.return_value = {"data": "changed"}
        with self.assertRaises(agent.UnsafeState):
            runtime.roll(NEW, {"data": "retained"})
        self.assertEqual([], actions)
        runtime, pods, template, actions = self.runtime()
        pods["n3"]["spec"]["containers"][0]["image"] = "unknown-format"
        with self.assertRaises(agent.UnsafeState):
            runtime.roll(NEW, {"data": "retained"})
        self.assertEqual([], actions)

    def test_health_failure_stops_before_replacing_another_voter(self):
        runtime, pods, template, actions = self.runtime()
        runtime._verified.side_effect = agent.ReleaseError("canary failed")
        with self.assertRaises(agent.ReleaseError):
            runtime.roll(NEW, {"data": "retained"})
        self.assertEqual([("delete", "n2")], actions)


class ProvenanceTests(unittest.TestCase):
    def test_signed_verifier_is_bound_to_digest_source_ref_and_workflow(self):
        image = "ghcr.io/example/elastickv@sha256:" + "c" * 64
        with (
            unittest.mock.patch.dict(agent.os.environ, {"GH_TOKEN": "test-only"}),
            unittest.mock.patch.object(
                agent.subprocess,
                "run",
                return_value=Mock(returncode=0, stdout='[{"verified":true}]'),
            ) as run,
        ):
            agent.verify_image_provenance(image, NEW["source"])
        args = run.call_args.args[0]
        self.assertIn("oci://" + image, args)
        self.assertIn(NEW["source"], args)
        self.assertIn("refs/heads/main", args)
        self.assertIn("example/elastickv/.github/workflows/docker-image.yml", args)
        self.assertIn("--deny-self-hosted-runners", args)
        self.assertNotIn("test-only", args)

    def test_untrusted_unsigned_mismatched_or_unavailable_provenance_fails_closed(self):
        for result in (
            Mock(returncode=1, stdout=""),
            Mock(returncode=0, stdout="[]"),
            Mock(returncode=0, stdout="invalid"),
        ):
            with (
                self.subTest(result=result),
                unittest.mock.patch.dict(agent.os.environ, {"GH_TOKEN": "test-only"}),
                unittest.mock.patch.object(
                    agent.subprocess, "run", return_value=result
                ),
            ):
                with self.assertRaises(agent.ReleaseError):
                    agent.verify_image_provenance(
                        "ghcr.io/example/elastickv@sha256:" + "c" * 64, NEW["source"]
                    )


class GithubReadRetryTests(unittest.TestCase):
    def setUp(self):
        self.github = agent.GithubApp("example/elastickv-gitops")
        self.github.headers = Mock(return_value={"Authorization": "Bearer test-only"})
        self.path = "contents/" + agent.SPEC + "?ref=" + "a" * 40

    def error(self, code):
        return agent.urllib.error.HTTPError(
            "https://api.github.com/repos/example/elastickv-gitops/" + self.path,
            code,
            "test-only",
            {},
            None,
        )

    def test_transient_contents_404_retries_the_same_immutable_read(self):
        import io

        response = Mock()
        response.__enter__ = Mock(
            return_value=io.BytesIO(b'{"sha":"blob","content":"eA=="}')
        )
        response.__exit__ = Mock(return_value=False)
        with (
            unittest.mock.patch.object(
                agent.urllib.request, "urlopen", side_effect=[self.error(404), response]
            ) as open_url,
            unittest.mock.patch.object(agent.time, "sleep") as sleep,
        ):
            self.assertEqual("blob", self.github.request(self.path)["sha"])
            self.assertEqual(2, open_url.call_count)
            self.assertEqual(
                open_url.call_args_list[0].args[0].full_url,
                open_url.call_args_list[1].args[0].full_url,
            )
            sleep.assert_called_once_with(1)

    def test_persistent_404_fails_closed_after_three_attempts(self):
        with (
            unittest.mock.patch.object(
                agent.urllib.request, "urlopen", side_effect=self.error(404)
            ) as open_url,
            unittest.mock.patch.object(agent.time, "sleep") as sleep,
        ):
            with self.assertRaises(agent.ReleaseError):
                self.github.request(self.path)
            self.assertEqual(3, open_url.call_count)
            self.assertEqual(
                [unittest.mock.call(1), unittest.mock.call(2)], sleep.call_args_list
            )

    def test_other_errors_and_writes_are_never_retried(self):
        for method, path, code in [
            ("GET", self.path, 403),
            ("GET", "contents/" + agent.SPEC + "?ref=main", 404),
            ("GET", "contents/" + agent.SPEC, 404),
            ("GET", "git/ref/heads/main", 404),
            ("PUT", self.path, 404),
            ("PATCH", "git/refs/heads/main", 404),
            ("POST", "git/commits", 404),
        ]:
            with (
                self.subTest(method=method, path=path, code=code),
                unittest.mock.patch.object(
                    agent.urllib.request, "urlopen", side_effect=self.error(code)
                ) as open_url,
                unittest.mock.patch.object(agent.time, "sleep") as sleep,
            ):
                with self.assertRaises(agent.ReleaseError):
                    self.github.request(path, method)
                self.assertEqual(1, open_url.call_count)
                sleep.assert_not_called()


class ApprovedDigestTests(unittest.TestCase):
    def test_discovery_rejects_same_sha_with_an_unqualified_image(self):
        policy = {
            "repository": "example/elastickv",
            "required_workflows": [],
            "image": "ghcr.io/example/elastickv",
            "approved_transitions": [
                {
                    "from": OLD["source"],
                    "to": NEW["source"],
                    "readiness": True,
                    "image": NEW["image"],
                }
            ],
        }

        def response(url):
            if url.endswith("/commits/main"):
                return {"sha": NEW["source"]}
            if "/actions/runs?" in url:
                return {"workflow_runs": []}
            return {}

        with (
            unittest.mock.patch.object(agent, "http_json", side_effect=response),
            unittest.mock.patch.object(
                agent, "registry_image", return_value="unqualified@sha256:other"
            ),
        ):
            with self.assertRaises(agent.UnsafeState):
                agent.discover(policy, OLD)
        with (
            unittest.mock.patch.object(agent, "http_json", side_effect=response),
            unittest.mock.patch.object(
                agent, "registry_image", return_value=NEW["image"]
            ),
        ):
            self.assertEqual(NEW, agent.discover(policy, OLD))


if __name__ == "__main__":
    unittest.main()
