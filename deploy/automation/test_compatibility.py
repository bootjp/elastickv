import copy
import hashlib
import json
import pathlib
import unittest
from unittest.mock import Mock, patch

import agent
import compatibility
from compatibility_profile import (
    configuration_hash,
    qualified_profile,
)

ROOT = pathlib.Path(__file__).resolve().parent / "examples/gitops"
OLD = {
    "source": "a" * 40,
    "image": "ghcr.io/example/elastickv@sha256:" + "a" * 64,
    "readiness": True,
}
NEW = {
    "source": "b" * 40,
    "image": "ghcr.io/example/elastickv@sha256:" + "b" * 64,
    "readiness": True,
}


class OptionalCodeQLTests(unittest.TestCase):
    def test_qualification_without_codeql_id_waits_for_required_ci(self):
        fingerprint = hashlib.sha256(b"controller").hexdigest()
        config = {
            "qualification_source_sha256": fingerprint,
            "agent_sha256": fingerprint,
            "source_reviewer_sha256": "reviewer",
        }
        policy = {"repository": "example/elastickv", "required_workflows": ["Test"]}
        store = Mock(state={"last_good": OLD})
        with (
            patch.object(compatibility, "require_configured"),
            patch.object(pathlib.Path, "read_bytes", return_value=b"controller"),
            patch.object(
                pathlib.Path,
                "read_text",
                side_effect=[json.dumps(config), json.dumps(policy)],
            ),
            patch.object(
                compatibility.source_review, "fingerprint", return_value="reviewer"
            ),
            patch.object(agent, "validate_site_policy"),
            patch.object(agent, "GithubApp"),
            patch.object(agent, "GitState", return_value=store),
            patch.object(
                agent,
                "http_json",
                side_effect=[
                    {"sha": NEW["source"]},
                    {"workflow_runs": []},
                    {"workflow_runs": []},
                ],
            ),
            patch.object(agent, "registry_image") as image,
        ):
            compatibility.run()
        image.assert_not_called()


class ProfileTests(unittest.TestCase):
    def test_image_and_ledger_changes_do_not_hide_runtime_drift(self):
        files = {
            "apps/elastickv/statefulset.yaml": "spec:\n  image: " + OLD["image"] + "\n",
            "apps/elastickv/runtime.json": '{"args":[]}',
            "apps/elastickv/release-state.json": "{}",
        }
        before = configuration_hash(files)
        files["apps/elastickv/statefulset.yaml"] = files[
            "apps/elastickv/statefulset.yaml"
        ].replace(OLD["image"], NEW["image"])
        files["apps/elastickv/release-state.json"] = '{"transaction":1}'
        self.assertEqual(before, configuration_hash(files))
        files["apps/elastickv/runtime.json"] = '{"args":["--commitTSFence"]}'
        self.assertNotEqual(before, configuration_hash(files))

    def test_serialization_does_not_invalidate_own_rollout(self):
        import yaml

        files = {
            "apps/elastickv/statefulset.yaml": "spec:\n  image: " + OLD["image"] + "\n",
            "apps/elastickv/kustomization.yaml": "resources: [statefulset.yaml]\n",
            "apps/elastickv/runtime.json": '{"readiness": true}',
        }
        before = configuration_hash(files)
        files["apps/elastickv/kustomization.yaml"] = yaml.safe_dump(
            yaml.safe_load(files["apps/elastickv/kustomization.yaml"])
        )
        files["apps/elastickv/runtime.json"] = json.dumps(
            json.loads(files["apps/elastickv/runtime.json"]), indent=2
        )
        self.assertEqual(before, configuration_hash(files))

    def test_pinned_profile_and_digest_both_fail_closed(self):
        policy = {
            "approved_transitions": [
                {
                    "from": OLD["source"],
                    "to": NEW["source"],
                    "image": NEW["image"],
                    "profile_sha256": "profile",
                }
            ]
        }
        qualified_profile(policy, OLD, NEW["source"], NEW["image"], "profile")
        for image, profile in [
            (OLD["image"], "profile"),
            (NEW["image"], "stale"),
            (NEW["image"], None),
        ]:
            with self.assertRaises(ValueError):
                qualified_profile(policy, OLD, NEW["source"], image, profile)


class ScopeTests(unittest.TestCase):
    def test_controller_cannot_write_production_or_unrelated_namespace(self):
        kube = compatibility.ScopedKubernetes.__new__(compatibility.ScopedKubernetes)
        for path, method in [
            ("/api/v1/namespaces/elastickv/pods/elastickv-0", "DELETE"),
            ("/api/v1/namespaces/elastickv-system/secrets/key", "GET"),
            ("/api/v1/persistentvolumes", "GET"),
            ("/api/v1/namespaces/other/pods", "GET"),
        ]:
            with patch.object(agent.Kubernetes, "request") as request:
                with self.assertRaises(agent.UnsafeState):
                    kube.request(path, method)
                request.assert_not_called()

    def test_claim_identity_change_or_mutable_registration_stops(self):
        kube = Mock()
        lease = Mock()
        runner = compatibility.Runner(kube, lease, {})
        ids = {"data-elastickv-" + str(i): str(i) for i in range(3)}
        rows = [
            {
                "claim": name,
                "uid": uid,
                "pv": "pv-" + uid,
                "pv_uid": "pvuid-" + uid,
                "path": "/srv/validation/elastickv-validation/" + uid,
            }
            for name, uid in ids.items()
        ]

        def get(path):
            if "/configmaps/" in path:
                return {
                    "immutable": True,
                    "data": {
                        "identities.json": json.dumps(
                            {"pvc_uids": ids, "volumes": rows}
                        )
                    },
                }
            if "/persistentvolumes/" in path:
                row = next(r for r in rows if r["pv"] == path.rsplit("/", 1)[-1])
                return {
                    "metadata": {"uid": row["pv_uid"]},
                    "spec": {
                        "claimRef": {
                            "namespace": "elastickv-validation",
                            "uid": row["uid"],
                            "name": row["claim"],
                        },
                        "csi": {
                            "driver": "nfs.csi.k8s.io",
                            "volumeAttributes": {
                                "share": "/srv/validation",
                                "subDir": "elastickv-validation/" + row["uid"],
                            },
                        },
                    },
                }
            name = path.rsplit("/", 1)[-1]
            return {
                "metadata": {
                    "uid": ids[name],
                    "name": name,
                    "namespace": "elastickv-validation",
                },
                "spec": {"volumeName": "pv-" + ids[name]},
                "status": {"phase": "Bound"},
            }

        kube.request.side_effect = get
        self.assertEqual(ids, runner.volumes())
        kube.request.side_effect = lambda path: {"immutable": False}
        with self.assertRaises(agent.UnsafeState):
            runner.volumes()

    def test_branch_guard_precedes_all_github_writes(self):
        github = Mock()
        store = Mock()
        with self.assertRaises(agent.UnsafeState):
            compatibility.publish(github, store, {}, "main", NEW, {}, b"", True, "test")
        github.request.assert_not_called()
        store.guard.assert_not_called()

    def test_sandbox_copies_runtime_tuning_but_rejects_unknown_protocols(self):
        base = {
            "env": [
                {"name": "POD_NAME", "value": "test"},
                {"name": "POD_NAMESPACE", "value": "elastickv-validation"},
                {"name": "GOMEMLIMIT", "value": "700MiB"},
            ]
        }
        actual = {
            "command": ["/app"],
            "args": [
                "--redisAddress=0.0.0.0:6379",
                "--sqsPublicEndpoint=https://sqs.example.com",
                "--adminEnabled",
            ],
            "env": [{"name": "ELASTICKV_RAFT_SEND_STREAM", "value": "false"}],
        }
        result = compatibility.test_container(base, actual, OLD)
        self.assertNotIn("example.com", json.dumps(result))
        self.assertEqual(OLD["image"], result["image"])
        for flag in [
            "--commitTSFence=true",
            "--tsoMode=phase-d",
            "--unknownFeature=true",
        ]:
            with self.assertRaises(agent.UnsafeState):
                compatibility.test_container(base, {**actual, "args": [flag]}, OLD)

    def test_production_roles_are_read_only_and_sandbox_never_deletes_claims(self):
        import yaml

        for role in yaml.safe_load_all(
            (ROOT / "apps/elastickv-compatibility/rbac.yaml").read_text()
        ):
            if role["kind"] != "Role":
                continue
            for rule in role["rules"]:
                if role["metadata"]["namespace"] == "elastickv":
                    self.assertEqual(["get"], rule["verbs"])
                if "persistentvolumeclaims" in rule["resources"]:
                    self.assertEqual(["get"], rule["verbs"])
        assets = json.loads(
            (ROOT / "apps/elastickv-compatibility/assets.json").read_text()
        )
        cleaner = assets["cleaner"]["spec"]["template"]["spec"]
        self.assertIs(cleaner["automountServiceAccountToken"], False)
        self.assertEqual(
            {"data-elastickv-" + str(i) for i in range(3)},
            {
                v["persistentVolumeClaim"]["claimName"]
                for v in cleaner["volumes"]
                if "persistentVolumeClaim" in v
            },
        )
        worker = assets["validator"]["spec"]["template"]["spec"]
        self.assertFalse(any("secret" in v for v in worker["volumes"]))


class QueuedQualificationTests(unittest.TestCase):
    def test_qualified_ancestor_is_selected_when_new_main_ci_is_pending(self):
        latest = "c" * 40
        policy = {
            "repository": "example/elastickv",
            "required_workflows": ["Test"],
            "codeql_workflow_id": 1,
            "image": "ghcr.io/example/elastickv",
            "approved_transitions": [
                {
                    "from": OLD["source"],
                    "to": NEW["source"],
                    "readiness": True,
                    "image": NEW["image"],
                    "profile_sha256": "profile",
                }
            ],
        }
        ancestry = "ahead"

        def response(url):
            if url.endswith("/commits/main"):
                return {"sha": latest}
            if "/compare/" + NEW["source"] + "..." in url:
                return {"status": ancestry}
            if "/actions/runs?" in url:
                runs = (
                    []
                    if latest in url
                    else [
                        {
                            "id": 1,
                            "name": "Test",
                            "head_sha": NEW["source"],
                            "head_branch": "main",
                            "event": "push",
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ]
                )
                return {"workflow_runs": runs}
            return {"status": "ahead", "files": []}

        with (
            patch.object(agent, "http_json", side_effect=response),
            patch.object(agent, "registry_image", return_value=NEW["image"]),
        ):
            self.assertEqual(NEW, agent.discover(policy, OLD, "profile"))
            with self.assertRaises(agent.UnsafeState):
                agent.discover(policy, OLD, "changed-profile")
        ancestry = "diverged"
        with (
            patch.object(agent, "http_json", side_effect=response),
            patch.object(agent, "registry_image") as registry,
        ):
            self.assertIsNone(agent.discover(policy, OLD, "profile"))
            registry.assert_not_called()


class RetagTests(unittest.TestCase):
    def test_current_source_is_not_redeployed_after_retag(self):
        policy = {"repository": "example/elastickv"}
        with (
            patch.object(agent, "http_json", return_value={"sha": OLD["source"]}),
            patch.object(agent, "registry_image") as image,
        ):
            self.assertIsNone(agent.discover(policy, OLD))
            image.assert_not_called()


class MandatoryQualificationTests(unittest.TestCase):
    def test_even_documentation_and_unpinned_legacy_entries_need_qualification(self):
        policy = {
            "require_qualification": True,
            "safe_paths": ["docs/*"],
            "approved_transitions": [],
        }
        comparison = {"status": "ahead", "files": [{"filename": "docs/readme.md"}]}
        self.assertIsNone(agent.compatible(policy, OLD, NEW["source"], comparison))
        policy["approved_transitions"] = [
            {"from": OLD["source"], "to": NEW["source"], "readiness": True}
        ]
        self.assertIsNone(agent.compatible(policy, OLD, NEW["source"], comparison))
        policy["approved_transitions"][0].update(
            image=NEW["image"],
            profile_sha256="profile",
            production_bindings={},
            evidence="proof",
        )
        self.assertIs(agent.compatible(policy, OLD, NEW["source"], comparison), True)


class CanaryOwnershipTests(unittest.TestCase):
    def test_only_the_expected_endpoint_and_owned_queue_name_is_accepted(self):
        endpoint = "https://sqs.example.test"
        name = "elastickv-release-owned"
        good = endpoint + "/000000000000/" + name
        self.assertEqual(good, agent.owned_canary_queue(endpoint, good, name))
        for url in [
            endpoint + "/000000000000/user-backup",
            good + "?q=1",
            good + "#other",
            good.replace("https:", "http:"),
            good.replace("sqs.example.test", "other.test"),
        ]:
            with self.assertRaises(agent.ReleaseError):
                agent.owned_canary_queue(endpoint, url, name)

    def test_unowned_queue_is_never_sent_to_or_deleted_during_canary_cleanup(self):
        import base64
        import io
        import sys
        import types

        runtime = agent.Runtime.__new__(agent.Runtime)
        runtime.namespace = "/api/v1/namespaces/elastickv-test"
        runtime.kube = Mock()
        runtime.lease = Mock()
        runtime.redis_canary = Mock()
        runtime.policy = {
            "endpoints": {
                s: "https://" + s + ".example.test" for s in ["dynamodb", "s3", "sqs"]
            }
        }
        runtime.kube.request.return_value = {
            "data": {
                "credentials.json": base64.b64encode(
                    json.dumps(
                        {
                            "credentials": [
                                {"access_key_id": "dummy", "secret_access_key": "dummy"}
                            ]
                        }
                    ).encode()
                ).decode()
            }
        }
        name = "elastickv-release-" + "a" * 16
        clients = {s: Mock() for s in ["dynamodb", "s3", "sqs"]}
        clients["dynamodb"].get_item.return_value = {"Item": {"value": {"S": name}}}
        clients["s3"].get_object.return_value = {"Body": io.BytesIO(name.encode())}
        clients["sqs"].create_queue.return_value = {
            "QueueUrl": "https://sqs.example.test/000000000000/user-backup"
        }
        boto = types.ModuleType("boto3")
        boto.client = lambda service, **kwargs: clients[service]
        cfg = types.ModuleType("botocore.config")
        cfg.Config = lambda **kwargs: None
        with (
            patch.dict(
                sys.modules,
                {
                    "boto3": boto,
                    "botocore": types.ModuleType("botocore"),
                    "botocore.config": cfg,
                },
            ),
            patch.object(
                agent.uuid, "uuid4", return_value=types.SimpleNamespace(hex="a" * 32)
            ),
        ):
            with self.assertRaises(agent.ReleaseError):
                runtime.canary()
        clients["sqs"].send_message.assert_not_called()
        clients["sqs"].delete_queue.assert_not_called()
        clients["dynamodb"].delete_table.assert_called_once_with(TableName=name)
        clients["s3"].delete_bucket.assert_called_once_with(Bucket=name)


class VolumeBindingTests(unittest.TestCase):
    def test_rebinding_or_path_change_never_reuses_the_registered_volume(self):
        from compatibility_profile import verify_dedicated_volume

        expected = {
            "claim": "data-elastickv-0",
            "uid": "claim-id",
            "pv": "pv-name",
            "pv_uid": "volume-id",
            "path": "/srv/validation/elastickv-validation/isolated",
        }
        pvc = {
            "metadata": {
                "name": expected["claim"],
                "namespace": "elastickv-validation",
                "uid": expected["uid"],
            },
            "status": {"phase": "Bound"},
            "spec": {"volumeName": expected["pv"]},
        }
        pv = {
            "metadata": {"uid": expected["pv_uid"]},
            "spec": {
                "claimRef": {
                    "name": expected["claim"],
                    "namespace": "elastickv-validation",
                    "uid": expected["uid"],
                },
                "csi": {
                    "driver": "nfs.csi.k8s.io",
                    "volumeAttributes": {
                        "share": "/srv/validation",
                        "subDir": "elastickv-validation/isolated",
                    },
                },
            },
        }
        verify_dedicated_volume(pvc, pv, expected, "elastickv-validation")
        for mutate in [
            lambda p: p["metadata"].update(uid="replaced"),
            lambda p: p["spec"]["claimRef"].update(namespace="elastickv"),
            lambda p: p["spec"]["csi"]["volumeAttributes"].update(
                subDir="elastickv/production"
            ),
        ]:
            changed = copy.deepcopy(pv)
            mutate(changed)
            with self.assertRaises(ValueError):
                verify_dedicated_volume(pvc, changed, expected, "elastickv-validation")


class RequalificationTests(unittest.TestCase):
    def test_review_controller_or_production_changes_invalidate_cached_proposal(self):
        from compatibility_profile import qualification_identity

        cfg = {"controller_image": "old", "automatic_paths": ["docs/*"]}
        reviews = {"version": 1, "reviewed_transitions": []}
        prod = {
            "claims": {"claim": "uid"},
            "pods": {"pod": {"uid": "old", "restarts": {}}},
        }

        def key(c, r, p, e, reason):
            return qualification_identity(c, r, OLD, NEW, "profile", p, e, reason)

        old = key(cfg, reviews, prod, False, "needs-review")
        reviewed = {
            "version": 1,
            "reviewed_transitions": [
                {"from": OLD["source"], "to": NEW["source"], "image": NEW["image"]}
            ],
        }
        self.assertNotEqual(old, key(cfg, reviewed, prod, True, "reviewed"))
        self.assertNotEqual(
            old,
            key(
                {**cfg, "controller_image": "new"}, reviews, prod, False, "needs-review"
            ),
        )
        changed = copy.deepcopy(prod)
        changed["claims"]["claim"] = "rebound"
        self.assertNotEqual(old, key(cfg, reviews, changed, False, "needs-review"))

    def test_latest_requalification_overrides_old_profile_for_same_source_pair(self):
        entries = [
            {
                "from": OLD["source"],
                "to": NEW["source"],
                "image": NEW["image"],
                "profile_sha256": p,
            }
            for p in ["stale", "current"]
        ]
        qualified_profile(
            {"approved_transitions": entries},
            OLD,
            NEW["source"],
            NEW["image"],
            "current",
        )
        with self.assertRaises(ValueError):
            qualified_profile(
                {"approved_transitions": entries},
                OLD,
                NEW["source"],
                NEW["image"],
                "stale",
            )

    def test_live_backup_mount_or_writable_sidecar_data_mount_is_refused(self):
        from compatibility_profile import verify_live_data_layout

        sts = json.loads(
            (ROOT / "apps/elastickv-compatibility/assets.json").read_text()
        )["statefulset"]
        verify_live_data_layout(sts)
        bad = copy.deepcopy(sts)
        bad["spec"]["template"]["spec"]["volumes"].append(
            {
                "name": "backup",
                "persistentVolumeClaim": {"claimName": "elastickv-backup"},
            }
        )
        with self.assertRaises(ValueError):
            verify_live_data_layout(bad)
        bad = copy.deepcopy(sts)
        bad["spec"]["template"]["spec"]["containers"].append(
            {
                "name": "sidecar",
                "volumeMounts": [{"name": "data", "mountPath": "/data"}],
            }
        )
        with self.assertRaises(ValueError):
            verify_live_data_layout(bad)

    def test_init_containers_cannot_bypass_data_layout_guard(self):
        from compatibility_profile import verify_live_data_layout

        sts = json.loads(
            (ROOT / "apps/elastickv-compatibility/assets.json").read_text()
        )["statefulset"]
        sts["spec"]["template"]["spec"]["initContainers"] = [
            {
                "name": "migrate",
                "volumeMounts": [{"name": "data", "mountPath": "/data"}],
            }
        ]
        with self.assertRaises(ValueError):
            verify_live_data_layout(sts)

    def test_lifecycle_hooks_cannot_bypass_qualification(self):
        from compatibility_profile import verify_live_data_layout

        sts = json.loads(
            (ROOT / "apps/elastickv-compatibility/assets.json").read_text()
        )["statefulset"]
        sts["spec"]["template"]["spec"]["containers"][0]["lifecycle"] = {
            "postStart": {"exec": {"command": ["touch", "/var/lib/elastickv/change"]}}
        }
        with self.assertRaises(ValueError):
            verify_live_data_layout(sts)

    def test_initial_binding_refuses_changed_pods_and_rollback_resume_retains_claim_guard(
        self,
    ):
        entry = {
            "from": OLD["source"],
            "to": NEW["source"],
            "image": NEW["image"],
            "profile_sha256": "profile",
            "production_bindings": {"claims": {"data": "uid"}, "pods": {"pod": "uid"}},
        }
        store = Mock()
        store.policy = {"require_qualification": True, "approved_transitions": [entry]}
        store.state = {"last_good": OLD, "transaction": None}
        store.profile.return_value = "profile"
        runtime = Mock()
        runtime.production_bindings.return_value = entry["production_bindings"]
        agent.bind_qualification(store, runtime, NEW)
        runtime.production_bindings.return_value = {
            "claims": {"data": "uid"},
            "pods": {"pod": "changed"},
        }
        with self.assertRaises(agent.UnsafeState):
            agent.bind_qualification(store, runtime, NEW)
        store.state["transaction"] = {
            "qualification": entry,
            "from": OLD,
            "to": NEW,
            "phase": "rollback",
        }
        agent.bind_qualification(store, runtime, None)
        self.assertEqual(entry, runtime.qualification)
        store.policy["approved_transitions"] = []
        with self.assertRaises(agent.UnsafeState):
            agent.bind_qualification(store, runtime, None)


if __name__ == "__main__":
    unittest.main()
