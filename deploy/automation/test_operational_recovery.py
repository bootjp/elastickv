"""Operational regressions using real admission and rollout control paths."""

import copy
import json
import unittest
from unittest.mock import MagicMock, Mock, patch

import agent
import compatibility
from test_compatibility import NEW, OLD, ROOT


class LiveLayoutTests(unittest.TestCase):
    def fixture(self):
        sts = json.loads(
            (ROOT / "apps/elastickv-compatibility/assets.json").read_text()
        )["statefulset"]
        sts["spec"]["template"]["spec"]["containers"][0]["image"] = OLD["image"]
        pods = {}
        for i in range(3):
            name = f"elastickv-{i}"
            spec = copy.deepcopy(sts["spec"]["template"]["spec"])
            spec["volumes"].append(
                {"name": "data", "persistentVolumeClaim": {"claimName": "data-" + name}}
            )
            pods[name] = {
                "metadata": {"name": name, "uid": name},
                "spec": spec,
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"name": "elastickv", "restartCount": 0}],
                },
            }

        def request(path):
            if "/statefulsets/" in path:
                return sts
            if "/pods/" in path:
                return pods[path.rsplit("/", 1)[-1]]
            return {"metadata": {"uid": path}, "status": {"phase": "Bound"}}

        return sts, pods, Mock(request=Mock(side_effect=request))

    def test_actual_pod_forbidden_layout_is_rejected_before_qualification(self):
        for kind in (
            "hook",
            "init",
            "writable-sidecar",
            "backup",
            "subpath",
            "wrong-claim",
        ):
            sts, pods, kube = self.fixture()
            spec = pods["elastickv-1"]["spec"]
            app = spec["containers"][0]
            if kind == "hook":
                app["lifecycle"] = {
                    "preStop": {
                        "exec": {"command": ["touch", "/var/lib/elastickv/stale"]}
                    }
                }
            if kind == "init":
                spec["initContainers"] = [{"name": "legacy", "image": "test"}]
            if kind == "writable-sidecar":
                spec["containers"].append(
                    {
                        "name": "legacy",
                        "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                    }
                )
            if kind == "backup":
                spec["volumes"].append(
                    {
                        "name": "backup",
                        "persistentVolumeClaim": {"claimName": "elastickv-backup"},
                    }
                )
            if kind == "subpath":
                next(m for m in app["volumeMounts"] if m["name"] == "data")[
                    "subPath"
                ] = "old"
            if kind == "wrong-claim":
                next(v for v in spec["volumes"] if v["name"] == "data")[
                    "persistentVolumeClaim"
                ]["claimName"] = "elastickv-backup"
            with self.subTest(kind=kind), self.assertRaises(agent.UnsafeState):
                compatibility.production(kube, OLD)

    def test_valid_bound_data_and_projected_service_account_volume_are_allowed(self):
        sts, pods, kube = self.fixture()
        pods["elastickv-0"]["spec"]["volumes"][-1]["persistentVolumeClaim"][
            "readOnly"
        ] = False
        for p in pods.values():
            p["spec"]["volumes"].append(
                {"name": "kube-api-access", "projected": {"sources": []}}
            )
            p["spec"]["containers"][0]["volumeMounts"].append(
                {
                    "name": "kube-api-access",
                    "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
                    "readOnly": True,
                }
            )
        self.assertEqual(3, len(compatibility.production(kube, OLD)["pods"]))


class RollbackRecoveryTests(unittest.TestCase):
    def fixture(self):
        names = [f"elastickv-{i}" for i in range(3)]
        leader = [names[0]]
        actions = []
        pods = {
            n: {
                "metadata": {"uid": n, "labels": {"controller-revision-hash": "old"}},
                "spec": {"containers": [{"name": "elastickv", "image": OLD["image"]}]},
                "status": {
                    "podIP": "127.0.0.1",
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
            for n in names
        }
        pods[names[0]]["metadata"]["labels"]["controller-revision-hash"] = "new"
        pods[names[0]]["spec"]["containers"][0]["image"] = NEW["image"]
        pods[names[0]]["status"]["conditions"][0]["status"] = "False"
        sts = {
            "metadata": {"generation": 2},
            "spec": {
                "replicas": 3,
                "updateStrategy": {"type": "OnDelete"},
                "template": {
                    "spec": {
                        "containers": [{"name": "elastickv", "image": OLD["image"]}]
                    }
                },
            },
            "status": {"observedGeneration": 2, "updateRevision": "old"},
        }

        def request(path, method="GET", data=None):
            if path == runtime.sts_path:
                return copy.deepcopy(sts)
            name = path.rsplit("/", 1)[-1]
            if method == "DELETE":
                self.assertNotEqual(name, leader[0])
                self.assertEqual({"preconditions": {"uid": name}}, data)
                actions.append(("delete", name))
                overrides.pop(name, None)
                pods[name]["metadata"]["uid"] += "-restored"
                pods[name]["metadata"]["labels"]["controller-revision-hash"] = "old"
                pods[name]["spec"]["containers"][0]["image"] = OLD["image"]
                pods[name]["status"]["conditions"][0]["status"] = "True"
            return copy.deepcopy(pods[name])

        git = Mock(state={"transaction": {"from": OLD, "to": NEW, "phase": "rollback"}})
        runtime = agent.Runtime(
            {
                "namespace": "elastickv",
                "statefulset": "elastickv",
                "members": names,
                "startup_timeout": 1,
            },
            Mock(),
            Mock(),
            git,
        )
        runtime.kube.request.side_effect = request
        runtime.pods = Mock(side_effect=lambda: copy.deepcopy(pods))
        runtime.pvcs = Mock(return_value={"data": "retained"})
        runtime.canary = Mock()
        runtime.wait = lambda predicate, timeout: predicate() or (_ for _ in ()).throw(
            agent.ReleaseError("predicate false")
        )
        overrides = {}

        def rpc(name, command, *args):
            if command == "configuration":
                return "\n".join(f'  id: "{n}"\n  suffrage: "voter"' for n in names)
            if command == "leadership_transfer_to_server":
                actions.append(("transfer", name, args[0]))
                leader[0] = args[0]
                return ""
            status = {
                "state": "LEADER" if name == leader[0] else "FOLLOWER",
                "leader_id": json.dumps(leader[0]),
                "term": "3",
                "configuration_index": "2",
                "pending_conf_change": "false",
                "fsm_pending": "0",
                "applied_index": "10",
                "commit_index": "10",
                "last_snapshot_index": "5",
                "last_contact_nanos": "1",
            }
            status.update(overrides.get(name, {}))
            return "\n".join(f"{k}: {v}" for k, v in status.items())

        runtime.rpc = Mock(side_effect=rpc)
        return runtime, pods, actions, overrides

    def test_rollback_transfers_live_unready_leader_then_restores_image(self):
        runtime, pods, actions, overrides = self.fixture()
        overrides["elastickv-0"] = {"fsm_pending": "1", "applied_index": "9"}
        response = MagicMock(status=200)
        response.__enter__.return_value = response
        with (
            patch.object(agent.urllib.request, "urlopen", return_value=response),
            patch.object(agent, "http_json", return_value={"ready": True}),
        ):
            runtime.roll(OLD, {"data": "retained"})
        self.assertEqual(
            [("transfer", "elastickv-0", "elastickv-1"), ("delete", "elastickv-0")],
            actions,
        )
        self.assertTrue(
            all(
                p["spec"]["containers"][0]["image"] == OLD["image"]
                for p in pods.values()
            )
        )
        runtime.canary.assert_called()

    def test_unproven_quorum_never_transfers_or_deletes(self):
        for failure in (
            "no-majority",
            "term",
            "configuration",
            "pending",
            "stale",
            "unapplied",
            "unknown-leader",
            "membership",
            "unreachable-leader",
            "leader-commit-ahead",
        ):
            runtime, pods, actions, overrides = self.fixture()
            if failure == "no-majority":
                pods["elastickv-1"]["status"]["conditions"][0]["status"] = "False"
            if failure == "term":
                overrides["elastickv-0"] = {"term": "4"}
            if failure == "configuration":
                overrides["elastickv-0"] = {"configuration_index": "3"}
            if failure == "pending":
                overrides["elastickv-0"] = {"pending_conf_change": "true"}
            if failure == "stale":
                overrides["elastickv-1"] = {"last_contact_nanos": "10000000001"}
            if failure == "unapplied":
                overrides["elastickv-1"] = {"applied_index": "9"}
            if failure == "unknown-leader":
                overrides["elastickv-1"] = {"leader_id": '"foreign"'}
            if failure == "leader-commit-ahead":
                overrides["elastickv-0"] = {"commit_index": "11"}
            if failure in ("membership", "unreachable-leader"):
                original = runtime.rpc.side_effect

                def bad_rpc(name, command, *args):
                    if failure == "membership" and command == "configuration":
                        return '  id: "foreign"\n  suffrage: "voter"'
                    if failure == "unreachable-leader" and name == "elastickv-0":
                        raise agent.ReleaseError("unreachable")
                    return original(name, command, *args)

                runtime.rpc.side_effect = bad_rpc
            with self.subTest(failure=failure), self.assertRaises(agent.ReleaseError):
                runtime.roll(OLD, {"data": "retained"})
            self.assertEqual([], actions)

    def test_ownership_pvc_and_pod_changes_stop_before_transfer(self):
        for failure in ("lease", "pvc", "pod", "image"):
            runtime, pods, actions, overrides = self.fixture()
            if failure == "lease":
                runtime.lease.guard.side_effect = agent.LostLease("lost")
            if failure == "pvc":
                runtime.pvcs.side_effect = [{"data": "retained"}, {"data": "rebound"}]
            if failure in ("pod", "image"):
                calls = [0]

                def changed():
                    calls[0] += 1
                    current = copy.deepcopy(pods)
                    if calls[0] >= 3:
                        if failure == "pod":
                            current["elastickv-1"]["metadata"]["uid"] = "recreated"
                        else:
                            current["elastickv-1"]["spec"]["containers"][0]["image"] = (
                                "unexpected"
                            )
                    return current

                runtime.pods.side_effect = changed
            with self.subTest(failure=failure), self.assertRaises(agent.ReleaseError):
                runtime.roll(OLD, {"data": "retained"})
            self.assertEqual([], actions)

    def test_failed_transfer_never_deletes_a_voter(self):
        runtime, pods, actions, overrides = self.fixture()
        original = runtime.rpc.side_effect

        def failed(name, command, *args):
            if command == "leadership_transfer_to_server":
                raise agent.ReleaseError("transfer failed")
            return original(name, command, *args)

        runtime.rpc.side_effect = failed
        with self.assertRaises(agent.ReleaseError):
            runtime.roll(OLD, {"data": "retained"})
        self.assertEqual([], actions)

    def test_transfer_without_confirmed_new_leader_never_deletes(self):
        runtime, pods, actions, overrides = self.fixture()
        original = runtime.rpc.side_effect

        def ignored(name, command, *args):
            if command == "leadership_transfer_to_server":
                actions.append(("transfer", name, args[0]))
                return ""
            return original(name, command, *args)

        runtime.rpc.side_effect = ignored
        with self.assertRaises(agent.ReleaseError):
            runtime.roll(OLD, {"data": "retained"})
        self.assertEqual([("transfer", "elastickv-0", "elastickv-1")], actions)

    def test_unverified_readindex_or_lost_ownership_after_transfer_never_deletes(self):
        for failure in ("readindex", "lease"):
            runtime, pods, actions, overrides = self.fixture()
            response = MagicMock(status=503 if failure == "readindex" else 200)
            response.__enter__.return_value = response
            if failure == "lease":
                runtime.lease.guard.side_effect = [None, None, agent.LostLease("lost")]
            with (
                self.subTest(failure=failure),
                patch.object(agent.urllib.request, "urlopen", return_value=response),
                self.assertRaises(agent.ReleaseError),
            ):
                runtime.roll(OLD, {"data": "retained"})
            self.assertEqual([("transfer", "elastickv-0", "elastickv-1")], actions)

    def test_forward_rollout_does_not_use_rollback_recovery(self):
        runtime, pods, actions, overrides = self.fixture()
        runtime.git.state["transaction"]["phase"] = "forward"
        with self.assertRaises(agent.ReleaseError):
            runtime.quorum(pods)
        self.assertEqual([], actions)
