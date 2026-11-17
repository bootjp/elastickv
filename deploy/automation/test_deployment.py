"""Portability, isolation and offline rendering regressions."""

import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

import prepare
import settings
import yaml


class ConfigurationTests(unittest.TestCase):
    def test_distinct_namespaces_paths_and_dedicated_storage_are_required(self):
        for update in [
            {"validation_namespace": "elastickv"},
            {"control_namespace": "elastickv"},
            {"application_path": "../escape"},
            {"code_path": "/absolute"},
            {"evidence_path": "apps/elastickv/proof"},
            {"nfs_root": "/"},
            {"backup_claims": ["data-elastickv-0"]},
            {"unknown": True},
        ]:
            with self.subTest(update=update), self.assertRaises(ValueError):
                settings.validate({**copy.deepcopy(settings.DEFAULTS), **update})

    def test_examples_cannot_activate_real_entrypoints(self):
        with self.assertRaises(ValueError):
            settings.validate(copy.deepcopy(settings.DEFAULTS), live=True)

    def test_site_change_invalidates_cached_qualification(self):
        import compatibility_profile as profile

        frame = (
            {"controller_image": "digest"},
            {},
            {"source": "a" * 40},
            {"source": "b" * 40},
            "profile",
            {"claims": {}, "pods": {}},
            True,
            "test",
        )
        first = profile.qualification_identity(*frame)
        from unittest.mock import patch

        with patch.dict(profile.SITE, {"control_namespace": "other-system"}):
            self.assertNotEqual(first, profile.qualification_identity(*frame))

    def test_unknown_feature_environment_cannot_be_qualified_as_legacy(self):
        import agent
        import compatibility

        base = {"env": []}
        actual = {
            "command": ["/app"],
            "args": [],
            "env": [{"name": "ELASTICKV_TSO_MODE", "value": "phase-d"}],
        }
        with self.assertRaises(agent.UnsafeState):
            compatibility.test_container(base, actual, {"image": "test"})


class RenderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = pathlib.Path(self.temp.name) / "gitops"
        self.config = copy.deepcopy(settings.DEFAULTS)
        self.config.update(
            gitops_repository="acme/platform",
            source_repository="acme/database",
            gitops_branch="production",
            source_branch="stable",
            application_image="ghcr.io/acme/database",
            control_namespace="release-system",
            production_namespace="database-prod",
            validation_namespace="database-tests",
            application_path="workloads/database",
            release_path="releases/controller",
            qualification_path="releases/qualification",
            evidence_path="evidence/database",
            code_path="release-tools",
            nfs_root="/exports/test-volumes",
            storage_class="test-nfs",
            backup_claims=["backup-primary", "backup-offsite"],
            commit_identity={
                "name": "Release Maintainer",
                "email": "release@acme.invalid",
            },
        )
        self.controller = "ghcr.io/acme/controller@sha256:" + "c" * 64

    def render(self):
        prepare.render(
            self.config,
            self.output,
            self.controller,
            "a" * 40,
            self.config["application_image"] + "@sha256:" + "a" * 64,
        )

    def test_custom_site_renders_consistently_and_stays_suspended(self):
        self.render()
        site = json.loads((self.output / "release-tools/deployment.json").read_text())
        self.assertEqual(self.config, site)
        for directory in ["releases/controller", "releases/qualification"]:
            docs = list(
                yaml.safe_load_all(
                    (self.output / directory / "controller.yaml").read_text()
                )
            )
            cron = next(d for d in docs if d["kind"] == "CronJob")
            self.assertTrue(cron["spec"]["suspend"])
            self.assertEqual("release-system", cron["metadata"]["namespace"])
            pod = cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            self.assertEqual(self.controller, pod["containers"][0]["image"])
            self.assertTrue(
                any(
                    e["name"] == "ELASTICKV_DEPLOYMENT_CONFIG"
                    for e in pod["containers"][0]["env"]
                )
            )
        policy = json.loads(
            (self.output / "releases/controller/policy.json").read_text()
        )
        self.assertEqual([], policy["approved_transitions"])
        self.assertTrue(policy["require_qualification"])
        assets = json.loads(
            (self.output / "releases/qualification/assets.json").read_text()
        )
        self.assertEqual("database-tests", assets["validation_policy"]["namespace"])
        self.assertEqual(
            self.controller,
            assets["cleaner"]["spec"]["template"]["spec"]["containers"][0]["image"],
        )
        self.assertEqual(
            "test-nfs",
            assets["statefulset"]["spec"]["volumeClaimTemplates"][0]["spec"][
                "storageClassName"
            ],
        )
        worker = assets["validator"]["spec"]["template"]["spec"]
        self.assertTrue(any(v["name"] == "deployment" for v in worker["volumes"]))
        self.assertFalse(any("secret" in v for v in worker["volumes"]))
        workflow = (
            self.output / ".github/workflows/elastickv-compatibility-approval.yml"
        ).read_text()
        self.assertIn("ref: production", workflow)
        self.assertNotIn("ref: main", workflow)
        self.assertIn("release-tools/deployment.json", workflow)
        role = list(
            yaml.safe_load_all(
                (self.output / "releases/qualification/rbac.yaml").read_text()
            )
        )
        claims = next(
            r
            for r in role
            if r["kind"] == "Role" and r["metadata"]["namespace"] == "database-prod"
        )["rules"][-1]["resourceNames"]
        self.assertEqual(
            set(
                ["data-elastickv-" + str(i) for i in range(3)]
                + self.config["backup_claims"]
            ),
            set(claims),
        )

    def test_nested_vendored_code_path_has_correct_repository_root(self):
        self.config["code_path"] = "infrastructure/release-tools"
        self.render()
        env = {
            **os.environ,
            "ELASTICKV_DEPLOYMENT_CONFIG": str(
                self.output / self.config["code_path"] / "deployment.json"
            ),
            "PYTHONPATH": str(self.output / self.config["code_path"]),
        }
        result = subprocess.check_output(
            [
                sys.executable,
                "-c",
                "import compatibility_pr;print(compatibility_pr.ROOT)",
            ],
            env=env,
            cwd=self.output,
            text=True,
        )
        self.assertEqual(str(self.output.resolve()), result.strip())

    def test_output_inside_source_package_is_refused_before_copy(self):
        from unittest.mock import patch

        source = pathlib.Path(self.temp.name) / "source"
        source.mkdir()
        with patch.object(prepare, "PACKAGE", source), self.assertRaises(ValueError):
            prepare.render(self.config, source / "nested")
        self.assertFalse((source / "nested").exists())

    def test_controller_code_image_and_policy_versions_must_match(self):
        import compatibility_pr

        self.render()
        policy = json.loads(
            (self.output / "releases/controller/policy.json").read_text()
        )
        config = json.loads(
            (self.output / "releases/qualification/policy.json").read_text()
        )
        controller = (self.output / "releases/controller/controller.yaml").read_text()
        code = self.output / "release-tools"
        compatibility_pr.verify_controller_binding(config, policy, controller, code)
        with self.assertRaises(ValueError):
            compatibility_pr.verify_controller_binding(
                config, {**policy, "controller_sha256": "stale"}, controller, code
            )
        with self.assertRaises(ValueError):
            compatibility_pr.verify_controller_binding(
                config,
                policy,
                controller.replace(
                    self.controller, "ghcr.io/acme/other@sha256:" + "d" * 64
                ),
                code,
            )
        (code / "agent.py").write_text("changed source")
        with self.assertRaises(ValueError):
            compatibility_pr.verify_controller_binding(config, policy, controller, code)

    def test_render_never_overwrites_existing_repository(self):
        self.output.mkdir()
        (self.output / "keep").write_text("keep")
        with self.assertRaises(ValueError):
            self.render()
        self.assertEqual("keep", (self.output / "keep").read_text())

    def test_custom_controller_config_is_loaded_in_a_fresh_process(self):
        self.render()
        script = """import agent,compatibility,compatibility_runtime as worker,compatibility_profile as profile
assert agent.SPEC=='workloads/database/statefulset.yaml'
assert agent.AUTHOR['email']=='release@acme.invalid'
assert compatibility.NS==worker.NS=='database-tests'
assert compatibility.PROD=='database-prod'
assert compatibility.CONTROL=='release-system'
assert profile.SPEC==agent.SPEC
assert agent.Lease(None).path=='/apis/coordination.k8s.io/v1/namespaces/release-system/leases/elastickv-release'
from unittest.mock import patch
with patch.object(agent.Kubernetes,'request') as request:
 try: compatibility.ScopedKubernetes.__new__(compatibility.ScopedKubernetes).request('/api/v1/namespaces/database-prod/pods/elastickv-0','DELETE')
 except agent.UnsafeState: pass
 else: raise AssertionError('Production write accepted')
 request.assert_not_called()
run=dict(id=1,name='Test',head_sha='a'*40,head_branch='stable',event='push',status='completed',conclusion='success')
assert agent.ci_ready([run],'a'*40,['Test'])
assert not agent.ci_ready([{**run,'head_branch':'main'}],'a'*40,['Test'])
"""
        environment = {
            **os.environ,
            "ELASTICKV_DEPLOYMENT_CONFIG": str(
                self.output / "release-tools/deployment.json"
            ),
            "PYTHONPATH": str(self.output / "release-tools"),
        }
        subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            env=environment,
            cwd=self.output,
            capture_output=True,
        )

    def test_rendered_kustomize_resources_have_no_duplicates(self):
        if not __import__("shutil").which("kubectl"):
            self.skipTest("kubectl unavailable")
        self.render()
        identities = set()
        for directory in [
            "workloads/database",
            "releases/controller",
            "releases/qualification",
        ]:
            text = subprocess.check_output(
                ["kubectl", "kustomize", str(self.output / directory)], text=True
            )
            for item in yaml.safe_load_all(text):
                identity = (
                    item["kind"],
                    item["metadata"].get("namespace", ""),
                    item["metadata"]["name"],
                )
                if directory == "workloads/database":
                    if item["kind"] == "Namespace":
                        self.assertEqual("database-prod", item["metadata"]["name"])
                    else:
                        self.assertEqual("database-prod", item["metadata"]["namespace"])
                self.assertNotIn(identity, identities)
                identities.add(identity)


class PublicationTests(unittest.TestCase):
    def test_examples_contain_no_private_addresses_or_real_release_history(self):
        import re

        for path in (settings.PACKAGE / "examples").rglob("*"):
            if not path.is_file():
                continue
            text = path.read_text()
            self.assertIsNone(
                re.search(
                    r"\b(?:192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+|172\.(?:1[6-9]|2[0-9]|3[01])\.\d+\.\d+)\b",
                    text,
                ),
                str(path),
            )
            for sha in re.findall(r"\b[0-9a-f]{40}\b", text):
                self.assertEqual({"0"}, set(sha), str(path))
            for host in re.findall(r"https?://([A-Za-z0-9.-]+)", text):
                self.assertTrue(
                    host.endswith((".svc", ".example.com", ".example.test", ".invalid"))
                    or host
                    in ("example.com", "github.com", "api.github.com", "kubernetes.io"),
                    (str(path), host),
                )


if __name__ == "__main__":
    unittest.main()
