"""Qualify images in a dedicated namespace; publish branches, never production state."""

import base64
import copy
import hashlib
import json
import os
import pathlib
import re
import time
import urllib.parse

import agent
import source_review
from compatibility_profile import (
    configuration_hash,
    live_runtime_hash,
    qualification_identity,
    verify_dedicated_volume,
    verify_live_data_layout,
    verify_running_pod_layout,
)
from settings import CONFIG as SITE
from settings import require_configured

NS = SITE["validation_namespace"]
PROD = SITE["production_namespace"]
CONTROL = SITE["control_namespace"]
REPO = SITE["gitops_repository"]
POLICY = SITE["release_path"] + "/policy.json"
CONFIG = SITE["qualification_path"] + "/policy.json"


class ScopedKubernetes(agent.Kubernetes):
    def request(self, path, method="GET", data=None):
        if (
            method == "GET"
            and path.startswith("/api/v1/persistentvolumes/")
            and path.rsplit("/", 1)[-1] in getattr(self, "allowed_pvs", set())
        ):
            return super().request(path, method, data)
        match = re.search(r"/namespaces/([^/]+)/", path)
        if not match or match[1] not in (NS, PROD, CONTROL):
            raise agent.UnsafeState("Unrelated Kubernetes namespace forbidden")
        if match[1] == PROD and method != "GET":
            raise agent.UnsafeState("Production writes forbidden")
        if match[1] == CONTROL and not path.endswith("/leases/elastickv-compatibility"):
            raise agent.UnsafeState(
                "Only the compatibility lease is writable in the control namespace"
            )
        if match[1] == NS and method != "GET":
            guard = getattr(self, "write_guard", None)
            if guard is None:
                raise agent.UnsafeState("Qualification lease guard is missing")
            guard()
        return super().request(path, method, data)


def get_json(github, path, head):
    value = github.request("contents/" + path + "?ref=" + head)
    return json.loads(base64.b64decode(value["content"]))


def runtime_inputs(store, head):
    files = {}
    for path in store.config_inputs(head):
        if path == agent.STATE:
            continue
        result = store.github.request("contents/" + path + "?ref=" + head)
        files[path] = base64.b64decode(result["content"]).decode()
    return files


def production(kube, baseline):
    prefix = "/api/v1/namespaces/" + PROD
    sts = kube.request("/apis/apps/v1/namespaces/" + PROD + "/statefulsets/elastickv")
    try:
        verify_live_data_layout(sts)
    except ValueError as error:
        raise agent.UnsafeState(str(error)) from None
    if (
        sts["spec"]["replicas"] != 3
        or sts["spec"]["updateStrategy"]["type"] != "OnDelete"
    ):
        raise agent.UnsafeState("Production is not the supported three-voter profile")
    container = next(
        c
        for c in sts["spec"]["template"]["spec"]["containers"]
        if c["name"] == "elastickv"
    )
    if container.get("envFrom"):
        raise agent.UnsafeState(
            "Unresolved template envFrom settings need profile review"
        )
    pods = {}
    for n in range(3):
        pod = kube.request(prefix + "/pods/elastickv-" + str(n))
        try:
            verify_running_pod_layout(pod, "data-elastickv-" + str(n))
        except ValueError as error:
            raise agent.UnsafeState(str(error)) from None
        actual = next(c for c in pod["spec"]["containers"] if c["name"] == "elastickv")
        image = actual["image"]
        if actual.get("envFrom") or any(
            actual.get(k, []) != container.get(k, [])
            for k in ("command", "args", "env", "envFrom", "workingDir")
        ):
            raise agent.UnsafeState(
                "Running production Pod runtime differs from qualified template"
            )
        if not agent.pod_ready(pod) or image != baseline["image"]:
            raise agent.UnsafeState("Production baseline is not stable")
        pods[pod["metadata"]["name"]] = {
            "uid": pod["metadata"]["uid"],
            "restarts": {
                c["name"]: c["restartCount"] for c in pod["status"]["containerStatuses"]
            },
        }
    claims = {}
    for name in ["data-elastickv-" + str(n) for n in range(3)] + SITE["backup_claims"]:
        claim = kube.request(prefix + "/persistentvolumeclaims/" + name)
        if claim.get("status", {}).get("phase") != "Bound" or claim["metadata"].get(
            "deletionTimestamp"
        ):
            raise agent.UnsafeState("Production/backup claim is not stable and Bound")
        claims[name] = claim["metadata"]["uid"]
    return {
        "claims": claims,
        "container": container,
        "pods": pods,
        "live_runtime_sha256": live_runtime_hash(sts),
    }


def test_container(base, actual, old):
    if actual.get("envFrom"):
        raise agent.UnsafeState(
            "Unresolved envFrom settings need compatibility profile review"
        )
    container = copy.deepcopy(base)
    if actual.get("command") != ["/app"]:
        raise agent.UnsafeState("Unknown production entrypoint")
    allowed = {
        "address",
        "raftId",
        "raftDataDir",
        "redisAddress",
        "dynamoAddress",
        "raftRedisMap",
        "raftDynamoMap",
        "s3Address",
        "s3Region",
        "s3PathStyleOnly",
        "s3CredentialsFile",
        "raftS3Map",
        "sqsAddress",
        "sqsRegion",
        "sqsCredentialsFile",
        "raftSqsMap",
        "pprofAddress",
        "sqsPublicEndpoint",
        "healthAddress",
    }
    ignored = ("admin", "keyviz", "metrics")
    args = []
    for value in actual["args"]:
        key = value.removeprefix("--").split("=", 1)[0]
        if key.startswith(ignored):
            continue
        if key not in allowed:
            raise agent.UnsafeState(
                "Runtime flag needs compatibility profile review: " + key
            )
        if key == "sqsPublicEndpoint":
            value = "--sqsPublicEndpoint=http://elastickv." + NS + ".svc:9324"
        if "." + PROD + ".svc" in value or any(
            host in value for host in actual.get("production_hosts", [])
        ):
            raise agent.UnsafeState("Production endpoint present in sandbox arguments")
        args.append(value)
    args += [
        "--raftBootstrap",
        "--raftBootstrapMembers="
        + ",".join(
            f"elastickv-{n}=elastickv-{n}.elastickv-peer.$(POD_NAMESPACE).svc:50051"
            for n in range(3)
        ),
    ]
    container["args"] = args
    safe_environment = {
        "ELASTICKV_RAFT_SEND_STREAM",
        "ELASTICKV_RAFT_SNAPSHOT_COUNT",
        "ELASTICKV_PEBBLE_CACHE_MB",
        "ELASTICKV_LOCK_RESOLVER_ENABLED",
        "ELASTICKV_FSM_COMPACTOR_ENABLED",
        "ELASTICKV_REDIS_DELTA_COMPACTOR_ENABLED",
        "ELASTICKV_RAFT_PRE_VOTE",
    }
    safe = []
    for entry in actual.get("env", []):
        name = entry["name"]
        if name.startswith("ELASTICKV_"):
            if name not in safe_environment or "valueFrom" in entry:
                raise agent.UnsafeState(
                    "Unknown or secret-based runtime tuning needs profile review"
                )
            safe.append(copy.deepcopy(entry))
    safe = [
        e
        for e in safe
        if e["name"]
        not in ("ELASTICKV_RAFT_SNAPSHOT_COUNT", "ELASTICKV_PEBBLE_CACHE_MB")
    ]
    container["env"] = (
        [
            e
            for e in container["env"]
            if e["name"] in ("POD_NAME", "POD_NAMESPACE", "GOMEMLIMIT")
        ]
        + safe
        + [
            {"name": "ELASTICKV_RAFT_SNAPSHOT_COUNT", "value": "64"},
            {"name": "ELASTICKV_PEBBLE_CACHE_MB", "value": "32"},
        ]
    )
    container["image"] = old["image"]
    return container


class Runner:
    def __init__(self, kube, lease, config):
        self.kube, self.lease, self.config = kube, lease, config
        self.core = "/api/v1/namespaces/" + NS
        self.batch = "/apis/batch/v1/namespaces/" + NS + "/jobs"
        self.sts = "/apis/apps/v1/namespaces/" + NS + "/statefulsets/elastickv"

    def optional(self, path):
        try:
            return self.kube.request(path)
        except agent.HttpReleaseError as error:
            if error.status != 404:
                raise
            return None

    def wait(self, predicate, timeout):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            self.lease.tick()
            if predicate():
                return
            time.sleep(3)
        raise agent.ReleaseError("Compatibility operation timed out")

    def remove_job(self, name):
        if self.optional(self.batch + "/" + name):
            self.kube.request(
                self.batch + "/" + name, "DELETE", {"propagationPolicy": "Foreground"}
            )
            self.wait(lambda: self.optional(self.batch + "/" + name) is None, 120)

    def volumes(self):
        cm = self.kube.request(
            self.core + "/configmaps/compatibility-volume-identities"
        )
        if cm.get("immutable") is not True:
            raise agent.UnsafeState("Dedicated volume registration must be immutable")
        registration = json.loads(cm["data"]["identities.json"])
        expected = registration["pvc_uids"]
        if set(expected) != {"data-elastickv-" + str(i) for i in range(3)}:
            raise agent.UnsafeState("Invalid dedicated claim inventory")
        rows = registration["volumes"]
        if {r["claim"] for r in rows} != set(expected) or len(rows) != 3:
            raise agent.UnsafeState("Invalid dedicated volume inventory")
        self.kube.allowed_pvs = {r["pv"] for r in rows}
        actual = {}
        for row in rows:
            pvc = self.kube.request(
                self.core + "/persistentvolumeclaims/" + row["claim"]
            )
            pv = self.kube.request("/api/v1/persistentvolumes/" + row["pv"])
            try:
                verify_dedicated_volume(pvc, pv, row, NS)
            except ValueError as error:
                raise agent.UnsafeState(str(error)) from None
            actual[row["claim"]] = pvc["metadata"]["uid"]
        if actual != expected:
            raise agent.UnsafeState("Dedicated claim identity changed")
        self.registered_volumes = rows
        return actual

    def completed(self, name):
        job = self.kube.request(self.batch + "/" + name)
        if job.get("status", {}).get("failed"):
            raise agent.ReleaseError("Compatibility Job failed: " + name)
        return job.get("status", {}).get("succeeded") == 1

    def clean(self, assets):
        self.volumes()
        self.remove_job("compatibility-validation")
        self.remove_job("compatibility-clean-synthetic")
        if self.optional(self.sts):
            self.kube.request(self.sts, "DELETE", {"propagationPolicy": "Foreground"})
        self.wait(
            lambda: not [
                p
                for p in self.kube.request(self.core + "/pods")["items"]
                if p["metadata"].get("labels", {}).get("app.kubernetes.io/name")
                == "elastickv"
            ],
            180,
        )
        self.volumes()
        lock = (
            "/apis/coordination.k8s.io/v1/namespaces/"
            + NS
            + "/leases/elastickv-release"
        )
        old = self.kube.request(lock)
        self.kube.request(
            lock,
            "PATCH",
            {
                "metadata": {"resourceVersion": old["metadata"]["resourceVersion"]},
                "spec": {"holderIdentity": ""},
            },
        )
        self.kube.request(self.batch, "POST", assets["cleaner"])
        self.wait(lambda: self.completed("compatibility-clean-synthetic"), 180)
        self.remove_job("compatibility-clean-synthetic")

    def configure(self, name, data):
        path = self.core + "/configmaps/" + name
        old = self.optional(path)
        if old:
            self.kube.request(
                path,
                "PATCH",
                {
                    "metadata": {"resourceVersion": old["metadata"]["resourceVersion"]},
                    "data": data,
                    "binaryData": None,
                },
            )
        else:
            self.kube.request(
                self.core + "/configmaps",
                "POST",
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": name, "namespace": NS},
                    "data": data,
                },
            )

    def validate(self, assets, baseline, candidate, profile, actual):
        self.clean(assets)
        policy = copy.deepcopy(assets["validation_policy"])
        policy.update(
            {
                "baseline": baseline,
                "candidate": candidate,
                "profile_sha256": profile,
                "controller_image": self.config["controller_image"],
                "pvc_uids": {
                    name.removeprefix("data-"): uid
                    for name, uid in self.volumes().items()
                },
                "agent_sha256": self.config["agent_sha256"],
                "volumes": self.registered_volumes,
            }
        )
        self.configure("compatibility-run", {"policy.json": json.dumps(policy)})
        self.configure("validation-evidence", {"summary.json": "{}"})
        sts = copy.deepcopy(assets["statefulset"])
        templates = sts["spec"]["volumeClaimTemplates"]
        if (
            len(templates) != 1
            or templates[0]["metadata"]["name"] != "data"
            or templates[0]["spec"].get("volumeName")
            or templates[0]["spec"].get("selector")
            or templates[0]["spec"]["storageClassName"] != SITE["storage_class"]
        ):
            raise agent.UnsafeState(
                "Only the registered dedicated claim template is allowed"
            )
        if any(
            "persistentVolumeClaim" in v
            for v in sts["spec"]["template"]["spec"].get("volumes", [])
        ):
            raise agent.UnsafeState("Unexpected explicit PVC mount")
        sts["spec"]["template"]["spec"]["containers"][0] = test_container(
            sts["spec"]["template"]["spec"]["containers"][0], actual, baseline
        )
        self.kube.request(
            "/apis/apps/v1/namespaces/" + NS + "/statefulsets", "POST", sts
        )
        self.wait(
            lambda: len(self.kube.request(self.core + "/pods")["items"]) >= 3
            and sum(
                agent.pod_ready(p)
                for p in self.kube.request(self.core + "/pods")["items"]
                if p["metadata"].get("labels", {}).get("app.kubernetes.io/name")
                == "elastickv"
            )
            == 3,
            600,
        )
        job = copy.deepcopy(assets["validator"])
        job["spec"]["template"]["spec"]["containers"][0]["image"] = self.config[
            "controller_image"
        ]
        self.kube.request(self.batch, "POST", job)
        self.wait(lambda: self.completed("compatibility-validation"), 2400)
        self.volumes()
        evidence = self.kube.request(self.core + "/configmaps/validation-evidence")
        report = json.loads(evidence["data"]["summary.json"])
        job_record = self.kube.request(self.batch + "/compatibility-validation")
        pods = self.kube.request(self.core + "/pods")["items"]
        if not any(
            p["metadata"]["uid"] == report.get("worker_pod_uid")
            and p.get("status", {}).get("phase") == "Succeeded"
            and any(
                o["uid"] == job_record["metadata"]["uid"]
                for o in p["metadata"].get("ownerReferences", [])
            )
            for p in pods
        ):
            raise agent.UnsafeState(
                "Evidence does not belong to the completed validation Job"
            )
        if (
            report.get("passed") is not True
            or report.get("profile_sha256") != profile
            or report.get("to_image") != candidate["image"]
        ):
            raise agent.UnsafeState("Missing/mismatched compatibility proof")
        return report, base64.b64decode(evidence["binaryData"]["observations.json.gz"])


def publish(
    github,
    store,
    config,
    branch,
    candidate,
    report,
    observations,
    eligible,
    reason,
    guard=None,
):
    if not re.fullmatch(
        r"feature/elastickv-validation-[0-9a-f]{8}-[0-9a-f]{8}-[0-9a-f]{12}-[0-9a-f]{12}",
        branch,
    ):
        raise agent.UnsafeState("Invalid qualification branch")
    if guard is None:
        raise agent.UnsafeState("Qualification lease guard is missing")
    guard()
    head = store.guard()
    prefix = f"{SITE['evidence_path']}/{candidate['source']}/{report['profile_sha256'][:12]}-{os.environ.get('POD_UID', 'manual').replace('-', '')[:8]}"
    source = report.pop("_source_review")
    report.update(
        {
            "automatic_eligible": eligible,
            "eligibility_reason": reason,
            "source_review_sha256": source_review.digest(source),
        }
    )
    artifacts = {
        prefix + "/source-review.json": source_review.canonical(source),
        prefix + "/summary.json": json.dumps(report, indent=2).encode(),
        prefix + "/observations.json.gz": observations,
        prefix + "/isolated_driver.py": pathlib.Path(
            "/app/compatibility_validation.py"
        ).read_bytes(),
        prefix + "/runtime_checks.py": pathlib.Path(
            "/app/compatibility_runtime.py"
        ).read_bytes(),
    }
    if eligible:
        policy = copy.deepcopy(store.policy)
        policy["approved_transitions"].append(
            {
                "from": report["from_source"],
                "to": candidate["source"],
                "readiness": True,
                "image": candidate["image"],
                "profile_sha256": report["profile_sha256"],
                "production_bindings": {
                    k: report["production_protection"][k]
                    for k in ("claims", "pods", "live_runtime_sha256")
                },
                "evidence": prefix + "/summary.json",
            }
        )
        artifacts[POLICY] = (json.dumps(policy, indent=2) + "\n").encode()
    tree = []
    for path, body in artifacts.items():
        guard()
        blob = github.request(
            "git/blobs",
            "POST",
            {"content": base64.b64encode(body).decode(), "encoding": "base64"},
        )["sha"]
        tree.append({"path": path, "mode": "100644", "type": "blob", "sha": blob})
    guard()
    base = github.request("git/commits/" + head)["tree"]["sha"]
    tree_sha = github.request("git/trees", "POST", {"base_tree": base, "tree": tree})[
        "sha"
    ]
    message = (
        "Qualify Elastickv "
        + candidate["source"][:12]
        + "\n\nSigned-off-by: "
        + agent.AUTHOR["name"]
        + " <"
        + agent.AUTHOR["email"]
        + ">"
    )
    commit = github.request(
        "git/commits",
        "POST",
        {
            "message": message,
            "tree": tree_sha,
            "parents": [head],
            "author": agent.AUTHOR,
            "committer": agent.AUTHOR,
        },
    )["sha"]
    guard()
    github.request("git/refs", "POST", {"ref": "refs/heads/" + branch, "sha": commit})
    return commit, head


def run():
    require_configured()
    if not __debug__:
        raise agent.UnsafeState("Python optimization must remain disabled")
    config = json.loads(pathlib.Path("/etc/compatibility/policy.json").read_text())
    if (
        hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
        != config["qualification_source_sha256"]
    ):
        raise agent.UnsafeState("Compatibility controller/config version mismatch")
    if (
        hashlib.sha256(pathlib.Path("/app/agent.py").read_bytes()).hexdigest()
        != config["agent_sha256"]
    ):
        raise agent.UnsafeState("Release engine/config version mismatch")
    if config.get("source_reviewer_sha256") != source_review.fingerprint():
        raise agent.UnsafeState("Source reviewer/config version mismatch")
    policy = json.loads(pathlib.Path("/etc/release/policy.json").read_text())
    agent.validate_site_policy(policy)
    github = agent.GithubApp(REPO)
    store = agent.GitState(github, policy)
    baseline = store.state["last_good"]
    if baseline.get("readiness") is not True:
        raise agent.UnsafeState(
            "Enable and verify baseline readiness before qualification"
        )
    if store.state.get("transaction"):
        agent.log("compatibility-waiting-for-release")
        return
    public = "https://api.github.com/repos/" + policy["repository"]
    main_sha = agent.http_json(public + "/commits/" + SITE["source_branch"])["sha"]
    sha = main_sha
    if sha == baseline["source"]:
        agent.log("compatibility-current", source=sha)
        return
    runs = []
    for event in ("push", "dynamic"):
        runs.extend(
            agent.http_json(
                public
                + "/actions/runs?"
                + urllib.parse.urlencode(
                    {"head_sha": sha, "event": event, "per_page": 100}
                )
            )["workflow_runs"]
        )
    if not agent.ci_ready(
        runs, sha, policy["required_workflows"], policy.get("codeql_workflow_id")
    ):
        agent.log("compatibility-waiting-for-ci", source=sha)
        return
    image = agent.registry_image(policy["image"], sha)
    if image in store.state.get("quarantined", []):
        agent.log("compatibility-quarantined", source=sha)
        return
    candidate = {"source": sha, "image": image, "readiness": True}
    head = store.guard()
    inputs = runtime_inputs(store, head)
    profile = configuration_hash(inputs)
    kube = ScopedKubernetes()
    lease = agent.Lease(kube)
    lease.path = (
        "/apis/coordination.k8s.io/v1/namespaces/"
        + CONTROL
        + "/leases/elastickv-compatibility"
    )
    lease.acquire()
    kube.write_guard = lease.guard
    try:
        before = production(kube, baseline)
        reviews = {}
        comparison = agent.http_json(
            public + "/compare/" + baseline["source"] + "..." + sha
        )
        source = source_review.collect(
            policy["repository"],
            baseline["source"],
            sha,
            image,
            comparison,
            lambda path: agent.http_json(public + "/" + path),
            limits=config.get("source_review_limits"),
            tick=lease.tick,
        )
        eligible = source_review.eligible(source, baseline["source"], sha, image)
        reason = "automated-source-review" if eligible else "source-review-blocked"
        identity = qualification_identity(
            config,
            reviews,
            baseline,
            candidate,
            profile,
            before,
            eligible,
            reason,
            source,
        )
        branch = (
            "feature/elastickv-validation-"
            + baseline["source"][:8]
            + "-"
            + sha[:8]
            + "-"
            + profile[:12]
            + "-"
            + identity[:12]
        )
        try:
            existing = github.request("git/ref/heads/" + branch)["object"]["sha"]
        except agent.HttpReleaseError as error:
            if error.status != 404:
                raise
            existing = None
        if existing:
            lease.guard()
            github.request(
                "dispatches",
                "POST",
                {
                    "event_type": "elastickv-compatibility-qualified",
                    "client_payload": {"branch": branch, "head": existing},
                },
            )
            agent.log("compatibility-awaiting-pr", branch=branch)
            return
        assets = json.loads(pathlib.Path("/etc/compatibility/assets.json").read_text())
        runner = Runner(kube, lease, config)
        report, observations = runner.validate(
            assets, baseline, candidate, profile, before["container"]
        )
        if production(kube, baseline) != before:
            raise agent.UnsafeState(
                "Production/backup baseline changed during validation"
            )
        if configuration_hash(runtime_inputs(store, store.guard())) != profile:
            raise agent.UnsafeState("Runtime profile changed during validation")
        current_main = agent.http_json(public + "/commits/" + SITE["source_branch"])[
            "sha"
        ]
        if current_main != sha and agent.http_json(
            public + "/compare/" + sha + "..." + current_main
        ).get("status") not in ("ahead", "identical"):
            raise agent.UnsafeState("Candidate is no longer on main")
        report["_source_review"] = source
        report["qualification_sha256"] = identity
        report["production_protection"] = {
            "pods": before["pods"],
            "claims": before["claims"],
            "live_runtime_sha256": before["live_runtime_sha256"],
            "unchanged": True,
        }
        commit, base = publish(
            github,
            store,
            config,
            branch,
            candidate,
            report,
            observations,
            eligible,
            reason,
            lease.guard,
        )
        lease.guard()
        github.request(
            "dispatches",
            "POST",
            {
                "event_type": "elastickv-compatibility-qualified",
                "client_payload": {"branch": branch, "head": commit},
            },
        )
        agent.log(
            "compatibility-qualified",
            source=sha,
            branch=branch,
            automatic_eligible=eligible,
            reason=reason,
        )
    finally:
        lease.release()


if __name__ == "__main__":
    try:
        run()
    except Exception as error:
        agent.log(
            "compatibility-stopped",
            error=type(error).__name__,
            reason=str(error)
            if isinstance(error, agent.ReleaseError)
            else "external operation failed",
        )
        raise SystemExit(1) from None
