"""Serialized, CI-gated Elastickv releases. Never restores or removes PVC data."""

import argparse
import base64
import datetime as dt
import fnmatch
import hashlib
import json
import os
import pathlib
import re
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import human_approval
from settings import CONFIG as SITE
from settings import app_path, require_configured

SPEC = app_path("statefulset.yaml")
KUSTOM = app_path("kustomization.yaml")
STATE = app_path("release-state.json")
POLICY = SITE["release_path"] + "/policy.json"
READINESS_PATCH = "runtime-readiness-patch.json"
AUTHOR = SITE["commit_identity"]


class ReleaseError(Exception):
    pass


class HttpReleaseError(ReleaseError):
    def __init__(self, status, method, path):
        self.status = status
        super().__init__(f"HTTP {status}: {method} {path}")


class LostLease(ReleaseError):
    pass


class UnsafeState(ReleaseError):
    pass


def log(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def http_json(url, method="GET", data=None, headers=None, context=None):
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=25, context=context) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        # Never copy request headers, response contents, or credentials into logs.
        status = exc.code
        exc.close()
        raise HttpReleaseError(
            status, method, urllib.parse.urlsplit(url).path
        ) from None


def ci_ready(runs, sha, required, codeql_workflow_id=None):
    latest = {}
    for run in sorted(runs, key=lambda r: r["id"], reverse=True):
        event = run.get("event")
        name = run["name"]
        if (
            codeql_workflow_id is not None
            and run.get("workflow_id") == codeql_workflow_id
            and event == "dynamic"
        ):
            name = "CodeQL"
        if (
            name == "CodeQL"
            and codeql_workflow_id is not None
            and run.get("workflow_id") != codeql_workflow_id
        ):
            continue
        expected_event = "dynamic" if name == "CodeQL" else "push"
        if (
            run["head_sha"] == sha
            and run.get("head_branch") == SITE["source_branch"]
            and event == expected_event
        ):
            latest.setdefault(name, run)
    return all(
        name in latest
        and latest[name]["status"] == "completed"
        and latest[name]["conclusion"] == "success"
        for name in required
    )


def compatible(policy, previous, candidate, comparison):
    if previous["source"] == candidate:
        return previous.get("readiness", False)
    for transition in reversed(policy.get("approved_transitions", [])):
        if transition["from"] == previous["source"] and transition["to"] == candidate:
            if not human_approval.valid_attestation(policy, transition):
                return None
            if policy.get("require_qualification") and (
                transition.get("readiness") is not True
                or not all(
                    k in transition
                    for k in (
                        "image",
                        "profile_sha256",
                        "production_bindings",
                        "evidence",
                    )
                )
            ):
                return None
            return transition["readiness"]
    if policy.get("require_qualification"):
        return None
    files = comparison.get("files", [])
    if comparison.get("status") not in ("ahead", "identical") or len(files) >= 300:
        return None
    if not files and comparison.get("status") != "identical":
        return None
    paths = []
    for file in files:
        paths.append(file["filename"])
        if "previous_filename" in file:
            paths.append(file["previous_filename"])
    if all(
        any(fnmatch.fnmatchcase(path, pattern) for pattern in policy["safe_paths"])
        for path in paths
    ):
        return previous.get("readiness", False)
    return None


def registry_image(name, sha):
    registry, repository = name.split("/", 1)
    query = urllib.parse.urlencode(
        {"service": registry, "scope": "repository:" + repository + ":pull"}
    )
    token = http_json("https://" + registry + "/token?" + query)["token"]
    headers = {
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
    }
    request = urllib.request.Request(
        f"https://{registry}/v2/{repository}/manifests/{sha}", headers=headers
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        payload = response.read()
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        if response.headers.get("Docker-Content-Digest") != digest:
            raise ReleaseError("Registry digest mismatch")
    manifest = json.loads(payload)

    def fetch_verified(path, expected):
        request = urllib.request.Request(
            f"https://{registry}/v2/{repository}/" + path, headers=headers
        )
        with urllib.request.urlopen(request, timeout=25) as response:
            raw = response.read()
        if "sha256:" + hashlib.sha256(raw).hexdigest() != expected:
            raise ReleaseError("Registry referenced blob digest mismatch")
        return json.loads(raw)

    if "manifests" in manifest:
        matches = [
            m
            for m in manifest["manifests"]
            if m.get("platform", {}).get("os") == "linux"
            and m.get("platform", {}).get("architecture") == "amd64"
        ]
        if len(matches) != 1:
            raise ReleaseError("Expected one linux/amd64 manifest")
        manifest = fetch_verified(
            "manifests/" + matches[0]["digest"], matches[0]["digest"]
        )
    config = fetch_verified(
        "blobs/" + manifest["config"]["digest"], manifest["config"]["digest"]
    )
    if config.get("os") != "linux" or config.get("architecture") != "amd64":
        raise ReleaseError("Image configuration is not linux/amd64")
    return name + "@" + digest


class GithubApp:
    def __init__(self, repository, directory="/var/run/github-app"):
        self.repository = repository
        self.directory = pathlib.Path(directory)
        self.token = None
        self.created = 0

    def headers(self):
        if self.token is None or time.monotonic() - self.created > 3000:

            def encoded(obj):
                return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=")

            now = int(time.time())
            claims = {
                "iat": now - 30,
                "exp": now + 540,
                "iss": self.directory.joinpath("githubAppID").read_text().strip(),
            }
            signing = encoded({"alg": "RS256", "typ": "JWT"}) + b"." + encoded(claims)
            signature = subprocess.run(
                [
                    "openssl",
                    "dgst",
                    "-sha256",
                    "-sign",
                    str(self.directory / "githubAppPrivateKey"),
                ],
                input=signing,
                capture_output=True,
                check=True,
            ).stdout
            jwt = (
                signing + b"." + base64.urlsafe_b64encode(signature).rstrip(b"=")
            ).decode()
            installation = (
                self.directory.joinpath("githubAppInstallationID").read_text().strip()
            )
            result = http_json(
                f"https://api.github.com/app/installations/{installation}/access_tokens",
                "POST",
                {
                    "repositories": [self.repository.split("/")[1]],
                    "permissions": {"contents": "write"},
                },
                {
                    "Authorization": "Bearer " + jwt,
                    "Accept": "application/vnd.github+json",
                },
            )
            self.token = result["token"]
            self.created = time.monotonic()
        return {
            "Authorization": "Bearer " + self.token,
            "Accept": "application/vnd.github+json",
        }

    def request(self, path, method="GET", data=None):
        # A transient Contents API 404 must not abort a release before its lease.
        # Only immutable reads are safe here: never retry writes or change refs.
        attempts = (
            3
            if method == "GET"
            and re.fullmatch(r"contents/[^?]+\?ref=[0-9a-f]{40}", path)
            else 1
        )
        for attempt in range(attempts):
            try:
                return http_json(
                    "https://api.github.com/repos/" + self.repository + "/" + path,
                    method,
                    data,
                    self.headers(),
                )
            except HttpReleaseError as exc:
                if exc.status != 404 or attempt + 1 == attempts:
                    raise
                delay = attempt + 1
                log(
                    "github-read-retry",
                    path=urllib.parse.urlsplit(path).path,
                    status=exc.status,
                    attempt=attempt + 1,
                    delay_seconds=delay,
                )
                time.sleep(delay)


class GitState:
    def __init__(self, github, policy):
        self.github = github
        self.policy = policy
        head = github.request("git/ref/heads/" + SITE["gitops_branch"])["object"]["sha"]
        self.files, self.shas = self.read_files(head)
        self.inputs = self.config_inputs(head)
        if json.loads(self.files[POLICY]) != policy:
            raise ReleaseError(
                "Mounted policy does not match Git; retry with current job template"
            )
        self.state = json.loads(self.files[STATE])

    def read_files(self, head):
        files, shas = {}, {}
        for path in (SPEC, KUSTOM, STATE, POLICY):
            result = self.github.request("contents/" + path + "?ref=" + head)
            files[path] = base64.b64decode(result["content"]).decode()
            shas[path] = result["sha"]
        return files, shas

    def config_inputs(self, head):
        tree = self.github.request("git/trees/" + head + "?recursive=1")
        if tree.get("truncated"):
            raise UnsafeState("Git configuration inventory is truncated")
        return {
            entry["path"]: entry["sha"]
            for entry in tree["tree"]
            if entry["type"] == "blob"
            and entry["path"].startswith(app_path() + "/")
            and not entry["path"].startswith(app_path("operations/"))
            and not entry["path"].endswith(".md")
        }

    def profile(self):
        from compatibility_profile import configuration_hash

        head = self.guard()
        files = {}
        for path in self.config_inputs(head):
            if path == STATE:
                continue
            result = self.github.request("contents/" + path + "?ref=" + head)
            files[path] = base64.b64decode(result["content"]).decode()
        return configuration_hash(files)

    def guard(self):
        head = self.github.request("git/ref/heads/" + SITE["gitops_branch"])["object"][
            "sha"
        ]
        _, shas = self.read_files(head)
        if shas != self.shas or self.config_inputs(head) != self.inputs:
            raise UnsafeState(
                "Controlled GitOps files changed concurrently; refusing to overwrite"
            )
        return head

    def commit(self, release, message, guard):
        import yaml

        guard()
        head = self.guard()
        texts = dict(self.files)
        image_re = (
            r"(?m)^(\s*image:\s*)"
            + re.escape(SITE["application_image"])
            + r"(?:@\S+|:\S+)[ \t]*$"
        )
        texts[SPEC], count = re.subn(
            image_re, lambda m: m[1] + release["image"], texts[SPEC]
        )
        if count != 1:
            raise ReleaseError("Expected exactly one application image pin")
        kustom = yaml.safe_load(texts[KUSTOM])
        patches = [
            p for p in kustom.get("patches", []) if p.get("path") != READINESS_PATCH
        ]
        if release.get("readiness"):
            patches.append(
                {
                    "target": {
                        "group": "apps",
                        "version": "v1",
                        "kind": "StatefulSet",
                        "name": "elastickv",
                    },
                    "path": READINESS_PATCH,
                }
            )
        kustom["patches"] = patches
        texts[KUSTOM] = yaml.safe_dump(kustom, sort_keys=False)
        texts[STATE] = json.dumps(self.state, indent=2) + "\n"
        base = self.github.request("git/commits/" + head)["tree"]["sha"]
        entries = [
            {"path": path, "mode": "100644", "type": "blob", "content": texts[path]}
            for path in (SPEC, KUSTOM, STATE)
            if texts[path] != self.files[path]
        ]
        if not entries:
            return
        tree = self.github.request(
            "git/trees", "POST", {"base_tree": base, "tree": entries}
        )["sha"]
        commit = self.github.request(
            "git/commits",
            "POST",
            {
                "message": message,
                "tree": tree,
                "parents": [head],
                "author": AUTHOR,
                "committer": AUTHOR,
            },
        )["sha"]
        guard()
        self.github.request(
            "git/refs/heads/" + SITE["gitops_branch"],
            "PATCH",
            {"sha": commit, "force": False},
        )
        self.files = texts
        for path in (SPEC, KUSTOM, STATE):
            raw = texts[path].encode()
            self.shas[path] = hashlib.sha1(
                f"blob {len(raw)}\0".encode() + raw
            ).hexdigest()
        self.inputs.update({path: self.shas[path] for path in (SPEC, KUSTOM, STATE)})
        log("gitops-commit", commit=commit, message=message)


class Kubernetes:
    def __init__(self):
        self.url = (
            "https://"
            + os.environ["KUBERNETES_SERVICE_HOST"]
            + ":"
            + os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        )
        self.directory = pathlib.Path("/var/run/secrets/kubernetes.io/serviceaccount")
        self.context = ssl.create_default_context(cafile=str(self.directory / "ca.crt"))

    def request(self, path, method="GET", data=None):
        headers = {
            "Authorization": "Bearer "
            + self.directory.joinpath("token").read_text().strip()
        }
        if method == "PATCH":
            headers["Content-Type"] = "application/merge-patch+json"
        return http_json(self.url + path, method, data, headers, self.context)


class Lease:
    def __init__(self, kube):
        self.kube = kube
        self.owner = os.environ.get("POD_UID", str(uuid.uuid4()))
        self.path = (
            "/apis/coordination.k8s.io/v1/namespaces/"
            + SITE["control_namespace"]
            + "/leases/elastickv-release"
        )
        self.last = 0

    def acquire(self):
        lease = self.kube.request(self.path)
        spec = lease.get("spec", {})
        if (
            spec.get("holderIdentity")
            and spec["holderIdentity"] != self.owner
            and spec.get("renewTime")
        ):
            renewed = dt.datetime.fromisoformat(
                spec["renewTime"].replace("Z", "+00:00")
            )
            if utcnow() < renewed + dt.timedelta(
                seconds=spec.get("leaseDurationSeconds", 300)
            ):
                raise LostLease("Another release holds the lease")
        self._update(lease)

    def _update(self, lease):
        self.kube.request(
            self.path,
            "PATCH",
            {
                "metadata": {"resourceVersion": lease["metadata"]["resourceVersion"]},
                "spec": {
                    "holderIdentity": self.owner,
                    "leaseDurationSeconds": 300,
                    "renewTime": utcnow().isoformat().replace("+00:00", "Z"),
                },
            },
        )
        self.last = time.monotonic()

    def guard(self):
        lease = self.kube.request(self.path)
        if lease.get("spec", {}).get("holderIdentity") != self.owner:
            raise LostLease("Release lease ownership changed")
        self._update(lease)

    def tick(self):
        if time.monotonic() - self.last > 30:
            self.guard()

    def release(self):
        lease = self.kube.request(self.path)
        if lease.get("spec", {}).get("holderIdentity") == self.owner:
            self.kube.request(
                self.path,
                "PATCH",
                {
                    "metadata": {
                        "resourceVersion": lease["metadata"]["resourceVersion"]
                    },
                    "spec": {"holderIdentity": "", "renewTime": None},
                },
            )


def pod_ready(pod):
    return not pod["metadata"].get("deletionTimestamp") and any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in pod.get("status", {}).get("conditions", [])
    )


def owned_canary_queue(endpoint, url, name):
    expected = urllib.parse.urlsplit(endpoint)
    actual = urllib.parse.urlsplit(url)
    if (
        actual.scheme != expected.scheme
        or actual.netloc != expected.netloc
        or not actual.path.startswith(expected.path.rstrip("/") + "/")
        or actual.path.rsplit("/", 1)[-1] != name
        or actual.query
        or actual.fragment
    ):
        raise ReleaseError("SQS response does not identify the owned canary queue")
    return url


class Runtime:
    def __init__(self, policy, kube, lease, git):
        self.policy, self.kube, self.lease, self.git = policy, kube, lease, git
        self.qualification = None
        self.namespace = "/api/v1/namespaces/" + policy["namespace"]
        self.sts_path = (
            "/apis/apps/v1/namespaces/"
            + policy["namespace"]
            + "/statefulsets/"
            + policy["statefulset"]
        )

    def pods(self):
        selector = urllib.parse.urlencode(
            {"labelSelector": "app.kubernetes.io/name=elastickv"}
        )
        result = self.kube.request(self.namespace + "/pods?" + selector)["items"]
        if {p["metadata"]["name"] for p in result} != set(self.policy["members"]):
            raise ReleaseError("Unexpected member inventory")
        return {p["metadata"]["name"]: p for p in result}

    def pvcs(self):
        identities = {}
        for name in self.policy["members"]:
            pvc = self.kube.request(
                self.namespace + "/persistentvolumeclaims/data-" + name
            )
            if (
                pvc["metadata"].get("deletionTimestamp")
                or pvc.get("status", {}).get("phase") != "Bound"
            ):
                raise UnsafeState("Data PVC is not stable and Bound")
            identities[name] = pvc["metadata"]["uid"]
        return identities

    def production_bindings(self):
        claims = {"data-" + name: uid for name, uid in self.pvcs().items()}
        for name in SITE["backup_claims"]:
            backup = self.kube.request(
                self.namespace + "/persistentvolumeclaims/" + name
            )
            if (
                backup["metadata"].get("deletionTimestamp")
                or backup.get("status", {}).get("phase") != "Bound"
            ):
                raise UnsafeState("Backup PVC is not stable and Bound")
            claims[name] = backup["metadata"]["uid"]
        pods = {
            name: {
                "uid": pod["metadata"]["uid"],
                "restarts": {
                    c["name"]: c["restartCount"]
                    for c in pod.get("status", {}).get("containerStatuses", [])
                },
            }
            for name, pod in self.pods().items()
        }
        from compatibility_profile import live_runtime_hash

        return {
            "claims": claims,
            "pods": pods,
            "live_runtime_sha256": live_runtime_hash(self.kube.request(self.sts_path)),
        }

    def check_qualified_storage(self):
        if not self.policy.get("require_qualification"):
            return
        from compatibility_profile import verify_live_data_layout

        try:
            verify_live_data_layout(self.kube.request(self.sts_path))
        except ValueError as error:
            raise UnsafeState(str(error)) from None
        actual = self.production_bindings()
        if not self.qualification or any(
            actual[k] != self.qualification["production_bindings"][k]
            for k in ("claims", "live_runtime_sha256")
        ):
            raise UnsafeState(
                "Production/backup PVC identity differs from qualification"
            )

    def rpc(self, name, command, *args):
        self.lease.tick()
        host = name + ".elastickv-peer." + self.policy["namespace"] + ".svc:50051"
        env = {
            **os.environ,
            "RAFTADMIN_ALLOW_INSECURE": "true",
            "RAFTADMIN_RPC_TIMEOUT_SECONDS": "20",
        }
        result = subprocess.run(
            ["raftadmin", host, command, *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=25,
        )
        if result.returncode:
            raise ReleaseError("Raft RPC failed: " + command + " on " + name)
        return result.stdout

    def quorum(self, pods):
        healthy = [name for name, pod in pods.items() if pod_ready(pod)]
        if len(healthy) < 2:
            raise ReleaseError(
                "Fewer than two healthy voters; automatic disruption forbidden"
            )
        statuses = {}
        for name in healthy:
            raw = self.rpc(name, "status")
            status = dict(
                line.split(": ", 1) for line in raw.splitlines() if ": " in line
            )
            config = self.rpc(name, "configuration")
            ids = set(re.findall(r'^  id: "([^"]+)"$', config, re.M))
            if (
                ids != set(self.policy["members"])
                or len(re.findall('suffrage: "voter"', config)) != 3
            ):
                raise ReleaseError("Unexpected Raft membership")
            if status["pending_conf_change"] != "false":
                raise ReleaseError("Pending membership change")
            if int(status["fsm_pending"]) or int(status["applied_index"]) != int(
                status["commit_index"]
            ):
                raise ReleaseError("Voter has not applied committed entries")
            if int(status["applied_index"]) < int(status["last_snapshot_index"]):
                raise ReleaseError("Voter has not applied its snapshot")
            if (
                status["state"] == "FOLLOWER"
                and not 0 <= int(status["last_contact_nanos"]) <= 10_000_000_000
            ):
                raise ReleaseError("Voter leader contact is unknown or stale")
            statuses[name] = status
        leaders = {json.loads(s["leader_id"]) for s in statuses.values()}
        if len(leaders) != 1 or next(iter(leaders)) not in healthy:
            raise ReleaseError("No common healthy leader")
        leader = next(iter(leaders))
        if (
            sum(
                s.get("state", "").strip(chr(34)).lower() == "leader"
                for s in statuses.values()
            )
            != 1
        ):
            raise ReleaseError("Expected exactly one leader")
        host = leader + ".elastickv-peer." + self.policy["namespace"] + ".svc"
        with urllib.request.urlopen(
            "http://" + host + ":8000/healthz/leader", timeout=20
        ) as response:
            if response.status != 200:
                raise ReleaseError("Leader ReadIndex verification failed")
        return leader

    def verify(self, release, canary=True):
        self.lease.tick()
        self.check_qualified_storage()
        pods = self.pods()
        if not all(pod_ready(p) for p in pods.values()):
            raise ReleaseError("Not all three voters are Ready")
        self.quorum(pods)
        if release.get("readiness"):
            for pod in pods.values():
                # Legacy Pods are allowed only during the explicit bootstrap transition.
                image = next(
                    c["image"]
                    for c in pod["spec"]["containers"]
                    if c["name"] == "elastickv"
                )
                if image != release["image"]:
                    continue
                result = http_json("http://" + pod["status"]["podIP"] + ":8081/readyz")
                if not result.get("ready"):
                    raise ReleaseError("Stateful readiness is false")
        if canary:
            self.canary()

    def accept(self, release):
        end = time.monotonic() + self.policy["healthy_window"]
        while True:
            self.verify(release, canary=False)
            if time.monotonic() >= end:
                break
            time.sleep(3)
        self.canary()

    def wait(self, predicate, timeout):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            self.lease.tick()
            try:
                result = predicate()
                if result:
                    return result
            except (LostLease, UnsafeState):
                raise
            except (ReleaseError, urllib.error.URLError, TimeoutError) as exc:
                last = type(exc).__name__
            time.sleep(3)
        raise ReleaseError("Readiness deadline exceeded: " + str(last))

    def roll(self, release, pvcs):
        self.wait(
            lambda: self.kube.request(self.sts_path)["spec"]["template"]["spec"][
                "containers"
            ][0]["image"]
            == release["image"],
            300,
        )
        template = self.kube.request(self.sts_path)
        if (
            template["spec"]["replicas"] != 3
            or template["spec"]["updateStrategy"]["type"] != "OnDelete"
        ):
            raise UnsafeState("Requires exactly three OnDelete voters")
        self.wait(
            lambda: self.kube.request(self.sts_path)
            .get("status", {})
            .get("observedGeneration", 0)
            >= template["metadata"]["generation"],
            120,
        )
        revision = self.kube.request(self.sts_path)["status"]["updateRevision"]
        while True:
            self.check_qualified_storage()
            if self.pvcs() != pvcs:
                raise UnsafeState("PVC identity changed")
            pods = self.pods()
            transaction = self.git.state["transaction"]
            allowed = {transaction["from"]["image"], transaction["to"]["image"]}
            if any(
                next(
                    c["image"]
                    for c in p["spec"]["containers"]
                    if c["name"] == "elastickv"
                )
                not in allowed
                for p in pods.values()
            ):
                raise UnsafeState(
                    "Unexpected image outside the recorded compatible transition"
                )
            remaining = [
                name
                for name, p in pods.items()
                if p["metadata"].get("labels", {}).get("controller-revision-hash")
                != revision
            ]
            if not remaining:
                self.wait(
                    lambda: all(pod_ready(p) for p in self.pods().values()),
                    self.policy["startup_timeout"],
                )
                return
            leader = self.wait(lambda: self.quorum(self.pods()), 120)
            unhealthy = [name for name, p in pods.items() if not pod_ready(p)]
            if unhealthy and unhealthy[0] not in remaining:
                self.wait(
                    lambda: all(pod_ready(p) for p in self.pods().values()),
                    self.policy["startup_timeout"],
                )
                continue
            selected = (
                unhealthy[0]
                if unhealthy
                else next((n for n in remaining if n != leader), remaining[0])
            )
            if selected == leader:
                target = next(
                    n for n, p in pods.items() if n != selected and pod_ready(p)
                )
                self.rpc(
                    leader,
                    "leadership_transfer_to_server",
                    target,
                    target
                    + ".elastickv-peer."
                    + self.policy["namespace"]
                    + ".svc:50051",
                )
                self.wait(lambda: self.quorum(self.pods()) == target, 90)
            self.lease.guard()
            self.git.guard()
            current = self.kube.request(self.sts_path)
            image = next(
                c["image"]
                for c in current["spec"]["template"]["spec"]["containers"]
                if c["name"] == "elastickv"
            )
            if (
                current["status"]["updateRevision"] != revision
                or current["spec"]["replicas"] != 3
                or current["spec"]["updateStrategy"]["type"] != "OnDelete"
                or image != release["image"]
            ):
                raise UnsafeState(
                    "Desired template or rollout policy changed during release"
                )
            current_pods = self.pods()
            if (
                sum(
                    pod_ready(p) for name, p in current_pods.items() if name != selected
                )
                != 2
            ):
                raise ReleaseError("Another voter is unavailable")
            if self.pvcs() != pvcs:
                raise UnsafeState("PVC identity changed before member replacement")
            # Recheck consensus after the Git/PVC API reads. If leadership moved,
            # select again instead of knowingly restarting the current leader.
            if self.quorum(current_pods) == selected:
                continue
            self.lease.guard()
            uid = current_pods[selected]["metadata"]["uid"]
            log("replace-member", member=selected, image=release["image"])
            self.kube.request(
                self.namespace + "/pods/" + selected,
                "DELETE",
                {"preconditions": {"uid": uid}},
            )

            def replaced():
                p = self.kube.request(self.namespace + "/pods/" + selected)
                return (
                    p["metadata"]["uid"] != uid
                    and pod_ready(p)
                    and p["metadata"].get("labels", {}).get("controller-revision-hash")
                    == revision
                )

            self.wait(replaced, self.policy["startup_timeout"])
            self.wait(lambda: self._verified(release), 120)

    def _verified(self, release):
        self.verify(release)
        return True

    def canary(self):
        import boto3
        from botocore.config import Config

        secret = self.kube.request(
            self.namespace + "/secrets/elastickv-api-credentials"
        )
        credentials = json.loads(base64.b64decode(secret["data"]["credentials.json"]))[
            "credentials"
        ][0]
        config = Config(
            connect_timeout=5,
            read_timeout=20,
            retries={"max_attempts": 1},
            s3={"addressing_style": "path"},
        )
        clients = {
            service: boto3.client(
                service,
                endpoint_url=self.policy["endpoints"][service],
                region_name="us-east-1",
                aws_access_key_id=credentials["access_key_id"],
                aws_secret_access_key=credentials["secret_access_key"],
                config=config,
            )
            for service in ("dynamodb", "s3", "sqs")
        }
        name = "elastickv-release-" + uuid.uuid4().hex[:16]
        created, queue = set(), None
        ddb, s3, sqs = (clients[s] for s in ("dynamodb", "s3", "sqs"))
        try:
            self.redis_canary(name)
            self.lease.tick()
            ddb.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            created.add("ddb")
            ddb.put_item(
                TableName=name, Item={"id": {"S": "probe"}, "value": {"S": name}}
            )
            item = ddb.get_item(
                TableName=name, Key={"id": {"S": "probe"}}, ConsistentRead=True
            )["Item"]
            if item["value"]["S"] != name:
                raise ReleaseError("DynamoDB readback mismatch")
            s3.create_bucket(Bucket=name)
            created.add("s3")
            s3.put_object(Bucket=name, Key="probe", Body=name.encode())
            if s3.get_object(Bucket=name, Key="probe")["Body"].read() != name.encode():
                raise ReleaseError("S3 readback mismatch")
            self.lease.tick()
            candidate_queue = sqs.create_queue(QueueName=name)["QueueUrl"]
            queue = owned_canary_queue(
                self.policy["endpoints"]["sqs"], candidate_queue, name
            )
            sqs.send_message(QueueUrl=queue, MessageBody=name)
            message = sqs.receive_message(
                QueueUrl=queue, WaitTimeSeconds=2, MaxNumberOfMessages=1
            )["Messages"][0]
            if message["Body"] != name:
                raise ReleaseError("SQS readback mismatch")
            sqs.delete_message(QueueUrl=queue, ReceiptHandle=message["ReceiptHandle"])
        finally:
            # Only objects created by this run are eligible for cleanup.
            cleanup = []
            if "ddb" in created:
                cleanup.append(lambda: ddb.delete_table(TableName=name))
            if "s3" in created:
                cleanup.extend(
                    [
                        lambda: s3.delete_object(Bucket=name, Key="probe"),
                        lambda: s3.delete_bucket(Bucket=name),
                    ]
                )
            if queue:
                cleanup.append(lambda: sqs.delete_queue(QueueUrl=queue))
            failures = []
            for action in cleanup:
                try:
                    self.lease.tick()
                    action()
                except Exception as exc:
                    failures.append(type(exc).__name__)
                    log(
                        "canary-cleanup-pending",
                        resource=name,
                        error=type(exc).__name__,
                    )
            if failures:
                raise ReleaseError(
                    "Canary cleanup incomplete; see owned resource name in log"
                )
        log("canary-passed")

    def redis_canary(self, key):
        def command(name, *parts):
            raw = b"*" + str(len(parts)).encode() + b"\r\n"
            for part in parts:
                item = part.encode()
                raw += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
            host = name + ".elastickv-peer." + self.policy["namespace"] + ".svc"
            with socket.create_connection((host, 6379), timeout=5) as connection:
                connection.settimeout(20)
                connection.sendall(raw)
                reader = connection.makefile("rb")
                value = reader.readline()
                if value.startswith(b"$") and int(value[1:-2]) >= 0:
                    value += reader.read(int(value[1:-2]) + 2)
                if value.startswith(b"-"):
                    raise ReleaseError("Redis canary failed")
                return value

        leader = self.quorum(self.pods())
        command(leader, "SET", key, key, "EX", "300")
        try:
            for name in self.policy["members"]:
                if key.encode() not in command(name, "GET", key):
                    raise ReleaseError("Redis readback mismatch")
        finally:
            command(leader, "DEL", key)


class Manager:
    def __init__(self, store, runtime, lease):
        self.store, self.runtime, self.lease = store, runtime, lease

    def execute(self, candidate=None):
        state = self.store.state
        if not state.get("transaction"):
            if candidate is None or candidate["image"] == state["last_good"]["image"]:
                return
            self.runtime.verify(state["last_good"])
            state["transaction"] = {
                "from": dict(state["last_good"]),
                "to": candidate,
                "phase": "forward",
                "pvc_uids": self.runtime.pvcs(),
            }
            qualified = getattr(self.runtime, "qualification", None)
            if isinstance(qualified, dict) and qualified:
                state["transaction"]["qualification"] = qualified
            self.store.commit(
                candidate, "Start verified Elastickv image release", self.lease.guard
            )
        transaction = state["transaction"]
        if transaction["phase"] == "forward":
            try:
                self.runtime.roll(transaction["to"], transaction["pvc_uids"])
                self.runtime.accept(transaction["to"])
            except (LostLease, UnsafeState):
                raise
            except Exception as exc:
                log("release-failed", error=type(exc).__name__)
                transaction["phase"] = "rollback"
                state.setdefault("quarantined", []).append(transaction["to"]["image"])
                self.store.commit(
                    transaction["from"],
                    "Revert failed Elastickv image release",
                    self.lease.guard,
                )
        if transaction["phase"] == "rollback":
            self.runtime.roll(transaction["from"], transaction["pvc_uids"])
            self.runtime.accept(transaction["from"])
            state["transaction"] = None
            self.store.commit(
                state["last_good"],
                "Complete Elastickv image rollback",
                self.lease.guard,
            )
            log("rollback-complete")
            return
        state["last_good"] = transaction["to"]
        state["transaction"] = None
        self.store.commit(
            state["last_good"],
            "Accept healthy Elastickv image release",
            self.lease.guard,
        )
        log("release-complete", image=state["last_good"]["image"])


def discover(policy, previous, profile=None, quarantined=()):
    from compatibility_profile import qualified_profile

    public = "https://api.github.com/repos/" + policy["repository"]
    head = http_json(public + "/commits/" + SITE["source_branch"])["sha"]
    if head == previous["source"]:
        log("current-source", source=head)
        return None
    candidates = [head]
    # A qualified main ancestor remains deployable if main advanced while its
    # isolated qualification/review ran. Only explicit digest pins may queue.
    candidates += [
        entry["to"]
        for entry in reversed(policy.get("approved_transitions", []))
        if entry["from"] == previous["source"]
        and "image" in entry
        and entry["to"] != head
    ]
    for sha in dict.fromkeys(candidates):
        approval = next(
            (
                a
                for a in reversed(policy.get("approved_transitions", []))
                if a["from"] == previous["source"] and a["to"] == sha
            ),
            None,
        )
        if approval and approval.get("image") in quarantined:
            log("quarantined-image", image=approval["image"])
            continue
        if policy.get("require_qualification") and (
            not approval
            or not all(
                k in approval
                for k in ("image", "profile_sha256", "production_bindings", "evidence")
            )
        ):
            log("compatibility-approval-required", source=sha)
            continue
        if sha != head:
            ancestry = http_json(public + "/compare/" + sha + "..." + head)
            if ancestry.get("status") not in ("ahead", "identical"):
                continue
        comparison = http_json(public + "/compare/" + previous["source"] + "..." + sha)
        readiness = compatible(policy, previous, sha, comparison)
        if readiness is None:
            log("compatibility-approval-required", source=sha)
            continue
        runs = []
        for event in ("push", "dynamic"):
            query = urllib.parse.urlencode(
                {"head_sha": sha, "event": event, "per_page": 100}
            )
            runs.extend(http_json(public + "/actions/runs?" + query)["workflow_runs"])
        if not ci_ready(
            runs, sha, policy["required_workflows"], policy.get("codeql_workflow_id")
        ):
            log("waiting-for-ci", source=sha)
            continue
        image = registry_image(policy["image"], sha)
        try:
            selected_profile = (
                profile()
                if callable(profile)
                and any(
                    a["from"] == previous["source"]
                    and a["to"] == sha
                    and "profile_sha256" in a
                    for a in policy.get("approved_transitions", [])
                )
                else profile
            )
            qualified_profile(policy, previous, sha, image, selected_profile)
        except ValueError as error:
            raise UnsafeState(str(error)) from None
        return {"source": sha, "image": image, "readiness": readiness}
    return None


def bind_qualification(store, runtime, candidate):
    if not store.policy.get("require_qualification") or not (
        candidate or store.state.get("transaction")
    ):
        return
    transaction = store.state.get("transaction")
    if transaction:
        qualified = transaction.get("qualification")
    else:
        qualified = next(
            (
                entry
                for entry in reversed(store.policy["approved_transitions"])
                if entry["from"] == store.state["last_good"]["source"]
                and entry["to"] == candidate["source"]
            ),
            None,
        )
    if transaction:
        current = next(
            (
                entry
                for entry in reversed(store.policy["approved_transitions"])
                if entry["from"] == transaction["from"]["source"]
                and entry["to"] == transaction["to"]["source"]
            ),
            None,
        )
        if (
            not qualified
            or not current
            or any(
                current.get(k) != qualified.get(k)
                for k in (
                    "image",
                    "readiness",
                    "profile_sha256",
                    "production_bindings",
                    "human_approval",
                )
            )
        ):
            raise UnsafeState(
                "Qualification was revoked or changed during the transaction"
            )
    if not qualified or qualified.get("profile_sha256") != store.profile():
        raise UnsafeState("Qualification is missing or its runtime profile changed")
    if not human_approval.valid_attestation(store.policy, qualified):
        raise UnsafeState("Exact-head human approval is missing")
    runtime.qualification = qualified
    runtime.check_qualified_storage()
    if not transaction and runtime.production_bindings() != qualified.get(
        "production_bindings"
    ):
        raise UnsafeState("Production Pods or claims changed since qualification")


def validate_site_policy(policy):
    human_approval.validate_policy(policy)
    if (
        policy.get("controller_sha256")
        != hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    ):
        raise UnsafeState("Release controller and policy source versions differ")
    workflows = policy.get("required_workflows")
    if (
        not isinstance(workflows, list)
        or not workflows
        or not all(isinstance(name, str) and name for name in workflows)
    ):
        raise UnsafeState("Require explicit, nonempty source CI workflow names")
    expected = {
        "repository": SITE["source_repository"],
        "gitops_repository": SITE["gitops_repository"],
        "image": SITE["application_image"],
        "namespace": SITE["production_namespace"],
        "statefulset": "elastickv",
        "promotion_reviewers": SITE["promotion_reviewers"],
        "members": ["elastickv-" + str(i) for i in range(3)],
    }
    if (
        any(policy.get(k) != v for k, v in expected.items())
        or policy.get("require_qualification") is not True
    ):
        raise UnsafeState(
            "Release policy differs from deployment settings or bypasses qualification"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    require_configured()
    policy = json.loads(
        pathlib.Path(
            os.environ.get("RELEASE_POLICY_FILE", "/etc/release/policy.json")
        ).read_text()
    )
    validate_site_policy(policy)
    kube = Kubernetes()
    lease = Lease(kube)
    # Dry-run never acquires a lease or changes Git/Kubernetes/application data.
    store = GitState(GithubApp(policy["gitops_repository"]), policy)
    if args.dry_run:
        candidate = discover(
            policy,
            store.state["last_good"],
            store.profile,
            store.state.get("quarantined", []),
        )
        log(
            "dry-run",
            candidate=candidate,
            recovery=bool(store.state.get("transaction")),
        )
        return
    lease.acquire()
    try:
        runtime = Runtime(policy, kube, lease, store)
        candidate = (
            None
            if store.state.get("transaction")
            else discover(
                policy,
                store.state["last_good"],
                store.profile,
                store.state.get("quarantined", []),
            )
        )
        if candidate and candidate["image"] in store.state.get("quarantined", []):
            log("quarantined-image", image=candidate["image"])
            return
        bind_qualification(store, runtime, candidate)
        Manager(store, runtime, lease).execute(candidate)
    finally:
        try:
            lease.release()
        except Exception as exc:
            log("lease-release-pending", error=type(exc).__name__)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        log(
            "agent-stopped",
            error=type(error).__name__,
            reason=str(error)
            if isinstance(error, ReleaseError)
            else "external operation failed",
        )
        raise SystemExit(1) from None
