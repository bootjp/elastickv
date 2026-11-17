"""Isolated old/new/old compatibility validation; never touches production/Git."""

import copy
import hashlib
import json
import pathlib
import platform

import agent
import compatibility_runtime as h
from settings import CONFIG as SITE
from settings import require_configured

assert platform.machine() == "x86_64", "Require amd64 runtime"
NS = h.NS
require_configured()
assert NS == SITE["validation_namespace"] and NS != SITE["production_namespace"]
policy = json.loads(pathlib.Path("/validation/policy.json").read_text())
assert policy["namespace"] == NS and policy["validation_only"] is True
assert all(
    __import__("urllib.parse").parse.urlsplit(u).hostname == "elastickv." + NS + ".svc"
    for u in policy["endpoints"].values()
)
assert (
    hashlib.sha256(pathlib.Path("/app/agent.py").read_bytes()).hexdigest()
    == policy["agent_sha256"]
)
OLD = policy["baseline"]
NEW = policy["candidate"]
for release in [OLD, NEW]:
    assert __import__("re").fullmatch("[0-9a-f]{40}", release["source"])
    assert __import__("re").fullmatch(
        __import__("re").escape(SITE["application_image"]) + r"@sha256:[0-9a-f]{64}",
        release["image"],
    )
    assert release["readiness"] is True
kube = h.ScopedKubernetes()
kube.expected_volumes = {r["claim"]: r for r in policy["volumes"]}
lease = agent.Lease(kube)
lease.path = (
    "/apis/coordination.k8s.io/v1/namespaces/" + NS + "/leases/elastickv-release"
)
path = "/apis/apps/v1/namespaces/" + NS + "/statefulsets/elastickv"


class TestStore:
    def __init__(self):
        self.state = {"last_good": dict(OLD), "transaction": None, "quarantined": []}
        self.expected = None

    def guard(self):
        actual = kube.request(path)
        assert (
            actual["metadata"]["namespace"] == NS
            and actual["spec"]["replicas"] == 3
            and actual["spec"]["updateStrategy"]["type"] == "OnDelete"
        )
        if self.expected is not None:
            assert actual["spec"]["template"] == self.expected, (
                "Test desired state drift"
            )

    def commit(self, release, message, guard):
        guard()
        self.guard()
        s = kube.request(path)
        t = copy.deepcopy(s["spec"]["template"])
        c = t["spec"]["containers"][0]
        assert release["image"] in (OLD["image"], NEW["image"])
        c["image"] = release["image"]
        c["args"] = [
            a
            for a in c["args"]
            if a != "--raftBootstrap" and not a.startswith("--raftBootstrapMembers=")
        ]
        kube.request(
            path,
            "PATCH",
            {
                "metadata": {"resourceVersion": s["metadata"]["resourceVersion"]},
                "spec": {"template": t},
            },
        )
        self.expected = t
        agent.log("test-template-change", message=message, image=release["image"])


store = TestStore()
runtime = agent.Runtime(policy, kube, lease, store)
proof = {
    "scope": NS,
    "scope_kind": "isolated-synthetic-kubernetes",
    "from_source": OLD["source"],
    "to_source": NEW["source"],
    "from_image": OLD["image"],
    "to_image": NEW["image"],
    "production_data_used": False,
    "architecture": platform.machine(),
    "worker_pod_uid": __import__("os").environ["POD_UID"],
    "new_features": {
        "commit_ts_fence": False,
        "timestamp_binding": False,
        "phase_d": False,
    },
    "agent_source_sha256": policy["agent_sha256"],
    "profile_sha256": policy["profile_sha256"],
    "controller_image": policy["controller_image"],
    "cases": {},
}


def verify_extra():
    c = h.clients(runtime)
    assert (
        c["s3"].get_object(Bucket=h.SEED, Key="candidate-written.bin")["Body"].read()
        == h.PAYLOAD
    )
    assert (
        c["dynamodb"].get_item(
            TableName=h.SEED,
            Key={"id": {"S": "candidate-written"}},
            ConsistentRead=True,
        )["Item"]["value"]["S"]
        == "preserved"
    )


def save():
    import base64
    import gzip

    summary = copy.deepcopy(proof)
    samples = {name: c.pop("samples", []) for name, c in summary["cases"].items()}
    summary["driver_sha256"] = hashlib.sha256(
        pathlib.Path(__file__).read_bytes()
    ).hexdigest()
    summary["runtime_sha256"] = hashlib.sha256(
        pathlib.Path(h.__file__).read_bytes()
    ).hexdigest()
    cm = "/api/v1/namespaces/" + NS + "/configmaps/validation-evidence"
    old = kube.request(cm)
    kube.request(
        cm,
        "PATCH",
        {
            "metadata": {"resourceVersion": old["metadata"]["resourceVersion"]},
            "data": {"summary.json": json.dumps(summary)},
            "binaryData": {
                "observations.json.gz": base64.b64encode(
                    gzip.compress(json.dumps(samples).encode())
                ).decode()
            },
        },
    )


lease.acquire()
try:
    pvcs = runtime.pvcs()
    assert pvcs == policy["pvc_uids"], "Dedicated PVC identity changed"
    proof["pvc_uids"] = pvcs
    store.state["transaction"] = {"from": OLD, "to": OLD}
    store.commit(OLD, "Retire test bootstrap flags", lease.guard)
    runtime.roll(OLD, pvcs)
    runtime.accept(OLD)
    store.state["transaction"] = None
    h.seed(runtime)
    proof["baseline_data"] = h.verify_seed(runtime)
    save()
    for phase, release in [("upgrade", NEW), ("downgrade", OLD), ("reupgrade", NEW)]:
        with h.Observer(runtime) as observer:
            agent.Manager(store, runtime, lease).execute(release)
        assert (
            store.state["last_good"] == release and store.state["transaction"] is None
        ), "Transition was rolled back or left incomplete"
        assert runtime.pvcs() == pvcs
        assert (
            observer.samples
            and observer.errors == 0
            and min(s["ready"] for s in observer.samples) >= 2
        )
        assert all(
            p["ready"] and p["image"] == release["image"]
            for p in h.inventory(runtime).values()
        )
        if phase == "upgrade":
            h.VALUES["compat-candidate-write"] = "preserved"
            h.redis(
                runtime,
                runtime.quorum(runtime.pods()),
                "SET",
                "compat-candidate-write",
                "preserved",
            )
            c = h.clients(runtime)
            c["dynamodb"].put_item(
                TableName=h.SEED,
                Item={"id": {"S": "candidate-written"}, "value": {"S": "preserved"}},
            )
            c["s3"].put_object(
                Bucket=h.SEED, Key="candidate-written.bin", Body=h.PAYLOAD
            )
            queue = c["sqs"].get_queue_url(QueueName=h.SEED)["QueueUrl"]
            c["sqs"].send_message(QueueUrl=queue, MessageBody="candidate-written")
            h.MESSAGES.add("candidate-written")
        proof["cases"][phase] = {
            "data": h.verify_seed(runtime),
            "minimum_ready": min(s["ready"] for s in observer.samples),
            "observation_errors": observer.errors,
            "samples": observer.samples,
        }
        verify_extra()
        save()
        agent.log("compatibility-case-passed", phase=phase)
    # Check actual snapshot indices, then restart every candidate member one at a time.
    snapshots = {
        n: dict(
            line.split(": ", 1)
            for line in runtime.rpc(n, "status").splitlines()
            if ": " in line
        )
        for n in h.MEMBERS
    }
    assert all(int(s["last_snapshot_index"]) > 0 for s in snapshots.values()), (
        "No snapshot observed"
    )
    proof["snapshots_before_restart"] = snapshots
    s = kube.request(path)
    t = copy.deepcopy(s["spec"]["template"])
    t.setdefault("metadata", {}).setdefault("annotations", {})[
        "compatibility-restart"
    ] = "candidate-snapshot-restore"
    kube.request(
        path,
        "PATCH",
        {
            "metadata": {"resourceVersion": s["metadata"]["resourceVersion"]},
            "spec": {"template": t},
        },
    )
    store.expected = t
    store.state["transaction"] = {"from": NEW, "to": NEW}
    with h.Observer(runtime) as observer:
        runtime.roll(NEW, pvcs)
        runtime.accept(NEW)
    store.state["transaction"] = None
    assert (
        runtime.pvcs() == pvcs
        and observer.errors == 0
        and min(s["ready"] for s in observer.samples) >= 2
    )
    proof["cases"]["snapshot-restart"] = {
        "data": h.verify_seed(runtime),
        "minimum_ready": min(s["ready"] for s in observer.samples),
        "observation_errors": observer.errors,
        "samples": observer.samples,
    }
    verify_extra()
    proof["passed"] = True
    proof["qualified_at"] = agent.utcnow().isoformat()
    save()
    agent.log("compatibility-passed", namespace=NS)
finally:
    lease.release()
