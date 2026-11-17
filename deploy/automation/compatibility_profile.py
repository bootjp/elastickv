"""Stable runtime-input binding and conservative automatic source eligibility."""

import hashlib
import json
import re

import yaml
from settings import CONFIG as SITE
from settings import app_path

SPEC = app_path("statefulset.yaml")
STATE = app_path("release-state.json")


def configuration_hash(files):
    entries = {}
    for path, text in files.items():
        if path == STATE or "/operations/" in path:
            continue
        if not path.startswith(app_path() + "/") or not path.endswith(
            (".yaml", ".yml", ".json")
        ):
            raise ValueError("Unexpected runtime input")
        if path == SPEC:
            text, count = re.subn(
                r"(?m)^(\s*image:\s*)"
                + re.escape(SITE["application_image"])
                + r"(?:@\S+|:\S+)[ \t]*$",
                lambda m: m[1] + "<qualified-image>",
                text,
            )
            if count != 1:
                raise ValueError("Require one controlled application image")
        value = (
            json.loads(text)
            if path.endswith(".json")
            else list(yaml.safe_load_all(text))
        )
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
        entries[path] = hashlib.sha256(canonical.encode()).hexdigest()
    if SPEC not in entries:
        raise ValueError("Missing application runtime input")
    return hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def qualified_profile(policy, previous, candidate, image, profile):
    entry = next(
        (
            a
            for a in reversed(policy.get("approved_transitions", []))
            if a["from"] == previous["source"] and a["to"] == candidate
        ),
        None,
    )
    if entry and "image" in entry and entry["image"] != image:
        raise ValueError("Published image differs from the qualified immutable digest")
    if entry and "profile_sha256" in entry and entry["profile_sha256"] != profile:
        raise ValueError("Production runtime configuration differs from qualification")


def verify_dedicated_volume(pvc, pv, expected, namespace):
    claim = pv["spec"].get("claimRef", {})
    csi = pv["spec"].get("csi", {})
    attrs = csi.get("volumeAttributes", {})
    path = attrs.get("share", "") + "/" + attrs.get("subDir", "")
    if (
        pvc["metadata"].get("namespace") != namespace
        or pvc["metadata"].get("name") != expected["claim"]
        or pvc["metadata"]["uid"] != expected["uid"]
        or pvc.get("status", {}).get("phase") != "Bound"
        or pvc["spec"]["volumeName"] != expected["pv"]
        or pv["metadata"]["uid"] != expected["pv_uid"]
        or claim.get("namespace") != namespace
        or claim.get("uid") != expected["uid"]
        or claim.get("name") != expected["claim"]
        or csi.get("driver") != "nfs.csi.k8s.io"
        or path != expected["path"]
        or not path.startswith(SITE["nfs_root"] + "/" + namespace + "/")
        or ".." in path.split("/")
    ):
        raise ValueError("Dedicated PVC/PV identity, owner or NFS path changed")


def qualification_identity(
    config,
    reviews,
    baseline,
    candidate,
    profile,
    production,
    eligible,
    reason,
    source_review=None,
):
    matched = [
        r
        for r in reviews.get("reviewed_transitions", [])
        if r.get("from") == baseline["source"] and r.get("to") == candidate["source"]
    ]
    frame = {
        "source_review": source_review,
        "deployment": SITE,
        "config": config,
        "reviews": matched,
        "review_version": reviews.get("version"),
        "baseline": baseline,
        "candidate": candidate,
        "profile": profile,
        "production": {
            "claims": production["claims"],
            "pods": production["pods"],
            "live_runtime_sha256": production.get("live_runtime_sha256"),
        },
        "eligible": eligible,
        "reason": reason,
    }
    return hashlib.sha256(
        json.dumps(frame, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def verify_live_data_layout(sts):
    spec = sts["spec"]
    templates = spec.get("volumeClaimTemplates", [])
    if (
        len(templates) != 1
        or templates[0]["metadata"]["name"] != "data"
        or templates[0]["spec"].get("volumeName")
        or templates[0]["spec"].get("selector")
    ):
        raise ValueError("Unexpected production data claim template")
    verify_pod_data_layout(spec["template"]["spec"])


def verify_running_pod_layout(pod, claim):
    verify_pod_data_layout(pod["spec"], claim=claim)


def verify_pod_data_layout(pod, *, claim=None):
    if pod.get("ephemeralContainers"):
        raise ValueError(
            "Production debug containers require a separate qualification profile"
        )
    data_volumes = [v for v in pod.get("volumes", []) if v["name"] == "data"]
    if claim is not None:
        if len(data_volumes) != 1 or data_volumes[0] != {
            "name": "data",
            "persistentVolumeClaim": {"claimName": claim},
        }:
            # Kubernetes may explicitly default readOnly to false.
            if len(data_volumes) != 1 or data_volumes[0] != {
                "name": "data",
                "persistentVolumeClaim": {"claimName": claim, "readOnly": False},
            }:
                raise ValueError(
                    "Running Pod data volume does not identify its owned claim"
                )
    elif data_volumes:
        raise ValueError("Production data must come from the claim template")
    if pod.get("initContainers"):
        raise ValueError(
            "Production init containers require a separate qualification profile"
        )
    for volume in pod.get("volumes", []):
        if claim is not None and volume["name"] == "data":
            continue
        if set(volume) - {
            "name",
            "secret",
            "configMap",
            "emptyDir",
            "projected",
            "downwardAPI",
        }:
            raise ValueError("Unexpected production volume source")
    for container in pod["containers"]:
        if container.get("lifecycle"):
            raise ValueError(
                "Production lifecycle hooks require a separate qualification profile"
            )
        mounts = container.get("volumeMounts", [])
        if container["name"] != "elastickv":
            if any(
                m["name"] == "data" and not m.get("readOnly", False) for m in mounts
            ):
                raise ValueError("Sidecar may not write production data")
            continue
        data = [m for m in mounts if m["name"] == "data"]
        if (
            len(data) != 1
            or data[0]["mountPath"] != "/var/lib/elastickv"
            or data[0].get("readOnly")
            or data[0].get("subPath")
            or data[0].get("subPathExpr")
        ):
            raise ValueError("Unexpected production data mount")
        if any(
            m["name"] != "data"
            and (
                m["mountPath"] == "/var/lib/elastickv"
                or m["mountPath"].startswith("/var/lib/elastickv/")
            )
            for m in mounts
        ):
            raise ValueError("Another volume overlaps production data")


def live_runtime_hash(sts):
    template = json.loads(json.dumps(sts["spec"]["template"]))
    containers = [c for c in template["spec"]["containers"] if c["name"] == "elastickv"]
    if len(containers) != 1:
        raise ValueError("Require exactly one application container")
    containers[0]["image"] = "<qualified-image>"
    return hashlib.sha256(
        json.dumps(template, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
