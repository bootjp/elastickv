"""Seal three dedicated NFS CSI volumes; never mount or alter their contents."""

import argparse
import datetime
import json
import posixpath
import subprocess

from compatibility_profile import verify_dedicated_volume
from settings import CONFIG as SITE
from settings import require_configured


def register(context=None, dry_run=False):
    require_configured()
    command = ["kubectl"] + (["--context", context] if context else [])
    namespace = SITE["validation_namespace"]

    def get(kind, name, ns=None):
        args = command + (["-n", ns] if ns else [])
        return json.loads(
            subprocess.check_output(args + ["get", kind, name, "-o", "json"])
        )

    def create(value):
        subprocess.run(
            command + ["create", "-f", "-"],
            input=json.dumps(value).encode(),
            check=True,
        )

    production = set()
    for name in ["data-elastickv-" + str(i) for i in range(3)] + SITE["backup_claims"]:
        claim = get("pvc", name, SITE["production_namespace"])
        if claim.get("status", {}).get("phase") != "Bound":
            raise ValueError("Production and backup claims must already be Bound")
        production.add(claim["spec"]["volumeName"])
    rows = []
    for index in range(3):
        name = "data-elastickv-" + str(index)
        pvc = get("pvc", name, namespace)
        if (
            pvc.get("status", {}).get("phase") != "Bound"
            or pvc["spec"]["volumeName"] in production
        ):
            raise ValueError(
                "Validation claims must be Bound and disjoint from protected claims"
            )
        pv = get("pv", pvc["spec"]["volumeName"])
        attributes = pv["spec"].get("csi", {}).get("volumeAttributes", {})
        share, subdirectory = attributes.get("share", ""), attributes.get("subDir", "")
        path = share + "/" + subdirectory
        if (
            share != SITE["nfs_root"]
            or posixpath.normpath(path) != path
            or not subdirectory.startswith(namespace + "/")
        ):
            raise ValueError(
                "Require a dedicated namespace directory beneath the configured NFS root"
            )
        row = {
            "claim": name,
            "uid": pvc["metadata"]["uid"],
            "pv": pv["metadata"]["name"],
            "pv_uid": pv["metadata"]["uid"],
            "path": path,
        }
        verify_dedicated_volume(pvc, pv, row, namespace)
        rows.append(row)
    if len({r["pv"] for r in rows}) != 3 or len({r["path"] for r in rows}) != 3:
        raise ValueError("Validation volumes must be independent")
    data = {
        "registered_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "pvc_uids": {r["claim"]: r["uid"] for r in rows},
        "volumes": rows,
    }
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "immutable": True,
        "metadata": {"name": "compatibility-volume-identities", "namespace": namespace},
        "data": {"identities.json": json.dumps(data)},
    }
    role_name = "elastickv-validation-volumes-" + namespace
    role = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": role_name},
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["persistentvolumes"],
                "resourceNames": [r["pv"] for r in rows],
                "verbs": ["get"],
            }
        ],
    }
    binding = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": role_name},
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": role_name,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "elastickv-compatibility",
                "namespace": SITE["control_namespace"],
            },
            {
                "kind": "ServiceAccount",
                "name": "compatibility-validator",
                "namespace": namespace,
            },
        ],
    }
    if dry_run:
        print(json.dumps({"dry_run": True, "registration": data}, indent=2))
    else:
        # Create, never patch: an existing sealed identity needs investigation.
        for value in (configmap, role, binding):
            create(value)
        print(
            json.dumps(
                {
                    "registered_claims": list(data["pvc_uids"]),
                    "protected_volumes_disjoint": True,
                }
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--context", help="Explicit kubectl context; otherwise use the current context"
    )
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()
    register(arguments.context, arguments.dry_run)
