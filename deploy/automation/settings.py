"""Site configuration shared by the controller, validator and trusted PR tools.

Importing modules is offline. Mutating entrypoints require an explicit deployment
file; examples never silently become a live deployment configuration.
"""

import copy
import json
import os
import pathlib
import re

PACKAGE = pathlib.Path(__file__).resolve().parent
DEFAULTS = {
    "gitops_repository": "example/elastickv-gitops",
    "gitops_branch": "main",
    "source_repository": "example/elastickv",
    "source_branch": "main",
    "application_image": "ghcr.io/example/elastickv",
    "control_namespace": "elastickv-system",
    "production_namespace": "elastickv",
    "validation_namespace": "elastickv-validation",
    "backup_claims": ["elastickv-backup"],
    "application_path": "apps/elastickv",
    "release_path": "apps/elastickv-release",
    "qualification_path": "apps/elastickv-compatibility",
    "evidence_path": "docs/operations/elastickv-compatibility",
    "code_path": "automation",
    "storage_class": "validation-storage",
    "nfs_root": "/srv/validation",
    "commit_identity": {"name": "Release Operator", "email": "operator@example.com"},
    "bot_login": "github-actions[bot]",
    "promotion_reviewers": [],
}


def validate(config, live=False):
    if set(config) != set(DEFAULTS):
        raise ValueError("Unknown or missing deployment setting")
    for key in ("control_namespace", "production_namespace", "validation_namespace"):
        if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", config[key]):
            raise ValueError("Invalid namespace: " + key)
    if (
        len(
            {
                config[k]
                for k in (
                    "control_namespace",
                    "production_namespace",
                    "validation_namespace",
                )
            }
        )
        != 3
    ):
        raise ValueError(
            "Control, production and validation namespaces must be distinct"
        )
    for key in (
        "application_path",
        "release_path",
        "qualification_path",
        "evidence_path",
        "code_path",
    ):
        path = pathlib.PurePosixPath(config[key])
        if (
            path.is_absolute()
            or ".." in path.parts
            or str(path) != config[key]
            or config[key] in ("", ".")
        ):
            raise ValueError("Require a normalized repository-relative path: " + key)
    paths = [
        config[k]
        for k in (
            "application_path",
            "release_path",
            "qualification_path",
            "evidence_path",
            "code_path",
        )
    ]
    if any(
        a == b or a.startswith(b + "/") or b.startswith(a + "/")
        for i, a in enumerate(paths)
        for b in paths[i + 1 :]
    ):
        raise ValueError("Controlled paths must not overlap")
    for key in ("gitops_repository", "source_repository"):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", config[key]):
            raise ValueError("Invalid repository: " + key)
    for key in ("gitops_branch", "source_branch"):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", config[key]):
            raise ValueError("Branch must be a simple branch name: " + key)
    if not re.fullmatch(r"[a-z0-9.-]+/[a-z0-9_./-]+", config["application_image"]):
        raise ValueError("Require an untagged registry/repository image name")
    claims = config["backup_claims"]
    if not isinstance(claims, list) or not claims or len(set(claims)) != len(claims):
        raise ValueError("Require distinct protected backup claims")
    for claim in claims:
        if not re.fullmatch(r"[a-z0-9][-a-z0-9.]*", claim) or claim.startswith(
            "data-elastickv-"
        ):
            raise ValueError("Invalid protected backup claim")
    root = pathlib.PurePosixPath(config["nfs_root"])
    if (
        not root.is_absolute()
        or str(root) != config["nfs_root"]
        or ".." in root.parts
        or str(root) == "/"
    ):
        raise ValueError("Require an explicit, normalized validation NFS root")
    identity = config["commit_identity"]
    if (
        set(identity) != {"name", "email"}
        or not identity["name"]
        or not re.fullmatch(r"[^\s<>@]+@[^\s<>@]+", identity["email"])
    ):
        raise ValueError("Invalid release commit identity")
    reviewers = config["promotion_reviewers"]
    if (
        not isinstance(reviewers, list)
        or len(set(reviewers)) != len(reviewers)
        or any(not re.fullmatch(r"[A-Za-z0-9-]+", v) for v in reviewers)
    ):
        raise ValueError("Invalid human promotion reviewers")
    if live and not reviewers:
        raise ValueError("Configure at least one human promotion reviewer")
    if live and (
        "example/" in config["gitops_repository"]
        or "example/" in config["source_repository"]
        or "example/" in config["application_image"]
        or identity["email"].endswith("@example.com")
    ):
        raise ValueError("Replace example repository, image and identity settings")
    return config


def load():
    config = copy.deepcopy(DEFAULTS)
    filename = os.environ.get("ELASTICKV_DEPLOYMENT_CONFIG")
    if filename:
        config = json.loads(pathlib.Path(filename).read_text())
    return validate(config)


CONFIG = load()


def require_configured():
    if not os.environ.get("ELASTICKV_DEPLOYMENT_CONFIG"):
        raise ValueError(
            "Set ELASTICKV_DEPLOYMENT_CONFIG to an explicit deployment.json"
        )
    validate(CONFIG, live=True)


def app_path(suffix=""):
    return CONFIG["application_path"] + ("/" + suffix if suffix else "")


def repository_root():
    root = PACKAGE
    for _ in pathlib.PurePosixPath(CONFIG["code_path"]).parts:
        root = root.parent
    return root
