"""Render an offline, suspended GitOps scaffold; never apply or overwrite files."""

import argparse
import hashlib
import json
import pathlib
import re
import shutil

import yaml
from settings import DEFAULTS, PACKAGE, validate


def render(
    config, output, controller_image=None, baseline_source=None, baseline_image=None
):
    validate(config)
    output = pathlib.Path(output)
    if (
        output.resolve() == PACKAGE.resolve()
        or PACKAGE.resolve() in output.resolve().parents
    ):
        raise ValueError("Output must be outside the source package")
    if output.exists() and any(output.iterdir()):
        raise ValueError(
            "Output directory must be empty; existing repositories are never overwritten"
        )
    if controller_image and not re.fullmatch(
        r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}", controller_image
    ):
        raise ValueError("Controller image must use an immutable digest")
    if baseline_source and not re.fullmatch(r"[0-9a-f]{40}", baseline_source):
        raise ValueError("Baseline source must be a full commit SHA")
    if baseline_image and not re.fullmatch(
        re.escape(config["application_image"]) + r"@sha256:[0-9a-f]{64}", baseline_image
    ):
        raise ValueError(
            "Baseline image must belong to the configured application repository"
        )

    replacements = {
        DEFAULTS[key]: config[key]
        for key in (
            "application_path",
            "release_path",
            "qualification_path",
            "evidence_path",
            "code_path",
            "validation_namespace",
            "control_namespace",
            "application_image",
            "gitops_repository",
            "source_repository",
            "storage_class",
        )
    }
    expression = re.compile(
        "|".join(re.escape(v) for v in sorted(replacements, key=len, reverse=True))
    )

    def text(value):
        value = expression.sub(lambda match: replacements[match.group()], value)
        return value.replace(
            ".elastickv.svc", "." + config["production_namespace"] + ".svc"
        )

    def transform(value, key=None):
        if isinstance(value, dict):
            if (
                value.get("kind") == "Namespace"
                and value["metadata"]["name"] == DEFAULTS["production_namespace"]
            ):
                value = {
                    **value,
                    "metadata": {
                        **value["metadata"],
                        "name": config["production_namespace"],
                    },
                }
            return {k: transform(v, k) for k, v in value.items()}
        if isinstance(value, list):
            result = []
            for item in value:
                if item == DEFAULTS["backup_claims"][0]:
                    result.extend(config["backup_claims"])
                else:
                    result.append(transform(item, key))
            return result
        if isinstance(value, str):
            if (
                key in ("namespace", "kubernetes.io/metadata.name")
                and value == DEFAULTS["production_namespace"]
            ):
                return config["production_namespace"]
            return text(value)
        return value

    output.mkdir(parents=True, exist_ok=True)
    for source in sorted((PACKAGE / "examples/gitops").rglob("*")):
        if not source.is_file():
            continue
        relative = text(source.relative_to(PACKAGE / "examples/gitops").as_posix())
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.suffix == ".json":
            target.write_text(
                json.dumps(transform(json.loads(source.read_text())), indent=2) + "\n"
            )
        elif source.suffix == ".yaml":
            target.write_text(
                yaml.safe_dump_all(
                    [transform(v) for v in yaml.safe_load_all(source.read_text())],
                    sort_keys=False,
                )
            )
        else:
            content = text(source.read_text())
            if relative.startswith(".github/workflows/"):
                content = content.replace(
                    "ref: main", "ref: " + config["gitops_branch"]
                )
            target.write_text(content)

    code = output / config["code_path"]
    code.mkdir(parents=True, exist_ok=True)
    for source in PACKAGE.glob("*.py"):
        shutil.copyfile(source, code / source.name)
    shutil.copytree(PACKAGE / "examples", code / "examples")
    (code / "deployment.json").write_text(json.dumps(config, indent=2) + "\n")

    release = output / config["release_path"]
    qualification = output / config["qualification_path"]
    app = output / config["application_path"]
    policy = json.loads((release / "policy.json").read_text())
    policy.update(
        repository=config["source_repository"],
        gitops_repository=config["gitops_repository"],
        namespace=config["production_namespace"],
        image=config["application_image"],
        approved_transitions=[],
        require_qualification=True,
        require_human_approval=True,
        promotion_reviewers=config["promotion_reviewers"],
    )
    policy["baseline_source"] = baseline_source or "0" * 40
    policy["controller_sha256"] = hashlib.sha256(
        (code / "agent.py").read_bytes()
    ).hexdigest()
    (release / "policy.json").write_text(json.dumps(policy, indent=2) + "\n")
    ledger = json.loads((app / "release-state.json").read_text())
    ledger["last_good"].update(
        source=baseline_source or "0" * 40,
        image=baseline_image or config["application_image"] + "@sha256:" + "0" * 64,
    )
    (app / "release-state.json").write_text(json.dumps(ledger, indent=2) + "\n")
    spec = app / "statefulset.yaml"
    spec.write_text(
        spec.read_text().replace(
            config["application_image"] + "@sha256:" + "0" * 64,
            ledger["last_good"]["image"],
        )
    )
    qualified = json.loads((qualification / "policy.json").read_text())
    for key, filename in (
        ("agent_sha256", "agent.py"),
        ("qualification_source_sha256", "compatibility.py"),
    ):
        qualified[key] = hashlib.sha256((code / filename).read_bytes()).hexdigest()
    import source_review

    qualified["source_reviewer_sha256"] = source_review.fingerprint()
    if controller_image:
        qualified["controller_image"] = controller_image
    (qualification / "policy.json").write_text(json.dumps(qualified, indent=2) + "\n")

    # Site settings are non-secret; mount identical settings in each trust domain.
    for directory in (release, qualification):
        (directory / "deployment.json").write_text(json.dumps(config, indent=2) + "\n")
        path = directory / "controller.yaml"
        documents = list(yaml.safe_load_all(path.read_text()))
        for document in documents:
            if document.get("kind") != "CronJob":
                continue
            document["spec"]["suspend"] = True
            pod = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            if controller_image:
                pod["containers"][0]["image"] = controller_image
            mount_settings(pod)
        path.write_text(yaml.safe_dump_all(documents, sort_keys=False))
        kustomization = directory / "kustomization.yaml"
        value = yaml.safe_load(kustomization.read_text())
        if directory == release:
            value.setdefault("configMapGenerator", []).append(
                {
                    "name": "elastickv-automation-site",
                    "namespace": config["control_namespace"],
                    "files": ["deployment.json"],
                }
            )
        if directory == qualification:
            value["configMapGenerator"].append(
                {
                    "name": "elastickv-automation-site",
                    "namespace": config["validation_namespace"],
                    "files": ["deployment.json"],
                }
            )
        kustomization.write_text(yaml.safe_dump(value, sort_keys=False))
    assets_path = qualification / "assets.json"
    assets = json.loads(assets_path.read_text())
    mount_settings(assets["validator"]["spec"]["template"]["spec"])
    if controller_image:
        for key in ("validator", "cleaner"):
            assets[key]["spec"]["template"]["spec"]["containers"][0]["image"] = (
                controller_image
            )
    assets_path.write_text(json.dumps(assets, indent=2) + "\n")
    print(
        json.dumps(
            {"output": str(output), "schedulers_suspended": True, "approvals": 0}
        )
    )


def mount_settings(pod):
    pod.setdefault("volumes", []).append(
        {"name": "deployment", "configMap": {"name": "elastickv-automation-site"}}
    )
    for container in pod["containers"]:
        container.setdefault("env", []).append(
            {
                "name": "ELASTICKV_DEPLOYMENT_CONFIG",
                "value": "/etc/automation/deployment.json",
            }
        )
        container.setdefault("volumeMounts", []).append(
            {"name": "deployment", "mountPath": "/etc/automation", "readOnly": True}
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--controller-image")
    parser.add_argument("--baseline-source")
    parser.add_argument("--baseline-image")
    args = parser.parse_args()
    render(
        json.loads(pathlib.Path(args.config).read_text()),
        args.output,
        args.controller_image,
        args.baseline_source,
        args.baseline_image,
    )
