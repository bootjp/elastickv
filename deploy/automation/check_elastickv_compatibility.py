"""Validate newly added exact-SHA approvals and their isolated test evidence."""

import argparse
import gzip
import hashlib
import json
import pathlib
import re
import subprocess

import agent
from settings import CONFIG as SITE
from settings import app_path, repository_root, require_configured

ROOT = repository_root()
POLICY = SITE["release_path"] + "/policy.json"
STATE = app_path("release-state.json")


def validate(before, after, baseline, root=ROOT, *, source_draft=False):
    previous = before["approved_transitions"]
    ignored = (
        "approved_transitions",
        "require_qualification",
        "require_human_approval",
        "promotion_reviewers",
    )
    old_other = {k: v for k, v in before.items() if k not in ignored}
    new_other = {k: v for k, v in after.items() if k not in ignored}
    old_required = before.get("require_qualification", False)
    new_required = after.get("require_qualification", False)
    old_human = before.get("require_human_approval", False)
    new_human = after.get("require_human_approval", False)
    if (
        old_other != new_other
        or any(
            type(v) is not bool
            for v in (old_required, new_required, old_human, new_human)
        )
        or (old_required and not new_required)
        or (old_human and not new_human)
    ):
        raise ValueError("Approval PR may not weaken the release policy")
    if old_human and before.get("promotion_reviewers") != after.get(
        "promotion_reviewers"
    ):
        raise ValueError("A machine approval cannot change human reviewers")
    if new_human:
        import human_approval

        human_approval.validate_policy(after)
    if after["approved_transitions"][: len(previous)] != previous:
        raise ValueError("Existing approvals must remain unchanged")
    added = after["approved_transitions"][len(previous) :]
    if not added and (
        (not old_required and new_required) or (not old_human and new_human)
    ):
        if baseline.get("transaction"):
            raise ValueError(
                "Cannot enable mandatory qualification during an active release"
            )
        return None  # Strengthening the gate grants no image permission.
    if len(added) != 1:
        raise ValueError("Require exactly one new transition")
    approval = added[0]
    if baseline.get("transaction"):
        raise ValueError("Production transaction must be idle")
    if approval["from"] != baseline["last_good"]["source"]:
        raise ValueError("Evidence baseline no longer matches production")
    if (
        not re.fullmatch("[0-9a-f]{40}", approval["to"])
        or approval.get("readiness") is not True
    ):
        raise ValueError("Require an exact SHA and private HTTP readiness")
    expected = approval.get("evidence", "")
    pattern = (
        re.escape(SITE["evidence_path"])
        + "/"
        + approval["to"]
        + r"/(?:[0-9a-f-]{8,80}/)?summary\.json"
    )
    if not re.fullmatch(pattern, expected):
        raise ValueError("Unexpected evidence path")
    report = json.loads((root / expected).read_text())
    if (
        report.get("passed") is not True
        or report.get("from_source") != approval["from"]
        or report.get("to_source") != approval["to"]
        or report.get("to_image") != approval.get("image")
        or report.get("from_image") != baseline["last_good"]["image"]
        or report.get("scope_kind") != "isolated-synthetic-kubernetes"
        or report.get("production_data_used") is not False
    ):
        raise ValueError("Evidence identity or result mismatch")
    if not re.fullmatch(
        re.escape(SITE["application_image"]) + r"@sha256:[0-9a-f]{64}",
        approval["image"],
    ):
        raise ValueError("Require immutable application digest")
    if report.get("new_features") != {
        "commit_ts_fence": False,
        "timestamp_binding": False,
        "phase_d": False,
    }:
        raise ValueError("Optional protocol activation requires separate review")
    if len(report.get("pvc_uids", {})) != 3:
        raise ValueError("Require three independently owned test PVCs")
    if set(report.get("cases", {})) != {
        "upgrade",
        "downgrade",
        "reupgrade",
        "snapshot-restart",
    }:
        raise ValueError("Missing compatibility scenario")
    for case in report["cases"].values():
        data = case["data"]
        if (
            case["minimum_ready"] < 2
            or case.get("observation_errors", 0) != 0
            or data["redis_keys_verified_on_each_of_three_members"] < 128
            or data["dynamodb_consistent_read"] is not True
            or data["s3_bytes"] < 262144
            or data["sqs_message_bodies_preserved"] < 2
        ):
            raise ValueError("Insufficient data/quorum evidence")
    if len(report.get("snapshots_before_restart", {})) != 3 or any(
        int(s["last_snapshot_index"]) <= 0
        for s in report["snapshots_before_restart"].values()
    ):
        raise ValueError("No native snapshots verified")
    folder = root / pathlib.Path(expected).parent
    if after.get("require_human_approval"):
        import source_review

        source = json.loads((folder / "source-review.json").read_text())
        permitted = source_review.eligible(
            source, approval["from"], approval["to"], approval["image"]
        )
        blocked_draft = (
            source_draft
            and source.get("complete") is True
            and source.get("decision") == "blocked"
        )
        if source_review.digest(source) != report.get("source_review_sha256") or not (
            permitted or blocked_draft
        ):
            raise ValueError("Missing, stale or blocked automated source review")
    driver = folder / "isolated_driver.py"
    if hashlib.sha256(driver.read_bytes()).hexdigest() != report.get("driver_sha256"):
        raise ValueError("Validation driver hash mismatch")
    with gzip.open(folder / "observations.json.gz", "rb") as stream:
        raw = stream.read(20 * 1024 * 1024 + 1)
    if len(raw) > 20 * 1024 * 1024:
        raise ValueError("Observation archive too large")
    observations = json.loads(raw)
    for name, case in report["cases"].items():
        samples = observations.get(name, [])
        if not samples or min(s["ready"] for s in samples) != case["minimum_ready"]:
            raise ValueError("Summary does not match full observations")
    if "production_bindings" in approval:
        binding = approval["production_bindings"]
        protection = report.get("production_protection", {})
        if (
            protection.get("unchanged") is not True
            or binding
            != {k: protection.get(k) for k in ("claims", "pods", "live_runtime_sha256")}
            or set(binding["claims"])
            != {"data-elastickv-" + str(i) for i in range(3)}
            | set(SITE["backup_claims"])
            or set(binding["pods"]) != {"elastickv-" + str(i) for i in range(3)}
            or set(binding["claims"].values()) & set(report["pvc_uids"].values())
        ):
            raise ValueError("Production/backup identity binding is missing or unsafe")
    elif after.get("require_qualification"):
        raise ValueError(
            "Strict qualification needs production/backup identity binding"
        )
    if "profile_sha256" in approval:
        from compatibility_profile import configuration_hash

        if report.get("profile_sha256") != approval["profile_sha256"]:
            raise ValueError("Profile does not match approval")
        files = {
            p.relative_to(root).as_posix(): p.read_text()
            for p in (root / SITE["application_path"]).rglob("*")
            if p.is_file() and p.suffix != ".md" and "/operations/" not in p.as_posix()
        }
        if configuration_hash(files) != approval["profile_sha256"]:
            raise ValueError("Production runtime inputs changed since qualification")
    return approval


def live_check(approval, policy):
    sha = approval["to"]
    repository = policy["repository"]
    runs = []
    for event in ("push", "dynamic"):
        runs.extend(
            agent.http_json(
                f"https://api.github.com/repos/{repository}/actions/runs?head_sha={sha}&event={event}&per_page=100"
            )["workflow_runs"]
        )
    if not agent.ci_ready(
        runs, sha, policy["required_workflows"], policy.get("codeql_workflow_id")
    ):
        raise ValueError("Exact-commit required CI not successful")
    if agent.registry_image(policy["image"], sha) != approval["image"]:
        raise ValueError("Published image no longer matches evidence")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    require_configured()
    if not re.fullmatch("[0-9a-f]{40}", args.base):
        raise SystemExit("Use a full base SHA")

    def read(path):
        return json.loads(
            subprocess.check_output(
                ["git", "-C", str(ROOT), "show", args.base + ":" + path]
            )
        )

    after = json.loads((ROOT / POLICY).read_text())
    before = read(POLICY)
    if before == after:
        print(json.dumps({"compatibility_review": "no-new-approval"}))
        raise SystemExit(0)
    approval = validate(before, after, read(STATE))
    if approval is None:
        print(json.dumps({"compatibility_review": "qualification-gate-enabled"}))
        raise SystemExit(0)
    if args.live:
        live_check(approval, after)
    print(
        json.dumps(
            {
                "compatibility_review": "passed",
                "from": approval["from"],
                "to": approval["to"],
                "image": approval["image"],
            }
        )
    )
