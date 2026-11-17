"""Review a data-only qualification branch; create/merge its tightly scoped PR."""

import argparse
import base64
import hashlib
import json
import os
import pathlib
import re
import tempfile
import urllib.parse

import check_elastickv_compatibility as check
import human_approval
import source_review
import yaml
from compatibility_profile import qualification_identity
from settings import CONFIG as SITE
from settings import repository_root, require_configured

ROOT = repository_root()
REPO = SITE["gitops_repository"]
POLICY = check.POLICY
CONFIG = SITE["qualification_path"] + "/policy.json"
BRANCH = (
    r"feature/elastickv-validation-[0-9a-f]{8}-[0-9a-f]{8}-[0-9a-f]{12}-[0-9a-f]{12}"
)


def verify_controller_binding(config, policy, controller, code):
    for field, filename in (
        ("agent_sha256", "agent.py"),
        ("qualification_source_sha256", "compatibility.py"),
    ):
        if hashlib.sha256((code / filename).read_bytes()).hexdigest() != config.get(
            field
        ):
            raise ValueError("Controller source and configured image version differ")
    if policy.get("controller_sha256") != config["agent_sha256"]:
        raise ValueError("Production and qualification engines differ")
    jobs = [
        item
        for item in yaml.safe_load_all(controller)
        if item and item.get("kind") == "CronJob"
    ]
    if (
        len(jobs) != 1
        or jobs[0]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][
            "image"
        ]
        != config["controller_image"]
    ):
        raise ValueError("Production controller image differs from qualification")


def api(path, method="GET", data=None):
    return check.agent.http_json(
        "https://api.github.com/repos/" + REPO + "/" + path,
        method,
        data,
        {
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
        },
    )


def content(path, sha):
    result = api("contents/" + path + "?ref=" + sha)
    return base64.b64decode(result["content"])


def read(path, sha):
    return json.loads(content(path, sha))


def review(branch, head, *, merged_base=None):
    if not re.fullmatch(BRANCH, branch) or not re.fullmatch("[0-9a-f]{40}", head):
        raise ValueError("Invalid machine qualification identity")
    if api("git/ref/heads/" + branch)["object"]["sha"] != head:
        raise ValueError("Machine branch changed")
    commit = api("git/commits/" + head)
    if len(commit["parents"]) != 1 or any(
        any(commit[who].get(k) != v for k, v in SITE["commit_identity"].items())
        for who in ("author", "committer")
    ):
        raise ValueError("Unexpected qualification commit provenance")
    base = commit["parents"][0]["sha"]
    main = api("git/ref/heads/" + SITE["gitops_branch"])["object"]["sha"]
    if merged_base is not None:
        main = merged_base  # Only merge_pr supplies the verified merge parent for recovery.
    for trusted in [
        SITE["code_path"] + "/" + name
        for name in [
            "compatibility_pr.py",
            "check_elastickv_compatibility.py",
            "compatibility_profile.py",
            "settings.py",
            "source_review.py",
            "human_approval.py",
        ]
    ]:
        if content(trusted, main) != (ROOT / trusted).read_bytes():
            raise ValueError("Trusted review code advanced; retry on current main")
    if api("compare/" + base + "..." + main).get("status") not in (
        "ahead",
        "identical",
    ):
        raise ValueError("Qualification base no longer belongs to main")
    diff = api("compare/" + base + "..." + head)["files"]
    summaries = [
        f["filename"]
        for f in diff
        if f["filename"].startswith(SITE["evidence_path"] + "/")
        and f["filename"].endswith("/summary.json")
    ]
    if len(summaries) != 1 or len(diff) > 6:
        raise ValueError("Expected one bounded qualification record")
    folder = summaries[0].rsplit("/", 1)[0]
    if not re.fullmatch(
        re.escape(SITE["evidence_path"]) + r"/[0-9a-f]{40}/[0-9a-f-]{8,80}", folder
    ):
        raise ValueError("Invalid qualification artifact path")
    allowed = {POLICY} | {
        folder + "/" + name
        for name in [
            "source-review.json",
            "summary.json",
            "observations.json.gz",
            "isolated_driver.py",
            "runtime_checks.py",
        ]
    }
    if any(
        f["filename"] not in allowed
        or f["status"] != ("modified" if f["filename"] == POLICY else "added")
        for f in diff
    ):
        raise ValueError("Machine PR contains unrelated changes")
    report = read(summaries[0], head)
    expected_branch = (
        "feature/elastickv-validation-"
        + report["from_source"][:8]
        + "-"
        + report["to_source"][:8]
        + "-"
        + report["profile_sha256"][:12]
        + "-"
        + report["qualification_sha256"][:12]
    )
    if (
        expected_branch != branch
        or report.get("scope") != SITE["validation_namespace"]
        or report.get("architecture") != "x86_64"
    ):
        raise ValueError("Qualification branch/profile scope mismatch")
    if read(SITE["code_path"] + "/deployment.json", main) != SITE:
        raise ValueError("Deployment settings changed since qualification")
    config = read(CONFIG, main)
    before = read(POLICY, main)
    state = read(check.STATE, main)
    human_approval.validate_policy(before)
    verify_controller_binding(
        config,
        before,
        content(SITE["release_path"] + "/controller.yaml", main).decode(),
        ROOT / SITE["code_path"],
    )
    if (
        report.get("controller_image") != config["controller_image"]
        or report.get("agent_source_sha256") != config["agent_sha256"]
    ):
        raise ValueError("Qualification controller version is stale")
    if (
        hashlib.sha256(
            (ROOT / SITE["code_path"] / "compatibility_validation.py").read_bytes()
        ).hexdigest()
        != report["driver_sha256"]
    ):
        raise ValueError("Worker source does not match the reviewed controller")
    if hashlib.sha256(
        (ROOT / SITE["code_path"] / "compatibility_runtime.py").read_bytes()
    ).hexdigest() != report.get("runtime_sha256"):
        raise ValueError("Protocol check source differs from reviewed controller")
    comparison = check.agent.http_json(
        "https://api.github.com/repos/"
        + before["repository"]
        + "/compare/"
        + report["from_source"]
        + "..."
        + report["to_source"]
    )
    source = read(folder + "/source-review.json", head)
    expected_source = source_review.collect(
        before["repository"],
        state["last_good"]["source"],
        report["to_source"],
        report["to_image"],
        comparison,
        lambda path: check.agent.http_json(
            "https://api.github.com/repos/" + before["repository"] + "/" + path
        ),
        limits=config.get("source_review_limits"),
    )
    if (
        source != expected_source
        or source_review.digest(source) != report.get("source_review_sha256")
        or config.get("source_reviewer_sha256") != source_review.fingerprint()
    ):
        raise ValueError(
            "Source review is incomplete, stale or does not match immutable source"
        )
    eligible = source_review.eligible(
        source, state["last_good"]["source"], report["to_source"], report["to_image"]
    )
    reason = "automated-source-review" if eligible else "source-review-blocked"
    identity = qualification_identity(
        config,
        {},
        state["last_good"],
        {"source": report["to_source"], "image": report["to_image"], "readiness": True},
        report["profile_sha256"],
        report["production_protection"],
        eligible,
        reason,
        source,
    )
    if identity != report.get("qualification_sha256"):
        raise ValueError(
            "Qualification eligibility/controller/production frame changed"
        )
    if (
        report.get("automatic_eligible") != eligible
        or report.get("eligibility_reason") != reason
    ):
        raise ValueError("Source eligibility does not match current reviewed rules")
    after = read(POLICY, head)
    if eligible:
        approval = after["approved_transitions"][-1]
        if set(approval) != {
            "from",
            "to",
            "readiness",
            "image",
            "profile_sha256",
            "production_bindings",
            "evidence",
        }:
            raise ValueError("Unexpected automatic approval fields")
    else:
        if after != before:
            raise ValueError("Manual-review qualification cannot authorize deployment")
        after = json.loads(json.dumps(before))
        after["approved_transitions"].append(
            {
                "from": report["from_source"],
                "to": report["to_source"],
                "image": report["to_image"],
                "readiness": True,
                "profile_sha256": report["profile_sha256"],
                "production_bindings": {
                    k: report["production_protection"][k]
                    for k in ("claims", "pods", "live_runtime_sha256")
                },
                "evidence": summaries[0],
            }
        )
    with tempfile.TemporaryDirectory() as d:
        root = pathlib.Path(d)
        for path in allowed - {POLICY}:
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content(path, head))
        if (
            hashlib.sha256(
                (root / folder / "runtime_checks.py").read_bytes()
            ).hexdigest()
            != report["runtime_sha256"]
        ):
            raise ValueError("Protocol-check artifact hash mismatch")
        tree = api("git/trees/" + main + "?recursive=1")
        if tree.get("truncated"):
            raise ValueError("Runtime inventory truncated")
        for entry in tree["tree"]:
            path = entry["path"]
            if (
                entry["type"] == "blob"
                and path.startswith(SITE["application_path"] + "/")
                and "/operations/" not in path
                and not path.endswith(".md")
            ):
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content(path, main))
        approval = check.validate(before, after, state, root, source_draft=not eligible)
        check.live_check(approval, before)
    main_source = check.agent.http_json(
        "https://api.github.com/repos/"
        + before["repository"]
        + "/commits/"
        + SITE["source_branch"]
    )["sha"]
    if main_source != report["to_source"]:
        ancestry = check.agent.http_json(
            "https://api.github.com/repos/"
            + before["repository"]
            + "/compare/"
            + report["to_source"]
            + "..."
            + main_source
        )
        if ancestry.get("status") not in ("ahead", "identical"):
            raise ValueError("Candidate no longer belongs to main")
    return {
        "policy": before,
        "proposed_policy": after,
        "source_review": source,
        "source_review_path": folder + "/source-review.json",
        "eligible": eligible,
        "reason": reason,
        "report": report,
        "base": main,
        "changes": diff,
        "head": head,
        "branch": branch,
    }


def request_pr(branch, head):
    if not re.fullmatch(BRANCH, branch) or not re.fullmatch("[0-9a-f]{40}", head):
        raise ValueError("Invalid machine proposal identity")
    existing = api(
        "pulls?"
        + urllib.parse.urlencode(
            {"state": "all", "head": REPO.split("/")[0] + ":" + branch, "per_page": 100}
        )
    )
    if existing:
        pr = max(existing, key=lambda p: p["number"])
        if pr["state"] != "open":
            print(
                json.dumps(
                    {"event": "qualification-pr-already-closed", "number": pr["number"]}
                )
            )
            return
    result = review(branch, head)
    r = result["report"]
    if not existing:
        source = result["source_review"]
        source_url = (
            "https://github.com/"
            + REPO
            + "/blob/"
            + head
            + "/"
            + urllib.parse.quote(result["source_review_path"], safe="/")
        )
        audit_summary = (
            f"Automated source audit: {len(source['files'])} files, "
            f"{sum(f['severity'] == 'blocker' for f in source['findings'])} blocking findings, "
            f"{sum(f['severity'] == 'attention' for f in source['findings'])} attention items. "
            f"[Source report]({source_url}). Human promotion reviewers: "
            + ", ".join("@" + name for name in result["policy"]["promotion_reviewers"])
            + ".\n\n"
        )
        body = (
            f"Qualify `{r['from_source']}` → `{r['to_source']}` with immutable image `{r['to_image']}`.\n\n"
            f"Isolated old/new/old/new and snapshot-restart validation passed with synthetic Redis/DynamoDB/S3/SQS data, minimum two Ready voters and unchanged dedicated PVCs. Profile: `{r['profile_sha256']}`. Source decision: `{result['reason']}`.\n\n"
            f"The finite default-profile validation does not approve arbitrary data formats or optional protocol activation. The automated source-contract review and bounded diff excerpts are in source-review.json. Approve this exact PR head to authorize promotion; workflow execution approval is not PR approval. Known incompatibility or incomplete evidence cannot be overridden by approval. Production data and backups were not mounted. Author: {SITE['commit_identity']['name']}."
        )
        pr = api(
            "pulls",
            "POST",
            {
                "head": branch,
                "base": SITE["gitops_branch"],
                "title": "Qualify Elastickv " + r["to_source"][:12],
                "body": audit_summary + body,
                "draft": not result["eligible"],
            },
        )
    if result["eligible"]:
        checks = api("commits/" + head + "/check-runs?per_page=100")["check_runs"]
        current = [
            c
            for c in checks
            if c["name"] == "compatibility-review"
            and c["app"]["slug"] == "github-actions"
        ]
        latest = max(current, key=lambda c: c["id"]) if current else None
        if not latest or (
            latest["status"] == "completed" and latest.get("conclusion") != "success"
        ):
            dispatch_validation(pr["number"], head, False)
    print(
        json.dumps(
            {
                "event": "qualification-pr",
                "number": pr["number"],
                "url": pr["html_url"],
                "automatic": result["eligible"],
            }
        )
    )


def merge_pr(number, head):
    pr = api("pulls/" + str(number))
    if (
        pr["head"]["sha"] != head
        or pr["base"]["ref"] != SITE["gitops_branch"]
        or (
            pr["state"] != "open" and not (pr["state"] == "closed" and pr.get("merged"))
        )
        or pr["draft"]
        or pr["user"]["login"] != SITE["bot_login"]
        or pr["head"]["repo"]["full_name"] != REPO
        or pr["base"]["repo"]["full_name"] != REPO
    ):
        raise ValueError("Not an eligible machine approval PR")
    merged = pr.get("merge_commit_sha") if pr.get("merged") else None
    merge_commit = api("git/commits/" + merged) if merged else None
    if merged and (
        not re.fullmatch("[0-9a-f]{40}", merged)
        or len(merge_commit["parents"]) != 2
        or merge_commit["parents"][1]["sha"] != head
    ):
        raise ValueError("Unexpected standard merge provenance")
    result = (
        review(pr["head"]["ref"], head, merged_base=merge_commit["parents"][0]["sha"])
        if merged
        else review(pr["head"]["ref"], head)
    )
    if not result["eligible"]:
        raise ValueError("Source review is required")
    checks = api("commits/" + head + "/check-runs?per_page=100")["check_runs"]
    matched = [
        c
        for c in checks
        if c["name"] == "compatibility-review" and c["app"]["slug"] == "github-actions"
    ]
    if (
        not matched
        or max(matched, key=lambda c: c["id"])["conclusion"] != "success"
        or max(matched, key=lambda c: c["id"])["status"] != "completed"
    ):
        raise ValueError("Latest compatibility CI/review not successful")
    consent = current_approval(pr, head, result["policy"], merged_sha=merged)
    if consent is None:
        print(
            json.dumps(
                {
                    "event": "promotion-awaiting-human-approval",
                    "number": number,
                    "head": head,
                }
            )
        )
        return
    main = result["base"]
    if merged is None:
        if api("git/ref/heads/" + SITE["gitops_branch"])["object"]["sha"] != main:
            raise ValueError("Main changed before merge; re-review required")
        final_pr = api("pulls/" + str(number))
        if current_approval(final_pr, head, result["policy"]) != consent:
            raise ValueError("Exact-head human approval changed or was revoked")
        # GitHub atomically checks the current PR state, exact head and branch rules.
        outcome = api(
            "pulls/" + str(number) + "/merge",
            "PUT",
            {
                "sha": head,
                "merge_method": "merge",
                "commit_title": "Merge qualified Elastickv "
                + result["report"]["to_source"][:12],
            },
        )
        if outcome.get("merged") is not True or not re.fullmatch(
            "[0-9a-f]{40}", outcome.get("sha", "")
        ):
            raise ValueError("GitHub did not merge the approved PR")
        merged = outcome["sha"]
        merge_commit = api("git/commits/" + merged)
    if [p["sha"] for p in merge_commit["parents"]] != [main, head]:
        raise ValueError("Main advanced during merge; no promotion permission recorded")
    # The standard merge contains no executable grant: only this second, guarded
    # commit records consent. Failure leaves an inert proposal and can be retried.
    if read(POLICY, merged) != result["proposed_policy"]:
        raise ValueError("Merged policy differs from reviewed proposal")
    tree_base = merge_commit["tree"]["sha"]
    proposed = json.loads(json.dumps(result["proposed_policy"]))
    proposed["approved_transitions"][-1]["human_approval"] = consent
    policy_blob = api(
        "git/blobs",
        "POST",
        {
            "content": base64.b64encode(
                (json.dumps(proposed, indent=2) + "\n").encode()
            ).decode(),
            "encoding": "base64",
        },
    )["sha"]
    entries = [{"path": POLICY, "mode": "100644", "type": "blob", "sha": policy_blob}]
    tree = api("git/trees", "POST", {"base_tree": tree_base, "tree": entries})["sha"]
    message = (
        "Record approved Elastickv "
        + result["report"]["to_source"][:12]
        + " (#"
        + str(number)
        + ")\n\nSigned-off-by: "
        + check.agent.AUTHOR["name"]
        + " <"
        + check.agent.AUTHOR["email"]
        + ">"
    )
    commit = api(
        "git/commits",
        "POST",
        {
            "tree": tree,
            "parents": [merged],
            "message": message,
            "author": check.agent.AUTHOR,
            "committer": check.agent.AUTHOR,
        },
    )["sha"]
    if api("git/ref/heads/" + SITE["gitops_branch"])["object"]["sha"] != merged:
        raise ValueError(
            "Main advanced after merge; retry only from the unchanged merge commit"
        )
    final_pr = api("pulls/" + str(number))
    if current_approval(final_pr, head, result["policy"], merged_sha=merged) != consent:
        raise ValueError("Exact-head human approval changed or was revoked")
    # No PR is merged by this ref write; it only records consent after the server merge.
    api(
        "git/refs/heads/" + SITE["gitops_branch"],
        "PATCH",
        {"sha": commit, "force": False},
    )
    print(
        json.dumps(
            {"event": "qualified-approval-merged", "number": number, "commit": commit}
        )
    )


def review_records(number):
    result = []
    for page in range(1, 11):
        rows = api("pulls/" + str(number) + "/reviews?per_page=100&page=" + str(page))
        result.extend(rows)
        if len(rows) < 100:
            return result
    raise ValueError("Review history exceeds its bounded inventory")


def current_approval(pr, head, policy, *, merged_sha=None):
    if merged_sha is not None:
        if (
            pr.get("state") != "closed"
            or not pr.get("merged")
            or pr.get("merge_commit_sha") != merged_sha
        ):
            return None
        pr = {
            **pr,
            "state": "open",
        }  # Assess reviews only on this verified server merge.
    return human_approval.approval(pr, head, review_records(pr["number"]), policy)


def dispatch_validation(number, head, promote):
    api(
        "actions/workflows/elastickv-compatibility-approval.yml/dispatches",
        "POST",
        {
            "ref": SITE["gitops_branch"],
            "inputs": {
                "pr_number": str(number),
                "head_sha": head,
                "promote": "true" if promote else "false",
            },
        },
    )


def dispatch_approved(number):
    pr = api("pulls/" + str(number))
    head = pr["head"]["sha"]
    if (
        pr["head"]["repo"]["full_name"] != REPO
        or pr["base"]["repo"]["full_name"] != REPO
        or pr["base"]["ref"] != SITE["gitops_branch"]
        or pr["user"]["login"] != SITE["bot_login"]
        or not re.fullmatch(BRANCH, pr["head"]["ref"])
    ):
        raise ValueError("Not a same-repository machine proposal")
    main = api("git/ref/heads/" + SITE["gitops_branch"])["object"]["sha"]
    policy = read(POLICY, main)
    if current_approval(pr, head, policy) is None:
        print(
            json.dumps({"event": "promotion-awaiting-human-approval", "number": number})
        )
        return
    dispatch_validation(number, head, True)


def record_check(number, head):
    pr = api("pulls/" + str(number))
    if (
        pr["head"]["sha"] != head
        or pr["head"]["repo"]["full_name"] != REPO
        or not re.fullmatch(BRANCH, pr["head"]["ref"])
    ):
        raise ValueError("Check target is not the immutable machine head")
    check_run = api(
        "check-runs",
        "POST",
        {"name": "compatibility-review", "head_sha": head, "status": "in_progress"},
    )["id"]
    try:
        result = review(pr["head"]["ref"], head)
        if not result["eligible"]:
            raise ValueError("Automated source review blocked promotion")
    except Exception:
        api(
            "check-runs/" + str(check_run),
            "PATCH",
            {
                "status": "completed",
                "conclusion": "failure",
                "completed_at": check.agent.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        raise
    api(
        "check-runs/" + str(check_run),
        "PATCH",
        {
            "status": "completed",
            "conclusion": "success",
            "completed_at": check.agent.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    print(json.dumps({"compatibility_review": "passed", "head": head}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["request", "review", "merge", "check", "dispatch-approved"]
    )
    parser.add_argument("--branch")
    parser.add_argument("--head")
    parser.add_argument("--pr", type=int)
    args = parser.parse_args()
    require_configured()
    if args.command == "request":
        request_pr(args.branch, args.head)
    elif args.command == "check":
        record_check(args.pr, args.head)
    elif args.command == "dispatch-approved":
        dispatch_approved(args.pr)
    elif args.command == "merge":
        merge_pr(args.pr, args.head)
    else:
        pr = api("pulls/" + str(args.pr))
        result = review(pr["head"]["ref"], args.head)
        if pr["head"]["sha"] != args.head or not result["eligible"]:
            raise SystemExit("Not automatically eligible")
        print(
            json.dumps(
                {
                    "compatibility_review": "passed",
                    "head": args.head,
                    "profile": result["report"]["profile_sha256"],
                }
            )
        )
