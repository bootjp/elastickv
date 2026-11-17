"""Exact-head human consent, independently of workflow execution approval."""

import re


def validate_policy(policy):
    reviewers = policy.get("promotion_reviewers")
    if (
        policy.get("require_human_approval") is not True
        or not isinstance(reviewers, list)
        or not reviewers
    ):
        raise ValueError("Explicit human promotion reviewers are required")
    if len(set(reviewers)) != len(reviewers) or any(
        not re.fullmatch(r"[A-Za-z0-9-]+", name) for name in reviewers
    ):
        raise ValueError("Promotion reviewers must be distinct human GitHub accounts")
    return set(reviewers)


def approval(pr, head, reviews, policy):
    allowed = validate_policy(policy)
    if pr.get("state") != "open" or pr.get("draft") or pr["head"]["sha"] != head:
        return None
    effective = {}
    for review in sorted(reviews, key=lambda r: r["id"]):
        state = review.get("state", "").upper()
        if state in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            effective[review["user"]["login"]] = review
    if any(
        r["state"].upper() == "CHANGES_REQUESTED"
        and (
            name in allowed
            or r.get("author_association") in ("OWNER", "MEMBER", "COLLABORATOR")
        )
        for name, r in effective.items()
    ):
        return None
    candidates = [
        r
        for name, r in effective.items()
        if name in allowed
        and r["user"].get("type") == "User"
        and r.get("author_association") in ("OWNER", "MEMBER", "COLLABORATOR")
        and r["state"].upper() == "APPROVED"
        and r.get("commit_id") == head
    ]
    if not candidates:
        return None
    selected = max(candidates, key=lambda r: r["id"])
    return {
        "reviewer": selected["user"]["login"],
        "review_id": selected["id"],
        "commit": head,
        "pull_request": pr["number"],
    }


def valid_attestation(policy, transition):
    if policy.get("require_human_approval") is not True:
        return True  # Old ledgers remain readable; live promotion policies require the new gate.
    try:
        allowed = validate_policy(policy)
        value = transition.get("human_approval", {})
        return (
            set(value) == {"reviewer", "review_id", "commit", "pull_request"}
            and value["reviewer"] in allowed
            and bool(re.fullmatch("[0-9a-f]{40}", value["commit"]))
            and all(
                type(value[k]) is int and value[k] > 0
                for k in ("review_id", "pull_request")
            )
        )
    except (TypeError, ValueError, KeyError):
        return False
