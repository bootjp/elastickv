"""Bounded, reproducible source-contract audit; never execute candidate code.

This is an automated risk assessment, not a proof of arbitrary Go semantics.
The promotion gate additionally requires isolated round-trip tests and a human
approval of the exact completed proposal head.
"""

import difflib
import gzip
import hashlib
import io
import itertools
import json
import pathlib
import re
import tarfile
import urllib.request

VERSION = 1
DEFAULT_LIMITS = {
    "max_files": 200,
    "max_file_bytes": 2 * 1024 * 1024,
    "max_total_file_bytes": 64 * 1024 * 1024,
    "max_archive_bytes": 64 * 1024 * 1024,
    "max_expanded_bytes": 512 * 1024 * 1024,
}
LIMIT_CEILINGS = {**DEFAULT_LIMITS, "max_files": 1000}
PERSISTENT = ("store/", "kv/", "proto/", "internal/raftengine/", "distribution/")


def fingerprint():
    return hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def git_blob(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def tree_files(tree):
    if tree.get("truncated") is not False:
        raise ValueError("Source inventory is truncated or incomplete")
    return {r["path"]: r for r in tree["tree"] if r["type"] in ("blob", "commit")}


class BudgetReader:
    def __init__(self, stream, maximum):
        self.stream, self.remaining = stream, maximum

    def read(self, size=-1):
        size = self.remaining + 1 if size < 0 else min(size, self.remaining + 1)
        data = self.stream.read(size)
        self.remaining -= len(data)
        if self.remaining < 0:
            raise ValueError("Source archive expansion exceeds its bound")
        return data


def download_archive(repository, sha, needed, limits, tick=lambda: None):
    request = urllib.request.Request(
        "https://codeload.github.com/" + repository + "/tar.gz/" + sha
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        compressed = response.read(limits["max_archive_bytes"] + 1)
    if len(compressed) > limits["max_archive_bytes"]:
        raise ValueError("Source archive exceeds its bound")
    result = {}
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as expanded:
        stream = BudgetReader(expanded, limits["max_expanded_bytes"])
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            for member in archive:
                tick()
                path = pathlib.PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts:
                    raise ValueError("Unnormalized source archive path")
                relative = "/".join(path.parts[1:])
                if relative not in needed:
                    continue
                if (
                    not member.isfile()
                    or relative in result
                    or member.size > limits["max_file_bytes"]
                ):
                    raise ValueError("Unsupported or oversized source archive entry")
                data = archive.extractfile(member).read(limits["max_file_bytes"] + 1)
                if (
                    len(data) != member.size
                    or git_blob(data) != needed[relative]["sha"]
                ):
                    raise ValueError(
                        "Source content does not match the immutable Git blob"
                    )
                result[relative] = data
    if set(result) != set(needed):
        raise ValueError("Source archive omitted required files")
    return result


def code_lines(text):
    # Removing comments is deliberately conservative; candidate code is never run.
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("//")
    ]


def marker_expression(lines, start, first):
    """Read a complete Go initializer without evaluating candidate code."""
    stack, parts = [], []
    quote, escaped = None, False
    last = None
    pairs = {")": "(", "]": "[", "}": "{"}
    for index in range(start, len(lines)):
        line = first if index == start else lines[index]
        for char in line:
            if not char.isspace():
                last = char
            if quote is not None:
                parts.append(char)
                if escaped:
                    escaped = False
                elif char == "\\" and quote != "`":
                    escaped = True
                elif char == quote:
                    quote = None
                continue
            if char in ('"', "'", "`"):
                quote = char
            elif char in "([{":
                stack.append(char)
            elif char in ")]}":
                if not stack or stack.pop() != pairs[char]:
                    return None, index
            elif char == ";" and not stack:
                return "".join(parts).strip() or None, index
            parts.append(char)
        if quote is not None and quote != "`":
            return None, index
        if (
            not stack
            and quote is None
            and last is not None
            and last not in "+-*/%&|^<>=!,:."
        ):
            return "".join(parts).strip(), index
    return None, len(lines) - 1


def version_literals(text):
    # Quoted literals survive comment removal, including raw multiline strings.
    text = re.sub(
        r"(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|`[^`]*`)|//[^\n]*|/\*[\s\S]*?\*/",
        lambda m: m[1] if m[1] is not None else "\n" * m[0].count("\n"),
        text,
    )
    declaration = re.compile(
        r"^[ \t]*(?:(?P<kind>const|var)[ \t]+)?(?P<name>[A-Za-z_]\w*)"
        r"(?:[ \t]+(?P<type>[\w.\[\]]+))?[ \t]*=[ \t]*(?P<expression>.*)$"
    )
    result, index = {}, 0
    lines = text.splitlines(keepends=True)
    while index < len(lines):
        match = declaration.match(lines[index].rstrip("\r\n"))
        if match:
            name = match["name"].lower()
            if "magic" in name or (
                "version" in name
                and any(
                    k in name
                    for k in (
                        "format",
                        "schema",
                        "snapshot",
                        "codec",
                        "wire",
                        "disk",
                        "hlc",
                        "mvcc",
                        "pebble",
                    )
                )
            ):
                expression, index = marker_expression(
                    lines, index, match["expression"] + "\n"
                )
                prior = result.get(match["name"], {})
                result[match["name"]] = {
                    "expression": expression,
                    "kind": match["kind"],
                    "type": match["type"],
                    "count": prior.get("count", 0) + 1,
                }
        index += 1
    return result


def same_marker_value(old, new):
    if old["expression"] is None or (
        old["expression"] != new["expression"] or old["kind"] != new["kind"]
    ):
        return False
    if old["type"] == new["type"]:
        return True
    # Do not evaluate expressions or equate different explicit wire widths.
    # A standalone untyped decimal const may acquire a fixed unsigned type
    # only if the identical literal is representable. Call-site semantics
    # still require source review, exact CI and isolated runtime evidence.
    bits = re.fullmatch(r"uint(8|16|32|64)", new["type"] or "")
    if (
        old["kind"] == "const"
        and old["type"] is None
        and bits
        and re.fullmatch(r"0|[1-9][0-9]{0,19}", old["expression"])
    ):
        return int(old["expression"]) < 1 << int(bits[1])
    return False


def retained_marker(name, old, new):
    if name in new:
        return name if same_marker_value(old[name], new[name]) else None
    # Only the leading ASCII export letter may change, within the same file.
    # Neither a second spelling nor duplicate declarations may justify a move.
    key = name[0].lower() + name[1:]
    before = [n for n in old if n[0].lower() + n[1:] == key]
    after = [n for n in new if n[0].lower() + n[1:] == key]
    if len(before) == len(after) == 1:
        target = after[0]
        if (
            old[name]["kind"] == new[target]["kind"] == "const"
            and old[name]["count"] == new[target]["count"] == 1
            and same_marker_value(old[name], new[target])
        ):
            return target
    return None


def diagnostic_evidence(path, data):
    parts = pathlib.PurePosixPath(path)
    return (
        path.startswith("docs/evidence/")
        and str(parts) == path
        and ".." not in parts.parts
        and "\\" not in path
        and path.endswith((".pprof", ".cpu", ".block", ".mutex", ".log.gz", ".tar.gz"))
        and (not data or data.startswith(b"\x1f\x8b\x08"))
    )


def feature_defaults(text):
    result = {}
    patterns = (
        r'\b[A-Za-z_]\w*\.(?:Bool|String)\(\s*"([^"]+)"\s*,\s*(true|false|"[^"]*")',
        r'\b[A-Za-z_]\w*\.(?:BoolVar|StringVar)\(\s*[^,]+,\s*"([^"]+)"\s*,\s*(true|false|"[^"]*")',
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            name = re.sub(r"[^a-z]", "", match[1].lower())
            if any(
                k in name
                for k in (
                    "encryption",
                    "committsfence",
                    "timestampbinding",
                    "phased",
                    "tso",
                )
            ):
                result[match[1]] = match[2]
    return result


def analyze(
    repository, previous, candidate, image, changes, inventories, contents, limits
):
    old_tree, new_tree = inventories
    old_files, new_files = contents
    findings, files = [], []

    def finding(severity, code, path, detail):
        findings.append(
            {"severity": severity, "code": code, "path": path, "detail": detail}
        )

    for change in sorted(changes, key=lambda c: c["filename"]):
        path = change["filename"]
        prior = change.get("previous_filename", path)
        old, new = old_files.get(prior, b""), new_files.get(path, b"")
        old_id, new_id = (
            old_tree.get(prior, {}).get("sha"),
            new_tree.get(path, {}).get("sha"),
        )
        record = {
            "path": path,
            "previous_path": prior,
            "status": change["status"],
            "old_blob": old_id,
            "new_blob": new_id,
            "old_mode": old_tree.get(prior, {}).get("mode"),
            "new_mode": new_tree.get(path, {}).get("mode"),
        }
        files.append(record)
        if change["status"] not in ("added", "modified", "removed", "renamed"):
            finding(
                "blocker", "unsupported-change", path, "Unsupported source change kind"
            )
        if any(
            r and r["mode"] not in ("100644", "100755")
            for r in (old_tree.get(prior), new_tree.get(path))
        ):
            finding(
                "blocker",
                "unsupported-source-mode",
                path,
                "Symlink/submodule changes need a separate profile",
            )
            continue
        if (
            (path.startswith(PERSISTENT) or prior.startswith(PERSISTENT))
            and change["status"] in ("removed", "renamed")
            and not prior.endswith("_test.go")
        ):
            finding(
                "blocker",
                "persistent-contract-removal",
                path,
                "A persistence/wire implementation or reader was removed or moved",
            )
        if change.get("unresolved_persistent_move"):
            finding(
                "blocker",
                "persistent-move-unresolved",
                path,
                "Unreported implementation deletion/addition may move code into persistence",
            )
        if (
            all(
                r is None or r["mode"] == "100644"
                for r in (old_tree.get(prior), new_tree.get(path))
            )
            and diagnostic_evidence(prior, old)
            and diagnostic_evidence(path, new)
        ):
            record["classification"] = "diagnostic-evidence"
            finding(
                "attention",
                "diagnostic-evidence",
                path,
                "Opaque gzip diagnostic artifact; contents are not runtime-source analysis",
            )
            continue
        try:
            before, after = old.decode("utf-8"), new.decode("utf-8")
        except UnicodeDecodeError:
            if not path.lower().endswith(
                (".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2")
            ):
                finding(
                    "blocker",
                    "unparsed-binary",
                    path,
                    "Non-asset binary source cannot be audited",
                )
            continue
        old_lines, new_lines = code_lines(before), code_lines(after)
        if len(old_lines) + len(new_lines) > 100000:
            raise ValueError("Changed source exceeds the line-analysis budget")
        removed = [
            line
            for op, a, b, c, d in difflib.SequenceMatcher(
                None, old_lines, new_lines, autojunk=True
            ).get_opcodes()
            if op in ("delete", "replace")
            for line in old_lines[a:b]
        ]
        record["review_excerpt"] = [
            line[:160]
            for line in itertools.islice(
                difflib.unified_diff(
                    old_lines, new_lines, fromfile=prior, tofile=path, n=2
                ),
                12,
            )
        ]
        if path.endswith(".proto"):
            if removed:
                finding(
                    "blocker",
                    "wire-contract-edit",
                    path,
                    "Existing protobuf declarations changed; additive compatibility is not established",
                )
            if any(
                "oneof " in line or "required " in line
                for line in set(new_lines) - set(old_lines)
            ):
                finding(
                    "blocker",
                    "wire-semantics-change",
                    path,
                    "New oneof/required semantics need a separate migration profile",
                )
        if path.startswith(PERSISTENT) or prior.startswith(PERSISTENT):
            new_versions = version_literals(after)
            old_versions = version_literals(before)
            if any(
                value["expression"] is None
                for value in [*old_versions.values(), *new_versions.values()]
            ):
                finding(
                    "blocker",
                    "persistent-marker-incomplete",
                    path,
                    "A complete persisted marker initializer could not be established",
                )
            for name in old_versions:
                target = retained_marker(name, old_versions, new_versions)
                if target is None:
                    finding(
                        "blocker",
                        "persistent-version-change",
                        path,
                        "Persisted format/version marker changed: " + name,
                    )
                elif target != name:
                    finding(
                        "attention",
                        "persistent-marker-rename",
                        path,
                        "Review export-only constant marker rename with unchanged initializer: "
                        + name
                        + " -> "
                        + target,
                    )
            if not path.endswith("_test.go"):
                finding(
                    "attention",
                    "persistence-logic",
                    path,
                    "Review the shown persistence/replication changes and round-trip evidence",
                )
        previous_defaults = feature_defaults(before)
        for name, value in feature_defaults(after).items():
            if (
                value not in ("false", '"legacy"', '"shadow"', '""')
                and previous_defaults.get(name) != value
            ):
                finding(
                    "blocker",
                    "feature-default-activation",
                    path,
                    "An unsupported protocol/encryption feature default changed: "
                    + name,
                )
        if path in ("go.mod", "go.sum"):
            finding(
                "attention",
                "dependency-change",
                path,
                "Dependency changes require attention to CI and persisted-data compatibility",
            )
        if path.startswith(".github/workflows/"):
            finding(
                "attention",
                "source-ci-change",
                path,
                "Source CI definitions changed; inspect coverage before approving",
            )
    report = {
        "version": VERSION,
        "method": "bounded-source-contract-audit-v1",
        "reviewer_sha256": fingerprint(),
        "repository": repository,
        "from_source": previous,
        "to_source": candidate,
        "image": image,
        "complete": True,
        "decision": "blocked"
        if any(f["severity"] == "blocker" for f in findings)
        else "approval-required",
        "limits": limits,
        "files": files,
        "findings": findings,
        "limitations": [
            "Static assessment is not a proof of arbitrary program semantics",
            "Exact-head human PR approval and isolated preservation tests are mandatory",
        ],
    }

    if len(canonical(report)) > 900 * 1024:
        # Excerpts are display hints, never audit inputs. Keep every immutable
        # file identity and finding even when a large report needs less display text.
        for record in files:
            record.pop("review_excerpt", None)
        report["review_excerpts_omitted_for_budget"] = True
    if len(canonical(report)) > 900 * 1024:
        raise ValueError("Source review report exceeds its publication budget")
    return report


def active_profile(config, previous):
    """A trusted, separately reviewed bootstrap profile expires with its baseline."""
    profile = config.get("relocation_profile")
    if profile is None:
        return None
    validate_profile_identity(profile)
    return profile if profile["from_source"] == previous else None


def candidate_source(config, previous, head):
    profile = active_profile(config, previous)
    return profile["to_source"] if profile else head


def validate_profile_identity(profile):
    if (
        not isinstance(profile, dict)
        or set(profile)
        != {"version", "repository", "from_source", "to_source", "image", "relocations"}
        or type(profile["version"]) is not int
        or profile["version"] != 1
        or not all(
            isinstance(profile[k], str)
            for k in ("repository", "from_source", "to_source", "image")
        )
        or not re.fullmatch(r"[\w.-]+/[\w.-]+", profile["repository"])
        or not all(
            re.fullmatch("[0-9a-f]{40}", profile[k])
            for k in ("from_source", "to_source")
        )
        or profile["from_source"] == profile["to_source"]
        or not re.fullmatch(r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}", profile["image"])
        or not isinstance(profile["relocations"], list)
        or not 0 < len(profile["relocations"]) <= 1000
        or len(canonical(profile)) > 256 * 1024
    ):
        raise ValueError("Invalid exact-pair relocation profile")


def verified_relocations(profile, identity, inventories):
    if profile is None:
        return set(), set()
    validate_profile_identity(profile)
    if (
        tuple(profile[k] for k in ("repository", "from_source", "to_source", "image"))
        != identity
    ):
        raise ValueError("Relocation profile is not for this exact source/image pair")
    old_tree, new_tree = inventories
    resolved, targets = set(), set()
    edges = 0
    for row in profile["relocations"]:
        if not isinstance(row, dict) or set(row) != {
            "old_path",
            "old_blob",
            "targets",
            "review",
        }:
            raise ValueError("Invalid relocation entry")
        prior = row["old_path"]
        if (
            not isinstance(prior, str)
            or prior in resolved
            or prior.startswith(PERSISTENT)
            or not prior.endswith((".go", ".proto"))
            or prior.endswith("_test.go")
            or prior not in old_tree
            or prior in new_tree
            or old_tree[prior]["sha"] != row["old_blob"]
            or old_tree[prior]["mode"] not in ("100644", "100755")
            or not isinstance(row["review"], str)
            or not 0 < len(row["review"].strip()) <= 2000
            or not isinstance(row["targets"], list)
            or not row["targets"]
        ):
            raise ValueError("Unbound or unsupported nonpersistent relocation")
        seen = set()
        for target in row["targets"]:
            edges += 1
            if (
                not isinstance(target, dict)
                or set(target) != {"path", "blob"}
                or edges > 1000
            ):
                raise ValueError("Invalid or unbounded relocation targets")
            path = target["path"]
            if (
                not isinstance(path, str)
                or path in seen
                or path not in new_tree
                or not path.endswith((".go", ".proto"))
                or path.endswith("_test.go")
                or new_tree[path]["mode"] not in ("100644", "100755")
                or new_tree[path]["sha"] != target["blob"]
            ):
                raise ValueError(
                    "Relocation target does not match immutable implementation"
                )
            seen.add(path)
            targets.add(path)
        resolved.add(prior)
    return resolved, targets


def collect(
    repository,
    previous,
    candidate,
    image,
    comparison,
    fetch_json,
    *,
    limits=None,
    relocation_profile=None,
    archive=download_archive,
    tick=lambda: None,
):
    overrides = limits or {}
    if set(overrides) - set(DEFAULT_LIMITS) or any(
        type(v) is not int or v <= 0 or v > LIMIT_CEILINGS[k]
        for k, v in overrides.items()
    ):
        raise ValueError("Invalid source review limits")
    limits = {**DEFAULT_LIMITS, **overrides}
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository) or not all(
        re.fullmatch("[0-9a-f]{40}", s) for s in (previous, candidate)
    ):
        raise ValueError("Invalid immutable source identity")
    changes = comparison.get("files", [])
    if comparison.get("status") != "ahead" or not changes or len(changes) > 300:
        raise ValueError("Incomplete or unbounded source comparison")
    inventories = []
    for sha in (previous, candidate):
        tick()
        inventories.append(tree_files(fetch_json("git/trees/" + sha + "?recursive=1")))
    old_tree, new_tree = inventories
    resolved, profile_targets = verified_relocations(
        relocation_profile, (repository, previous, candidate, image), inventories
    )
    actual = {
        p
        for p in set(old_tree) | set(new_tree)
        if (old_tree.get(p, {}).get("sha"), old_tree.get(p, {}).get("mode"))
        != (new_tree.get(p, {}).get("sha"), new_tree.get(p, {}).get("mode"))
    }
    described = {c["filename"] for c in changes} | {
        c["previous_filename"] for c in changes if c.get("previous_filename")
    }
    if len({c["filename"] for c in changes}) != len(changes):
        raise ValueError("Duplicate source comparison entries")
    if len(actual) > limits["max_files"]:
        raise ValueError("Complete source inventory exceeds the file budget")
    if len(changes) == 300:
        if not described <= actual:
            raise ValueError("Comparison names files outside the changed inventory")
        # GitHub returns at most 300 files, even with comparison pagination.
        # Keep API rename relationships after validating them against both trees:
        # a move into persistence must retain the same blocker as a smaller diff.
        renames, covered = [], set()
        for change in changes:
            if change["status"] != "renamed":
                continue
            prior, path = change.get("previous_filename"), change["filename"]
            if (
                prior == path
                or prior not in old_tree
                or prior in new_tree
                or path not in new_tree
                or path in old_tree
                or covered.intersection((prior, path))
            ):
                raise ValueError("Rename hint does not match disjoint tree paths")
            renames.append(
                {"filename": path, "previous_filename": prior, "status": "renamed"}
            )
            covered.update((prior, path))
        # The API may omit a move entirely. A Go/protobuf implementation deletion
        # plus a new persistence path cannot safely be treated as unrelated edits.
        remaining = actual - covered
        unresolved_removal = any(
            path not in new_tree
            and path not in resolved
            and path.endswith((".go", ".proto"))
            and not path.endswith("_test.go")
            for path in remaining
        )
        # Paths beyond the API cap still come from the complete immutable trees.
        changes = renames + [
            {
                "filename": path,
                "status": "added"
                if path not in old_tree
                else "removed"
                if path not in new_tree
                else "modified",
            }
            for path in sorted(remaining)
        ]
        if unresolved_removal:
            for change in changes:
                if (
                    change["status"] == "added"
                    and change["filename"].startswith(PERSISTENT)
                    and not change["filename"].endswith("_test.go")
                ):
                    change["unresolved_persistent_move"] = True
        described = actual
    elif actual != described:
        raise ValueError("Comparison omitted source changes")
    described |= profile_targets
    if len(described) > limits["max_files"]:
        raise ValueError("Relocation evidence exceeds the complete source file budget")
    contents = []
    total_source_bytes = 0
    for sha, tree in zip((previous, candidate), inventories):
        needed = {
            p: tree[p]
            for p in described
            if p in tree
            and tree[p]["type"] == "blob"
            and tree[p]["mode"] in ("100644", "100755")
        }
        if any(
            row.get("size", limits["max_file_bytes"] + 1) > limits["max_file_bytes"]
            for row in needed.values()
        ):
            raise ValueError("Changed source file exceeds review budget")
        total_source_bytes += sum(row["size"] for row in needed.values())
        if total_source_bytes > limits["max_total_file_bytes"]:
            raise ValueError("Changed source exceeds the aggregate memory budget")
        tick()
        data = archive(repository, sha, needed, limits, tick)
        if set(data) != set(needed) or any(
            git_blob(data[p]) != row["sha"] for p, row in needed.items()
        ):
            raise ValueError("Changed source archive does not match immutable blobs")
        contents.append(data)
    report = analyze(
        repository, previous, candidate, image, changes, inventories, contents, limits
    )
    if relocation_profile is not None:
        report["relocation_profile_sha256"] = digest(relocation_profile)
        report["resolved_nonpersistent_removals"] = sorted(resolved)
        report["findings"].extend(
            {
                "severity": "attention",
                "code": "reviewed-relocation",
                "path": path,
                "detail": "Exact blob-bound assembly disposition from the trusted bootstrap profile",
            }
            for path in sorted(resolved)
        )
    if len(canonical(report)) > 900 * 1024:
        raise ValueError("Relocation report exceeds its publication budget")
    return report


def eligible(report, previous, candidate, image):
    return (
        report.get("complete") is True
        and report.get("decision") == "approval-required"
        and report.get("from_source") == previous
        and report.get("to_source") == candidate
        and report.get("image") == image
        and report.get("reviewer_sha256") == fingerprint()
        and not any(f["severity"] == "blocker" for f in report.get("findings", []))
    )
