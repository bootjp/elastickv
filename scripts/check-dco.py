#!/usr/bin/env python3
"""Check DCO sign-offs on every commit in BASE..HEAD, including merges."""

import argparse
import re
import subprocess
import sys


def git(*args, input=None):
    return subprocess.run(
        ["git", *args], input=input, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=True,
    ).stdout


def resolve_commit(revision):
    return git(
        "rev-parse", "--verify", "--end-of-options", revision + "^{commit}"
    ).strip().decode("ascii")


def has_author_signoff(commit):
    name, email, message = git(
        "show", "--no-show-signature", "-s", "--format=%an%x00%ae%x00%B", commit
    ).split(b"\0", 2)
    if not name.strip() or not re.fullmatch(rb"[^\s<>@]+@[^\s<>@]+", email):
        return False
    identity = name + b" <" + email + b">"
    trailers = git("interpret-trailers", "--parse", input=message)
    for trailer in trailers.splitlines():
        key, separator, value = trailer.partition(b":")
        if separator and key.lower() == b"signed-off-by" and value.strip() == identity:
            return True
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", help="PR base commit or ref (excluded)")
    parser.add_argument("head", help="PR head commit or ref (included)")
    args = parser.parse_args()
    try:
        if git("rev-parse", "--is-shallow-repository").strip() != b"false":
            raise ValueError("shallow repository; fetch the complete history first")
        base, head = resolve_commit(args.base), resolve_commit(args.head)
        commits = git("rev-list", "--reverse", base + ".." + head).decode("ascii").splitlines()
        failures = [commit for commit in commits if not has_author_signoff(commit)]
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"DCO check could not inspect complete history: {error}", file=sys.stderr)
        return 1

    for commit in failures:
        print(
            f"{commit}: missing Signed-off-by trailer matching the commit author's name and email.",
            file=sys.stderr,
        )
    print(f"Checked {len(commits)} commit(s); {len(failures)} missing author sign-off(s).")
    if failures:
        print("See CONTRIBUTING.md for the DCO policy and repair instructions.", file=sys.stderr)
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(main())
