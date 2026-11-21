#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "$0")" && pwd)"
jar="$1"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/elastickv-cas-tlc.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

run_tlc() {
    java -Xmx512m -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers 1 \
        -metadir "$scratch/$1-states" -config "$module_dir/$1.cfg" \
        "$module_dir/SingleKeyCAS.tla"
}

expect_counterexample() {
    local config="$1"
    local invariant="$2"
    local log="$scratch/$config.log"
    local status

    if run_tlc "$config" >"$log" 2>&1; then
        cat "$log"
        echo "ERROR: $config passed without violating $invariant" >&2
        return 1
    else
        status=$?
    fi
    cat "$log"
    # The pinned TLC exits with 12 for an invariant violation.
    if [ "$status" -ne 12 ] || ! grep -Fxq "Error: Invariant $invariant is violated." "$log"; then
        echo "ERROR: $config did not produce the expected $invariant counterexample" >&2
        return 1
    fi
}

run_tlc SingleKeyCAS
expect_counterexample SingleKeyCAS_watcher_gap EqualAppliedIndexAgrees
expect_counterexample SingleKeyCAS_cleanup_gap SingleConsumption
expect_counterexample SingleKeyCAS_retention_gap VisibleHeadAgrees
