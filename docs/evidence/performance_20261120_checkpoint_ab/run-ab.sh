#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export GOMAXPROCS=8 ELASTICKV_PERF_SPLIT=1 ELASTICKV_PERF_PREFLIGHT_TIMING=1
unset ELASTICKV_FSM_SYNC_MODE ELASTICKV_PERF_PREFLIGHT_ONLY
{ date -u; uname -srmo; nproc; sha256sum one.test two.test; } > environment.log
for pair in 1 2 3; do
  for variant in one two; do
    run="pair-$pair-$variant"
    echo "START $run $(date -u +%FT%TZ)"
    rc=0
    "./$variant.test" -test.run='^TestPerformanceReadWriteSplit$' -test.v -test.count=1 -test.timeout=15m > "$run.log" 2>&1 || rc=$?
    echo "$rc" > "$run.exit"
    echo "END $run exit=$rc $(date -u +%FT%TZ)"
    if (( rc != 0 )); then exit "$rc"; fi
    grep 'PERF preflight-duration' "$run.log"
    grep 'PERF phase=migration-preflight' "$run.log"
  done
done
