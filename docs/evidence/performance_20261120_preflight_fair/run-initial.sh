#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
{ date -u; hostname; uname -srmo; lscpu; free -h; df -T .; sha256sum baseline.test candidate.test; } > final-environment.txt
run_one() {
 local mode="$1" pair="$2" variant="$3" code=0
 local run="${mode}-${pair}-${variant}"
 echo "START $run $(date -u +%FT%TZ)"; uptime
 if [[ "$mode" == prefix ]]; then
  env -u ELASTICKV_FSM_SYNC_MODE GOMAXPROCS=8 ELASTICKV_PERF_SPLIT=1 ELASTICKV_PERF_PREFLIGHT_TIMING=1 ELASTICKV_PERF_PREFLIGHT_ONLY=1 "./$variant.test" -test.run='^TestPerformanceReadWriteSplit$' -test.v -test.count=1 -test.timeout=15m > "$run.log" 2>&1 || code=$?
 else
  env -u ELASTICKV_FSM_SYNC_MODE -u ELASTICKV_PERF_PREFLIGHT_ONLY GOMAXPROCS=8 ELASTICKV_PERF_SPLIT=1 ELASTICKV_PERF_PREFLIGHT_TIMING=1 "./$variant.test" -test.run='^TestPerformanceReadWriteSplit$' -test.v -test.count=1 -test.timeout=15m > "$run.log" 2>&1 || code=$?
 fi
 echo "$code" > "$run.exit"
 echo "END $run exit=$code $(date -u +%FT%TZ)"
 grep -E 'PERF preflight-duration|PERF preflight-work|PERF cross-group|PERF phase=migration-preflight|PERF recovery.*planned=|--- (PASS|FAIL)' "$run.log" || true
 return "$code"
}
for pair in 1 2 3; do
 variants=(baseline candidate)
 if (( pair % 2 == 0 )); then variants=(candidate baseline); fi
 for variant in "${variants[@]}"; do run_one prefix "$pair" "$variant"; done
done
for variant in baseline candidate; do run_one full 1 "$variant"; done
