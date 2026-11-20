#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
{ date -u; hostname; uname -srmo; lscpu; free -h; df -T .; sha256sum baseline.test candidate.test; } > final-environment.txt
for pair in 1 2 3; do
 variants=(baseline candidate)
 if (( pair % 2 == 0 )); then variants=(candidate baseline); fi
 for variant in "${variants[@]}"; do
  if [[ -f STOP ]]; then echo 'CONTROLLED STOP before next run'; exit 0; fi
  run="final-${pair}-${variant}"
  echo "START $run $(date -u +%FT%TZ)"; uptime
  code=0
  env -u ELASTICKV_FSM_SYNC_MODE -u ELASTICKV_PERF_PREFLIGHT_ONLY GOMAXPROCS=8 ELASTICKV_PERF_SPLIT=1 ELASTICKV_PERF_PREFLIGHT_TIMING=1 "./$variant.test" -test.run='^TestPerformanceReadWriteSplit$' -test.v -test.count=1 -test.timeout=15m > "$run.log" 2>&1 || code=$?
  echo "$code" > "$run.exit"
  echo "END $run exit=$code $(date -u +%FT%TZ)"
  grep -E 'PERF preflight-duration|PERF cross-group|PERF phase=migration-preflight|PERF recovery.*planned=|--- (PASS|FAIL)' "$run.log" || true
  if (( code != 0 )); then exit "$code"; fi
 done
done
