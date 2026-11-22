#!/bin/bash
set -u
cd /home/bootjp/load-ramp-20261121
sha256sum loadtest baseline-v2.test candidate-v2.test > comparison/binary-sha256.txt
for entry in A1:baseline-v2 B1:candidate-v2 B2:candidate-v2 A2:baseline-v2; do
 name=${entry%:*}
 binary=${entry#*:}
 mkdir -p "comparison/$name"
 { date -u; uname -srmo; free -h; df -T .; } > "comparison/$name/environment.txt"
 status=0
 env -u ELASTICKV_FSM_SYNC_MODE -u ELASTICKV_LOAD_PROFILE_DIR -u ELASTICKV_LOAD_PROFILE_ONLY GOMAXPROCS=8 ELASTICKV_LOAD_RAMP=1 ELASTICKV_LOAD_BINARY=$PWD/loadtest ELASTICKV_LOAD_SPLIT=cross ELASTICKV_LOAD_RATES=500 ELASTICKV_LOAD_STAGE_DURATION=300s ELASTICKV_LOAD_CONCURRENCY=32 ELASTICKV_PERF_PREFLIGHT_TIMING=1 "./$binary.test" -test.run=^TestLinuxSplitLoadRamp$ -test.v -test.count=1 -test.timeout=15m > "comparison/$name/run.log" 2>&1 || status=$?
 printf "%s\n" "$status" > "comparison/$name/exit-code.txt"
 if [ "$status" != 0 ]; then printf "%s failed\n" "$name" > comparison/failed; exit "$status"; fi
 sleep 5
done
printf "completed\n" > comparison/completed
