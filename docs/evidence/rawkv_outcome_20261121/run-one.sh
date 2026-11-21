#!/bin/bash
set -u
cd /home/bootjp/outcome-20261121
trial=$1
binary=$2
if [ "$binary" = release.test ]; then
 while [ ! -f comparison/release-ready ]; do sleep 2; done
fi
mkdir -p "comparison/$trial"
{ date -u; uname -srmo; free -h; df -T .; } > "comparison/$trial/environment.txt"
sha256sum "$binary" ../load-ramp-20261121/loadtest > "comparison/$trial/binary-sha256.txt"
status=0
env -u ELASTICKV_FSM_SYNC_MODE -u ELASTICKV_LOAD_PROFILE_DIR -u ELASTICKV_LOAD_PROFILE_ONLY GOMAXPROCS=8 ELASTICKV_LOAD_RAMP=1 ELASTICKV_LOAD_BINARY=/home/bootjp/load-ramp-20261121/loadtest ELASTICKV_LOAD_RATES=500 ELASTICKV_LOAD_STAGE_DURATION=300s ELASTICKV_LOAD_CONCURRENCY=32 ELASTICKV_LOAD_SPLIT=cross ELASTICKV_PERF_PREFLIGHT_TIMING=1 "./$binary" -test.run=^TestLinuxSplitLoadRamp$ -test.v -test.count=1 -test.timeout=15m > "comparison/$trial/run.log" 2>&1 || status=$?
printf "%s\n" "$status" > "comparison/$trial/exit-code.txt"
exit "$status"
