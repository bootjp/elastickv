#!/usr/bin/env bash
set -u
mkdir -p /home/bootjp/elastickv-perf-results
cd /home/bootjp/elastickv/internal/server
sha256sum /home/bootjp/server-linux.test /home/bootjp/store-linux.test performance_split_test.go > /home/bootjp/elastickv-perf-results/sha256.txt
uname -a > /home/bootjp/elastickv-perf-results/environment.txt
lscpu >> /home/bootjp/elastickv-perf-results/environment.txt
free -h >> /home/bootjp/elastickv-perf-results/environment.txt
df -h / >> /home/bootjp/elastickv-perf-results/environment.txt
timedatectl show -p NTPSynchronized >> /home/bootjp/elastickv-perf-results/environment.txt
/home/bootjp/store-linux.test -test.run '^$' -test.bench '^BenchmarkApplyMutationsRaft_SyncMode$' -test.benchtime=1s -test.count=3 -test.benchmem > /home/bootjp/elastickv-perf-results/store_sync_linux.log 2>&1
store_exit=$?
printf '%s\n' "$store_exit" > /home/bootjp/elastickv-perf-results/store.exit
ELASTICKV_PERF_SPLIT=1 /home/bootjp/server-linux.test -test.run '^TestPerformanceReadWriteSplit$' -test.v -test.timeout=7m -test.cpuprofile=/home/bootjp/elastickv-perf-results/api_cpu_linux.pprof > /home/bootjp/elastickv-perf-results/api_split_linux.log 2>&1
api_exit=$?
printf '%s\n' "$api_exit" > /home/bootjp/elastickv-perf-results/api.exit
env -u ELASTICKV_PERF_SPLIT /home/bootjp/server-linux.test -test.run '^TestAutoSplitCrossGroupThreeNodeTwoGroupAcceptance$' -test.v -test.timeout=6m > /home/bootjp/elastickv-perf-results/auto_cross_group_linux.log 2>&1
auto_exit=$?
printf '%s\n' "$auto_exit" > /home/bootjp/elastickv-perf-results/auto.exit
printf '{"store":%s,"api":%s,"auto":%s}\n' "$store_exit" "$api_exit" "$auto_exit" > /home/bootjp/elastickv-perf-results/exits.json
if (( store_exit != 0 || api_exit != 0 || auto_exit != 0 )); then
  exit 1
fi
