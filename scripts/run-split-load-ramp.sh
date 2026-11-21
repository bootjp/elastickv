#!/usr/bin/env bash
set -euo pipefail
if [[ $(uname -s) != Linux ]]; then
  echo 'Run this script on Linux, or cross-build both binaries and invoke the opt-in test there.' >&2
  exit 2
fi
repo_dir=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$repo_dir"
output_dir=${1:-"${TMPDIR:-/tmp}/elastickv-split-load-$(date -u +%Y%m%dT%H%M%SZ)"}
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd -P)
case "$output_dir/" in
  "$repo_dir/"*) echo 'Results must be outside the repository to keep source snapshots bounded.' >&2; exit 2 ;;
esac
{
  date -u +%FT%TZ
  uname -srmo
  lscpu
  free -h
  df -T "$output_dir"
  git rev-parse HEAD
  git status --short
  go version
} > "$output_dir/environment.txt"
git diff --binary HEAD > "$output_dir/tracked-changes.patch"
git ls-files --others --exclude-standard -z | tar --null -T - -cf "$output_dir/untracked-source.tar"
sha256sum "$output_dir/tracked-changes.patch" "$output_dir/untracked-source.tar" > "$output_dir/source-sha256.txt"
env | LC_ALL=C sort | awk '/^(ELASTICKV_LOAD_|ELASTICKV_PERF_|GOMAXPROCS=)/' > "$output_dir/load-environment.txt"
go build -p 2 -o "$output_dir/loadtest" ./cmd/loadtest
go test -c -p 2 -o "$output_dir/server.test" ./internal/server
sha256sum "$output_dir/loadtest" "$output_dir/server.test" > "$output_dir/binary-sha256.txt"
status=0
env -u ELASTICKV_FSM_SYNC_MODE \
  GOMAXPROCS="${GOMAXPROCS:-8}" \
  ELASTICKV_LOAD_RAMP=1 ELASTICKV_LOAD_BINARY="$output_dir/loadtest" \
  ELASTICKV_PERF_PREFLIGHT_TIMING=1 \
  "$output_dir/server.test" -test.run='^TestLinuxSplitLoadRamp$' -test.count=1 -test.v -test.timeout=15m \
  > "$output_dir/run.log" 2>&1 || status=$?
printf '%s\n' "$status" > "$output_dir/exit-code.txt"
awk '/LOAD |PERF preflight|PASS|FAIL/ {print}' "$output_dir/run.log"
exit "$status"
