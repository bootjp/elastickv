#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LEIN_BIN="${LEIN:-$(command -v lein || true)}"
TIME_LIMIT="${TIME_LIMIT:-600}"
RATE="${RATE:-50}"
CONCURRENCY="${CONCURRENCY:-30}"
FAULT_INTERVAL="${FAULT_INTERVAL:-15}"
SNAPSHOT_COUNT="${SNAPSHOT_COUNT:-64}"
STORE_ROOT="${STORE_ROOT:-$REPO_ROOT/jepsen/store}"
GRPC_HOST_PORT="${GRPC_HOST_PORT:-n1:50051}"
MARKER="$(mktemp "${TMPDIR:-/tmp}/elastickv-streaming-soak.XXXXXX")"
ROUTE_KEY_BIN="$(mktemp "${TMPDIR:-/tmp}/elastickv-route-key.XXXXXX")"
LIST_ROUTES_BIN="$(mktemp "${TMPDIR:-/tmp}/elastickv-list-routes.XXXXXX")"
trap 'rm -f "$MARKER" "$ROUTE_KEY_BIN" "$LIST_ROUTES_BIN"' EXIT

if [ -z "$LEIN_BIN" ] || [ ! -x "$LEIN_BIN" ]; then
  echo "lein not found; set LEIN to an executable" >&2
  exit 127
fi

cd "$REPO_ROOT"
go build -o "$ROUTE_KEY_BIN" ./cmd/elastickv-route-key
go build -o "$LIST_ROUTES_BIN" ./cmd/elastickv-list-routes
T2_KEY="$("$ROUTE_KEY_BIN" jepsen_append_t2)"
T3_KEY="$("$ROUTE_KEY_BIN" jepsen_append_t3)"
SHARD_RANGES=":${T2_KEY}=1,${T2_KEY}:${T3_KEY}=2,${T3_KEY}:=3"

cd "$REPO_ROOT/jepsen"
mkdir -p tmp-home .lein
HOME="$(pwd)/tmp-home" LEIN_HOME="$(pwd)/.lein" \
  LEIN_JVM_OPTS="-Duser.home=$(pwd)/tmp-home" \
  "$LEIN_BIN" run -m elastickv.dynamodb-multi-table-workload \
    --nodes n1,n2,n3,n4,n5 \
    --raft-groups 1=50051,2=50052,3=50053 \
    --shard-ranges "$SHARD_RANGES" \
    --list-routes-bin "$LIST_ROUTES_BIN" \
    --grpc-host-port "$GRPC_HOST_PORT" \
    --faults partition,kill,pause \
    --fault-interval "$FAULT_INTERVAL" \
    --time-limit "$TIME_LIMIT" \
    --rate "$RATE" \
    --concurrency "$CONCURRENCY" \
    --raft-snapshot-count "$SNAPSHOT_COUNT" \
    --raft-dispatcher-lanes

metrics_files=()
while IFS= read -r -d '' file; do
  metrics_files+=("$file")
done < <(find "$STORE_ROOT" -type f -name elastickv-transport-metrics.prom -newer "$MARKER" -print0)
if [ "${#metrics_files[@]}" -lt 3 ]; then
  echo "Jepsen completed but fewer than three metrics evidence files were collected under $STORE_ROOT" >&2
  exit 1
fi

verification="$STORE_ROOT/raft-streaming-multigroup-verification.txt"
EXPECTED_GROUPS=1,2,3 MIN_NODES=3 \
  "$REPO_ROOT/scripts/verify-raft-streaming-multigroup-metrics.sh" "${metrics_files[@]}" \
  | tee "$verification"
echo "verification evidence: $verification"
