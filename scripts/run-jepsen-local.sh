#!/usr/bin/env bash
# Run the DynamoDB Jepsen workload locally against a 3-node elastickv cluster.
# Usage:
#   ./scripts/run-jepsen-local.sh               # build + start cluster + test
#   ./scripts/run-jepsen-local.sh --no-rebuild   # skip go build (reuse binary)
#   ./scripts/run-jepsen-local.sh --no-cluster   # skip cluster start (reuse running one)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY=/tmp/elastickv4-binary
DATA_DIR=/tmp/elastickv-test-run
PID_FILE=/tmp/elastickv-test-run.pid

BOOTSTRAP_MEMBERS="n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053"
RAFT_REDIS_MAP="127.0.0.1:50051=127.0.0.1:63791,127.0.0.1:50052=127.0.0.1:63792,127.0.0.1:50053=127.0.0.1:63793"
RAFT_S3_MAP="127.0.0.1:50051=127.0.0.1:63901,127.0.0.1:50052=127.0.0.1:63902,127.0.0.1:50053=127.0.0.1:63903"
RAFT_DYNAMO_MAP="127.0.0.1:50051=127.0.0.1:63801,127.0.0.1:50052=127.0.0.1:63802,127.0.0.1:50053=127.0.0.1:63803"

NO_REBUILD=false
NO_CLUSTER=false
for arg in "$@"; do
  case "$arg" in
    --no-rebuild) NO_REBUILD=true ;;
    --no-cluster) NO_CLUSTER=true ;;
  esac
done

# ---- stop any previously managed cluster ----
stop_cluster() {
  if [ -f "$PID_FILE" ]; then
    echo "[cluster] stopping previous cluster..."
    while IFS= read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
  fi
}

# ---- build ----
if ! $NO_REBUILD; then
  echo "[build] compiling elastickv..."
  cd "$REPO_ROOT"
  go build -o "$BINARY" .
  echo "[build] done -> $BINARY"
fi

# ---- start cluster ----
if ! $NO_CLUSTER; then
  stop_cluster
  rm -rf "$DATA_DIR"
  mkdir -p "$DATA_DIR"
  : > "$PID_FILE"

  echo "[cluster] starting 3-node cluster..."
  for node in 1 2 3; do
    nohup "$BINARY" \
      --address      "127.0.0.1:5005${node}" \
      --redisAddress "127.0.0.1:6379${node}" \
      --dynamoAddress "127.0.0.1:6380${node}" \
      --s3Address    "127.0.0.1:6390${node}" \
      --metricsAddress "" \
      --pprofAddress "" \
      --raftId       "n${node}" \
      --raftDataDir  "${DATA_DIR}/n${node}" \
      --raftBootstrapMembers "$BOOTSTRAP_MEMBERS" \
      --raftRedisMap "$RAFT_REDIS_MAP" \
      --raftS3Map    "$RAFT_S3_MAP" \
      --raftDynamoMap "$RAFT_DYNAMO_MAP" \
      > "${DATA_DIR}/n${node}.log" 2>&1 &
    echo $! >> "$PID_FILE"
  done

  echo "[cluster] waiting for all ports (redis 63791-3, dynamo 63801-3, s3 63901-3)..."
  for i in $(seq 1 90); do
    if nc -z 127.0.0.1 63791 && nc -z 127.0.0.1 63792 && nc -z 127.0.0.1 63793 \
      && nc -z 127.0.0.1 63801 && nc -z 127.0.0.1 63802 && nc -z 127.0.0.1 63803 \
      && nc -z 127.0.0.1 63901 && nc -z 127.0.0.1 63902 && nc -z 127.0.0.1 63903; then
      echo "[cluster] up after ${i}s"
      break
    fi
    sleep 1
    if [ "$i" -eq 90 ]; then
      echo "[cluster] FAILED to start - dumping logs:"
      for n in 1 2 3; do tail -n 50 "${DATA_DIR}/n${n}.log" || true; done
      exit 1
    fi
  done
fi

# ---- run Jepsen DynamoDB workload ----
echo "[jepsen] running DynamoDB workload..."
cd "$REPO_ROOT/jepsen"
set +e
lein run -m elastickv.dynamodb-workload \
  --local \
  --time-limit 30 \
  --rate 5 \
  --concurrency 5 \
  --dynamo-ports 63801,63802,63803 \
  --host 127.0.0.1
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
  echo "[jepsen] PASSED"
else
  echo "[jepsen] FAILED (exit $EXIT_CODE)"
fi
exit $EXIT_CODE
