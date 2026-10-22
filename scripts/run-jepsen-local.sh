#!/usr/bin/env bash
# Run the DynamoDB and filesystem Jepsen workloads locally against a 3-node
# elastickv cluster. When FUSE is usable (/dev/fuse plus fusermount3 from the
# fuse3 package, or fusermount), every node also mounts the shared filesystem
# under $DATA_DIR/fs-n{1,2,3} and the filesystem workload runs; otherwise the
# mounts and the filesystem workload are skipped and only DynamoDB runs.
# Usage:
#   ./scripts/run-jepsen-local.sh               # build + start cluster + test
#   ./scripts/run-jepsen-local.sh --no-rebuild   # skip go build (reuse binary)
#   ./scripts/run-jepsen-local.sh --no-cluster   # skip cluster start (reuse running one)
#   ./scripts/run-jepsen-local.sh --encrypted    # same, against a data-at-rest
#                                                # encrypted cluster (file KEK,
#                                                # storage + raft envelopes on)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY=/tmp/elastickv4-binary
ADMIN_BINARY=/tmp/elastickv4-admin-binary
DATA_DIR=/tmp/elastickv-test-run
PID_FILE=/tmp/elastickv-test-run.pid
KEK_FILE="${DATA_DIR}/kek.bin"

BOOTSTRAP_MEMBERS="n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053"
RAFT_REDIS_MAP="127.0.0.1:50051=127.0.0.1:63791,127.0.0.1:50052=127.0.0.1:63792,127.0.0.1:50053=127.0.0.1:63793"
RAFT_S3_MAP="127.0.0.1:50051=127.0.0.1:63901,127.0.0.1:50052=127.0.0.1:63902,127.0.0.1:50053=127.0.0.1:63903"
RAFT_DYNAMO_MAP="127.0.0.1:50051=127.0.0.1:63801,127.0.0.1:50052=127.0.0.1:63802,127.0.0.1:50053=127.0.0.1:63803"

NO_REBUILD=false
NO_CLUSTER=false
ENCRYPTED=false
for arg in "$@"; do
  case "$arg" in
    --no-rebuild) NO_REBUILD=true ;;
    --no-cluster) NO_CLUSTER=true ;;
    --encrypted) ENCRYPTED=true ;;
  esac
done

# ---- FUSE availability ----
FUSE_ENABLED=false
if [ -e /dev/fuse ] && command -v mountpoint >/dev/null 2>&1 \
  && { command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1; }; then
  FUSE_ENABLED=true
else
  echo "[fuse] /dev/fuse or fusermount3 not available; skipping filesystem mounts and workload"
fi

unmount_fs() {
  local dir="$1"
  if command -v fusermount3 >/dev/null 2>&1; then
    fusermount3 -uz "$dir" 2>/dev/null || true
  fi
  if command -v fusermount >/dev/null 2>&1; then
    fusermount -uz "$dir" 2>/dev/null || true
  fi
}

# Refuse to recurse into a FUSE mount that is still attached: rm -rf on
# $DATA_DIR would otherwise delete files through the mount.
ensure_unmounted() {
  for n in 1 2 3; do
    if command -v mountpoint >/dev/null 2>&1 && mountpoint -q "${DATA_DIR}/fs-n${n}" 2>/dev/null; then
      echo "[cluster] ${DATA_DIR}/fs-n${n} is still mounted; refusing to remove ${DATA_DIR}" >&2
      exit 1
    fi
  done
}

# ---- stop any previously managed cluster ----
stop_cluster() {
  if [ -f "$PID_FILE" ]; then
    echo "[cluster] stopping previous cluster..."
    while IFS= read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
  fi
  for n in 1 2 3; do
    unmount_fs "${DATA_DIR}/fs-n${n}"
  done
}

# ---- build ----
if ! $NO_REBUILD; then
  echo "[build] compiling elastickv..."
  cd "$REPO_ROOT"
  go build -o "$BINARY" .
  echo "[build] done -> $BINARY"
  if $ENCRYPTED; then
    go build -o "$ADMIN_BINARY" ./cmd/elastickv-admin
    echo "[build] done -> $ADMIN_BINARY"
  fi
fi

# ---- start cluster ----
if ! $NO_CLUSTER; then
  stop_cluster
  ensure_unmounted
  rm -rf "$DATA_DIR"
  mkdir -p "$DATA_DIR"
  : > "$PID_FILE"

  # Encrypted mode: one random 32-byte file KEK shared by every node
  # (owner-only mode, as kek.NewFileWrapper requires). The nodes start
  # encryption-capable but still cleartext; the §7.1 cutover below flips
  # the storage and raft envelopes on once the cluster is up.
  if $ENCRYPTED; then
    head -c 32 /dev/urandom > "$KEK_FILE"
    chmod 600 "$KEK_FILE"
  fi

  echo "[cluster] starting 3-node cluster (encrypted=${ENCRYPTED})..."
  for node in 1 2 3; do
    ENC_ARGS=()
    if $ENCRYPTED; then
      mkdir -p "${DATA_DIR}/n${node}"
      ENC_ARGS=(--encryption-enabled --kekFile "$KEK_FILE" \
                --encryptionSidecarPath "${DATA_DIR}/n${node}/keys.json")
    fi
    FS_ARGS=()
    if $FUSE_ENABLED; then
      mkdir -p "${DATA_DIR}/fs-n${node}"
      FS_ARGS=(--filesystemMount "${DATA_DIR}/fs-n${node}" \
               --filesystemRootUID "$(id -u)" \
               --filesystemRootGID "$(id -g)")
    fi
    nohup "$BINARY" \
      --address      "127.0.0.1:5005${node}" \
      --redisAddress "127.0.0.1:6379${node}" \
      --dynamoAddress "127.0.0.1:6380${node}" \
      --s3Address    "127.0.0.1:6390${node}" \
      "${FS_ARGS[@]}" \
      "${ENC_ARGS[@]}" \
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

  mounts_ready() {
    if ! $FUSE_ENABLED; then
      return 0
    fi
    mountpoint -q "${DATA_DIR}/fs-n1" && mountpoint -q "${DATA_DIR}/fs-n2" \
      && mountpoint -q "${DATA_DIR}/fs-n3"
  }

  echo "[cluster] waiting for all ports (redis 63791-3, dynamo 63801-3, s3 63901-3) and FUSE mounts..."
  for i in $(seq 1 90); do
    if nc -z 127.0.0.1 63791 && nc -z 127.0.0.1 63792 && nc -z 127.0.0.1 63793 \
      && nc -z 127.0.0.1 63801 && nc -z 127.0.0.1 63802 && nc -z 127.0.0.1 63803 \
      && nc -z 127.0.0.1 63901 && nc -z 127.0.0.1 63902 && nc -z 127.0.0.1 63903 \
      && mounts_ready; then
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

  if $ENCRYPTED; then
    echo "[cluster] enabling data-at-rest encryption (bootstrap + storage/raft envelope cutover)..."
    "$REPO_ROOT/scripts/jepsen-enable-encryption.sh" "$ADMIN_BINARY" "$KEK_FILE" \
      127.0.0.1:50051 127.0.0.1:50052 127.0.0.1:50053
  fi
fi

# ---- run Jepsen DynamoDB workloads ----
cd "$REPO_ROOT/jepsen"

EXIT_CODE=0

echo "[jepsen] running DynamoDB list-append workload..."
set +e
lein run -m elastickv.dynamodb-workload \
  --local \
  --time-limit 30 \
  --rate 5 \
  --concurrency 5 \
  --dynamo-ports 63801,63802,63803 \
  --host 127.0.0.1
APPEND_EXIT=$?
set -e
if [ $APPEND_EXIT -ne 0 ]; then
  echo "[jepsen] list-append FAILED (exit $APPEND_EXIT)"
  EXIT_CODE=$APPEND_EXIT
fi

# All DynamoDB attribute types currently supported by elastickv.
# Each runs as its own register/linearizable test.
TYPES=(string number binary bool null string-set number-set binary-set list map)
declare -A TYPE_RESULT
for t in "${TYPES[@]}"; do
  echo "[jepsen] running DynamoDB types workload: ${t}..."
  set +e
  lein run -m elastickv.dynamodb-types-workload \
    --local \
    --time-limit 20 \
    --rate 5 \
    --concurrency 4 \
    --value-type "$t" \
    --dynamo-ports 63801,63802,63803 \
    --host 127.0.0.1
  TYPE_EXIT=$?
  set -e
  if [ $TYPE_EXIT -ne 0 ]; then
    echo "[jepsen] type=${t} FAILED (exit $TYPE_EXIT)"
    EXIT_CODE=$TYPE_EXIT
    TYPE_RESULT[$t]="fail(${TYPE_EXIT})"
  else
    TYPE_RESULT[$t]="pass"
  fi
done

echo
echo "[jepsen] per-type summary:"
for t in "${TYPES[@]}"; do
  printf '  %-12s %s\n' "$t" "${TYPE_RESULT[$t]}"
done

# Filesystem register workload over the FUSE mounts, in both write modes.
FS_MODES=(overwrite rename)
declare -A FS_RESULT
for mode in "${FS_MODES[@]}"; do
  if ! $FUSE_ENABLED; then
    echo "[jepsen] skipping filesystem workload: write-mode=${mode} (FUSE unavailable)"
    FS_RESULT[$mode]="skipped"
    continue
  fi
  echo "[jepsen] running filesystem workload: write-mode=${mode}..."
  set +e
  lein run -m elastickv.filesystem-workload \
    --local \
    --time-limit 20 \
    --rate 5 \
    --concurrency 6 \
    --write-mode "$mode" \
    --mount-dirs "${DATA_DIR}/fs-n1,${DATA_DIR}/fs-n2,${DATA_DIR}/fs-n3"
  FS_EXIT=$?
  set -e
  if [ $FS_EXIT -ne 0 ]; then
    echo "[jepsen] filesystem write-mode=${mode} FAILED (exit $FS_EXIT)"
    EXIT_CODE=$FS_EXIT
    FS_RESULT[$mode]="fail(${FS_EXIT})"
  else
    FS_RESULT[$mode]="pass"
  fi
done

echo
echo "[jepsen] filesystem summary:"
for mode in "${FS_MODES[@]}"; do
  printf '  %-10s %s\n' "$mode" "${FS_RESULT[$mode]}"
done

if [ $EXIT_CODE -eq 0 ]; then
  echo "[jepsen] PASSED"
else
  echo "[jepsen] FAILED (exit $EXIT_CODE)"
fi
exit $EXIT_CODE
