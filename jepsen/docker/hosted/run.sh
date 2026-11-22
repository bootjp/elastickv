#!/usr/bin/env bash
set -euo pipefail

fault=${1:?Usage: run.sh partition|kill|pause}
case "$fault" in partition|kill|pause) ;; *) echo 'Unsupported fault' >&2; exit 2 ;; esac
adapter=${2:-redis}
case "$adapter" in redis|dynamodb|s3|s3-offload|grpc-raw|grpc-transactional|filesystem-overwrite|filesystem-rename|sqs-standard|sqs-htfifo|hotspot) ;; *) echo 'Unsupported adapter' >&2; exit 2 ;; esac
if [[ $adapter == hotspot && $fault == pause ]]; then
  echo 'Hotspot acceptance supports partition and kill only' >&2
  exit 2
fi
time_limit=${JEPSEN_TIME_LIMIT:-90}
rate=${JEPSEN_RATE:-5}
concurrency=${JEPSEN_CONCURRENCY:-6}
key_count=${JEPSEN_KEY_COUNT:-8}
max_writes=${JEPSEN_MAX_WRITES_PER_KEY:-256}
if [[ $adapter == hotspot ]]; then
  concurrency=${JEPSEN_CONCURRENCY:-24}
  max_writes=${JEPSEN_MAX_WRITES_PER_KEY:-32}
fi
max_txn_length=${JEPSEN_MAX_TXN_LENGTH:-4}
min_cycles=${JEPSEN_MIN_FAULT_CYCLES:-1}
repeat_faults=${JEPSEN_REPEAT_FAULTS:-false}
recovery_time=10
if [[ $adapter == filesystem-* ]]; then
  # A restarted strict mount may use the full startup retry budget while its
  # Raft leader and HLC lease recover after a kill or partition.
  recovery_time=90
fi
extra_args=()
case "$repeat_faults" in
  true) extra_args+=(--repeat-faults) ;;
  false) ;;
  *) echo 'JEPSEN_REPEAT_FAULTS must be true or false' >&2; exit 2 ;;
esac
export JEPSEN_REPO
JEPSEN_REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$JEPSEN_REPO"
for value in "$time_limit" "$concurrency" "$key_count" "$max_writes" "$max_txn_length" "$min_cycles"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ || ${#value} -gt 9 ]]; then
    echo 'Integer workload parameters must be positive and at most nine digits' >&2
    exit 2
  fi
done
if [[ "$adapter" == s3* || "$adapter" == grpc-* ]]; then
  # Preserve the aggregate budget while bounding each Knossos search.
  source jepsen/s3_budget.sh
  s3_budget "$key_count" "$max_writes"
  key_count=$S3_KEY_COUNT
  max_writes=$S3_MAX_OPS_PER_KEY
fi
if [[ "$adapter" == filesystem-* ]]; then
  source jepsen/filesystem_budget.sh
  filesystem_budget "${adapter#filesystem-}" "$key_count" "$max_writes"
  key_count=$FS_KEY_COUNT
  max_writes=$FS_MAX_WRITES
fi
export JEPSEN_IMAGE="elastickv-jepsen-hosted:local"
export JEPSEN_SSH_DIR
JEPSEN_SSH_DIR=$(mktemp -d)
export JEPSEN_M2_DIR="${HOME}/.m2"
mkdir -p "$JEPSEN_M2_DIR"
artifact_dir="$JEPSEN_REPO/artifacts/jepsen-$adapter-$fault"
mkdir -p "$artifact_dir"
project="jepsen-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}-$adapter-$fault"
compose=(docker compose -p "$project" -f jepsen/docker/hosted/compose.yml)
if [[ "$adapter" == filesystem-* ]]; then
  compose+=(-f jepsen/docker/hosted/compose.fuse.yml)
fi

finish() {
  status=$?
  trap - EXIT
  set +e
  "${compose[@]}" logs --no-color > "$artifact_dir/containers.log" 2>&1
  "${compose[@]}" ps -a > "$artifact_dir/containers.txt" 2>&1
  for node in n1 n2 n3; do
    container=$("${compose[@]}" ps -aq "$node")
    if [[ -n "$container" ]]; then
      mkdir -p "$artifact_dir/$node"
      docker cp "$container:/var/log/elastickv.log" "$artifact_dir/$node/" 2>> "$artifact_dir/collection.log"
    fi
  done
  if [[ -d jepsen/store ]]; then
    # Timestamp directories contain colons, which upload-artifact rejects.
    tar -czf "$artifact_dir/store.tar.gz" -C jepsen store || status=1
  elif [[ "$status" == 0 ]]; then
    echo 'Successful run has no Jepsen store' >> "$artifact_dir/collection.log"
    status=1
  fi
  "${compose[@]}" down --volumes --remove-orphans >> "$artifact_dir/cleanup.log" 2>&1 || status=1
  printf '%s\n' "$status" > "$artifact_dir/exit-code.txt"
  rm -rf "$JEPSEN_SSH_DIR"
  exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$adapter" == filesystem-* && ! -c /dev/fuse ]]; then
  echo 'Filesystem fault tests require /dev/fuse on the Hosted runner' >&2
  exit 1
fi

ssh-keygen -q -t ed25519 -N '' -f "$JEPSEN_SSH_DIR/id_ed25519"
go_version=$(awk '$1 == "toolchain" {sub(/^go/, "", $2); print $2}' go.mod)
docker build --build-arg "GO_VERSION=$go_version" -t "$JEPSEN_IMAGE" jepsen/docker/hosted
"${compose[@]}" up -d --wait n1 n2 n3

workload_adapter=$adapter
if [[ $adapter == s3-offload ]]; then
  workload_adapter=s3
  extra_args+=(--s3-offload)
  for purpose in admin peer; do
    token="$JEPSEN_SSH_DIR/$purpose.token"
    (umask 077; openssl rand -hex 32 > "$token")
    for node in n1 n2 n3; do
      "${compose[@]}" cp "$token" "$node:/run/jepsen-$purpose.token"
    done
  done
fi

git rev-parse HEAD > "$artifact_dir/commit.txt"
printf 'adapter=%s\nfault=%s\ntime-limit=%s\nfault-interval=5\nrecovery-time=%s\nrate=%s\nconcurrency=%s\nkey-count=%s\nmax-writes-per-key=%s\nmax-txn-length=%s\nrepeat-faults=%s\nmin-fault-cycles=%s\n' \
  "$adapter" "$fault" "$time_limit" "$recovery_time" "$rate" "$concurrency" "$key_count" "$max_writes" \
  "$max_txn_length" "$repeat_faults" "$min_cycles" > "$artifact_dir/parameters.txt"
"${compose[@]}" config > "$artifact_dir/compose.yml"
# These are SSH-managed nodes: --local and --host would disable the nemesis.
if [[ $adapter == hotspot ]]; then
  workload=(lein run -m elastickv.hotspot-workload
    --nodes 'n1,n2,n3' --ssh-user root --ssh-key /run/jepsen-ssh/id_ed25519
    --faults "$fault" --fault-interval 5 --time-limit "$time_limit"
    --rate "$rate" --concurrency "$concurrency" --max-writes-per-key "$max_writes")
else
  workload=(lein run -m elastickv.hosted-fault-workload
    --adapter "$workload_adapter" --nodes 'n1,n2,n3' --ssh-user root --ssh-key /run/jepsen-ssh/id_ed25519
    --faults "$fault" --fault-interval 5 --time-limit "$time_limit"
    --recovery-time "$recovery_time" --rate "$rate" --concurrency "$concurrency" --key-count "$key_count"
    --max-writes-per-key "$max_writes" --max-txn-length "$max_txn_length"
    --min-fault-cycles "$min_cycles" "${extra_args[@]}")
fi
"${compose[@]}" run --rm control "${workload[@]}" 2>&1 | tee "$artifact_dir/control.log"
if [[ $adapter == s3-offload ]]; then
  for node in n1 n2 n3; do
    "${compose[@]}" cp "$node:/var/log/elastickv-transport-metrics.prom" "$artifact_dir/$node.prom"
  done
  for op in put get; do
    awk -v op="$op" '/^elastickv_s3_object_duration_seconds_count[{]/ && /mode="offload"/ && /outcome="success"/ && index($0, "operation=\"" op "\"") && $NF+0 > 0 { found=1 } END { exit !found }' "$artifact_dir"/*.prom || {
      echo "Missing successful offloaded $op in node metrics" >&2
      exit 1
    }
  done
fi
