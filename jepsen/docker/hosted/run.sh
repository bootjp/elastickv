#!/usr/bin/env bash
set -euo pipefail

fault=${1:?Usage: run.sh partition|kill|pause}
case "$fault" in partition|kill|pause) ;; *) echo 'Unsupported fault' >&2; exit 2 ;; esac
adapter=${2:-redis}
case "$adapter" in redis|dynamodb|s3|sqs-standard|sqs-htfifo) ;; *) echo 'Unsupported adapter' >&2; exit 2 ;; esac
time_limit=${JEPSEN_TIME_LIMIT:-90}
rate=${JEPSEN_RATE:-5}
concurrency=${JEPSEN_CONCURRENCY:-6}
key_count=${JEPSEN_KEY_COUNT:-8}
max_writes=${JEPSEN_MAX_WRITES_PER_KEY:-256}
max_txn_length=${JEPSEN_MAX_TXN_LENGTH:-4}
min_cycles=${JEPSEN_MIN_FAULT_CYCLES:-1}
repeat_faults=${JEPSEN_REPEAT_FAULTS:-false}
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
if [[ "$adapter" == s3 ]]; then
  # Preserve the aggregate budget while bounding each Knossos search.
  source jepsen/s3_budget.sh
  s3_budget "$key_count" "$max_writes"
  key_count=$S3_KEY_COUNT
  max_writes=$S3_MAX_OPS_PER_KEY
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

ssh-keygen -q -t ed25519 -N '' -f "$JEPSEN_SSH_DIR/id_ed25519"
go_version=$(awk '$1 == "toolchain" {sub(/^go/, "", $2); print $2}' go.mod)
docker build --build-arg "GO_VERSION=$go_version" -t "$JEPSEN_IMAGE" jepsen/docker/hosted
"${compose[@]}" up -d --wait n1 n2 n3

git rev-parse HEAD > "$artifact_dir/commit.txt"
printf 'adapter=%s\nfault=%s\ntime-limit=%s\nfault-interval=5\nrecovery-time=10\nrate=%s\nconcurrency=%s\nkey-count=%s\nmax-writes-per-key=%s\nmax-txn-length=%s\nrepeat-faults=%s\nmin-fault-cycles=%s\n' \
  "$adapter" "$fault" "$time_limit" "$rate" "$concurrency" "$key_count" "$max_writes" \
  "$max_txn_length" "$repeat_faults" "$min_cycles" > "$artifact_dir/parameters.txt"
"${compose[@]}" config > "$artifact_dir/compose.yml"
# These are SSH-managed nodes: --local and --host would disable the nemesis.
"${compose[@]}" run --rm control \
  lein run -m elastickv.hosted-fault-workload \
    --adapter "$adapter" --nodes n1,n2,n3 --ssh-user root --ssh-key /run/jepsen-ssh/id_ed25519 \
    --faults "$fault" --fault-interval 5 --time-limit "$time_limit" \
    --recovery-time 10 --rate "$rate" --concurrency "$concurrency" --key-count "$key_count" \
    --max-writes-per-key "$max_writes" --max-txn-length "$max_txn_length" \
    --min-fault-cycles "$min_cycles" "${extra_args[@]}" \
  2>&1 | tee "$artifact_dir/control.log"
