#!/usr/bin/env bash
set -euo pipefail

fault=${1:?Usage: run.sh partition|kill|pause}
case "$fault" in partition|kill|pause) ;; *) echo 'Unsupported fault' >&2; exit 2 ;; esac
export JEPSEN_REPO
JEPSEN_REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$JEPSEN_REPO"
export JEPSEN_IMAGE="elastickv-jepsen-hosted:local"
export JEPSEN_SSH_DIR
JEPSEN_SSH_DIR=$(mktemp -d)
export JEPSEN_M2_DIR="${HOME}/.m2"
mkdir -p "$JEPSEN_M2_DIR"
artifact_dir="$JEPSEN_REPO/artifacts/jepsen-$fault"
mkdir -p "$artifact_dir"
project="jepsen-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}-$fault"
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
printf 'fault=%s\ntime-limit=%s\nfault-interval=5\nrecovery-time=10\nconcurrency=6\nkey-count=8\nmax-writes-per-key=256\n' \
  "$fault" "${JEPSEN_TIME_LIMIT:-90}" > "$artifact_dir/parameters.txt"
"${compose[@]}" config > "$artifact_dir/compose.yml"
# These are SSH-managed nodes: --local and --host would disable the nemesis.
"${compose[@]}" run --rm control \
  lein run -m elastickv.hosted-fault-workload \
    --nodes n1,n2,n3 --ssh-user root --ssh-key /run/jepsen-ssh/id_ed25519 \
    --faults "$fault" --fault-interval 5 --time-limit "${JEPSEN_TIME_LIMIT:-90}" \
    --recovery-time 10 --rate 5 --concurrency 6 --key-count 8 --max-writes-per-key 256 \
  2>&1 | tee "$artifact_dir/control.log"
