#!/usr/bin/env bash
set -euo pipefail

# Engine rollback: etcd/raft → hashicorp
#
# This script performs a stop-the-world rollback of all cluster nodes from
# the etcd/raft engine back to the HashiCorp Raft engine. All nodes must be
# stopped simultaneously because the two engines are protocol-incompatible
# and cannot coexist in a running cluster.
#
# Usage:
#   ROLLING_UPDATE_ENV_FILE=/path/to/deploy.env ./scripts/engine-rollback.sh
#
# The script reuses the same deploy.env as rolling-update.sh. It will:
#   1. Build the etcd-raft-rollback tool for remote architectures
#   2. Stop all containers (stop-the-world)
#   3. On each node, run the FSM rollback and archive etcd artifacts
#   4. Start all nodes with --raftEngine hashicorp via rolling-update.sh
#
# Required environment: same as rolling-update.sh (NODES, etc.)
#
# Safety:
#   - etcd raft artifacts are archived, not deleted
#   - Rollback is aborted if any node fails to stop
#   - The FSM Pebble store (fsm.db) is preserved as-is

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -n "${ROLLING_UPDATE_ENV_FILE:-}" ]]; then
  if [[ ! -f "$ROLLING_UPDATE_ENV_FILE" ]]; then
    echo "ROLLING_UPDATE_ENV_FILE not found: $ROLLING_UPDATE_ENV_FILE" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$ROLLING_UPDATE_ENV_FILE"
fi

SSH_USER="${SSH_USER:-${USER:-$(id -un)}}"
CONTAINER_NAME="${CONTAINER_NAME:-elastickv}"
DATA_DIR="${DATA_DIR:-/var/lib/elastickv}"
RAFT_PORT="${RAFT_PORT:-50051}"
NODES="${NODES:-}"
SSH_TARGETS="${SSH_TARGETS:-}"
SSH_CONNECT_TIMEOUT_SECONDS="${SSH_CONNECT_TIMEOUT_SECONDS:-10}"
SSH_STRICT_HOST_KEY_CHECKING="${SSH_STRICT_HOST_KEY_CHECKING:-accept-new}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-60}"
ROLLBACK_REMOTE_BIN="${ROLLBACK_REMOTE_BIN:-/tmp/etcd-raft-rollback}"

if [[ -z "$NODES" ]]; then
  echo "NODES is required" >&2
  exit 1
fi

SSH_BASE_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout="${SSH_CONNECT_TIMEOUT_SECONDS}"
  -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING}"
)
SCP_BASE_OPTS=(-q "${SSH_BASE_OPTS[@]}")

NODE_IDS=()
NODE_HOSTS=()
ROLLBACK_TMP_DIR=""

cleanup() {
  if [[ -n "$ROLLBACK_TMP_DIR" && -d "$ROLLBACK_TMP_DIR" ]]; then
    rm -rf "$ROLLBACK_TMP_DIR"
  fi
}
trap cleanup EXIT

contains_value() {
  local needle="$1"
  shift
  local v
  for v in "$@"; do
    if [[ "$v" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

lookup_mapping() {
  local key="$1"
  local mapping="$2"
  local pair entry_key entry_value

  [[ -n "$mapping" ]] || return 1
  IFS=',' read -r -a pairs <<< "$mapping"
  for pair in "${pairs[@]}"; do
    pair="${pair//[[:space:]]/}"
    [[ -n "$pair" ]] || continue
    [[ "$pair" == *=* ]] || continue
    entry_key="${pair%%=*}"
    entry_value="${pair#*=}"
    if [[ "$entry_key" == "$key" ]]; then
      printf '%s\n' "$entry_value"
      return 0
    fi
  done
  return 1
}

parse_nodes() {
  local pair node_id node_host
  IFS=',' read -r -a pairs <<< "$NODES"
  for pair in "${pairs[@]}"; do
    pair="${pair//[[:space:]]/}"
    [[ -n "$pair" ]] || continue
    if [[ "$pair" != *=* ]]; then
      echo "invalid NODES entry: $pair" >&2
      exit 1
    fi
    node_id="${pair%%=*}"
    node_host="${pair#*=}"
    if [[ -z "$node_id" || -z "$node_host" ]]; then
      echo "invalid NODES entry: $pair" >&2
      exit 1
    fi
    if contains_value "$node_id" "${NODE_IDS[@]+"${NODE_IDS[@]}"}"; then
      echo "duplicate raft ID in NODES: $node_id" >&2
      exit 1
    fi
    NODE_IDS+=("$node_id")
    NODE_HOSTS+=("$node_host")
  done
  if [[ "${#NODE_IDS[@]}" -eq 0 ]]; then
    echo "NODES did not contain any nodes" >&2
    exit 1
  fi
}

node_host_by_id() {
  local wanted_id="$1"
  local i
  for i in "${!NODE_IDS[@]}"; do
    if [[ "${NODE_IDS[$i]}" == "$wanted_id" ]]; then
      printf '%s\n' "${NODE_HOSTS[$i]}"
      return 0
    fi
  done
  return 1
}

ssh_target_by_id() {
  local node_id="$1"
  local target
  target="$(lookup_mapping "$node_id" "$SSH_TARGETS" || true)"
  if [[ -z "$target" ]]; then
    target="$(node_host_by_id "$node_id")"
  fi
  if [[ "$target" == *@* ]]; then
    printf '%s\n' "$target"
    return 0
  fi
  printf '%s@%s\n' "$SSH_USER" "$target"
}

build_peers_string() {
  local parts=()
  local i
  for i in "${!NODE_IDS[@]}"; do
    parts+=("${NODE_IDS[$i]}=${NODE_HOSTS[$i]}:${RAFT_PORT}")
  done
  (IFS=,; printf '%s\n' "${parts[*]}")
}

build_rollback_variant() {
  local goos="$1"
  local goarch="$2"
  local out="${ROLLBACK_TMP_DIR}/etcd-raft-rollback-${goos}-${goarch}"

  if [[ -x "$out" ]]; then
    printf '%s\n' "$out"
    return 0
  fi

  echo "[engine-rollback] building etcd-raft-rollback for ${goos}/${goarch}" >&2
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build -C "$REPO_ROOT" -o "$out" ./cmd/etcd-raft-rollback
  chmod +x "$out"
  printf '%s\n' "$out"
}

copy_rollback_tool() {
  local node_id="$1"
  local ssh_target="$2"

  scp "${SCP_BASE_OPTS[@]}" "$ROLLBACK_LINUX_AMD64_BIN" "${ssh_target}:${ROLLBACK_REMOTE_BIN}-amd64"
  scp "${SCP_BASE_OPTS[@]}" "$ROLLBACK_LINUX_ARM64_BIN" "${ssh_target}:${ROLLBACK_REMOTE_BIN}-arm64"

  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    ROLLBACK_REMOTE_BIN="$ROLLBACK_REMOTE_BIN" \
    'bash -s' <<'INSTALL'
set -euo pipefail
case "$(uname -m)" in
  x86_64|amd64)  cp "${ROLLBACK_REMOTE_BIN}-amd64" "$ROLLBACK_REMOTE_BIN" ;;
  aarch64|arm64)  cp "${ROLLBACK_REMOTE_BIN}-arm64" "$ROLLBACK_REMOTE_BIN" ;;
  *)
    echo "unsupported remote architecture: $(uname -m)" >&2
    exit 1
    ;;
esac
chmod +x "$ROLLBACK_REMOTE_BIN"
rm -f "${ROLLBACK_REMOTE_BIN}-amd64" "${ROLLBACK_REMOTE_BIN}-arm64"
INSTALL
}

stop_node() {
  local node_id="$1"
  local ssh_target="$2"

  echo "==> [${node_id}] stopping container"
  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    CONTAINER_NAME="$CONTAINER_NAME" \
    'bash -s' <<'STOP'
set -euo pipefail
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
STOP
}

verify_node_stopped() {
  local node_id="$1"
  local ssh_target="$2"

  local status
  status="$(ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    CONTAINER_NAME="$CONTAINER_NAME" \
    'bash -s' <<'CHECK'
set -euo pipefail
docker inspect --format "{{.State.Status}}" "$CONTAINER_NAME" 2>/dev/null || echo "missing"
CHECK
  )"
  if [[ "$status" != "missing" ]]; then
    echo "error: container on ${node_id} is still ${status} after stop" >&2
    return 1
  fi
}

rollback_node() {
  local node_id="$1"
  local ssh_target="$2"
  local peers_string="$3"

  echo "==> [${node_id}] rolling back etcd → hashicorp"

  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    env \
      ROLLBACK_BIN="$ROLLBACK_REMOTE_BIN" \
      DATA_DIR="$DATA_DIR" \
      NODE_ID="$node_id" \
      PEERS="$peers_string" \
    bash -s <<'ROLLBACK'
set -euo pipefail

node_dir="${DATA_DIR%/}/${NODE_ID}"
fsm_store="${node_dir}/fsm.db"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
backup_dir="${node_dir}/etcd-backup-${ts}"

if ! sudo -n true 2>/dev/null; then
  echo "error: passwordless sudo is required on this host" >&2
  exit 1
fi

if sudo -n test -d "${node_dir}/raft.db"; then
  echo "hashicorp raft data already exists at ${node_dir}/raft.db; skipping rollback"
  # Still ensure the engine marker is correct
  echo "hashicorp" | sudo -n tee "${node_dir}/raft-engine" > /dev/null
  exit 0
fi

if ! sudo -n test -d "$fsm_store"; then
  echo "error: FSM store not found at ${fsm_store}" >&2
  exit 1
fi

rollback_dest="$(sudo -n mktemp -d "/tmp/hashicorp-rollback-${NODE_ID}-XXXXXX")"

echo "  running etcd-raft-rollback"
sudo -n "$ROLLBACK_BIN" \
  -fsm-store "$fsm_store" \
  -dest "${rollback_dest}/data" \
  -peers "$PEERS"

echo "  moving hashicorp artifacts into place"
sudo -n mv "${rollback_dest}/data/raft.db" "${node_dir}/raft.db"
sudo -n mv "${rollback_dest}/data/snapshots" "${node_dir}/snapshots"
sudo -n rm -rf "$rollback_dest"

echo "  archiving etcd raft artifacts"
sudo -n mkdir -p "$backup_dir"
if sudo -n test -d "${node_dir}/member"; then
  sudo -n mv "${node_dir}/member" "${backup_dir}/member"
fi

echo "  updating engine marker"
echo "hashicorp" | sudo -n tee "${node_dir}/raft-engine" > /dev/null

echo "  rollback complete for ${NODE_ID}"
ROLLBACK
}

wait_for_grpc() {
  local node_host="$1"
  local ssh_target="$2"
  local i

  for ((i = 0; i < HEALTH_TIMEOUT_SECONDS; i++)); do
    if ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
         bash -lc "exec 3<>/dev/tcp/${node_host}/${RAFT_PORT}" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# ── Main ──

parse_nodes
PEERS_STRING="$(build_peers_string)"

echo "[engine-rollback] cluster: ${NODE_IDS[*]}"
echo "[engine-rollback] peers: ${PEERS_STRING}"
echo ""
echo "This will STOP ALL NODES and roll back from etcd/raft to hashicorp."
echo "There will be cluster downtime during rollback."
echo ""
read -r -p "Continue? [y/N] " confirm
if [[ "$confirm" != [yY] ]]; then
  echo "aborted"
  exit 1
fi

# Step 1: Build rollback tool
echo ""
echo "[engine-rollback] building rollback tool"
ROLLBACK_TMP_DIR="$(mktemp -d)"
ROLLBACK_LINUX_AMD64_BIN="$(build_rollback_variant linux amd64)"
ROLLBACK_LINUX_ARM64_BIN="$(build_rollback_variant linux arm64)"

# Step 2: Stop all nodes
echo ""
echo "[engine-rollback] stopping all nodes"
for node_id in "${NODE_IDS[@]}"; do
  stop_node "$node_id" "$(ssh_target_by_id "$node_id")"
done

echo "[engine-rollback] verifying all nodes stopped"
for node_id in "${NODE_IDS[@]}"; do
  verify_node_stopped "$node_id" "$(ssh_target_by_id "$node_id")"
done
echo "[engine-rollback] all nodes stopped"

# Step 3: Copy rollback tool and run rollback on each node
echo ""
echo "[engine-rollback] running rollback on each node"
for node_id in "${NODE_IDS[@]}"; do
  ssh_target="$(ssh_target_by_id "$node_id")"
  echo "==> [${node_id}] deploying rollback tool"
  copy_rollback_tool "$node_id" "$ssh_target"
  rollback_node "$node_id" "$ssh_target" "$PEERS_STRING"
done

# Step 4: Start all nodes with hashicorp engine using rolling-update.sh
echo ""
echo "[engine-rollback] starting all nodes with hashicorp engine"
RAFT_ENGINE=hashicorp \
  ROLLING_UPDATE_ENV_FILE="${ROLLING_UPDATE_ENV_FILE:-}" \
  exec "${SCRIPT_DIR}/rolling-update.sh"
