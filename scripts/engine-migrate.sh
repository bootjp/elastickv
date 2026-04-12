#!/usr/bin/env bash
set -euo pipefail

# Engine migration: hashicorp → etcd/raft
#
# This script performs a stop-the-world migration of all cluster nodes from
# the HashiCorp Raft engine to the etcd/raft engine. All nodes must be stopped
# simultaneously because the two engines are protocol-incompatible and cannot
# coexist in a running cluster.
#
# Usage:
#   ROLLING_UPDATE_ENV_FILE=/path/to/deploy.env ./scripts/engine-migrate.sh
#
# The script reuses the same deploy.env as rolling-update.sh. It will:
#   1. Build the etcd-raft-migrate tool for remote architectures
#   2. Stop all containers (stop-the-world)
#   3. On each node, run the FSM migration and archive hashicorp artifacts
#   4. Start all nodes with --raftEngine etcd via rolling-update.sh
#
# Required environment: same as rolling-update.sh (NODES, etc.)
# The RAFT_ENGINE variable in deploy.env is ignored during migration; the
# script hardcodes the target engine to etcd.
#
# Safety:
#   - HashiCorp raft artifacts are archived, not deleted
#   - Migration is aborted if any node fails to stop
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
MIGRATE_REMOTE_BIN="${MIGRATE_REMOTE_BIN:-/tmp/etcd-raft-migrate}"

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
MIGRATE_TMP_DIR=""

cleanup() {
  if [[ -n "$MIGRATE_TMP_DIR" && -d "$MIGRATE_TMP_DIR" ]]; then
    rm -rf "$MIGRATE_TMP_DIR"
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

build_migrate_variant() {
  local goos="$1"
  local goarch="$2"
  local out="${MIGRATE_TMP_DIR}/etcd-raft-migrate-${goos}-${goarch}"

  if [[ -x "$out" ]]; then
    printf '%s\n' "$out"
    return 0
  fi

  echo "[engine-migrate] building etcd-raft-migrate for ${goos}/${goarch}" >&2
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build -C "$REPO_ROOT" -o "$out" ./cmd/etcd-raft-migrate
  chmod +x "$out"
  printf '%s\n' "$out"
}

copy_migrate_tool() {
  local node_id="$1"
  local ssh_target="$2"

  scp "${SCP_BASE_OPTS[@]}" "$MIGRATE_LINUX_AMD64_BIN" "${ssh_target}:${MIGRATE_REMOTE_BIN}-amd64"
  scp "${SCP_BASE_OPTS[@]}" "$MIGRATE_LINUX_ARM64_BIN" "${ssh_target}:${MIGRATE_REMOTE_BIN}-arm64"

  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    MIGRATE_REMOTE_BIN="$MIGRATE_REMOTE_BIN" \
    'bash -s' <<'INSTALL'
set -euo pipefail
case "$(uname -m)" in
  x86_64|amd64)  cp "${MIGRATE_REMOTE_BIN}-amd64" "$MIGRATE_REMOTE_BIN" ;;
  aarch64|arm64)  cp "${MIGRATE_REMOTE_BIN}-arm64" "$MIGRATE_REMOTE_BIN" ;;
  *)
    echo "unsupported remote architecture: $(uname -m)" >&2
    exit 1
    ;;
esac
chmod +x "$MIGRATE_REMOTE_BIN"
rm -f "${MIGRATE_REMOTE_BIN}-amd64" "${MIGRATE_REMOTE_BIN}-arm64"
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

migrate_node() {
  local node_id="$1"
  local ssh_target="$2"
  local peers_string="$3"

  echo "==> [${node_id}] migrating hashicorp → etcd"

  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    env \
      MIGRATE_BIN="$MIGRATE_REMOTE_BIN" \
      DATA_DIR="$DATA_DIR" \
      NODE_ID="$node_id" \
      PEERS="$peers_string" \
    bash -s <<'MIGRATE'
set -euo pipefail

node_dir="${DATA_DIR%/}/${NODE_ID}"
fsm_store="${node_dir}/fsm.db"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
backup_dir="${node_dir}/hashicorp-backup-${ts}"

if ! sudo -n true 2>/dev/null; then
  echo "error: passwordless sudo is required on this host" >&2
  exit 1
fi

if sudo -n test -d "${node_dir}/member"; then
  echo "etcd raft data already exists at ${node_dir}/member; skipping migration"
  # Still ensure the engine marker is correct
  echo "etcd" | sudo -n tee "${node_dir}/raft-engine" > /dev/null
  exit 0
fi

if ! sudo -n test -d "$fsm_store"; then
  echo "error: FSM store not found at ${fsm_store}" >&2
  exit 1
fi

migrate_dest="$(sudo -n mktemp -d "/tmp/etcd-migrate-${NODE_ID}-XXXXXX")"

echo "  running etcd-raft-migrate"
sudo -n "$MIGRATE_BIN" \
  -fsm-store "$fsm_store" \
  -dest "${migrate_dest}/data" \
  -peers "$PEERS"

echo "  moving etcd artifacts into place"
sudo -n mv "${migrate_dest}/data/member" "${node_dir}/member"
sudo -n rm -rf "$migrate_dest"

echo "  archiving hashicorp raft artifacts"
sudo -n mkdir -p "$backup_dir"
for name in raft.db snapshots; do
  if sudo -n test -e "${node_dir}/${name}"; then
    sudo -n mv "${node_dir}/${name}" "${backup_dir}/${name}"
  fi
done

# Archive legacy boltdb files if present
for name in logs.dat stable.dat; do
  if sudo -n test -e "${node_dir}/${name}"; then
    sudo -n mv "${node_dir}/${name}" "${backup_dir}/${name}"
  fi
done

# Remove any stale migration temp files
sudo -n rm -rf "${node_dir}/raft.db.migrating" 2>/dev/null || true

echo "  updating engine marker"
echo "etcd" | sudo -n tee "${node_dir}/raft-engine" > /dev/null

echo "  migration complete for ${NODE_ID}"
MIGRATE
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

echo "[engine-migrate] cluster: ${NODE_IDS[*]}"
echo "[engine-migrate] peers: ${PEERS_STRING}"
echo ""
echo "This will STOP ALL NODES and migrate from hashicorp to etcd/raft."
echo "There will be cluster downtime during migration."
echo ""
read -r -p "Continue? [y/N] " confirm
if [[ "$confirm" != [yY] ]]; then
  echo "aborted"
  exit 1
fi

# Step 1: Build migrate tool
echo ""
echo "[engine-migrate] building migration tool"
MIGRATE_TMP_DIR="$(mktemp -d)"
MIGRATE_LINUX_AMD64_BIN="$(build_migrate_variant linux amd64)"
MIGRATE_LINUX_ARM64_BIN="$(build_migrate_variant linux arm64)"

# Step 2: Stop all nodes
echo ""
echo "[engine-migrate] stopping all nodes"
for node_id in "${NODE_IDS[@]}"; do
  stop_node "$node_id" "$(ssh_target_by_id "$node_id")"
done

echo "[engine-migrate] verifying all nodes stopped"
for node_id in "${NODE_IDS[@]}"; do
  verify_node_stopped "$node_id" "$(ssh_target_by_id "$node_id")"
done
echo "[engine-migrate] all nodes stopped"

# Step 3: Copy migrate tool and run migration on each node
echo ""
echo "[engine-migrate] running migration on each node"
for node_id in "${NODE_IDS[@]}"; do
  ssh_target="$(ssh_target_by_id "$node_id")"
  echo "==> [${node_id}] deploying migrate tool"
  copy_migrate_tool "$node_id" "$ssh_target"
  migrate_node "$node_id" "$ssh_target" "$PEERS_STRING"
done

# Step 4: Start all nodes with etcd engine using rolling-update.sh
echo ""
echo "[engine-migrate] starting all nodes with etcd engine"
RAFT_ENGINE=etcd \
  ROLLING_UPDATE_ENV_FILE="${ROLLING_UPDATE_ENV_FILE:-}" \
  exec "${SCRIPT_DIR}/rolling-update.sh"
