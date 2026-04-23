#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  NODES="n1=raft-1.internal,n2=raft-2.internal,n3=raft-3.internal" ./scripts/rolling-update.sh

Required environment:
  NODES
    Comma-separated raft node map in rollout order: "<raftId>=<advertised-host>,..."

Optional environment:
  ROLLING_UPDATE_ENV_FILE
    Shell env file to source before evaluating the rest of the settings.

  SSH_TARGETS
    Comma-separated SSH target map when SSH hosts differ from advertised hosts:
    "<raftId>=<ssh-host-or-user@host>,..."
    If omitted, the script SSHes to the advertised host and prefixes SSH_USER.

  ROLLING_ORDER
    Comma-separated raft IDs to override the rollout order.

  IMAGE
  SSH_USER
  CONTAINER_NAME
  DATA_DIR
  SERVER_ENTRYPOINT
  RAFT_ENGINE
  RAFT_PORT
  REDIS_PORT
  DYNAMO_PORT
  RAFT_TO_REDIS_MAP
  RAFT_TO_S3_MAP
  S3_PORT
  ENABLE_S3
  S3_REGION
  S3_CREDENTIALS_FILE
    Path to an S3 credentials file on each target host. This file must already
    exist and be readable on every remote node; it will be bind-mounted into
    the container at the same path.
  S3_PATH_STYLE_ONLY
  HEALTH_TIMEOUT_SECONDS
  LEADERSHIP_TRANSFER_TIMEOUT_SECONDS
  LEADER_DISCOVERY_TIMEOUT_SECONDS
  ROLLING_DELAY_SECONDS
  SSH_CONNECT_TIMEOUT_SECONDS
  SSH_STRICT_HOST_KEY_CHECKING
  RAFTADMIN_BIN
  RAFTADMIN_REMOTE_BIN
  RAFTADMIN_RPC_TIMEOUT_SECONDS
  RAFTADMIN_ALLOW_INSECURE

Notes:
  - If RAFT_TO_REDIS_MAP is unset, it is derived automatically from NODES,
    RAFT_PORT, and REDIS_PORT.
  - If RAFT_TO_S3_MAP is unset, it is derived automatically from NODES,
    RAFT_PORT, and S3_PORT.
  - If RAFTADMIN_BIN is set, it must already be executable on the local control host.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ -n "${ROLLING_UPDATE_ENV_FILE:-}" ]]; then
  if [[ ! -f "$ROLLING_UPDATE_ENV_FILE" ]]; then
    echo "ROLLING_UPDATE_ENV_FILE not found: $ROLLING_UPDATE_ENV_FILE" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$ROLLING_UPDATE_ENV_FILE"
fi

IMAGE="${IMAGE:-ghcr.io/bootjp/elastickv:latest}"
SSH_USER="${SSH_USER:-${USER:-$(id -un)}}"
CONTAINER_NAME="${CONTAINER_NAME:-elastickv}"
DATA_DIR="${DATA_DIR:-/var/lib/elastickv}"
SERVER_ENTRYPOINT="${SERVER_ENTRYPOINT:-/app}"
RAFT_ENGINE="${RAFT_ENGINE:-etcd}"
RAFT_PORT="${RAFT_PORT:-50051}"
REDIS_PORT="${REDIS_PORT:-6379}"
DYNAMO_PORT="${DYNAMO_PORT:-8000}"
S3_PORT="${S3_PORT:-9000}"
ENABLE_S3="${ENABLE_S3:-true}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_CREDENTIALS_FILE="${S3_CREDENTIALS_FILE:-}"
S3_PATH_STYLE_ONLY="${S3_PATH_STYLE_ONLY:-true}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-60}"
LEADERSHIP_TRANSFER_TIMEOUT_SECONDS="${LEADERSHIP_TRANSFER_TIMEOUT_SECONDS:-30}"
LEADER_DISCOVERY_TIMEOUT_SECONDS="${LEADER_DISCOVERY_TIMEOUT_SECONDS:-30}"
ROLLING_DELAY_SECONDS="${ROLLING_DELAY_SECONDS:-2}"
SSH_CONNECT_TIMEOUT_SECONDS="${SSH_CONNECT_TIMEOUT_SECONDS:-10}"
SSH_STRICT_HOST_KEY_CHECKING="${SSH_STRICT_HOST_KEY_CHECKING:-accept-new}"
RAFTADMIN_REMOTE_BIN="${RAFTADMIN_REMOTE_BIN:-/tmp/elastickv-raftadmin}"
RAFTADMIN_RPC_TIMEOUT_SECONDS="${RAFTADMIN_RPC_TIMEOUT_SECONDS:-5}"
RAFTADMIN_ALLOW_INSECURE="${RAFTADMIN_ALLOW_INSECURE:-true}"
NODES="${NODES:-}"
SSH_TARGETS="${SSH_TARGETS:-}"
ROLLING_ORDER="${ROLLING_ORDER:-}"
RAFT_TO_REDIS_MAP="${RAFT_TO_REDIS_MAP:-}"
RAFT_TO_S3_MAP="${RAFT_TO_S3_MAP:-}"

if [[ -z "$NODES" ]]; then
  echo "NODES is required" >&2
  usage >&2
  exit 1
fi

SSH_BASE_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout="${SSH_CONNECT_TIMEOUT_SECONDS}"
  -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING}"
)
SCP_BASE_OPTS=(-q "${SSH_BASE_OPTS[@]}")

RAFTADMIN_LOCAL_BIN="${RAFTADMIN_BIN:-}"
RAFTADMIN_TMP_DIR=""
RAFTADMIN_LINUX_AMD64_BIN=""
RAFTADMIN_LINUX_ARM64_BIN=""

NODE_IDS=()
NODE_HOSTS=()
ROLLING_NODE_IDS=()

cleanup() {
  if [[ -n "$RAFTADMIN_TMP_DIR" && -d "$RAFTADMIN_TMP_DIR" ]]; then
    rm -rf "$RAFTADMIN_TMP_DIR"
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
    if contains_value "$node_id" "${NODE_IDS[@]}"; then
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

prepare_rolling_order() {
  local entry

  if [[ -z "$ROLLING_ORDER" ]]; then
    ROLLING_NODE_IDS=("${NODE_IDS[@]}")
    return 0
  fi

  IFS=',' read -r -a entries <<< "$ROLLING_ORDER"
  for entry in "${entries[@]}"; do
    entry="${entry//[[:space:]]/}"
    [[ -n "$entry" ]] || continue
    if ! contains_value "$entry" "${NODE_IDS[@]}"; then
      echo "ROLLING_ORDER references unknown raft ID: $entry" >&2
      exit 1
    fi
    if contains_value "$entry" "${ROLLING_NODE_IDS[@]}"; then
      echo "ROLLING_ORDER contains duplicate raft ID: $entry" >&2
      exit 1
    fi
    ROLLING_NODE_IDS+=("$entry")
  done

  if [[ "${#ROLLING_NODE_IDS[@]}" -eq 0 ]]; then
    echo "ROLLING_ORDER did not contain any nodes" >&2
    exit 1
  fi
}

derive_raft_to_redis_map() {
  local parts=()
  local i

  for i in "${!NODE_IDS[@]}"; do
    parts+=("${NODE_HOSTS[$i]}:${RAFT_PORT}=${NODE_HOSTS[$i]}:${REDIS_PORT}")
  done

  (
    IFS=,
    printf '%s\n' "${parts[*]}"
  )
}

derive_raft_to_s3_map() {
  local parts=()
  local i

  for i in "${!NODE_IDS[@]}"; do
    parts+=("${NODE_HOSTS[$i]}:${RAFT_PORT}=${NODE_HOSTS[$i]}:${S3_PORT}")
  done

  (
    IFS=,
    printf '%s\n' "${parts[*]}"
  )
}

ensure_local_raftadmin() {
  if [[ -n "$RAFTADMIN_LOCAL_BIN" ]]; then
    if [[ ! -x "$RAFTADMIN_LOCAL_BIN" ]]; then
      echo "RAFTADMIN_BIN is not executable: $RAFTADMIN_LOCAL_BIN" >&2
      exit 1
    fi
    return 0
  fi

  RAFTADMIN_TMP_DIR="$(mktemp -d)"
  echo "[rolling-update] preparing native raftadmin helper build workspace"
  if [[ ! -f "${REPO_ROOT}/cmd/raftadmin/main.go" ]]; then
    echo "failed to locate repo raftadmin helper source at ${REPO_ROOT}/cmd/raftadmin" >&2
    exit 1
  fi
}

build_raftadmin_variant() {
  local goos="$1"
  local goarch="$2"
  local out

  if [[ -n "$RAFTADMIN_LOCAL_BIN" ]]; then
    printf '%s\n' "$RAFTADMIN_LOCAL_BIN"
    return 0
  fi

  out="${RAFTADMIN_TMP_DIR}/raftadmin-${goos}-${goarch}"
  if [[ -x "$out" ]]; then
    printf '%s\n' "$out"
    return 0
  fi

  echo "[rolling-update] building native raftadmin helper for ${goos}/${goarch}" >&2
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
    go build -C "$REPO_ROOT" -o "$out" ./cmd/raftadmin
  chmod +x "$out"
  printf '%s\n' "$out"
}

ensure_remote_raftadmin_binaries() {
  if [[ -n "$RAFTADMIN_LOCAL_BIN" ]]; then
    return 0
  fi
  RAFTADMIN_LINUX_AMD64_BIN="$(build_raftadmin_variant linux amd64)"
  RAFTADMIN_LINUX_ARM64_BIN="$(build_raftadmin_variant linux arm64)"
}

copy_raftadmin_to_remote() {
  local node_id="$1"
  local ssh_target="$2"

  echo "==> [helper@${node_id}] copying raftadmin"

  if [[ -n "$RAFTADMIN_LOCAL_BIN" ]]; then
    scp "${SCP_BASE_OPTS[@]}" "$RAFTADMIN_LOCAL_BIN" "${ssh_target}:${RAFTADMIN_REMOTE_BIN}"
  else
    scp "${SCP_BASE_OPTS[@]}" "$RAFTADMIN_LINUX_AMD64_BIN" "${ssh_target}:${RAFTADMIN_REMOTE_BIN}-amd64"
    scp "${SCP_BASE_OPTS[@]}" "$RAFTADMIN_LINUX_ARM64_BIN" "${ssh_target}:${RAFTADMIN_REMOTE_BIN}-arm64"
  fi

  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    RAFTADMIN_REMOTE_BIN="$RAFTADMIN_REMOTE_BIN" \
    HAS_CUSTOM_RAFTADMIN_BIN="${RAFTADMIN_LOCAL_BIN:+1}" \
    'bash -s' <<'REMOTE_HELPER'
set -euo pipefail

if [[ -n "${HAS_CUSTOM_RAFTADMIN_BIN:-}" ]]; then
  chmod +x "$RAFTADMIN_REMOTE_BIN"
  exit 0
fi

case "$(uname -m)" in
  x86_64|amd64)
    cp "${RAFTADMIN_REMOTE_BIN}-amd64" "$RAFTADMIN_REMOTE_BIN"
    ;;
  aarch64|arm64)
    cp "${RAFTADMIN_REMOTE_BIN}-arm64" "$RAFTADMIN_REMOTE_BIN"
    ;;
  *)
    echo "unsupported remote architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

chmod +x "$RAFTADMIN_REMOTE_BIN"

# Clean up architecture-specific helper binaries after installing the final binary.
rm -f "${RAFTADMIN_REMOTE_BIN}-amd64" "${RAFTADMIN_REMOTE_BIN}-arm64"
REMOTE_HELPER
}

update_one_node() {
  local node_id="$1"
  local node_host="$2"
  local ssh_target="$3"
  local all_node_ids_csv all_node_hosts_csv

  all_node_ids_csv="$(IFS=,; echo "${NODE_IDS[*]}")"
  all_node_hosts_csv="$(IFS=,; echo "${NODE_HOSTS[*]}")"

  echo "==> [$node_id@$node_host] start"

  copy_raftadmin_to_remote "$node_id" "$ssh_target"

  ssh "${SSH_BASE_OPTS[@]}" "$ssh_target" \
    env \
      IMAGE="$IMAGE" \
      RAFTADMIN_BIN_PATH="$RAFTADMIN_REMOTE_BIN" \
      CONTAINER_NAME="$CONTAINER_NAME" \
      DATA_DIR="$DATA_DIR" \
      SERVER_ENTRYPOINT="$SERVER_ENTRYPOINT" \
      RAFT_ENGINE="$RAFT_ENGINE" \
      RAFT_PORT="$RAFT_PORT" \
      REDIS_PORT="$REDIS_PORT" \
      DYNAMO_PORT="$DYNAMO_PORT" \
      S3_PORT="$S3_PORT" \
      ENABLE_S3="$ENABLE_S3" \
      S3_REGION="$S3_REGION" \
      S3_CREDENTIALS_FILE="$S3_CREDENTIALS_FILE" \
      S3_PATH_STYLE_ONLY="$S3_PATH_STYLE_ONLY" \
      HEALTH_TIMEOUT_SECONDS="$HEALTH_TIMEOUT_SECONDS" \
      LEADERSHIP_TRANSFER_TIMEOUT_SECONDS="$LEADERSHIP_TRANSFER_TIMEOUT_SECONDS" \
      LEADER_DISCOVERY_TIMEOUT_SECONDS="$LEADER_DISCOVERY_TIMEOUT_SECONDS" \
      RAFTADMIN_RPC_TIMEOUT_SECONDS="$RAFTADMIN_RPC_TIMEOUT_SECONDS" \
      RAFTADMIN_ALLOW_INSECURE="$RAFTADMIN_ALLOW_INSECURE" \
      NODE_ID="$node_id" \
      NODE_HOST="$node_host" \
      ALL_NODE_IDS_CSV="$all_node_ids_csv" \
      ALL_NODE_HOSTS_CSV="$all_node_hosts_csv" \
      RAFT_TO_REDIS_MAP="$RAFT_TO_REDIS_MAP" \
      RAFT_TO_S3_MAP="$RAFT_TO_S3_MAP" \
      EXTRA_ENV="${EXTRA_ENV:-}" \
      bash -s <<'REMOTE'
set -euo pipefail

IFS=, read -r -a ALL_NODE_IDS <<< "$ALL_NODE_IDS_CSV"
IFS=, read -r -a ALL_NODE_HOSTS <<< "$ALL_NODE_HOSTS_CSV"

grpc_healthy() {
  bash -lc "exec 3<>/dev/tcp/${NODE_HOST}/${RAFT_PORT}" 2>/dev/null
}

peer_grpc_healthy() {
  local peer_host="$1"
  bash -lc "exec 3<>/dev/tcp/${peer_host}/${RAFT_PORT}" 2>/dev/null
}

wait_for_grpc() {
  local i
  for ((i = 0; i < HEALTH_TIMEOUT_SECONDS; i++)); do
    if grpc_healthy; then
      return 0
    fi
    sleep 1
  done
  return 1
}

extract_proto_string() {
  local field="$1"
  local payload="$2"

  printf '%s' "$payload" |
    sed -nE "s/.*${field}:[[:space:]]+\"([^\"]*)\".*/\1/p" |
    tail -n1
}

extract_proto_enum() {
  local field="$1"
  local payload="$2"

  printf '%s' "$payload" |
    sed -nE "s/.*${field}:[[:space:]]+([A-Z_]+).*/\1/p" |
    tail -n1
}

raftadmin_text() {
  local addr="$1"
  shift

  if command -v timeout >/dev/null 2>&1; then
    timeout "${RAFTADMIN_RPC_TIMEOUT_SECONDS}s" "$RAFTADMIN_BIN_PATH" "$addr" "$@" 2>&1
    return $?
  fi

  "$RAFTADMIN_BIN_PATH" "$addr" "$@" 2>&1
}

raft_leader_addr() {
  local addr="$1"
  local output

  output="$(raftadmin_text "$addr" leader)" || return 1
  extract_proto_string "address" "$output"
}

raft_state() {
  local addr="$1"
  local output
  local state

  output="$(raftadmin_text "$addr" state)" || return 1
  state="$(extract_proto_enum "state" "$output")"
  if [[ -z "$state" ]]; then
    printf '%s\n' "FOLLOWER"
    return 0
  fi
  printf '%s\n' "$state"
}

cluster_leader_addr() {
  local i addr state leader

  for i in "${!ALL_NODE_HOSTS[@]}"; do
    addr="${ALL_NODE_HOSTS[$i]}:${RAFT_PORT}"
    state="$(raft_state "$addr" || true)"
    if [[ "$state" == "LEADER" ]]; then
      printf '%s\n' "$addr"
      return 0
    fi
  done

  for i in "${!ALL_NODE_HOSTS[@]}"; do
    addr="${ALL_NODE_HOSTS[$i]}:${RAFT_PORT}"
    leader="$(raft_leader_addr "$addr" || true)"
    if [[ -n "$leader" ]]; then
      printf '%s\n' "$leader"
      return 0
    fi
  done

  return 1
}

wait_for_cluster_leader() {
  local i leader

  for ((i = 0; i < LEADER_DISCOVERY_TIMEOUT_SECONDS; i++)); do
    leader="$(cluster_leader_addr || true)"
    if [[ -n "$leader" ]]; then
      printf '%s\n' "$leader"
      return 0
    fi
    sleep 1
  done

  return 1
}

cluster_reachability_summary() {
  local i addr summary reachable state

  summary=()
  for i in "${!ALL_NODE_HOSTS[@]}"; do
    addr="${ALL_NODE_HOSTS[$i]}:${RAFT_PORT}"
    if peer_grpc_healthy "${ALL_NODE_HOSTS[$i]}"; then
      reachable="up"
      state="$(raft_state "$addr" || echo unknown)"
    else
      reachable="down"
      state="unreachable"
    fi
    summary+=("${ALL_NODE_IDS[$i]}=${addr}:${reachable}:${state}")
  done

  printf '%s\n' "${summary[*]}"
}

choose_transfer_candidate() {
  local i

  for i in "${!ALL_NODE_IDS[@]}"; do
    if [[ "${ALL_NODE_IDS[$i]}" == "$NODE_ID" ]]; then
      continue
    fi
    if peer_grpc_healthy "${ALL_NODE_HOSTS[$i]}"; then
      printf '%s %s\n' "${ALL_NODE_IDS[$i]}" "${ALL_NODE_HOSTS[$i]}"
      return 0
    fi
  done

  return 1
}

wait_for_leader_change() {
  local old_leader="$1"
  local expected_leader="${2:-}"
  local i leader

  for ((i = 0; i < LEADERSHIP_TRANSFER_TIMEOUT_SECONDS; i++)); do
    leader="$(cluster_leader_addr || true)"
    if [[ -n "$leader" && "$leader" != "$old_leader" ]]; then
      if [[ -n "$expected_leader" && "$leader" != "$expected_leader" ]]; then
        echo "leadership moved away from $old_leader, but elected $leader instead of preferred $expected_leader"
      else
        echo "leadership moved from $old_leader to $leader"
      fi
      return 0
    fi
    sleep 1
  done

  return 1
}

ensure_not_leader_before_restart() {
  local current_leader candidate_id candidate_host candidate_addr rpc_output local_state

  current_leader="$(wait_for_cluster_leader || true)"
  if [[ -z "$current_leader" ]]; then
    local_state="$(raft_state "${NODE_HOST}:${RAFT_PORT}" || echo unknown)"
    echo "unable to determine current cluster leader within ${LEADER_DISCOVERY_TIMEOUT_SECONDS}s; refusing to restart $NODE_ID safely" >&2
    echo "local raft state on ${NODE_HOST}:${RAFT_PORT}: ${local_state}" >&2
    echo "cluster reachability: $(cluster_reachability_summary)" >&2
    return 1
  fi

  if [[ "$current_leader" != "${NODE_HOST}:${RAFT_PORT}" ]]; then
    echo "node is not leader ($current_leader); safe to restart"
    return 0
  fi

  if ! grpc_healthy; then
    echo "node is current leader but its local gRPC endpoint is unreachable; refusing restart" >&2
    return 1
  fi

  if ! read -r candidate_id candidate_host < <(choose_transfer_candidate); then
    echo "node is leader but no healthy peer is available as transfer target" >&2
    return 1
  fi
  candidate_addr="${candidate_host}:${RAFT_PORT}"

  echo "node is leader; transferring leadership to ${candidate_id}@${candidate_addr}"
  rpc_output="$(raftadmin_text "${NODE_HOST}:${RAFT_PORT}" leadership_transfer_to_server "${candidate_id}" "${candidate_addr}")" || {
    echo "targeted leadership transfer RPC failed: $rpc_output" >&2
    echo "falling back to generic leadership transfer"
    rpc_output="$(raftadmin_text "${NODE_HOST}:${RAFT_PORT}" leadership_transfer)" || {
      echo "generic leadership transfer RPC failed: $rpc_output" >&2
      return 1
    }
    candidate_addr=""
  }

  if ! wait_for_leader_change "${NODE_HOST}:${RAFT_PORT}" "$candidate_addr"; then
    echo "leadership did not move away from ${NODE_HOST}:${RAFT_PORT} within ${LEADERSHIP_TRANSFER_TIMEOUT_SECONDS}s" >&2
    return 1
  fi

  return 0
}

stop_container() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}

run_container() {
  local s3_creds_volume=()
  local s3_creds_flag=()
  local s3_flags=()
  if [[ "${ENABLE_S3}" == "true" && -n "${S3_CREDENTIALS_FILE:-}" ]]; then
    if [[ ! -f "$S3_CREDENTIALS_FILE" || ! -r "$S3_CREDENTIALS_FILE" ]]; then
      echo "S3_CREDENTIALS_FILE is set to '$S3_CREDENTIALS_FILE' but the file is missing or not readable; aborting docker run" >&2
      exit 1
    fi
    s3_creds_volume=(-v "${S3_CREDENTIALS_FILE}:${S3_CREDENTIALS_FILE}:ro")
    s3_creds_flag=(--s3CredentialsFile "$S3_CREDENTIALS_FILE")
  fi
  if [[ "${ENABLE_S3}" == "true" ]]; then
    s3_flags=(
      --s3Address "${NODE_HOST}:${S3_PORT}"
      --s3Region "$S3_REGION"
      --s3PathStyleOnly="$S3_PATH_STYLE_ONLY"
      --raftS3Map "$RAFT_TO_S3_MAP"
      "${s3_creds_flag[@]}"
    )
  fi

  # Pass through additional container environment variables from EXTRA_ENV.
  # Accepts a whitespace-separated list of KEY=VALUE pairs, e.g.:
  #   EXTRA_ENV="ELASTICKV_RAFT_DISPATCHER_LANES=1 ELASTICKV_PEBBLE_CACHE_MB=512"
  # Each pair is forwarded as a single `-e KEY=VALUE` flag so VALUE may contain
  # characters that bash would otherwise interpret; pairs themselves must not
  # contain whitespace.
  local extra_env_flags=()
  if [[ -n "${EXTRA_ENV:-}" ]]; then
    # shellcheck disable=SC2206
    local -a extra_env_pairs=( ${EXTRA_ENV} )
    local pair
    for pair in "${extra_env_pairs[@]}"; do
      extra_env_flags+=(-e "$pair")
    done
  fi

  docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --network host \
    -v "$DATA_DIR:$DATA_DIR" \
    "${s3_creds_volume[@]}" \
    "${extra_env_flags[@]}" \
    "$IMAGE" "$SERVER_ENTRYPOINT" \
    --address "${NODE_HOST}:${RAFT_PORT}" \
    --redisAddress "${NODE_HOST}:${REDIS_PORT}" \
    --dynamoAddress "${NODE_HOST}:${DYNAMO_PORT}" \
    --raftId "$NODE_ID" \
    --raftEngine "$RAFT_ENGINE" \
    --raftDataDir "$DATA_DIR" \
    --raftRedisMap "$RAFT_TO_REDIS_MAP" \
    "${s3_flags[@]}" >/dev/null
}

require_passwordless_sudo() {
  if ! sudo -n true 2>/dev/null; then
    echo "error: passwordless sudo is required on this host; configure NOPASSWD sudo for the remote user" >&2
    exit 1
  fi
}

archive_legacy_dir() {
  local dir="$1"
  local ts backup_dir moved

  moved=0
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  backup_dir="${dir%/}/legacy-boltdb-${ts}"

  sudo -n mkdir -p "$backup_dir"
  for name in logs.dat stable.dat; do
    if sudo -n test -e "$dir/$name"; then
      sudo -n mv "$dir/$name" "$backup_dir/$name"
      moved=1
    fi
  done

  if [[ "$moved" -eq 1 ]]; then
    echo "archived legacy raft files from $dir to $backup_dir; node will resync from the cluster"
    return 0
  fi

  sudo -n rmdir "$backup_dir" 2>/dev/null || true
  return 1
}

archive_default_legacy_dir() {
  local node_data_dir

  node_data_dir="${DATA_DIR%/}/${NODE_ID}"
  if sudo -n test -d "$node_data_dir"; then
    archive_legacy_dir "$node_data_dir" || true
    sudo -n rm -rf "${node_data_dir}/raft.db.migrating" 2>/dev/null || true
  fi
}

archive_legacy_dirs_from_logs() {
  local logs="$1"
  local found=0
  local dir

  while IFS= read -r dir; do
    [[ -n "$dir" ]] || continue
    archive_legacy_dir "$dir" || true
    sudo -n rm -rf "${dir}/raft.db.migrating" 2>/dev/null || true
    found=1
  done < <(
    printf '%s\n' "$logs" |
      sed -nE 's/.*legacy boltdb Raft storage "[^"]+" found in ([^;]+);.*/\1/p' |
      sort -u
  )

  [[ "$found" -eq 1 ]]
}

docker pull "$IMAGE" >/dev/null
new_image_id="$(docker image inspect "$IMAGE" --format "{{.Id}}")"
running_image_id="$(docker inspect --format "{{.Image}}" "$CONTAINER_NAME" 2>/dev/null || true)"
running_status="$(docker inspect --format "{{.State.Status}}" "$CONTAINER_NAME" 2>/dev/null || echo missing)"

if [[ "$new_image_id" == "$running_image_id" && "$running_status" == "running" ]]; then
  if grpc_healthy; then
    echo "image unchanged and gRPC healthy; skip"
    exit 0
  fi
  echo "container is running but gRPC is not reachable; recreating"
fi

require_passwordless_sudo
sudo -n mkdir -p "$DATA_DIR"
if [[ "$running_status" == "running" ]]; then
  ensure_not_leader_before_restart
fi
stop_container
archive_default_legacy_dir
run_container

if ! wait_for_grpc; then
  logs="$(docker logs --tail 200 "$CONTAINER_NAME" 2>&1 || true)"
  if printf '%s\n' "$logs" | grep -q 'legacy boltdb Raft storage'; then
    echo "detected legacy BoltDB raft storage in container logs; archiving and retrying"
    stop_container
    if archive_legacy_dirs_from_logs "$logs"; then
      run_container
      if wait_for_grpc; then
        echo "updated successfully"
        exit 0
      fi
      echo "gRPC port did not come up on ${NODE_HOST}:${RAFT_PORT} after legacy cleanup retry" >&2
      docker logs --tail 200 "$CONTAINER_NAME" || true
      exit 1
    fi
  fi

  echo "gRPC port did not come up on ${NODE_HOST}:${RAFT_PORT}" >&2
  printf '%s\n' "$logs" >&2
  exit 1
fi

echo "updated successfully"
REMOTE

  echo "==> [$node_id@$node_host] done"
}

parse_nodes
prepare_rolling_order

if [[ -z "$RAFT_TO_REDIS_MAP" ]]; then
  RAFT_TO_REDIS_MAP="$(derive_raft_to_redis_map)"
fi

if [[ "${ENABLE_S3}" == "true" && -z "$RAFT_TO_S3_MAP" ]]; then
  RAFT_TO_S3_MAP="$(derive_raft_to_s3_map)"
fi

ensure_local_raftadmin
ensure_remote_raftadmin_binaries

echo "[rolling-update] target image: $IMAGE"
for node_id in "${ROLLING_NODE_IDS[@]}"; do
  update_one_node "$node_id" "$(node_host_by_id "$node_id")" "$(ssh_target_by_id "$node_id")"
  sleep "$ROLLING_DELAY_SECONDS"
done

echo "[rolling-update] all nodes completed"
