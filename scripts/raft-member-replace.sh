#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  TARGET_NODE=n2 ROLLING_UPDATE_ENV_FILE=deploy.env \
    REPLACEMENT_CONFIRM=n2 ./scripts/raft-member-replace.sh --execute
  TARGET_NODE=n2 ROLLING_UPDATE_ENV_FILE=deploy.env \
    ./scripts/raft-member-replace.sh --dry-run

This command replaces one voter in one Raft group by fencing the old process,
removing its membership, archiving its data, joining a fresh learner, waiting
for catch-up, promoting it, and restarting it without join flags.

Required environment:
  TARGET_NODE
    Raft ID to replace. It must be a current voter and exist in NODES.
  NODES
    Same ID-to-host map used by scripts/rolling-update.sh. It may be supplied
    through ROLLING_UPDATE_ENV_FILE.
  REPLACEMENT_CONFIRM
    Must exactly match TARGET_NODE for --execute.
  REPLACEMENT_VERIFY_COMMAND
    Application-level write/read verification command. It runs after the
    replacement is a stable voter. The command must return zero.

Fencing:
  REPLACEMENT_FENCE_MODE=external (default)
    Requires REPLACEMENT_FENCE_COMMAND. The command must prevent the old VM or
    process from returning with stale data. The target Raft RPC must become
    unreachable before membership removal.
  REPLACEMENT_FENCE_MODE=container
    Stops CONTAINER_NAME over SSH. Use only when the host lifecycle guarantees
    the stopped container cannot restart independently during the operation.

Optional environment:
  ROLLING_UPDATE_ENV_FILE       Env file used by rolling-update.sh.
  ROLLING_UPDATE_SCRIPT         Default: scripts/rolling-update.sh.
  RAFTADMIN_BIN                 Default: build ./cmd/raftadmin locally.
  SSH_BIN                       Default: ssh.
  SSH_TARGETS, SSH_USER         Same meaning as rolling-update.sh.
  RAFT_PORT                     Default: 50051.
  CONTAINER_NAME                Default: elastickv.
  DATA_DIR                      Default: /var/lib/elastickv.
  REPLACEMENT_STATE_FILE        Durable local resume state.
  REPLACEMENT_DATA_MODE         archive (default) or delete.
  REPLACEMENT_FENCE_COMMAND     Required for external fencing.
  REPLACEMENT_FENCE_VERIFY_SECONDS
                                Consecutive target downtime required (default: 3).
  REPLACEMENT_STABILITY_SECONDS Default: 10.
  REPLACEMENT_TIMEOUT_SECONDS   Default: 120.
  REPLACEMENT_POLL_SECONDS      Default: 1.
  REPLACEMENT_KEEP_STATE        Keep completed state when true (default).
  RAFTADMIN_ALLOW_INSECURE      Default: true.
  RAFTADMIN_RPC_TIMEOUT_SECONDS Default: 5.
  SSH_CONNECT_TIMEOUT_SECONDS   Default: 10.
  SSH_STRICT_HOST_KEY_CHECKING  Default: accept-new.

The state file makes the sequence resumable. A resumed run validates the live
membership against its recorded stage and fails closed on conflicting changes.
No-quorum recovery and multi-group replacement are outside this command.
EOF
}

MODE=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --execute)
      [[ -z "$MODE" ]] || { echo "choose exactly one mode" >&2; exit 1; }
      MODE="execute"
      ;;
    --dry-run)
      [[ -z "$MODE" ]] || { echo "choose exactly one mode" >&2; exit 1; }
      MODE="dry-run"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [[ -z "$MODE" ]]; then
  echo "--execute or --dry-run is required" >&2
  usage >&2
  exit 1
fi

# The deployment env is inventory, while explicitly exported replacement
# controls are the operator's command for this run. Preserve the latter across
# sourcing so an old commented-in recovery value cannot retarget an operation.
INPUT_TARGET_NODE_SET="${TARGET_NODE+x}"
INPUT_TARGET_NODE="${TARGET_NODE:-}"
INPUT_REPLACEMENT_CONFIRM_SET="${REPLACEMENT_CONFIRM+x}"
INPUT_REPLACEMENT_CONFIRM="${REPLACEMENT_CONFIRM:-}"
INPUT_REPLACEMENT_STATE_FILE_SET="${REPLACEMENT_STATE_FILE+x}"
INPUT_REPLACEMENT_STATE_FILE="${REPLACEMENT_STATE_FILE:-}"
INPUT_REPLACEMENT_DATA_MODE_SET="${REPLACEMENT_DATA_MODE+x}"
INPUT_REPLACEMENT_DATA_MODE="${REPLACEMENT_DATA_MODE:-}"
INPUT_REPLACEMENT_FENCE_MODE_SET="${REPLACEMENT_FENCE_MODE+x}"
INPUT_REPLACEMENT_FENCE_MODE="${REPLACEMENT_FENCE_MODE:-}"
INPUT_REPLACEMENT_FENCE_COMMAND_SET="${REPLACEMENT_FENCE_COMMAND+x}"
INPUT_REPLACEMENT_FENCE_COMMAND="${REPLACEMENT_FENCE_COMMAND:-}"
INPUT_REPLACEMENT_VERIFY_COMMAND_SET="${REPLACEMENT_VERIFY_COMMAND+x}"
INPUT_REPLACEMENT_VERIFY_COMMAND="${REPLACEMENT_VERIFY_COMMAND:-}"

if [[ -n "${ROLLING_UPDATE_ENV_FILE:-}" ]]; then
  if [[ ! -f "$ROLLING_UPDATE_ENV_FILE" ]]; then
    echo "ROLLING_UPDATE_ENV_FILE not found: $ROLLING_UPDATE_ENV_FILE" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$ROLLING_UPDATE_ENV_FILE"
fi

[[ -z "$INPUT_TARGET_NODE_SET" ]] || TARGET_NODE="$INPUT_TARGET_NODE"
[[ -z "$INPUT_REPLACEMENT_CONFIRM_SET" ]] || REPLACEMENT_CONFIRM="$INPUT_REPLACEMENT_CONFIRM"
[[ -z "$INPUT_REPLACEMENT_STATE_FILE_SET" ]] || REPLACEMENT_STATE_FILE="$INPUT_REPLACEMENT_STATE_FILE"
[[ -z "$INPUT_REPLACEMENT_DATA_MODE_SET" ]] || REPLACEMENT_DATA_MODE="$INPUT_REPLACEMENT_DATA_MODE"
[[ -z "$INPUT_REPLACEMENT_FENCE_MODE_SET" ]] || REPLACEMENT_FENCE_MODE="$INPUT_REPLACEMENT_FENCE_MODE"
[[ -z "$INPUT_REPLACEMENT_FENCE_COMMAND_SET" ]] || REPLACEMENT_FENCE_COMMAND="$INPUT_REPLACEMENT_FENCE_COMMAND"
[[ -z "$INPUT_REPLACEMENT_VERIFY_COMMAND_SET" ]] || REPLACEMENT_VERIFY_COMMAND="$INPUT_REPLACEMENT_VERIFY_COMMAND"

TARGET_NODE="${TARGET_NODE:-}"
NODES="${NODES:-}"
SSH_TARGETS="${SSH_TARGETS:-}"
SSH_USER="${SSH_USER:-${USER:-$(id -un)}}"
RAFT_PORT="${RAFT_PORT:-50051}"
CONTAINER_NAME="${CONTAINER_NAME:-elastickv}"
DATA_DIR="${DATA_DIR:-/var/lib/elastickv}"
RAFTADMIN_BIN="${RAFTADMIN_BIN:-}"
RAFTADMIN_ALLOW_INSECURE="${RAFTADMIN_ALLOW_INSECURE:-true}"
RAFTADMIN_RPC_TIMEOUT_SECONDS="${RAFTADMIN_RPC_TIMEOUT_SECONDS:-5}"
ROLLING_UPDATE_SCRIPT="${ROLLING_UPDATE_SCRIPT:-${SCRIPT_DIR}/rolling-update.sh}"
SSH_BIN="${SSH_BIN:-ssh}"
SSH_CONNECT_TIMEOUT_SECONDS="${SSH_CONNECT_TIMEOUT_SECONDS:-10}"
SSH_STRICT_HOST_KEY_CHECKING="${SSH_STRICT_HOST_KEY_CHECKING:-accept-new}"
REPLACEMENT_CONFIRM="${REPLACEMENT_CONFIRM:-}"
REPLACEMENT_FENCE_MODE="${REPLACEMENT_FENCE_MODE:-external}"
REPLACEMENT_FENCE_COMMAND="${REPLACEMENT_FENCE_COMMAND:-}"
REPLACEMENT_FENCE_VERIFY_SECONDS="${REPLACEMENT_FENCE_VERIFY_SECONDS:-3}"
REPLACEMENT_DATA_MODE="${REPLACEMENT_DATA_MODE:-archive}"
REPLACEMENT_VERIFY_COMMAND="${REPLACEMENT_VERIFY_COMMAND:-}"
REPLACEMENT_STABILITY_SECONDS="${REPLACEMENT_STABILITY_SECONDS:-10}"
REPLACEMENT_TIMEOUT_SECONDS="${REPLACEMENT_TIMEOUT_SECONDS:-120}"
REPLACEMENT_POLL_SECONDS="${REPLACEMENT_POLL_SECONDS:-1}"
REPLACEMENT_KEEP_STATE="${REPLACEMENT_KEEP_STATE:-true}"

NODE_IDS=()
NODE_HOSTS=()
STATE_STAGE=0
STATE_TARGET=""
STATE_TARGET_ADDRESS=""
STATE_OPERATION_ID=""
STATE_ARCHIVE_PATH=""
STATE_EXPECTED_VOTERS=""
STATE_MIN_CATCH_UP_INDEX=0
STATE_DATA_MODE=""
RAFTADMIN_TMP_DIR=""
LOCK_DIR=""
LOCK_ACQUIRED=false

log() {
  printf '[raft-member-replace] %s\n' "$*"
}

fail() {
  printf '[raft-member-replace] error: %s\n' "$*" >&2
  exit 1
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

validate_settings() {
  [[ -n "$TARGET_NODE" ]] || fail "TARGET_NODE is required"
  [[ "$TARGET_NODE" =~ ^[A-Za-z0-9._-]+$ ]] || fail "TARGET_NODE contains unsupported characters"
  [[ -n "$NODES" ]] || fail "NODES is required"
  [[ "$DATA_DIR" == /* ]] || fail "DATA_DIR must be absolute"
  state_value_valid "$DATA_DIR" || fail "DATA_DIR contains characters unsupported by the resume state"
  [[ -x "$ROLLING_UPDATE_SCRIPT" ]] || fail "ROLLING_UPDATE_SCRIPT is not executable: $ROLLING_UPDATE_SCRIPT"
  is_uint "$RAFT_PORT" || fail "RAFT_PORT must be an unsigned integer"
  is_uint "$REPLACEMENT_STABILITY_SECONDS" || fail "REPLACEMENT_STABILITY_SECONDS must be an unsigned integer"
  is_uint "$REPLACEMENT_FENCE_VERIFY_SECONDS" || fail "REPLACEMENT_FENCE_VERIFY_SECONDS must be an unsigned integer"
  is_uint "$REPLACEMENT_TIMEOUT_SECONDS" || fail "REPLACEMENT_TIMEOUT_SECONDS must be an unsigned integer"
  is_uint "$REPLACEMENT_POLL_SECONDS" || fail "REPLACEMENT_POLL_SECONDS must be an unsigned integer"
  (( REPLACEMENT_TIMEOUT_SECONDS > 0 )) || fail "REPLACEMENT_TIMEOUT_SECONDS must be positive"
  (( REPLACEMENT_POLL_SECONDS > 0 )) || fail "REPLACEMENT_POLL_SECONDS must be positive"
  (( REPLACEMENT_FENCE_VERIFY_SECONDS > 0 )) || fail "REPLACEMENT_FENCE_VERIFY_SECONDS must be positive"
  case "$REPLACEMENT_FENCE_MODE" in
    external)
      if [[ "$MODE" == "execute" && -z "$REPLACEMENT_FENCE_COMMAND" ]]; then
        fail "REPLACEMENT_FENCE_COMMAND is required for external fencing"
      fi
      ;;
    container) ;;
    *) fail "REPLACEMENT_FENCE_MODE must be external or container" ;;
  esac
  case "$REPLACEMENT_DATA_MODE" in
    archive|delete) ;;
    *) fail "REPLACEMENT_DATA_MODE must be archive or delete" ;;
  esac
  if [[ "$MODE" == "execute" ]]; then
    [[ "$REPLACEMENT_CONFIRM" == "$TARGET_NODE" ]] || fail "REPLACEMENT_CONFIRM must exactly match TARGET_NODE"
    [[ -n "$REPLACEMENT_VERIFY_COMMAND" ]] || fail "REPLACEMENT_VERIFY_COMMAND is required for --execute"
  fi
}

contains_value() {
  local needle="$1"
  shift
  local value
  for value in "$@"; do
    [[ "$value" == "$needle" ]] && return 0
  done
  return 1
}

lookup_mapping() {
  local key="$1"
  local mapping="$2"
  local pair entry_key entry_value
  local -a pairs=()

  [[ -n "$mapping" ]] || return 1
  IFS=',' read -r -a pairs <<< "$mapping"
  for pair in "${pairs[@]}"; do
    pair="${pair//[[:space:]]/}"
    [[ -n "$pair" && "$pair" == *=* ]] || continue
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
  local -a pairs=()

  IFS=',' read -r -a pairs <<< "$NODES"
  for pair in "${pairs[@]}"; do
    pair="${pair//[[:space:]]/}"
    [[ -n "$pair" ]] || continue
    [[ "$pair" == *=* ]] || fail "invalid NODES entry: $pair"
    node_id="${pair%%=*}"
    node_host="${pair#*=}"
    [[ -n "$node_id" && -n "$node_host" ]] || fail "invalid NODES entry: $pair"
    [[ "$node_id" =~ ^[A-Za-z0-9._-]+$ ]] || fail "raft ID contains unsupported characters: $node_id"
    [[ "$node_host" != *'|'* ]] || fail "raft host contains unsupported characters: $node_host"
    contains_value "$node_id" "${NODE_IDS[@]}" && fail "duplicate raft ID in NODES: $node_id"
    NODE_IDS+=("$node_id")
    NODE_HOSTS+=("$node_host")
  done
  (( ${#NODE_IDS[@]} > 0 )) || fail "NODES did not contain any nodes"
  contains_value "$TARGET_NODE" "${NODE_IDS[@]}" || fail "TARGET_NODE is not present in NODES: $TARGET_NODE"
}

node_host_by_id() {
  local wanted="$1"
  local i
  for i in "${!NODE_IDS[@]}"; do
    if [[ "${NODE_IDS[$i]}" == "$wanted" ]]; then
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
  [[ -n "$target" ]] || target="$(node_host_by_id "$node_id")"
  if [[ "$target" == *@* ]]; then
    printf '%s\n' "$target"
  else
    printf '%s@%s\n' "$SSH_USER" "$target"
  fi
}

cleanup() {
  [[ -z "$RAFTADMIN_TMP_DIR" || ! -d "$RAFTADMIN_TMP_DIR" ]] || rm -rf "$RAFTADMIN_TMP_DIR"
  [[ "$LOCK_ACQUIRED" != "true" || -z "$LOCK_DIR" || ! -d "$LOCK_DIR" ]] || rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

ensure_raftadmin() {
  if [[ -n "$RAFTADMIN_BIN" ]]; then
    [[ -x "$RAFTADMIN_BIN" ]] || fail "RAFTADMIN_BIN is not executable: $RAFTADMIN_BIN"
    return 0
  fi
  RAFTADMIN_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/raft-member-replace.XXXXXX")"
  RAFTADMIN_BIN="${RAFTADMIN_TMP_DIR}/raftadmin"
  log "building raftadmin"
  (cd "$REPO_ROOT" && go build -o "$RAFTADMIN_BIN" ./cmd/raftadmin)
}

raftadmin() {
  RAFTADMIN_ALLOW_INSECURE="$RAFTADMIN_ALLOW_INSECURE" \
    RAFTADMIN_RPC_TIMEOUT_SECONDS="$RAFTADMIN_RPC_TIMEOUT_SECONDS" \
    "$RAFTADMIN_BIN" "$@"
}

field_value() {
  local field="$1"
  awk -F ': ' -v field="$field" '$1 == field { value=$2; gsub(/^"|"$/, "", value); print value; exit }'
}

configuration_rows() {
  awk '
    /^servers \{/ { id=""; address=""; suffrage=""; in_server=1; next }
    in_server && /^[[:space:]]+id:/ {
      value=$0; sub(/^[^:]*:[[:space:]]*"/, "", value); sub(/"[[:space:]]*$/, "", value); id=value; next
    }
    in_server && /^[[:space:]]+address:/ {
      value=$0; sub(/^[^:]*:[[:space:]]*"/, "", value); sub(/"[[:space:]]*$/, "", value); address=value; next
    }
    in_server && /^[[:space:]]+suffrage:/ {
      value=$0; sub(/^[^:]*:[[:space:]]*"/, "", value); sub(/"[[:space:]]*$/, "", value); suffrage=value; next
    }
    in_server && /^}/ { print id "|" address "|" suffrage; in_server=0 }
  '
}

status_for() {
  raftadmin "$1" status 2>/dev/null
}

discover_leader() {
  local address status state leader_address leader_id
  local i
  for i in "${!NODE_HOSTS[@]}"; do
    address="${NODE_HOSTS[$i]}:${RAFT_PORT}"
    status="$(status_for "$address" || true)"
    [[ -n "$status" ]] || continue
    state="$(printf '%s\n' "$status" | field_value state)"
    if [[ "$state" == "LEADER" ]]; then
      printf '%s|%s\n' "${NODE_IDS[$i]}" "$address"
      return 0
    fi
    leader_id="$(printf '%s\n' "$status" | field_value leader_id)"
    leader_address="$(printf '%s\n' "$status" | field_value leader_address)"
    if [[ -n "$leader_id" && -n "$leader_address" ]]; then
      printf '%s|%s\n' "$leader_id" "$leader_address"
      return 0
    fi
  done
  return 1
}

configuration_from_leader() {
  local leader leader_address
  leader="$(discover_leader)" || return 1
  leader_address="${leader#*|}"
  raftadmin "$leader_address" configuration
}

member_suffrage() {
  local wanted="$1"
  local config="$2"
  local id address suffrage
  while IFS='|' read -r id address suffrage; do
    if [[ "$id" == "$wanted" ]]; then
      printf '%s\n' "$suffrage"
      return 0
    fi
  done < <(printf '%s\n' "$config" | configuration_rows)
  return 1
}

member_address() {
  local wanted="$1"
  local config="$2"
  local id address suffrage
  while IFS='|' read -r id address suffrage; do
    if [[ "$id" == "$wanted" ]]; then
      printf '%s\n' "$address"
      return 0
    fi
  done < <(printf '%s\n' "$config" | configuration_rows)
  return 1
}

voter_csv() {
  local config="$1"
  local id address suffrage result=""
  while IFS='|' read -r id address suffrage; do
    [[ "$suffrage" == "voter" ]] || continue
    result+="${result:+,}${id}"
  done < <(printf '%s\n' "$config" | configuration_rows)
  printf '%s\n' "$result"
}

same_csv_set() {
  local left="$1"
  local right="$2"
  [[ "$(printf '%s' "$left" | tr ',' '\n' | sed '/^$/d' | sort | tr '\n' ',')" == \
     "$(printf '%s' "$right" | tr ',' '\n' | sed '/^$/d' | sort | tr '\n' ',')" ]]
}

leader_status_ready() {
  local address="$1"
  local status pending config_index commit applied
  status="$(status_for "$address")" || return 1
  pending="$(printf '%s\n' "$status" | field_value pending_conf_change)"
  config_index="$(printf '%s\n' "$status" | field_value configuration_index)"
  commit="$(printf '%s\n' "$status" | field_value commit_index)"
  applied="$(printf '%s\n' "$status" | field_value applied_index)"
  [[ "$pending" == "false" ]] || return 1
  is_uint "$config_index" && (( config_index > 0 )) || return 1
  is_uint "$commit" && is_uint "$applied" && (( applied >= commit )) || return 1
  printf '%s|%s|%s\n' "$config_index" "$commit" "$applied"
}

select_transfer_candidate() {
  local config="$1"
  local required_index="$2"
  local expected_leader="$3"
  local id address suffrage status state applied observed_leader
  while IFS='|' read -r id address suffrage; do
    [[ "$suffrage" == "voter" && "$id" != "$TARGET_NODE" ]] || continue
    status="$(status_for "$address" || true)"
    [[ -n "$status" ]] || continue
    state="$(printf '%s\n' "$status" | field_value state)"
    applied="$(printf '%s\n' "$status" | field_value applied_index)"
    observed_leader="$(printf '%s\n' "$status" | field_value leader_id)"
    if [[ "$state" == "FOLLOWER" && "$observed_leader" == "$expected_leader" ]] && \
      is_uint "$applied" && (( applied >= required_index )); then
      printf '%s|%s\n' "$id" "$address"
      return 0
    fi
  done < <(printf '%s\n' "$config" | configuration_rows)
  return 1
}

preflight() {
  local leader leader_id leader_address config target_address target_suffrage
  local ready config_index commit applied voters quorum reachable=0
  local id address suffrage voter_status voter_state voter_leader

  leader="$(discover_leader)" || fail "no reachable Raft leader; no-quorum recovery is not supported"
  leader_id="${leader%%|*}"
  leader_address="${leader#*|}"
  ready="$(leader_status_ready "$leader_address")" || fail "leader is not fully applied or has a pending configuration change"
  IFS='|' read -r config_index commit applied <<< "$ready"
  config="$(raftadmin "$leader_address" configuration)" || fail "cannot read leader configuration"
  target_suffrage="$(member_suffrage "$TARGET_NODE" "$config" || true)"
  target_address="$(member_address "$TARGET_NODE" "$config" || true)"
  [[ "$target_suffrage" == "voter" ]] || fail "target must be a current voter; got ${target_suffrage:-absent}"
  [[ "$target_address" == "$(node_host_by_id "$TARGET_NODE"):${RAFT_PORT}" ]] || \
    fail "target address mismatch: membership=$target_address inventory=$(node_host_by_id "$TARGET_NODE"):${RAFT_PORT}"

  voters=0
  while IFS='|' read -r id address suffrage; do
    [[ "$suffrage" == "voter" ]] || continue
    voters=$((voters + 1))
    [[ "$id" == "$TARGET_NODE" ]] && continue
    voter_status="$(status_for "$address" || true)"
    voter_state="$(printf '%s\n' "$voter_status" | field_value state)"
    voter_leader="$(printf '%s\n' "$voter_status" | field_value leader_id)"
    if [[ "$voter_state" == "LEADER" && "$id" == "$leader_id" ]] || \
      [[ "$voter_state" == "FOLLOWER" && "$voter_leader" == "$leader_id" ]]; then
      reachable=$((reachable + 1))
    fi
  done < <(printf '%s\n' "$config" | configuration_rows)
  quorum=$((voters / 2 + 1))
  (( reachable >= quorum )) || fail "only $reachable non-target voters are reachable; $quorum are required to commit removal"

  if [[ "$leader_id" == "$TARGET_NODE" ]]; then
    local candidate candidate_id candidate_address
    candidate="$(select_transfer_candidate "$config" "$commit" "$leader_id")" || fail "target is leader and no caught-up transfer candidate exists"
    candidate_id="${candidate%%|*}"
    candidate_address="${candidate#*|}"
    log "transferring leadership from $TARGET_NODE to $candidate_id"
    raftadmin "$leader_address" leadership_transfer_to_server "$candidate_id" "$candidate_address" >/dev/null
    wait_for_leader "$candidate_id" >/dev/null || fail "leadership did not transfer to $candidate_id"
  fi

  STATE_EXPECTED_VOTERS="$(voter_csv "$config")"
  STATE_TARGET_ADDRESS="$target_address"
  log "preflight passed: leader=$leader_id leader_applied=$applied voters=$voters non_target_reachable=$reachable quorum=$quorum config_index=$config_index"
}

wait_for_leader() {
  local expected_id="${1:-}"
  local elapsed=0 leader leader_id
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    leader="$(discover_leader || true)"
    if [[ -n "$leader" ]]; then
      leader_id="${leader%%|*}"
      if [[ -z "$expected_id" || "$leader_id" == "$expected_id" ]]; then
        printf '%s\n' "$leader"
        return 0
      fi
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

wait_target_down() {
  local elapsed=0 down_for=0
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    if ! status_for "$STATE_TARGET_ADDRESS" >/dev/null; then
      down_for=$((down_for + REPLACEMENT_POLL_SECONDS))
      if (( down_for >= REPLACEMENT_FENCE_VERIFY_SECONDS )); then
        return 0
      fi
    else
      down_for=0
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

wait_target_up() {
  local elapsed=0
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    if status_for "$STATE_TARGET_ADDRESS" >/dev/null; then
      return 0
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

run_operator_command() {
  local command="$1"
  local operator_target_node="$TARGET_NODE"
  local operator_target_address="$STATE_TARGET_ADDRESS"
  local operator_target_ssh
  operator_target_ssh="$(ssh_target_by_id "$TARGET_NODE")"
  (
    if [[ -n "${ROLLING_UPDATE_ENV_FILE:-}" ]]; then
      set -a
      # shellcheck disable=SC1090
      source "$ROLLING_UPDATE_ENV_FILE"
      set +a
    fi
    env \
      TARGET_NODE="$operator_target_node" \
      TARGET_ADDRESS="$operator_target_address" \
      TARGET_SSH="$operator_target_ssh" \
      bash -lc "$command"
  )
}

remote_target_action() {
  local action="$1"
  local ssh_target
  ssh_target="$(ssh_target_by_id "$TARGET_NODE")"
  "$SSH_BIN" \
    -o BatchMode=yes \
    -o ConnectTimeout="$SSH_CONNECT_TIMEOUT_SECONDS" \
    -o StrictHostKeyChecking="$SSH_STRICT_HOST_KEY_CHECKING" \
    "$ssh_target" bash -s -- "$action" "$CONTAINER_NAME" "$DATA_DIR" "$STATE_ARCHIVE_PATH" "$REPLACEMENT_DATA_MODE" <<'REMOTE'
set -euo pipefail
action="$1"
container="$2"
data_dir="$3"
archive_path="$4"
data_mode="$5"

sudo_cmd=()
if [[ "$(id -u)" -ne 0 ]] && command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
  sudo_cmd=(sudo -n)
fi

case "$action" in
  stop)
    docker stop "$container" >/dev/null 2>&1 || true
    running="$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || echo false)"
    [[ "$running" == "false" ]]
    ;;
  reset-data)
    docker_running="$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || echo false)"
    [[ "$docker_running" == "false" ]]
    if [[ "$data_mode" == "archive" ]]; then
      if [[ -e "$archive_path" && -e "$data_dir" ]]; then
        echo "both data and archive paths exist: $data_dir $archive_path" >&2
        exit 1
      fi
      if [[ -e "$data_dir" ]]; then
        "${sudo_cmd[@]}" mv "$data_dir" "$archive_path"
      fi
      [[ -e "$archive_path" ]] || { echo "target data directory was absent and no archive exists" >&2; exit 1; }
    else
      if [[ -e "$data_dir" ]]; then
        "${sudo_cmd[@]}" rm -rf --one-file-system "$data_dir"
      fi
      [[ ! -e "$data_dir" ]]
    fi
    ;;
  *)
    echo "unknown remote action: $action" >&2
    exit 1
    ;;
esac
REMOTE
}

fence_target() {
  case "$REPLACEMENT_FENCE_MODE" in
    external)
      log "running external fence for $TARGET_NODE"
      run_operator_command "$REPLACEMENT_FENCE_COMMAND"
      ;;
    container)
      log "stopping target container; host lifecycle must keep it fenced"
      remote_target_action stop
      ;;
  esac
  wait_target_down || fail "target Raft RPC is still reachable after fencing"
  log "fence verified: $STATE_TARGET_ADDRESS is unreachable"
}

config_index_from_leader() {
  local leader address ready
  leader="$(wait_for_leader)" || return 1
  address="${leader#*|}"
  ready="$(leader_status_ready "$address")" || return 1
  printf '%s\n' "${ready%%|*}"
}

wait_member_state() {
  local expected="$1"
  local elapsed=0 leader address status config actual pending
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    leader="$(discover_leader || true)"
    if [[ -n "$leader" ]]; then
      address="${leader#*|}"
      status="$(status_for "$address" || true)"
      pending="$(printf '%s\n' "$status" | field_value pending_conf_change)"
      config="$(raftadmin "$address" configuration 2>/dev/null || true)"
      actual="$(member_suffrage "$TARGET_NODE" "$config" || true)"
      if [[ "$pending" == "false" ]]; then
        case "$expected" in
          absent) [[ -z "$actual" ]] && return 0 ;;
          *) [[ "$actual" == "$expected" ]] && return 0 ;;
        esac
      fi
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

wait_configuration_index() {
  local minimum="$1"
  local elapsed=0 leader address status current pending
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    leader="$(discover_leader || true)"
    if [[ -n "$leader" ]]; then
      address="${leader#*|}"
      status="$(status_for "$address" || true)"
      current="$(printf '%s\n' "$status" | field_value configuration_index)"
      pending="$(printf '%s\n' "$status" | field_value pending_conf_change)"
      if is_uint "$current" && (( current >= minimum )) && [[ "$pending" == "false" ]]; then
        return 0
      fi
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

remove_target() {
  local config current leader address config_index output change_index
  config="$(configuration_from_leader)" || fail "cannot read configuration before removal"
  current="$(member_suffrage "$TARGET_NODE" "$config" || true)"
  if [[ -z "$current" ]]; then
    log "target is already absent; resuming after committed removal"
    return 0
  fi
  [[ "$current" == "voter" ]] || fail "target changed from voter to $current before removal"
  ! status_for "$STATE_TARGET_ADDRESS" >/dev/null || fail "target returned after fencing"
  leader="$(wait_for_leader)" || fail "no leader available for removal"
  address="${leader#*|}"
  config_index="$(config_index_from_leader)" || fail "leader is not ready for removal"
  log "removing $TARGET_NODE at configuration index $config_index"
  output="$(raftadmin "$address" remove_server "$TARGET_NODE" "$config_index")"
  change_index="$(printf '%s\n' "$output" | field_value index)"
  if ! is_uint "$change_index" || (( change_index <= config_index )); then
    fail "remove_server returned an invalid configuration index"
  fi
  wait_member_state absent || fail "remove_server did not commit"
  wait_configuration_index "$change_index" || fail "leader status did not publish committed removal index $change_index"
}

run_rollout() {
  local join_node="$1"
  local rollout_target="$TARGET_NODE"
  (
    if [[ -n "${ROLLING_UPDATE_ENV_FILE:-}" ]]; then
      set -a
      # shellcheck disable=SC1090
      source "$ROLLING_UPDATE_ENV_FILE"
      set +a
    fi
    unset ROLLING_UPDATE_ENV_FILE RAFT_BOOTSTRAP_MEMBERS
    export ROLLING_ORDER="$rollout_target"
    export RAFT_JOIN_NODE="$join_node"
    exec "$ROLLING_UPDATE_SCRIPT"
  )
}

add_learner() {
  local config current leader address status commit config_index output change_index
  config="$(configuration_from_leader)" || fail "cannot read configuration before learner add"
  current="$(member_suffrage "$TARGET_NODE" "$config" || true)"
  if [[ "$current" == "learner" ]]; then
    (( STATE_MIN_CATCH_UP_INDEX > 0 )) || fail "learner committed but catch-up floor is missing from replacement state"
    log "target is already a learner; resuming after committed add"
    return 0
  fi
  [[ -z "$current" ]] || fail "target unexpectedly has suffrage $current before learner add"
  wait_target_up || fail "join deployment did not expose the target RaftAdmin endpoint"
  leader="$(wait_for_leader)" || fail "no leader available for learner add"
  address="${leader#*|}"
  status="$(status_for "$address")" || fail "cannot read leader status before learner add"
  commit="$(printf '%s\n' "$status" | field_value commit_index)"
  config_index="$(printf '%s\n' "$status" | field_value configuration_index)"
  [[ "$(printf '%s\n' "$status" | field_value pending_conf_change)" == "false" ]] || fail "a configuration change is pending"
  if ! is_uint "$commit" || ! is_uint "$config_index" || (( config_index == 0 )); then
    fail "invalid leader indexes before learner add"
  fi
  STATE_MIN_CATCH_UP_INDEX="$commit"
  # Persist the floor before proposing. If the RPC commits but the control
  # process exits before advancing its stage, promotion still has a durable,
  # non-zero catch-up bound on resume.
  write_state
  log "adding learner $TARGET_NODE at configuration index $config_index; catch-up floor=$commit"
  output="$(raftadmin "$address" add_learner "$TARGET_NODE" "$STATE_TARGET_ADDRESS" "$config_index")"
  change_index="$(printf '%s\n' "$output" | field_value index)"
  if ! is_uint "$change_index" || (( change_index <= config_index )); then
    fail "add_learner returned an invalid configuration index"
  fi
  wait_member_state learner || fail "add_learner did not commit"
  wait_configuration_index "$change_index" || fail "leader status did not publish committed learner index $change_index"
}

wait_for_catch_up() {
  local elapsed=0 status state leader_id applied
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    status="$(status_for "$STATE_TARGET_ADDRESS" || true)"
    state="$(printf '%s\n' "$status" | field_value state)"
    leader_id="$(printf '%s\n' "$status" | field_value leader_id)"
    applied="$(printf '%s\n' "$status" | field_value applied_index)"
    if [[ "$state" == "FOLLOWER" && -n "$leader_id" ]] && is_uint "$applied" && (( applied >= STATE_MIN_CATCH_UP_INDEX )); then
      log "learner caught up: applied_index=$applied floor=$STATE_MIN_CATCH_UP_INDEX"
      return 0
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

promote_learner() {
  local config current leader address config_index output change_index
  config="$(configuration_from_leader)" || fail "cannot read configuration before promotion"
  current="$(member_suffrage "$TARGET_NODE" "$config" || true)"
  if [[ "$current" == "voter" ]]; then
    log "target is already a voter; resuming after committed promotion"
    return 0
  fi
  [[ "$current" == "learner" ]] || fail "target must be a learner before promotion; got ${current:-absent}"
  (( STATE_MIN_CATCH_UP_INDEX > 0 )) || fail "catch-up floor is missing from replacement state"
  wait_for_catch_up || fail "learner did not reach catch-up floor"
  leader="$(wait_for_leader)" || fail "no leader available for promotion"
  address="${leader#*|}"
  config_index="$(config_index_from_leader)" || fail "leader is not ready for promotion"
  log "promoting $TARGET_NODE at configuration index $config_index"
  output="$(raftadmin "$address" promote_learner "$TARGET_NODE" "$config_index" "$STATE_MIN_CATCH_UP_INDEX" false)"
  change_index="$(printf '%s\n' "$output" | field_value index)"
  if ! is_uint "$change_index" || (( change_index <= config_index )); then
    fail "promote_learner returned an invalid configuration index"
  fi
  wait_member_state voter || fail "promote_learner did not commit"
  wait_configuration_index "$change_index" || fail "leader status did not publish committed promotion index $change_index"
}

verify_cluster_once() {
  local leader leader_id address status pending config voters id member_address suffrage member_status state member_leader applied commit
  leader="$(discover_leader)" || return 1
  leader_id="${leader%%|*}"
  address="${leader#*|}"
  status="$(status_for "$address")" || return 1
  pending="$(printf '%s\n' "$status" | field_value pending_conf_change)"
  [[ "$pending" == "false" ]] || return 1
  config="$(raftadmin "$address" configuration 2>/dev/null)" || return 1
  voters="$(voter_csv "$config")"
  same_csv_set "$voters" "$STATE_EXPECTED_VOTERS" || return 1
  while IFS='|' read -r id member_address suffrage; do
    [[ "$suffrage" == "voter" ]] || continue
    member_status="$(status_for "$member_address" || true)"
    [[ -n "$member_status" ]] || return 1
    state="$(printf '%s\n' "$member_status" | field_value state)"
    member_leader="$(printf '%s\n' "$member_status" | field_value leader_id)"
    if [[ "$state" == "LEADER" ]]; then
      [[ "$id" == "$leader_id" ]] || return 1
    else
      [[ "$state" == "FOLLOWER" && "$member_leader" == "$leader_id" ]] || return 1
    fi
    applied="$(printf '%s\n' "$member_status" | field_value applied_index)"
    commit="$(printf '%s\n' "$member_status" | field_value commit_index)"
    is_uint "$applied" && is_uint "$commit" && (( applied >= commit )) || return 1
  done < <(printf '%s\n' "$config" | configuration_rows)
  printf '%s\n' "${leader%%|*}"
}

verify_stability() {
  local elapsed=0 stable=0 expected_leader="" leader
  while (( elapsed < REPLACEMENT_TIMEOUT_SECONDS )); do
    leader="$(verify_cluster_once || true)"
    if [[ -n "$leader" && ( -z "$expected_leader" || "$leader" == "$expected_leader" ) ]]; then
      expected_leader="$leader"
      stable=$((stable + REPLACEMENT_POLL_SECONDS))
      if (( stable >= REPLACEMENT_STABILITY_SECONDS )); then
        log "cluster stable for ${stable}s with leader=$leader"
        return 0
      fi
    else
      expected_leader="$leader"
      stable=0
    fi
    sleep "$REPLACEMENT_POLL_SECONDS"
    elapsed=$((elapsed + REPLACEMENT_POLL_SECONDS))
  done
  return 1
}

state_value_valid() {
  [[ "$1" =~ ^[A-Za-z0-9_./,:@-]*$ ]]
}

write_state() {
  local tmp value
  for value in "$STATE_STAGE" "$STATE_TARGET" "$STATE_TARGET_ADDRESS" "$STATE_OPERATION_ID" \
    "$STATE_ARCHIVE_PATH" "$STATE_EXPECTED_VOTERS" "$STATE_MIN_CATCH_UP_INDEX" "$STATE_DATA_MODE"; do
    state_value_valid "$value" || fail "replacement state contains an unsupported value"
  done
  mkdir -p "$(dirname "$REPLACEMENT_STATE_FILE")"
  tmp="${REPLACEMENT_STATE_FILE}.tmp.$$"
  {
    printf 'stage=%s\n' "$STATE_STAGE"
    printf 'target=%s\n' "$STATE_TARGET"
    printf 'target_address=%s\n' "$STATE_TARGET_ADDRESS"
    printf 'operation_id=%s\n' "$STATE_OPERATION_ID"
    printf 'archive_path=%s\n' "$STATE_ARCHIVE_PATH"
    printf 'expected_voters=%s\n' "$STATE_EXPECTED_VOTERS"
    printf 'min_catch_up_index=%s\n' "$STATE_MIN_CATCH_UP_INDEX"
    printf 'data_mode=%s\n' "$STATE_DATA_MODE"
  } > "$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$REPLACEMENT_STATE_FILE"
}

load_state() {
  local key value
  [[ -f "$REPLACEMENT_STATE_FILE" ]] || return 1
  while IFS='=' read -r key value; do
    state_value_valid "$value" || fail "invalid value in state file for $key"
    case "$key" in
      stage) STATE_STAGE="$value" ;;
      target) STATE_TARGET="$value" ;;
      target_address) STATE_TARGET_ADDRESS="$value" ;;
      operation_id) STATE_OPERATION_ID="$value" ;;
      archive_path) STATE_ARCHIVE_PATH="$value" ;;
      expected_voters) STATE_EXPECTED_VOTERS="$value" ;;
      min_catch_up_index) STATE_MIN_CATCH_UP_INDEX="$value" ;;
      data_mode) STATE_DATA_MODE="$value" ;;
      "") ;;
      *) fail "unknown key in state file: $key" ;;
    esac
  done < "$REPLACEMENT_STATE_FILE"
  if ! is_uint "$STATE_STAGE" || ! is_uint "$STATE_MIN_CATCH_UP_INDEX"; then
    fail "invalid numeric value in state file"
  fi
  (( STATE_STAGE <= 9 )) || fail "state stage is outside the implemented range: $STATE_STAGE"
  [[ "$STATE_TARGET" == "$TARGET_NODE" ]] || fail "state file belongs to target $STATE_TARGET, not $TARGET_NODE"
  [[ "$STATE_TARGET_ADDRESS" == "$(node_host_by_id "$TARGET_NODE"):${RAFT_PORT}" ]] || fail "state target address no longer matches NODES"
  [[ -n "$STATE_OPERATION_ID" && -n "$STATE_ARCHIVE_PATH" && -n "$STATE_EXPECTED_VOTERS" ]] || fail "state file is incomplete"
  [[ "$STATE_DATA_MODE" == "$REPLACEMENT_DATA_MODE" ]] || fail "REPLACEMENT_DATA_MODE changed from $STATE_DATA_MODE to $REPLACEMENT_DATA_MODE"
  log "resuming operation $STATE_OPERATION_ID at stage $STATE_STAGE"
}

advance() {
  STATE_STAGE="$1"
  write_state
}

validate_settings
parse_nodes
TARGET_HOST="$(node_host_by_id "$TARGET_NODE")"
REPLACEMENT_STATE_FILE="${REPLACEMENT_STATE_FILE:-${REPO_ROOT}/.state/raft-member-replacement-${TARGET_NODE}.state}"

if [[ "$MODE" == "dry-run" ]]; then
  cat <<EOF
[raft-member-replace] dry run only; no RPC, SSH, build, or deploy is performed
[raft-member-replace] target: ${TARGET_NODE} (${TARGET_HOST}:${RAFT_PORT})
[raft-member-replace] fence mode: ${REPLACEMENT_FENCE_MODE}
[raft-member-replace] data mode: ${REPLACEMENT_DATA_MODE}
[raft-member-replace] state file: ${REPLACEMENT_STATE_FILE}
[raft-member-replace] plan:
  1. verify current voter identity, leader readiness, and surviving quorum
  2. transfer leadership away from the target if needed
  3. fence the old instance and verify its Raft RPC is unreachable
  4. remove_server using the current configuration index
  5. archive or delete only the target data directory
  6. deploy only the target with RAFT_JOIN_NODE=${TARGET_NODE}
  7. add_learner using the current configuration index
  8. wait for applied_index to reach the recorded catch-up floor
  9. promote_learner without skipping the catch-up check
 10. redeploy only the target without join flags
 11. verify stable voters and run the application write/read check
EOF
  exit 0
fi

LOCK_DIR="${REPLACEMENT_STATE_FILE}.lock"
mkdir -p "$(dirname "$REPLACEMENT_STATE_FILE")"
mkdir "$LOCK_DIR" 2>/dev/null || fail "another replacement process holds $LOCK_DIR"
LOCK_ACQUIRED=true
ensure_raftadmin

if ! load_state; then
  STATE_TARGET="$TARGET_NODE"
  STATE_TARGET_ADDRESS="${TARGET_HOST}:${RAFT_PORT}"
  STATE_OPERATION_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  STATE_ARCHIVE_PATH="${DATA_DIR}.replacement-${TARGET_NODE}-${STATE_OPERATION_ID}"
  STATE_DATA_MODE="$REPLACEMENT_DATA_MODE"
  preflight
  advance 1
fi

if (( STATE_STAGE < 1 )); then
  preflight
  advance 1
fi
if (( STATE_STAGE < 2 )); then
  fence_target
  advance 2
elif (( STATE_STAGE < 5 )); then
  wait_target_down || fail "fenced target is reachable again before removal"
fi
if (( STATE_STAGE < 3 )); then
  remove_target
  advance 3
fi
if (( STATE_STAGE < 4 )); then
  wait_member_state absent || fail "target membership returned before data reset"
  log "resetting only $TARGET_NODE data at $DATA_DIR"
  remote_target_action reset-data
  advance 4
fi
if (( STATE_STAGE < 5 )); then
  wait_member_state absent || fail "target membership returned before join deployment"
  log "deploying $TARGET_NODE in learner join mode"
  run_rollout "$TARGET_NODE"
  advance 5
fi
if (( STATE_STAGE < 6 )); then
  add_learner
  advance 6
fi
if (( STATE_STAGE < 7 )); then
  promote_learner
  advance 7
fi
if (( STATE_STAGE < 8 )); then
  log "redeploying $TARGET_NODE from durable membership without join flags"
  run_rollout ""
  advance 8
fi
if (( STATE_STAGE < 9 )); then
  verify_stability || fail "cluster did not reach a stable fully-applied voter configuration"
  log "running application write/read verification"
  run_operator_command "$REPLACEMENT_VERIFY_COMMAND"
  advance 9
fi

log "replacement completed: target=$TARGET_NODE operation=$STATE_OPERATION_ID"
if [[ "$REPLACEMENT_KEEP_STATE" != "true" ]]; then
  rm -f "$REPLACEMENT_STATE_FILE"
fi
