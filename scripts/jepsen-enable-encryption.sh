#!/usr/bin/env bash
# Drive a freshly started, encryption-capable elastickv cluster through the
# §7.1 cleartext→encrypted rollout so the Jepsen workloads run against an
# encrypted cluster:
#
#   1. wrap two fresh DEKs under the file KEK (`encryption wrap-dek`)
#   2. propose the DEK pair + writer batch (`encryption bootstrap`)
#   3. flip the storage envelope cluster flag (`encryption enable-storage-envelope`)
#   4. flip the raft envelope cutover (`encryption enable-raft-envelope`)
#   5. wait until every member reports both flags active
#
# Every node must already be running with
#   --encryption-enabled --kekFile <kek-file> --encryptionSidecarPath <path>
# (see scripts/run-jepsen-local.sh --encrypted and .github/workflows/jepsen-test.yml).
#
# Usage:
#   scripts/jepsen-enable-encryption.sh <elastickv-admin binary> <kek-file> <grpc-endpoint>...
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <elastickv-admin binary> <kek-file> <grpc-endpoint>..." >&2
  exit 2
fi
ADMIN="$1"
KEK_FILE="$2"
shift 2
ENDPOINTS=("$@")

STORAGE_DEK_ID=1
RAFT_DEK_ID=2
RPC_TIMEOUT=60s
# Mutating RPCs are leader-only; a follower returns an error and the loop
# moves on to the next endpoint. Rounds cover a leader election in flight.
ROUNDS=15

status_field() {
  # status_field <endpoint> <field>: print the numeric/boolean value of one
  # `encryption status` line (e.g. full_node_id, local_epoch,
  # storage_envelope_active, raft_envelope_cutover_index).
  "$ADMIN" encryption status --endpoint "$1" --timeout "$RPC_TIMEOUT" 2>/dev/null \
    | awk -v key="$2:" '$1 == key { print $2; exit }'
}

# try_on_leader <label> <subcommand> [extra args...]: run the mutating
# subcommand against each endpoint in turn, with the proposer identity read
# from that endpoint's own capability report, until one accepts it.
try_on_leader() {
  local label="$1" sub="$2"
  shift 2
  local round ep node_id epoch
  for round in $(seq 1 "$ROUNDS"); do
    for ep in "${ENDPOINTS[@]}"; do
      node_id="$(status_field "$ep" full_node_id || true)"
      epoch="$(status_field "$ep" local_epoch || true)"
      if [ -z "$node_id" ] || [ -z "$epoch" ]; then
        continue
      fi
      if "$ADMIN" encryption "$sub" --endpoint "$ep" --timeout "$RPC_TIMEOUT" \
           --proposer-node-id "$node_id" --proposer-local-epoch "$epoch" "$@"; then
        echo "[encryption] ${label}: accepted by ${ep} (full_node_id=${node_id} local_epoch=${epoch})"
        return 0
      fi
    done
    echo "[encryption] ${label}: no endpoint accepted the proposal (round ${round}/${ROUNDS}); retrying..."
    sleep 2
  done
  echo "[encryption] ${label}: FAILED after ${ROUNDS} rounds" >&2
  return 1
}

# ---- 1. wrap DEKs ----
WRAPPED_STORAGE="$("$ADMIN" encryption wrap-dek --kek-file "$KEK_FILE")"
WRAPPED_RAFT="$("$ADMIN" encryption wrap-dek --kek-file "$KEK_FILE")"

# ---- 2. bootstrap (skip when a DEK pair is already active) ----
if [ "$(status_field "${ENDPOINTS[0]}" active_storage_id || true)" = "$STORAGE_DEK_ID" ]; then
  echo "[encryption] bootstrap: storage dek ${STORAGE_DEK_ID} already active; skipping"
else
  DISCOVER=()
  for ep in "${ENDPOINTS[@]}"; do DISCOVER+=(--discover-from "$ep"); done
  bootstrapped=false
  for round in $(seq 1 "$ROUNDS"); do
    for ep in "${ENDPOINTS[@]}"; do
      if "$ADMIN" encryption bootstrap --endpoint "$ep" --timeout "$RPC_TIMEOUT" \
           --storage-dek-id "$STORAGE_DEK_ID" --raft-dek-id "$RAFT_DEK_ID" \
           --wrapped-storage-dek "$WRAPPED_STORAGE" --wrapped-raft-dek "$WRAPPED_RAFT" \
           "${DISCOVER[@]}"; then
        echo "[encryption] bootstrap: accepted by ${ep}"
        bootstrapped=true
        break 2
      fi
    done
    echo "[encryption] bootstrap: no endpoint accepted the proposal (round ${round}/${ROUNDS}); retrying..."
    sleep 2
  done
  if ! $bootstrapped; then
    echo "[encryption] bootstrap: FAILED after ${ROUNDS} rounds" >&2
    exit 1
  fi
fi

# ---- 3. storage envelope ----
if [ "$(status_field "${ENDPOINTS[0]}" storage_envelope_active || true)" = "true" ]; then
  echo "[encryption] enable-storage-envelope: already active; skipping"
else
  try_on_leader "enable-storage-envelope" enable-storage-envelope
fi

# ---- 4. raft envelope ----
cutover="$(status_field "${ENDPOINTS[0]}" raft_envelope_cutover_index || true)"
if [ -n "$cutover" ] && [ "$cutover" != "0" ]; then
  echo "[encryption] enable-raft-envelope: cutover already at index ${cutover}; skipping"
else
  try_on_leader "enable-raft-envelope" enable-raft-envelope
fi

# ---- 5. wait for every member to observe both flags ----
for i in $(seq 1 30); do
  all_active=true
  for ep in "${ENDPOINTS[@]}"; do
    active="$(status_field "$ep" storage_envelope_active || true)"
    cutover="$(status_field "$ep" raft_envelope_cutover_index || true)"
    if [ "$active" != "true" ] || [ -z "$cutover" ] || [ "$cutover" = "0" ]; then
      all_active=false
    fi
  done
  if $all_active; then
    echo "[encryption] every member reports storage_envelope_active=true and raft_envelope_cutover_index>0 after ${i}s"
    for ep in "${ENDPOINTS[@]}"; do
      echo "[encryption] status ${ep}:"
      "$ADMIN" encryption status --endpoint "$ep" --timeout "$RPC_TIMEOUT" | sed 's/^/    /'
    done
    exit 0
  fi
  sleep 1
done
echo "[encryption] FAILED: not every member observed the cutover within 30s" >&2
for ep in "${ENDPOINTS[@]}"; do
  "$ADMIN" encryption status --endpoint "$ep" --timeout "$RPC_TIMEOUT" || true
done
exit 1
