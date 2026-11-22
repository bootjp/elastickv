#!/usr/bin/env bash
# Run from the repository root on an isolated runner with Java 21 and Leiningen.
set -euo pipefail
umask 077
work=$(mktemp -d)
pids=()
cleanup() {
  local pid marker killed_pid restarted_pid cleanup_failed=0
  for pid in "${pids[@]}"; do
    kill -CONT "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
  done
  wait || true
  if [[ -f "$work/killed-n3" && -f "$work/n3.pid" ]]; then
    read -r killed_pid _ < "$work/killed-n3"
    restarted_pid=$(< "$work/n3.pid")
    if [[ "$restarted_pid" != "$killed_pid" ]]; then
      if kill -0 "$restarted_pid" 2>/dev/null; then
        kill -TERM "$restarted_pid" 2>/dev/null || true
        echo "Restarted n3 (PID $restarted_pid) survived cleanup; preserving $work" >&2
        cleanup_failed=1
      else
        printf '%s\n' "$restarted_pid" > "$work/cleanup-n3"
      fi
    fi
  fi
  for marker in "$work"/{before-publish,published-attempt,held-n3,caught-n3,caught-n4,killed-n3,cleanup-n3} "$work"/partition-drop-*; do
    if [[ -f "$marker" && -n "${logs:-}" ]]; then
      cp "$marker" "$logs/"
    fi
  done
  if ((cleanup_failed)); then exit 1; fi
  rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export PREFLIGHT_DIR="$work"
logs=${PREFLIGHT_LOG_DIR:-$work/logs}
mkdir -p "$logs"
logs=$(cd "$logs" && pwd -P)
unset SPLIT_MEMBERSHIP_FIXTURE
build_args=()
export SPLIT_MEMBERSHIP_FAULT=${SPLIT_MEMBERSHIP_FAULT:-none}
case "$SPLIT_MEMBERSHIP_FAULT" in
  none|kill|partition|pause) ;;
  *) echo "Invalid SPLIT_MEMBERSHIP_FAULT: $SPLIT_MEMBERSHIP_FAULT" >&2; exit 2 ;;
esac
for arg in "$@"; do
  if [[ "$arg" == --online-member-change ]]; then
    python3 scripts/jepsen-split-membership-overlay.py "$PWD" "$work/overlay" "$SPLIT_MEMBERSHIP_FAULT"
    build_args=(-overlay "$work/overlay/overlay.json")
    export SPLIT_MEMBERSHIP_FIXTURE="$work"
  fi
done
go build "${build_args[@]}" -o "$work/server" .
for bin in elastickv-split elastickv-list-routes jepsen-grpc-client raftadmin; do
  go build -o "$work/$bin" "./cmd/$bin"
done
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
  -addext subjectAltName=IP:127.0.0.1 -keyout "$work/key.pem" -out "$work/cert.pem" 2>/dev/null
openssl rand -hex 32 > "$work/token"
export RAFTADMIN_TLS=true RAFTADMIN_TLS_CA_CERT="$work/cert.pem"
export ELASTICKV_ENABLE_MIGRATION_IMPORT_OPCODE=true ELASTICKV_ENABLE_MIGRATION_PROMOTE_OPCODE=true ELASTICKV_ENABLE_MIGRATION_CLEANUP_OPCODE=true
peers='1=n1@127.0.0.1:25101,n2@127.0.0.1:25102,n3@127.0.0.1:25103;2=n1@127.0.0.1:25201,n2@127.0.0.1:25202,n3@127.0.0.1:25203'
launch_node() {
  local n=$1 node_peers=$2
  if [[ ! -f "$work/n$n.pid" ]]; then : > "$logs/n$n.log"; fi
  SPLIT_MEMBERSHIP_NODE="n$n" "$work/server" --raftId "n$n" --raftEngine etcd --raftDataDir "$work/data$n" \
    --address "127.0.0.1:2510$n" --raftGroups "1=127.0.0.1:2510$n,2=127.0.0.1:2520$n" \
    --raftGroupPeers "$node_peers" --shardRanges ':=1' --redisAddress "127.0.0.1:2530$n" \
    --metricsAddress '' --pprofAddress '' --dynamoAddress '' \
    --grpcTLSCAFile "$work/cert.pem" --grpcTLSCertFile "$work/cert.pem" --grpcTLSKeyFile "$work/key.pem" \
    --splitMigrationTokenFile "$work/token" --splitMigrationPeerAuthEnforced >> "$logs/n$n.log" 2>&1 &
  server_pid=$!
  printf '%s\n' "$server_pid" > "$work/n$n.pid.tmp"
  mv "$work/n$n.pid.tmp" "$work/n$n.pid"
}
restartable_target_leader() {
  local server_pid=''
  # This background function owns n3 and receives cleanup's TERM directly.
  trap 'if [[ -n "$server_pid" ]]; then kill -TERM "$server_pid" 2>/dev/null || true; wait "$server_pid" || true; fi; exit 0' TERM
  launch_node 3 "$peers"
  local first_pid=$server_pid exit_code=0
  wait "$server_pid" || exit_code=$?
  server_pid=''
  [[ "$exit_code" -eq 137 ]] || exit 1
  printf '%s %s\n' "$first_pid" "$exit_code" > "$work/killed-n3.tmp"
  mv "$work/killed-n3.tmp" "$work/killed-n3"
  until [[ -f "$work/restart-n3" ]]; do sleep 0.1; done
  launch_node 3 "$peers"
  wait "$server_pid"
}
for n in 1 2 3; do
  if [[ "$n" == 3 && -n "${SPLIT_MEMBERSHIP_FIXTURE:-}" && "$SPLIT_MEMBERSHIP_FAULT" == kill ]]; then
    restartable_target_leader &
    pids+=("$!")
  else
    launch_node "$n" "$peers"
    pids+=("$server_pid")
  fi
done
if [[ -n "${SPLIT_MEMBERSHIP_FIXTURE:-}" ]]; then
  joining_peers='1=n1@127.0.0.1:25101,n2@127.0.0.1:25102,n3@127.0.0.1:25103,n4@127.0.0.1:25104;2=n1@127.0.0.1:25201,n2@127.0.0.1:25202,n3@127.0.0.1:25203,n4@127.0.0.1:25204'
  launch_node 4 "$joining_peers"
  pids+=("$server_pid")
fi
for n in 1 2 3; do
  ready=false
  for ((i=0; i<60; i++)); do
    if ! kill -0 "${pids[n-1]}" 2>/dev/null; then
      tail -50 "$logs/n$n.log" >&2
      exit 1
    fi
    if "$work/elastickv-list-routes" --address "127.0.0.1:2510$n" --tls-ca-file "$work/cert.pem" 2>/dev/null | jq -e '.routes | length == 1' >/dev/null; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ $ready != true ]]; then tail -50 "$logs/n$n.log" >&2; exit 1; fi
done
cd jepsen
"${LEIN:-lein}" run -m elastickv.split-preflight-workload --local "$@"
