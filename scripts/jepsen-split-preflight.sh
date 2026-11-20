#!/usr/bin/env bash
# Run from the repository root on an isolated runner with Java 21 and Leiningen.
set -euo pipefail
umask 077
work=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]}"; do
    kill -CONT "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
  done
  wait || true
  rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export PREFLIGHT_DIR="$work"
logs=${PREFLIGHT_LOG_DIR:-$work/logs}
mkdir -p "$logs"
go build -o "$work/server" .
for bin in elastickv-split elastickv-list-routes jepsen-grpc-client raftadmin; do
  go build -o "$work/$bin" "./cmd/$bin"
done
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
  -addext subjectAltName=IP:127.0.0.1 -keyout "$work/key.pem" -out "$work/cert.pem" 2>/dev/null
openssl rand -hex 32 > "$work/token"
export RAFTADMIN_TLS=true RAFTADMIN_TLS_CA_CERT="$work/cert.pem"
export ELASTICKV_ENABLE_MIGRATION_IMPORT_OPCODE=true ELASTICKV_ENABLE_MIGRATION_PROMOTE_OPCODE=true ELASTICKV_ENABLE_MIGRATION_CLEANUP_OPCODE=true
peers='1=n1@127.0.0.1:25101,n2@127.0.0.1:25102,n3@127.0.0.1:25103;2=n1@127.0.0.1:25201,n2@127.0.0.1:25202,n3@127.0.0.1:25203'
for n in 1 2 3; do
  "$work/server" --raftId "n$n" --raftEngine etcd --raftDataDir "$work/data$n" \
    --address "127.0.0.1:2510$n" --raftGroups "1=127.0.0.1:2510$n,2=127.0.0.1:2520$n" \
    --raftGroupPeers "$peers" --shardRanges ':=1' --redisAddress "127.0.0.1:2530$n" \
    --metricsAddress '' --pprofAddress '' --dynamoAddress '' \
    --grpcTLSCAFile "$work/cert.pem" --grpcTLSCertFile "$work/cert.pem" --grpcTLSKeyFile "$work/key.pem" \
    --splitMigrationTokenFile "$work/token" --splitMigrationPeerAuthEnforced > "$logs/n$n.log" 2>&1 &
  pids+=("$!")
  echo "$!" > "$work/n$n.pid"
done
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
"${LEIN:-lein}" run -m elastickv.split-preflight-workload --local
