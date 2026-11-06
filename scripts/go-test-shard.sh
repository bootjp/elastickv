#!/usr/bin/env bash
set -euo pipefail

# Run one shard of the race test suite. CI fans the shards out as parallel
# jobs; together they cover every package exactly once.
#
#   adapter      ./adapter without the full-length gRPC sequences
#   adapter-seq  the full-length gRPC sequences, in a fresh package budget
#   root         the server package (.)
#   raft-crypto  internal/encryption and internal/raftengine/etcd
#   rest         every other package
#
# Shards are sized from per-package wall time on CI; rebalance here when a
# package grows enough to dominate its shard.

seq_tests='^(Test_consistency_satisfy_write_after_read_sequence|Test_grpc_transaction)$'
mod=$(go list -m)
heavy=(
  "$mod/adapter"
  "$mod"
  "$mod/internal/encryption"
  "$mod/internal/raftengine/etcd"
)

run() { go test -race -timeout=30m "$@"; }

case "${1:-}" in
  adapter) run ./adapter -skip "$seq_tests" ;;
  adapter-seq) run ./adapter -run "$seq_tests" ;;
  root) run . ;;
  raft-crypto) run ./internal/encryption ./internal/raftengine/etcd ;;
  rest)
    mapfile -t pkgs < <(go list ./... | grep -vxF -f <(printf '%s\n' "${heavy[@]}"))
    run "${pkgs[@]}"
    ;;
  *)
    echo "usage: $0 adapter|adapter-seq|root|raft-crypto|rest" >&2
    exit 2
    ;;
esac
