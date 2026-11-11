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

native_memory_tests=${2:-}
run() {
	local skip=$1
	shift
	if [[ -n "$native_memory_tests" ]]; then
		skip="${skip:+$skip|}$native_memory_tests"
	fi
	if [[ -n "$skip" ]]; then
		go test -race -timeout=30m "$@" -skip "$skip"
  else
    go test -race -timeout=30m "$@"
  fi
}

case "${1:-}" in
	adapter) run "$seq_tests" ./adapter ;;
	adapter-seq) run "" ./adapter -run "$seq_tests" ;;
	root) run "" . ;;
	raft-crypto) run "" ./internal/encryption ./internal/raftengine/etcd ;;
  rest)
    # Discover in a plain assignment so a go list failure (which omits
    # erroneous packages) aborts the shard under set -e instead of being
    # lost inside a process substitution.
    all=$(go list ./...)
    mapfile -t pkgs < <(printf '%s\n' "$all" | grep -vxF -f <(printf '%s\n' "${heavy[@]}") || true)
    if [[ ${#pkgs[@]} -eq 0 || -z "${pkgs[0]}" ]]; then
      echo "rest shard selected no packages" >&2
      exit 1
    fi
		run "" "${pkgs[@]}"
    ;;
  *)
    echo "usage: $0 adapter|adapter-seq|root|raft-crypto|rest" >&2
    exit 2
    ;;
esac
