#!/usr/bin/env bash
# Run the Composed-1 M5 multi-table DynamoDB Jepsen workload against a
# single-process two-group elastickv cluster.
#
# Why this script exists separately from run-jepsen-local.sh: the M5
# workload requires a multi-Raft-group cluster topology that the
# existing 3-node single-group layout cannot provide.  Per the design
# doc (docs/design/2026_06_02_partial_composed1_m5_jepsen_route_shuffle.md
# §3.3), today's `validateShardRanges` / `buildShardGroups` only
# support a "single process hosts all groups" model — separate
# processes per group fail validation or race on Raft listeners.
# So this script launches ONE process hosting BOTH single-member
# groups, with two Raft listeners (50051, 50054) and one shared
# DynamoDB endpoint (63801).
#
# Trade-off accepted: partition / kill nemeses can't isolate one
# group from the other since they share a process.  Only the
# (future) route-shuffle nemesis exercises the cross-group path
# meaningfully under this topology.  True distributed multi-group is
# M6+ work — see the parent design doc.
#
# Usage:
#   ./scripts/run-jepsen-m5-local.sh                  # build + start + test
#   ./scripts/run-jepsen-m5-local.sh --no-rebuild     # skip go build
#   ./scripts/run-jepsen-m5-local.sh --no-cluster     # reuse running cluster
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY=/tmp/elastickv4-m5-binary
ROUTE_KEY_BIN=/tmp/elastickv4-m5-route-key
LIST_ROUTES_BIN=/tmp/elastickv4-m5-list-routes
DATA_DIR=/tmp/elastickv-m5-test-run
PID_FILE=/tmp/elastickv-m5-test-run.pid

# ---- topology: one process, two single-member Raft groups ----
RAFT_ADDR_G1="127.0.0.1:50051"
RAFT_ADDR_G2="127.0.0.1:50054"
DYNAMO_ADDR="127.0.0.1:63801"
PROC_ADDR="$RAFT_ADDR_G1"           # the process's primary gRPC address
RAFT_GROUPS="1=$RAFT_ADDR_G1,2=$RAFT_ADDR_G2"
RAFT_DYNAMO_MAP="$RAFT_ADDR_G1=$DYNAMO_ADDR,$RAFT_ADDR_G2=$DYNAMO_ADDR"

NO_REBUILD=false
NO_CLUSTER=false
for arg in "$@"; do
  case "$arg" in
    --no-rebuild) NO_REBUILD=true ;;
    --no-cluster) NO_CLUSTER=true ;;
  esac
done

# ---- build (server + route-key + list-routes helpers) ----
if ! $NO_REBUILD; then
  # Pre-flight: cmd/elastickv-list-routes lands in PR #925.  If this
  # branch is run before #925 merges, `go build` would emit an
  # opaque package-not-found error.  Surface the cross-PR dependency
  # in a machine-readable way (claude[bot] suggestion on PR #924).
  if [ ! -d "$REPO_ROOT/cmd/elastickv-list-routes" ]; then
    echo "[error] cmd/elastickv-list-routes/ not found in this branch." >&2
    echo "        PR #924 depends on PR #925 (setup-hook + list-routes CLI)." >&2
    echo "        Merge #925 first, or check out the integrated branch." >&2
    exit 1
  fi
  echo "[build] compiling elastickv server..."
  cd "$REPO_ROOT"
  go build -o "$BINARY" .
  echo "[build] compiling elastickv-route-key helper..."
  go build -o "$ROUTE_KEY_BIN" ./cmd/elastickv-route-key
  echo "[build] compiling elastickv-list-routes helper..."
  # Used by the Jepsen workload's setup-hook verification
  # (verify-multi-group-routing!).  Confirms the cluster booted with
  # >=2 distinct Raft groups before any workload op runs.
  go build -o "$LIST_ROUTES_BIN" ./cmd/elastickv-list-routes
  echo "[build] done -> $BINARY, $ROUTE_KEY_BIN, $LIST_ROUTES_BIN"
fi

# ---- compute --shardRanges boundary keys ----
# Multi-table workload uses tables jepsen_append_t{1..4}.  Tables 1-2
# go to group 1, tables 3-4 to group 2.  Boundary keys are the
# byte-for-byte route-key encoding of the table names — computed via
# the elastickv-route-key Go helper rather than inlined in shell so
# the base64 encoding stays in sync with adapter/dynamodb.go's
# encodeDynamoSegment (codex P1 #1 on PR #905 ffb9c73f).
#
# Guard: every helper binary must exist before continuing.  Runs
# unconditionally — catches both --no-rebuild (helpers expected from
# a previous run) AND a fresh-build environment where a helper
# somehow produced a non-executable.  Failing fast with a clear
# remediation message is strictly better than letting `set -e`
# swallow a misleading 'No such file or directory' deeper in the
# script (gemini medium + claude[bot] minor on PR #924).
for bin in "$ROUTE_KEY_BIN" "$LIST_ROUTES_BIN" "$BINARY"; do
  if [ ! -x "$bin" ]; then
    echo "[error] required helper not found at $bin." >&2
    echo "        Re-run without --no-rebuild to compile the helpers." >&2
    exit 1
  fi
done
T3_KEY="$("$ROUTE_KEY_BIN" jepsen_append_t3)"
# Issue #930 fix: --shardRanges must cover every routing key.  Without
# a [<empty>, T1_KEY) range, any table whose base64-encoded name sorts
# before "amVwc2VuX2FwcGVuZF90MQ" (= base64("jepsen_append_t1"))
# returns "no route for key" from ShardedCoordinator.dispatchTxn, and
# createTableWithRetry silently swallows that as ACTIVE.
#
# Group 1: [<empty>, T3_KEY)   — default + tables 1, 2
# Group 2: [T3_KEY, +inf)      — tables 3, 4
#
# Note: assigning the default range to group 1 (not a third group) keeps
# the topology consistent with the 1-process-2-groups launch.
SHARD_RANGES=":${T3_KEY}=1,${T3_KEY}:=2"
echo "[shard-ranges] $SHARD_RANGES"

# ---- stop any previously managed cluster ----
stop_cluster() {
  if [ -f "$PID_FILE" ]; then
    echo "[cluster] stopping previous cluster..."
    while IFS= read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
  fi
}

# ---- start cluster: ONE process hosting both groups ----
if ! $NO_CLUSTER; then
  # Install the cleanup hook BEFORE starting the cluster so an
  # exception during launch (e.g. bind-port collision) still
  # tears down the half-started state.  EXIT covers normal flow,
  # INT/TERM cover user Ctrl-C and CI cancellation.  Without
  # this the failure path leaks background processes that hold
  # the Raft / Dynamo ports for the next run (gemini medium on
  # PR #924).
  trap stop_cluster EXIT INT TERM
  stop_cluster
  rm -rf "$DATA_DIR"
  mkdir -p "$DATA_DIR"
  : > "$PID_FILE"

  echo "[cluster] starting single-process two-group cluster..."
  # Notes on flag selection:
  #   --raftBootstrap  : boolean; each group is single-member so no
  #                      peer discovery is needed.  --raftBootstrapMembers
  #                      is rejected by resolveBootstrapServers on any
  #                      multi-group process (main.go:735-741) and so
  #                      MUST NOT appear here (codex P2 + claude[bot]
  #                      P2 on PR #905 3ca2a7f7).
  #   --raftGroups     : declares both groups with distinct Raft
  #                      listeners.
  #   --shardRanges    : places t1-t2 in group 1 and t3-t4 in group 2.
  #                      Both flags are required for the multi-group
  #                      contract: --shardRanges alone collapses
  #                      everything to the default group 1
  #                      (coderabbit Major on PR #905 f92a029e).
  #   --raftDynamoMap  : both Raft addresses point at the same Dynamo
  #                      endpoint since there's only one process.
  nohup "$BINARY" \
    --address      "$PROC_ADDR" \
    --dynamoAddress "$DYNAMO_ADDR" \
    --redisAddress "" \
    --s3Address    "" \
    --sqsAddress   "" \
    --metricsAddress "" \
    --pprofAddress "" \
    --raftId       "n1" \
    --raftDataDir  "${DATA_DIR}/n1" \
    --raftBootstrap \
    --raftGroups   "$RAFT_GROUPS" \
    --shardRanges  "$SHARD_RANGES" \
    --raftDynamoMap "$RAFT_DYNAMO_MAP" \
    > "${DATA_DIR}/n1.log" 2>&1 &
  echo $! >> "$PID_FILE"

  echo "[cluster] waiting for Dynamo endpoint ($DYNAMO_ADDR)..."
  for i in $(seq 1 90); do
    # Use bash's built-in /dev/tcp probe rather than `nc` so the
    # script runs on minimal CI images that may not ship netcat
    # (gemini medium on PR #924).
    if (echo > /dev/tcp/127.0.0.1/63801) >/dev/null 2>&1; then
      echo "[cluster] up after ${i}s"
      break
    fi
    sleep 1
    if [ "$i" -eq 90 ]; then
      echo "[cluster] FAILED to start - dumping log:"
      tail -n 100 "${DATA_DIR}/n1.log" || true
      exit 1
    fi
  done
fi

# ---- run M5 Jepsen multi-table workload ----
cd "$REPO_ROOT/jepsen"

# Resolve lein: prefer LEIN env override, then PATH (works on CI), then
# the macOS Homebrew default.  Failing to find lein is fatal.
LEIN_BIN="${LEIN:-$(command -v lein || echo /opt/homebrew/bin/lein)}"
if [ ! -x "$LEIN_BIN" ]; then
  echo "[jepsen] lein not found (tried \$LEIN, PATH, /opt/homebrew/bin/lein)" >&2
  exit 127
fi

echo "[jepsen] running DynamoDB multi-table list-append workload via $LEIN_BIN..."
mkdir -p tmp-home .lein
# --list-routes-bin / --grpc-host-port wire the setup-hook verification
# (verify-multi-group-routing!) at the workload's first setup! call.
# Without them the hook falls back to PATH lookup which fails when
# run from this script's tmp build.
#
# --host 127.0.0.1 — without this the workload's open! resolves the
# DynamoDB client hostname from (name node) where node is one of
# default-nodes ["n1" "n2" "n3" "n4" "n5"]; these are virtual labels,
# not real hostnames, and DNS resolution fails with 'nodename nor
# servname provided'.  --host overrides via cli/common-cli-opts'
# --host -> :host -> :dynamo-host -> make-ddb-client wiring.  Required
# for the single-process two-group topology this script launches —
# every "node" client talks to the same loopback DynamoDB endpoint.
HOME="$(pwd)/tmp-home" LEIN_HOME="$(pwd)/.lein" \
  LEIN_JVM_OPTS="-Duser.home=$(pwd)/tmp-home" \
  "$LEIN_BIN" run -m elastickv.dynamodb-multi-table-workload \
    --local \
    --time-limit 30 \
    --rate 5 \
    --host 127.0.0.1 \
    --dynamo-port 63801 \
    --list-routes-bin "$LIST_ROUTES_BIN" \
    --grpc-host-port  "$PROC_ADDR" \
  || EXIT_CODE=$?

EXIT_CODE=${EXIT_CODE:-0}

# ---- teardown ----
# Cluster shutdown is handled by the `trap stop_cluster EXIT INT TERM`
# installed above the cluster launch.  No explicit teardown call is
# needed here; doing so would double-call stop_cluster on success
# (harmless but noisy) and double-call on failure (which is also
# harmless since stop_cluster is idempotent, but the EXIT trap path
# is the canonical one — see gemini medium on PR #924).

exit "$EXIT_CODE"
