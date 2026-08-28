#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 METRICS_FILE [METRICS_FILE ...]" >&2
  exit 2
fi

EXPECTED_GROUPS="${EXPECTED_GROUPS:-1,2,3}"
MIN_NODES="${MIN_NODES:-3}"

case "$MIN_NODES" in
  ''|*[!0-9]*)
    echo "MIN_NODES must be a positive integer" >&2
    exit 2
    ;;
esac
if [ "$MIN_NODES" -lt 1 ]; then
  echo "MIN_NODES must be a positive integer" >&2
  exit 2
fi

if [ "$#" -lt "$MIN_NODES" ]; then
  echo "expected metrics from at least $MIN_NODES nodes, got $#" >&2
  exit 1
fi

for file in "$@"; do
  if [ ! -s "$file" ]; then
    echo "missing or empty metrics evidence: $file" >&2
    exit 1
  fi
  if ! awk '
    /^elastickv_raft_/ {
      if ($NF !~ /^([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$/) {
        print "invalid metric value in " FILENAME ": " $0 > "/dev/stderr"
        bad = 1
      }
      if (index($0, "group=\"") == 0 || index($0, "node_id=\"") == 0 || index($0, "node_address=\"") == 0) {
        print "missing required metric labels in " FILENAME ": " $0 > "/dev/stderr"
        bad = 1
      }
    }
    END { exit bad }
  ' "$file"; then
    exit 1
  fi
done

seen_node_ids=","
seen_node_addresses=","
for file in "$@"; do
  node_ids="$(sed -n 's/.*node_id="\([^"]*\)".*/\1/p' "$file" | sort -u)"
  node_id_count="$(printf '%s\n' "$node_ids" | awk 'NF { count++ } END { print count + 0 }')"
  if [ "$node_id_count" -ne 1 ]; then
    echo "expected exactly one node_id in $file, got $node_id_count" >&2
    exit 1
  fi
  node_id="$(printf '%s\n' "$node_ids" | awk 'NF { print; exit }')"
  case "$node_id" in
    *[!A-Za-z0-9._-]*|'')
      echo "invalid node_id $node_id in $file" >&2
      exit 1
      ;;
  esac
  case "$seen_node_ids" in
    *,"$node_id",*)
      echo "duplicate node_id $node_id across metrics files" >&2
      exit 1
      ;;
  esac
  seen_node_ids="${seen_node_ids}${node_id},"

  node_addresses="$(sed -n 's/.*node_address="\([^"]*\)".*/\1/p' "$file" | sort -u)"
  node_address_count="$(printf '%s\n' "$node_addresses" | awk 'NF { count++ } END { print count + 0 }')"
  if [ "$node_address_count" -ne 1 ]; then
    echo "expected exactly one node_address in $file, got $node_address_count" >&2
    exit 1
  fi
  node_address="$(printf '%s\n' "$node_addresses" | awk 'NF { print; exit }')"
  case "$node_address" in
    *[!A-Za-z0-9._:\[\]-]*|'')
      echo "invalid node_address $node_address in $file" >&2
      exit 1
      ;;
  esac
  case "$seen_node_addresses" in
    *,"$node_address",*)
      echo "duplicate node_address $node_address across metrics files" >&2
      exit 1
      ;;
  esac
  seen_node_addresses="${seen_node_addresses}${node_address},"
done

metric_total() {
  local metric="$1"
  local group="$2"
  shift 2
  awk -v metric="$metric" -v group="$group" '
    index($0, metric "{") == 1 && index($0, "group=\"" group "\"") > 0 { total += $NF }
    END { printf "%.0f", total + 0 }
  ' "$@"
}

metric_code_total() {
  local group="$1"
  local code="$2"
  shift 2
  awk -v group="$group" -v code="$code" '
    index($0, "elastickv_raft_dispatch_errors_by_code_total{") == 1 &&
      index($0, "group=\"" group "\"") > 0 &&
      index($0, "code=\"" code "\"") > 0 { total += $NF }
    END { printf "%.0f", total + 0 }
  ' "$@"
}

group_activity_file_count() {
  local group="$1"
  shift
  local count=0
  local file
  for file in "$@"; do
    if awk -v group="$group" '
      index($0, "elastickv_raft_send_stream_messages_total{") == 1 &&
        index($0, "group=\"" group "\"") > 0 && $NF > 0 { found = 1 }
      END { exit !found }
    ' "$file"; then
      count=$((count + 1))
    fi
  done
  printf '%d' "$count"
}

IFS=',' read -r -a groups <<< "$EXPECTED_GROUPS"
printf 'streaming_multigroup_soak_metrics_v1\n'
for group in "${groups[@]}"; do
  case "$group" in
    ''|*[!0-9]*|0)
      echo "EXPECTED_GROUPS must contain positive numeric group IDs" >&2
      exit 2
      ;;
  esac
  opens="$(metric_total elastickv_raft_send_stream_opens_total "$group" "$@")"
  reconnects="$(metric_total elastickv_raft_send_stream_reconnects_total "$group" "$@")"
  messages="$(metric_total elastickv_raft_send_stream_messages_total "$group" "$@")"
  snapshots="$(metric_total elastickv_raft_snapshot_stream_sends_total "$group" "$@")"
  snapshot_bytes="$(metric_total elastickv_raft_snapshot_stream_payload_bytes_total "$group" "$@")"
  deadlines="$(metric_code_total "$group" DeadlineExceeded "$@")"
  exhausted="$(metric_code_total "$group" ResourceExhausted "$@")"
  drops="$(metric_total elastickv_raft_dispatch_dropped_total "$group" "$@")"
  step_full="$(metric_total elastickv_raft_step_queue_full_total "$group" "$@")"
  activity_nodes="$(group_activity_file_count "$group" "$@")"

  if [ "$activity_nodes" -lt "$MIN_NODES" ]; then
    echo "group $group has stream activity on only $activity_nodes nodes, want at least $MIN_NODES" >&2
    exit 1
  fi
  if [ "$opens" -eq 0 ] || [ "$reconnects" -eq 0 ] || [ "$messages" -eq 0 ]; then
    echo "group $group missing stream activity: opens=$opens reconnects=$reconnects messages=$messages" >&2
    exit 1
  fi
  if [ "$snapshots" -eq 0 ] || [ "$snapshot_bytes" -eq 0 ]; then
    echo "group $group missing snapshot activity: sends=$snapshots bytes=$snapshot_bytes" >&2
    exit 1
  fi
  if [ "$deadlines" -eq 0 ] && [ "$exhausted" -eq 0 ] && [ "$drops" -eq 0 ] && [ "$step_full" -eq 0 ]; then
    echo "group $group missing backpressure evidence" >&2
    exit 1
  fi
  printf 'group=%s activity_nodes=%s opens=%s reconnects=%s messages=%s snapshots=%s snapshot_bytes=%s deadline_errors=%s resource_exhausted=%s drops=%s step_full=%s\n' \
    "$group" "$activity_nodes" "$opens" "$reconnects" "$messages" "$snapshots" "$snapshot_bytes" "$deadlines" "$exhausted" "$drops" "$step_full"
done
printf 'result=pass\n'
