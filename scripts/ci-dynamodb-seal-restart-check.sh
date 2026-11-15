#!/usr/bin/env bash
set -euo pipefail

phase=${1:?usage: ci-dynamodb-seal-restart-check.sh capture|verify}
mode=${2:-off}
before=/tmp/elastickv-ci/dynamodb-seal-before.jsonl

read_items() {
  local node=$1 response request cursor=null next page
  for page in {1..1000}; do
    request=$(jq -nc --argjson cursor "$cursor" \
      '{TableName:"jepsen_append",ConsistentRead:true,Limit:100} +
       (if $cursor == null then {} else {ExclusiveStartKey:$cursor} end)')
    response=$(curl -fsS --retry 10 --retry-all-errors --retry-delay 1 --max-time 10 \
      -H 'X-Amz-Target: DynamoDB_20120810.Scan' \
      -H 'Content-Type: application/x-amz-json-1.0' \
      --data "$request" \
      "http://127.0.0.1:6380${node}/")
    jq -e '(.Items | type == "array") and
      ((.LastEvaluatedKey == null) or (.LastEvaluatedKey | type == "object"))' \
      <<< "$response" >/dev/null
    jq -cS '.Items[]' <<< "$response"
    next=$(jq -c '.LastEvaluatedKey // null' <<< "$response")
    if [[ $next == null ]]; then
      break
    fi
    if [[ $next == "$cursor" ]]; then
      echo "DynamoDB Scan cursor did not advance on node $node" >&2
      return 1
    fi
    cursor=$next
    if (( page == 1000 )); then
      echo "DynamoDB Scan exceeded 1000 pages on node $node" >&2
      return 1
    fi
  done | LC_ALL=C sort
}

case $phase in
  capture)
    snapshots=/tmp/elastickv-ci/dynamodb-seal-snapshot-indexes.txt
    : > "$snapshots"
    for node in 1 2 3; do
      status=$(/tmp/elastickv-raftadmin-bin "127.0.0.1:5005${node}" status)
      snapshot=$(awk '$1 == "last_snapshot_index:" { print $2 }' <<< "$status")
      sidecar="/tmp/elastickv-ci/n${node}/keys.json"
      floor=0
      if [[ $mode == on && ! -f $sidecar ]]; then
        echo "node $node is missing its encrypted sidecar" >&2
        exit 1
      fi
      if [[ -e $sidecar ]]; then
        floor=$(jq -er '.raft_applied_index' "$sidecar")
      fi
      if [[ ! $snapshot =~ ^[0-9]+$ || ! $floor =~ ^[0-9]+$ ]] || \
        (( snapshot <= floor )) || [[ $mode == on && $floor == 0 ]]; then
        echo "node $node has no snapshot beyond its sidecar: snapshot=${snapshot:-missing} sidecar=$floor" >&2
        exit 1
      fi
      echo "node $node snapshot index: $snapshot; sidecar index: $floor" | tee -a "$snapshots"
    done
    read_items 1 > "$before"
    if ! jq -es 'any(.[]; has("val"))' "$before" >/dev/null; then
      echo "DynamoDB seal workload left no items to verify" >&2
      exit 1
    fi
    ;;
  verify)
    for node in 1 2 3; do
      after="/tmp/elastickv-ci/dynamodb-seal-after-n${node}.jsonl"
      read_items "$node" > "$after"
      if ! cmp -s "$before" "$after"; then
        echo "DynamoDB seal data changed after restart on node $node" >&2
        diff -u "$before" "$after" || true
        exit 1
      fi
    done
    echo "DynamoDB seal items survived snapshot restart on all nodes"
    ;;
  *) echo "invalid phase: $phase" >&2; exit 2 ;;
esac
