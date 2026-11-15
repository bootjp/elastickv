#!/usr/bin/env bash
set -euo pipefail

phase=${1:?usage: ci-dynamodb-seal-restart-check.sh capture|verify}
mode=${2:-off}
before=/tmp/elastickv-ci/dynamodb-seal-before.jsonl

read_items() {
  local node=$1 key response
  for key in {0..11}; do
    response=$(curl -fsS --retry 10 --retry-all-errors --retry-delay 1 --max-time 10 \
      -H 'X-Amz-Target: DynamoDB_20120810.GetItem' \
      -H 'Content-Type: application/x-amz-json-1.0' \
      --data "{\"TableName\":\"jepsen_append\",\"Key\":{\"pk\":{\"S\":\"${key}\"}},\"ConsistentRead\":true}" \
      "http://127.0.0.1:6380${node}/")
    jq -cS '.Item // {}' <<< "$response"
  done
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
