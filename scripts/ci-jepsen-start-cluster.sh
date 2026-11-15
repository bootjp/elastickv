#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: ci-jepsen-start-cluster.sh off|on}
phase=${2:-initial}
if [[ $mode != off && $mode != on ]]; then
  echo "invalid encryption mode: $mode" >&2
  exit 2
fi
if [[ $phase != initial && $phase != restart ]]; then
  echo "invalid start phase: $phase" >&2
  exit 2
fi

mkdir -p /tmp/elastickv-ci
bootstrap_members='n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053'
raft_redis_map='127.0.0.1:50051=127.0.0.1:63791,127.0.0.1:50052=127.0.0.1:63792,127.0.0.1:50053=127.0.0.1:63793'
raft_s3_map='127.0.0.1:50051=127.0.0.1:63901,127.0.0.1:50052=127.0.0.1:63902,127.0.0.1:50053=127.0.0.1:63903'
raft_sqs_map='127.0.0.1:50051=127.0.0.1:63501,127.0.0.1:50052=127.0.0.1:63502,127.0.0.1:50053=127.0.0.1:63503'

if [[ $phase == restart && $mode == on && ! -f /tmp/elastickv-ci/kek.bin ]]; then
  echo 'encrypted restart is missing the original KEK' >&2
  exit 1
fi
if [[ $phase == initial && $mode == on ]]; then
  if [[ -e /tmp/elastickv-ci/kek.bin ]]; then
    echo 'initial cluster start found an existing KEK' >&2
    exit 1
  fi
  head -c 32 /dev/urandom > /tmp/elastickv-ci/kek.bin
  chmod 600 /tmp/elastickv-ci/kek.bin
fi

recovery=()
if [[ $phase == restart ]]; then
  # Recover only after every previous owner has stopped; never fence a live mount.
  if [[ -f /tmp/elastickv-demo.pid ]]; then
    while read -r pid; do
      state=$(ps -o stat= -p "$pid" | tr -d ' ' || true)
      if [[ -n $state && $state != Z* ]]; then
        echo "refusing filesystem session recovery while node $pid is still running" >&2
        exit 1
      fi
    done < /tmp/elastickv-demo.pid
  fi
  recovery=(--filesystemStrictRecover)
fi

: > /tmp/elastickv-demo.pid
for node in 1 2 3; do
  if [[ $phase == restart && ! -d /tmp/elastickv-ci/n${node} ]]; then
    echo "node $node is missing its original Raft data directory" >&2
    exit 1
  fi
  encrypted=()
  if [[ $mode == on ]]; then
    mkdir -p "/tmp/elastickv-ci/n${node}"
    encrypted=(--encryption-enabled --kekFile /tmp/elastickv-ci/kek.bin \
      --encryptionSidecarPath "/tmp/elastickv-ci/n${node}/keys.json")
  fi
  mkdir -p "/tmp/elastickv-ci/fs-n${node}"
  ELASTICKV_RAFT_SNAPSHOT_COUNT=20 nohup /tmp/elastickv-bin \
    --address "127.0.0.1:5005${node}" \
    --redisAddress "127.0.0.1:6379${node}" \
    --dynamoAddress "127.0.0.1:6380${node}" \
    --s3Address "127.0.0.1:6390${node}" \
    --sqsAddress "127.0.0.1:6350${node}" \
    --filesystemMount "/tmp/elastickv-ci/fs-n${node}" \
    "${recovery[@]}" \
    --filesystemRootUID "$(id -u)" \
    --filesystemRootGID "$(id -g)" \
    --metricsAddress "" \
    --pprofAddress "" \
    --raftId "n${node}" \
    --raftDataDir "/tmp/elastickv-ci/n${node}" \
    --raftBootstrapMembers "$bootstrap_members" \
    --raftRedisMap "$raft_redis_map" \
    --raftS3Map "$raft_s3_map" \
    --raftSqsMap "$raft_sqs_map" \
    "${encrypted[@]}" \
    >> "/tmp/elastickv-demo-n${node}.log" 2>&1 &
  echo $! >> /tmp/elastickv-demo.pid
done

for _ in {1..90}; do
  ready=true
  for node in 1 2 3; do
    nc -z 127.0.0.1 "6379${node}" && nc -z 127.0.0.1 "6380${node}" && \
      nc -z 127.0.0.1 "6390${node}" && nc -z 127.0.0.1 "6350${node}" && \
      mountpoint -q "/tmp/elastickv-ci/fs-n${node}" || ready=false
  done
  if [[ $ready == true ]]; then
    echo "Cluster is up"
    exit 0
  fi
  sleep 1
done

for node in 1 2 3; do
  tail -n 200 "/tmp/elastickv-demo-n${node}.log" || true
done
echo "Demo cluster failed to start" >&2
exit 1
