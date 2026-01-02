# Local Jepsen VM Cluster

This brings up a 5-node Jepsen lab (control + 5 DB nodes) using Vagrant and VirtualBox.

## Requirements
- VirtualBox (tested with 7.x)
- Vagrant 2.3+

## Boot and Login
```bash
cd jepsen
vagrant up          # spins up ctrl, n1..n5 (first run takes a few minutes)
vagrant ssh ctrl    # control node
```

The repository is rsynced into `~/elastickv` on the control node. SSH between nodes uses the bundled Vagrant insecure key.

## Run Jepsen workload
On the control node:
```bash
cd ~/elastickv/jepsen
HOME=$(pwd)/tmp-home LEIN_HOME=$(pwd)/.lein LEIN_JVM_OPTS="-Duser.home=$(pwd)/tmp-home" \
  lein run -m elastickv.redis-workload \
  --nodes n1,n2,n3,n4,n5 \
  --time-limit 60 --rate 10 --concurrency 10 \
  --faults partition,kill,clock
```

The test will:
- build Linux/amd64 elastickv + raftadmin binaries on the control node,
- deploy them to each VM under `/opt/elastickv/bin`,
- start the cluster (bootstrap on `n1`, join others),
- run the Redis append workload with the Jepsen combined nemesis (partitions + process kills by default).

## Tear down
```bash
cd jepsen
vagrant destroy -f
```

## Notes
- Ports: Redis 6379, gRPC/Raft 50051 on each node.
- SSH defaults: user `vagrant`, key `~/.ssh/id_rsa` inside `ctrl`.
- Adjust fault mix via `--faults partition,kill,clock` (aliases: `reboot` maps to `kill`).
