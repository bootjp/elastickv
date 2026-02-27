# Dockerで4〜5ノードのElastickvクラスタを複数VMで手動構築する（docker compose不使用）

このドキュメントは、`docker run` のみで Elastickv の Raft クラスタを **複数VM** 上に構築する手順です。  
`docker compose` は使いません。**単一VMへの全ノード同居は想定しません。**

## 想定構成

- 1ノード = 1VM
- VM数は 4 または 5
- 各VMは相互にTCP疎通できる（少なくとも `50051/tcp`）
- 各VMに Docker Engine が入っている

例（5ノード）:

| ノードID | VM名 | IP |
| --- | --- | --- |
| n1 | vm1 | 10.0.0.11 |
| n2 | vm2 | 10.0.0.12 |
| n3 | vm3 | 10.0.0.13 |
| n4 | vm4 | 10.0.0.14 |
| n5 | vm5 | 10.0.0.15 |

4ノード構成にする場合は `n5` を除外してください。

## ノード数と耐障害性

| ノード数 | クォーラム | 許容同時障害数 |
| --- | --- | --- |
| 4 | 3 | 1 |
| 5 | 3 | 2 (推奨) |

耐障害性を重視するなら 5 ノード構成を推奨します。

## 手順

### 1) すべてのVMでイメージ取得

```bash
docker pull ghcr.io/bootjp/elastickv:latest
```

### 2) すべてのVMでデータディレクトリ作成

```bash
sudo mkdir -p /var/lib/elastickv
```

### 3) 各VMでノード起動（`docker run`）

`--network host` を使い、RaftアドレスにVMの固定IPを指定します。  
`--raftBootstrap` を付けるのは `n1` のみです。  
`n1` は固定メンバー一覧 `--raftBootstrapMembers` を使ってクラスタを初期化します。

`--raftRedisMap` は **Raft通信の設定ではなく**、Redisコマンドをリーダー側Redisへプロキシするときの対応表です。  
Raftノード間通信は `--address`（gRPC）で行われます。

補足: `RaftAdmin/AddVoter` で後からノード追加する場合、`address` にはそのノードの `--address`（Adapter/raftadmin を提供している共有 gRPC エンドポイント）を指定してください。

共通の `RAFT_TO_REDIS_MAP`（5ノード例）:

```bash
RAFT_TO_REDIS_MAP="10.0.0.11:50051=10.0.0.11:6379,10.0.0.12:50051=10.0.0.12:6379,10.0.0.13:50051=10.0.0.13:6379,10.0.0.14:50051=10.0.0.14:6379,10.0.0.15:50051=10.0.0.15:6379"
```

4ノード構成では最後の `10.0.0.15` エントリを削除してください。

固定メンバー一覧（5ノード例）:

```bash
RAFT_BOOTSTRAP_MEMBERS="n1=10.0.0.11:50051,n2=10.0.0.12:50051,n3=10.0.0.13:50051,n4=10.0.0.14:50051,n5=10.0.0.15:50051"
```

4ノード構成では `n5` を削除してください。

`n1` (bootstrap node) の起動例:

```bash
docker rm -f elastickv 2>/dev/null || true

docker run -d \
  --name elastickv \
  --restart unless-stopped \
  --network host \
  -v /var/lib/elastickv:/var/lib/elastickv \
  ghcr.io/bootjp/elastickv:latest /app \
  --address "10.0.0.11:50051" \
  --redisAddress "0.0.0.0:6379" \
  --dynamoAddress "0.0.0.0:8000" \
  --raftId "n1" \
  --raftDataDir "/var/lib/elastickv" \
  --raftRedisMap "${RAFT_TO_REDIS_MAP}" \
  --raftBootstrapMembers "${RAFT_BOOTSTRAP_MEMBERS}" \
  --raftBootstrap
```

`n2` 以降（非bootstrap）の起動例:

```bash
docker rm -f elastickv 2>/dev/null || true

docker run -d \
  --name elastickv \
  --restart unless-stopped \
  --network host \
  -v /var/lib/elastickv:/var/lib/elastickv \
  ghcr.io/bootjp/elastickv:latest /app \
  --address "10.0.0.12:50051" \
  --redisAddress "0.0.0.0:6379" \
  --dynamoAddress "0.0.0.0:8000" \
  --raftId "n2" \
  --raftDataDir "/var/lib/elastickv" \
  --raftRedisMap "${RAFT_TO_REDIS_MAP}"
```

`n3`〜`n5` も同様に `--address` と `--raftId` を置き換えて起動します。

### 4) クラスタ収束確認

以下は `n1` 上で実行します。

```bash
GRPCURL_IMG="fullstorydev/grpcurl:v1.9.3"

# 全ノードのgRPC起動待ち
for ip in 10.0.0.11 10.0.0.12 10.0.0.13 10.0.0.14 10.0.0.15; do
  until docker run --rm --network host "${GRPCURL_IMG}" \
    -plaintext "${ip}:50051" list >/dev/null 2>&1; do
    sleep 1
  done
done

# メンバー確認
docker run --rm --network host "${GRPCURL_IMG}" \
  -plaintext -d '{}' 10.0.0.11:50051 RaftAdmin/GetConfiguration
```

4ノード構成では `10.0.0.15` / `n5` を上記ループから除外してください。

### 5) 動作確認

`n1` など任意のノードで:

```bash
docker run --rm --network host fullstorydev/grpcurl:v1.9.3 \
  -plaintext -d '{}' 10.0.0.11:50051 RaftAdmin/Leader
```

任意のクライアント端末から:

```bash
redis-cli -h 10.0.0.11 -p 6379 SET health ok
redis-cli -h 10.0.0.12 -p 6379 GET health
```

### 6) 耐障害性確認

5ノード構成なら、2ノード停止まで継続可能です。  
例: `n4`, `n5` のVMで以下を実行:

```bash
docker stop elastickv
```

その後、他ノードに対して書き込み・読み取りできることを確認:

```bash
redis-cli -h 10.0.0.11 -p 6379 SET survive yes
redis-cli -h 10.0.0.12 -p 6379 GET survive
```

## 停止・クリーンアップ

各VMで実行:

```bash
docker rm -f elastickv 2>/dev/null || true
```

データも削除する場合:

```bash
sudo rm -rf /var/lib/elastickv/*
```
