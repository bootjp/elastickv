# RawKV migration write 回復の実装・検証

対象の基準 commit は `5ccfbc74a31f6b21aa4d631704ca922a0749c18e`。
計画の独立レビューは [review](../2026_11_19_read_write_split_plan_review.md)。
最初の実装は P0 のみとし、runner の batching はまだ変更していない。
測定対象の書き込み API は gRPC RawKV adapter の RawPut、回帰試験では RawDelete も使う。

## 原因と変更

公開 raw 操作は caller の route version が未指定だと `ObservedRouteVersion=0` で proposal へ進んでいた。
migration serving の creation admission は追跡された version を要求するため、split 後の移動子で `ErrRouteCutoverPending` になり、公開 RPC は結果不明エラーを返す。
短い公開 API 回帰を固定 commit に追加すると失敗し、同じ試験が修正後に成功した。

変更は `kv/sharded_coordinator.go` と `kv/sharded_coordinator_migration_version.go` のみ。
routing 開始時の version を捕捉し、選択した group の migration footprint に交差する raw request に限って補う。
caller が明示した version は保持し、通常 key は unpinned のままにする。
FSM/proposal の ownership、configuration、timestamp 検証を緩めず、旧所有者への書き込みは引き続き拒否する。
結果不明の mutation を自動再送する変更も含めない。
差分は [production.patch](production.patch)、source と Linux binary の hash は [source_sha256.json](source_sha256.json)。
VM 上でも binary の SHA-256 `38a8c742a3d3a47104e324aef0c115bc6714ed489c30a6d14a82c2451fc2ff5b` が一致した。

## 診断 fixture の修正

最初の Linux 診断と route version 修正だけの追試には、本番 `startDistributionStartup` が行う `ConfigureGRPCForwardToken` が欠けていた。
このため認証強制された follower-to-leader Forward が失敗し、95% という初回のエラー率は migration の製品障害率として解釈できない。
[route_version_only_linux.log](route_version_only_linux.log) は、この条件で直結 3 経路が各 600/600 成功、転送 2 経路が各 0/600 だった記録。
これは転送側の fixture 不備を識別した中間結果であり、P0 の完了証拠にはしない。

最終追試では既存の本番 sender auth を設定し、テスト終了時にリセットする。
転送の製品コードやサーバー側の認証は変更しない。
異なる auth 条件、migration 中に受理されたデータ量、profile 有無の間で QPS や migration 時間の改善率を算出しない。

## 検証条件

専用 Proxmox VM 150、pve16intel、Ubuntu 24.04、Xeon Gold 6140、8 vCPU、8 GiB、local-lvm。
3 Raft nodes / 2 groups は一つの Linux process 内で動き、外部への API 公開はしない。
Mac の他作業の変更を含めない detached checkout から Go 1.27.3、`GOOS=linux GOARCH=amd64 CGO_ENABLED=0` で構築する。
TLS 付き RawKV、既定の durable Sync を使う。

```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -p 2 ./internal/server -o server-fixed-v2.test
ELASTICKV_PERF_SPLIT=1 ./server-fixed-v2.test -test.run=^TestPerformanceReadWriteSplit$ -test.v -test.timeout=15m
```

same-group split の後、その右子を cross-group split する。
Done 後に全 node の routing engine が catalog version と所有 group に収束したことを検証する。
source/target leader が同居した場合は target の leadership を別 node に移す。
影響範囲外、残留子の直結/転送、移動子の直結/旧 source leader 経由の計 5 経路で、それぞれ 100 ms 間隔で 600 件、無再試行で送る。
各成功 write の固有値を正規所有者から読み返す。
これは steady-state の回復確認であり、障害注入や lost-update history checker を含む包括的な Jepsen 実験ではない。

## 個別チェック

- `route-version-red.log`: 修正前の公開 RawPut 回帰が `route cutover pending` で失敗。
- `regression-final.log`: version 保持、通常キー、旧所有者拒否、および公開 RawPut/RawDelete の回帰が成功。
- `lint-final-v2.log`: 対象 package の新規差分 lint、0 issues。
- `server-build-and-flags.log`: server のコンパイルと migration flag 回帰、成功。
- `jepsen-java21.log`: Java 21 による通常の Jepsen harness unit tests、349 tests / 1,400 assertions、失敗・エラーとも 0。Redis workload tests も含む。
- `jepsen-redis-java21.log`: 独立した `jepsen/redis` scaffold は `jepsen.redis` namespace 不足で実行失敗。この gate は未完了。
- `full-go-test.log.gz`: 共有 checkout の全体 `go test -p 2 ./...` は成功していない。別作業の store 変更もあるため、この実行単独では本差分への帰属を判定しない。adapter の既存 2 integration tests が 10 分で timeout、macOS FUSE の 2 tests が失敗。ビルド中の source 編集で一度発生した server の importcfg error は、source を固定した再ビルドで解消。
- `fuse-platform-control.log`: FUSE の該当 2 tests の個別再実行は成功。全体実行時の失敗を取り消して全 suite 成功とは扱わない。

完了していない包括的 gate と、今回の局所回帰の成功を分けて扱う。

## Linux の最終結果

[final_linux.log](final_linux.log)、[final_linux.exit](final_linux.exit) に記録した最終試験は成功、終了コード 0、342.95 秒。
migration の Done は 262.928 秒、全 ingress の route version は 6、追加の収束待ちは 0 ms だった。
この時間は batching の改善結果ではない。
source leader は node 1、target leader は node 2、いずれも term 2。

| 経路 | 予定 / 送信 / 成功 | エラー | 最初の成功 | 最大 schedule lag（write+read 終了まで） |
| --- | --- | --- | --- | --- |
| 影響範囲外・直結 | 600 / 600 / 600 | 0 | 15 ms | 27 ms |
| 残留子・直結 | 600 / 600 / 600 | 0 | 15 ms | 27 ms |
| 残留子・follower 転送 | 600 / 600 / 600 | 0 | 17 ms | 27 ms |
| 移動子・直結 | 600 / 600 / 600 | 0 | 14 ms | 21 ms |
| 移動子・旧 source leader 経由 | 600 / 600 / 600 | 0 | 14 ms | 20 ms |

収束後の約 60 秒の固定レート試験で、計 3,000 write と正規所有者からの値照合が成功した。
その前の Done 後 closed-loop mixed は write 1,080 件成功 / 0 errors、write-only は 1,109 件成功 / 0 errors。
元の初回診断からエラー率の改善幅や速度倍率を算出しない。

追加の対象 package suite は `go test -p 2 ./kv ./adapter -skip '^(Test_consistency_satisfy_write_after_read_sequence|Test_grpc_transaction)$' -count=1 -timeout=5m` で成功した（[scope-suite-final.log](scope-suite-final.log)）。
除外した 2 tests は、変更を加えていない固定 commit の別 checkout でも各 120 秒 timeout を再現した（[baseline control](adapter-clean-baseline-control.log.gz)）。
この除外付き成功を全体 `go test ./...` 成功とは扱わない。

次の実装は preflight の bounded 逐次処理。
copy は期待 job/checkpoint の比較を強化してから page 単位の上限を導入する。
移動時間、foreground p99、durable restart は、その変更を加えた同条件の比較で評価する。

証跡を回収後、VM 150 を通常 shutdown し、`qm status 150` の `stopped` を確認した（[vm-final-state.log](vm-final-state.log)）。
