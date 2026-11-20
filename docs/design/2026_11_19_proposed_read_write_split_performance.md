# 読み書きと group split の性能改善計画

Status: In progress (P0/P1 implemented; measured trade-offs remain)
Author: bootjp
Date: 2026-11-19

## 測定対象と限界

開始時の commit は `5ccfbc74a31f6b21aa4d631704ca922a0749c18e`。
初期測定機は Apple M1 Max（10 CPU、64 GiB）、Darwin arm64、Go 1.27.3。
同じホストで別作業のテストとビルドが動いており、途中の load average は 26.51 だった。
元の checkout に他作業の変更が発生したため、固定 commit の別 checkout でも検証した。
続けて Proxmox に専用 VM を作成し、Linux amd64 でも同じ commit を確認した。
今回の数値は局所的な費用と改善候補を探す診断値とし、production の容量や改善率の保証には使わない。

**same-group split** は range を二つに分け、同じ Raft group に残す操作を指す。
**cross-group split migration** は右側のデータを既存の別 Raft group に移し、所有権を切り替える操作を指す。
group 自体の新設、ノード追加、replica の物理配置変更とは区別する。

## Linux VM の測定結果

Proxmox の pve16intel 上に VM 150（elastickv-perf-20261119）を作成した。
Ubuntu 24.04、kernel 6.8.0-124、Xeon Gold 6140、8 vCPU、8 GiB、local-lvm の 48 GiB disk を使う。
初期 OS 更新を完了し、再起動後に CPU 数と NTP 同期を確認してから測定した。
Go 1.27.3、`CGO_ENABLED=0` の static test binary を使い、source と target の leader はそれぞれ node 2 と node 1 だった。
物理ホストは他 VM と共有し、3 Raft nodes はこの一つの VM 内に置く。

| 条件 | 成功 ops/s | 成功操作の p99 | 成功 / エラー |
| --- | ---: | ---: | ---: |
| 移動前 read-only、8 workers | 28,968 | 0.599 ms | 57,939 / 0 |
| 移動前 write-only、8 workers | 308.9 | 37.012 ms | 625 / 0 |
| 移動前 mixed の read、4 workers | 8,001 | 4.297 ms | 16,149 / 0 |
| 移動前 mixed の write、4 workers | 277.5 | 24.143 ms | 560 / 0 |
| same-group split 後 mixed の write | 259.3 | 27.605 ms | 524 / 0 |
| cross-group の Done 後 mixed の write | 340.3 | 10.967 ms | 682 / 7,810 |
| cross-group の Done 後 read-only | 20,935 | 0.815 ms | 41,879 / 0 |
| cross-group の Done 後 write-only | 430.5 | 15.428 ms | 865 / 16,222 |

same-group split の応答は 17.074 ms、cross-group は約 231 秒で Done になった。
移動中は read error 12,143 件、write error 731,824 件を記録した。
Done 後の write-only RPC は約 95% がエラーを返したため、成功 QPS の増加や成功操作だけの p99 低下を性能改善と判断しない。
診断テストはこれらの write error を検出して失敗した。
追試で、初回の診断 fixture が本番 `startDistributionStartup` の `ConfigureGRPCForwardToken` を呼ばず、follower 転送の認証を欠いていたことが判明した。
したがって、上表の Done 後エラー全件を製品の migration 不具合に帰属させることはできず、95% を本番の障害率として扱わない。
leader 直結でも RawKV の経路バージョン未設定による拒否が再現し、この別の原因は短い公開 API 回帰で修正前失敗・修正後成功を確認した。
認証設定を本番と一致させた追試を別 evidence に保存し、異なる fixture の間で QPS 改善率を算出しない。
全キーの値照合では追加の assertion failure は記録されなかったが、定数値を使うため lost update の検査にはならない。

store の 64 B 単発 Raft apply は、3 回の中央値で Sync が 2.755 ms/op、NoSync が 3.538 µs/op だった。
API の Raft WAL、通信、admission 全体を含まない比較なので、この比率を API の改善率にはしない。
Linux の既存自動 cross-group acceptance は 230.37 秒で成功した。
store benchmark と既存 acceptance は成功、追加診断は post-Done write error により失敗という結果を分けて扱う。
測定後に証跡を回収し、VM 150 は停止した。
Linux の CPU profile は migration を含む全期間を対象にしており、syscall、gRPC、runtime の費用と失敗要求の処理が混在している。
正常な steady state の最適化では phase ごとに profile を分ける。

再現条件と測定値は [metadata](../evidence/performance_20261119/metadata.json) と [Linux API 結果](../evidence/performance_20261119/api_split_linux.txt) に保存する。

## 現在の実装

手動の `SplitRange` は同一 group 内の range 分割を行う。
自動 split は既定で無効で、`--autoSplit` で有効にする。
`--autoSplitCrossGroup` も有効にすると、scheduler が選んだ target に対して `StartSplitMigration` を呼ぶ。
target 選択には、既存の候補 group と負荷の証拠が必要になる。
実装根拠は [flag と Splitter の配線](../../internal/serverwiring/autosplit.go)、[target 選択](../../distribution/autosplit/cross_group_scheduler.go)、[cross-group E2E](../../internal/server/autosplit_cross_group_e2e_test.go)。

データ移動は copy、fence、cutover、promotion、read drain、cleanup を伴う。
runner の通常間隔は 1 秒で、一回の呼出しで一つの runnable job を進める。
copy は chunk 1 MiB、scan budget 4 MiB、voter probe は 500 ms、source の read fence grace は 30 秒を使う。
これらは [runner](../../adapter/split_job_runner.go) と [実行ループ](../../adapter/distribution_server.go) にある。
30 秒の安全待ちを含む Done までの時間と、書込みが再開できるまでの時間は別々に測る必要がある。

## 初期 microbenchmark

既存 benchmark を `-benchtime=500ms -count=3 -benchmem` で実行した。
下表は 3 回の中央値で、時間の揺れが大きい項目は範囲も示す。
lease 判定は fake engine、candidate 解決は memory store、TSO fanout は 1 ms の模擬 backend を使う。
これらを実ネットワーク API の QPS に換算しない。

| 対象 | 時間 | 割り当て量 | 解釈 |
| --- | ---: | ---: | --- |
| Pebble Raft apply、Sync、64 B、逐次 | 5.14 ms/op | 約 314 B/op | 単発 apply の WAL 同期費用を含む |
| 同 NoSync | 2.20 µs/op | 約 321 B/op | Raft 永続化を含まないため API 性能には直接対応しない |
| lease 判定 | 45 ns/op | 0 B/op | 判定部分だけの費用 |
| group lease 判定 | 48 ns/op | 0 B/op | 判定部分だけの費用 |
| admission、16 partitions、1キー | 5.75 µs/op | 10,009 B/op、45 allocs/op | 選択数は 1 でも全体の費用が残る |
| admission、128 partitions、1キー | 13.79 µs/op | 44,088 B/op、157 allocs/op | partition 数に応じて費用が増える |
| admission、512 partitions、1キー | 36.02 µs/op | 152,713 B/op、541 allocs/op | 再 split 後の通常アクセスの改善候補 |
| admission、512 partitions、全範囲 | 5.60 ms/op（2.22–11.32） | 約 2.78 MB/op、約 14,852 allocs/op | allocation と実行時間の揺れを要調査 |
| staged candidate、dense 64キー | 154.5 µs/op | 32,656 B/op、468 allocs/op | memory store 上の比較対象 |
| staged candidate、sparse 64キー | 23.17 ms/op（21.18–28.36） | 約 531 KB/op、41,642 allocs/op | history を含む走査と exact probe の費用が残る |

admission benchmark は durable setup と serving-view clone を計測区間の外に置く。
したがって、上表の allocation は serving view の取得全体より小さい。
通常の serving view 取得も state の clone を行うため、別の計測項目として追加する。
根拠は [benchmark](../../kv/migration_frontier_admission_benchmark_test.go) と [serving view](../../store/migration_serving_admission.go)。

## API と split の再現テスト

[TestPerformanceReadWriteSplit](../../internal/server/performance_split_test.go) を追加し、明示指定時だけ動かす。
3 Raft nodes と 2 groups を同一プロセスで動かし、TLS 付き RawKV RPC を一つの固定 ingress に送る。
256キー、値 1 KiB、read-only 8 workers、write-only 8 workers、mixed 4 readers + 4 writers を測る。
初期状態、same-group split 後、cross-group migration 中、移動後を比較する。
成功 QPS、成功操作の p50/p99、エラー件数と worker 順で選んだエラー例、観測した job phase の時刻を残す。
既存の `first_error` は時系列の最初を保証しない。
測定時間には停止時の in-flight RPC の終了待ちも含める。
成功だけの latency と error rate を必ず併記する。

共有 checkout での予備実行では、read-only は 34,727 ops/s（p99 1.511 ms）、write-only は 116 ops/s（p99 113.760 ms）だった。
mixed は読み 14,167 ops/s、書き 51.7 ops/s で、same-group split の catalog commit 応答は 63.867 ms だった。
cross-group は約 2 秒で Backfill、47.9 秒で Fence、97.1 秒で DeltaCopy、144.0 秒で Cutover、148.0 秒で Cleanup を観測した。
180 秒の観測期限では Done に到達せず、診断テストは失敗した。
その間の読み取りは成功 2,465,500 件、エラー 8,053 件、書込みは成功 6,261 件、エラー 675,624 件だった。
記録された read error の一例は `route cutover pending`、記録された write error の一例は `Operation outcome unknown` で、いずれも gRPC code は `Unknown` だった。
全エラーがこの二種類であるとは確認していない。
この実行では他作業のソース変更とホスト負荷があるため、固定 commit での追試結果と混ぜない。
観測期限は既存 acceptance test に合わせて 5 分にした。
期限を延ばすこと自体を性能改善とは扱わず、180 秒で未完了だった結果も保存する。

固定 commit の追試でもテストは失敗した。
この実行は CPU profile を有効にしており、source/target の leader はどちらも node 0 だった。
read-only は 46,694 ops/s（p99 0.510 ms）、write-only は 49.3 ops/s（p99 203.169 ms）、same-group split の RPC 応答は 75.281 ms だった。
cross-group は 226.4 秒で Done を観測した。
移動中の読み取りは成功 3,333,490 件、エラー 9,348 件、書込みは成功 7,378 件、エラー 922,597 件だった。
Done 直後の約 2 秒でも書込みは成功 74 件、エラー 4,468 件で、記録されたエラーの一例は `Operation outcome unknown` だった。
読み取りは同期間に成功 31,734 件、エラー 0 件だった。
当時の診断テストはこの書込み失敗で停止したため、その後の read-only/write-only と最終全キー照合には進んでいない。
診断テストは、通常 phase の失敗を記録しても残りの測定を続けるよう変更した。

Darwin の CPU profile では syscall と runtime の待機関連処理への帰属が大きく、JSON/hash の最適化だけで API が改善すると判断できる結果ではなかった。
CPU profile だけで syscall の内訳や fsync の wall time は確定できないため、Linux で再現を確認し、block/mutex と storage latency も調べる。
実測値と状態は [証跡](../evidence/performance_20261119/metadata.json) に記録する。

固定 commit の既存 `TestAutoSplitCrossGroupThreeNodeTwoGroupAcceptance` は 239.36 秒で成功した。
これは自動選択からデータ移動と移動後 read までの機能確認であり、移動後 write の成功を意味しない。
負荷なしの所要時間も負荷中の 226 秒と近いため、今回の長い総所要時間を foreground 負荷だけの影響とは判断しない。

値は全期間で同じにしており、これは到達性、exists、値の一致の確認である。
更新順序、最新更新の欠落、線形化可能性を証明する workload ではない。
cross-group の測定は既存 helper の通常 HLC 構成であり、専用 group 0 の centralized TSO 構成ではない。
各キーの更新は worker ごとに分け、OCC 競合の影響を限定している。
mixed の reader と writer が扱うキー集合も分かれるため、同じキーの同時更新と読取りの性能は測っていない。
workload は closed-loop で、open-loop 負荷下の queueing latency や coordinated omission を評価しない。

```sh
ELASTICKV_PERF_SPLIT=1 go test ./internal/server \
  -run '^TestPerformanceReadWriteSplit$' -count=1 -timeout=7m -v
```

same-group split の RPC 応答時間は catalog commit まで、全ノードへの catalog 反映は別に待つ。
split RPC 自体の最中に same-group の foreground 負荷はかけないため、その瞬間の停止時間は未計測である。
cross-group は開始から Done まで foreground 負荷を継続する。
同一マシン上の group 移動と固定 ingress の forwarding を含むため、ノード分散による scale-out 効果の検証には別マシンでの追試が必要になる。

## レビュー後の実装順序

[独立レビュー](../evidence/2026_11_19_read_write_split_plan_review.md)の指摘を反映し、最初の実装単位を P0 の RawKV write 回復に絞る。
次の runner 変更は同一 job の preflight の bounded 逐次化から始める。
copy の逐次化は、開始時の期待 job と checkpoint を照合して古い更新を拒否する回帰試験を先行させる。
copy は一回の呼出しで複数 page を処理するため、外側の呼出し回数だけを step budget にしない。

P0 の短い回帰は、実際の migration FSM と store を使い、公開 RawPut/RawDelete を通す。
同一 group split の右子をさらに cross-group split し、影響範囲外の `b`、残留子の `n`、移動子の `y` を分ける。
移動先で失敗する内部エラーは `ErrRouteCutoverPending` で、raw request の `ObservedRouteVersion` が未設定だった。
呼出側が明示した version を維持し、migration の footprint に交差する raw request だけに、routing 開始時に観測した version を設定する修正に着手した。
FSM と proposal の ownership/configuration 検証、timestamp floor、旧所有者の拒否、結果不明の無再送は維持する。
通常の raw request は unpinned のままにする。

Linux の追加検証は、routing engine 上の両子の group/version の収束を確認した後、残留子と移動子それぞれの leader 直結と follower ingress を 60 秒測る。
旧 source leader が移動先へ転送する経路を含めるため、必要な場合は検証前に target leadership を別 node へ移す。
影響範囲外の対照も加え、各経路で毎秒 10 件、予定 600 件を無再試行で送り、送信数と成功数を照合する。
固有キーと固有値を正規所有者から読み返し、公開 code と ErrorInfo の domain/reason を経路別に記録する。
leader の同居を forwarding の証拠に数えない。

P1 の時間上限は次の page を開始する判断に用い、受領済みの durable ack の保存を打ち切るために使わない。
preflight の完了 witness を保存した時点でその batch を返し、fence や lock drain は次の通常処理に委ねる。
CAS 不一致、no-progress、依存先待ち、leader/configuration 変更では継続を止める。
既存 selector は最初の runnable job を選ぶため、初回変更では占有上限までを保証し、job 間の公平性は保証しない。
公平性が必要な場合は、複数 job を逐次選択する scheduler の変更を別に設計する。

## 最初の実装の検証結果

P0 の RawKV 経路バージョン補完を実装した。
本番と同じ Forward 認証を設定した Linux 追試は成功し、Done 後の mixed/write-only の write error は 0 件だった。
経路収束後の約 60 秒に、影響範囲外、残留子の直結/転送、移動子の直結/旧 source leader 経由の各 600 件、計 3,000 件の書き込みと値照合が成功した。
migration は 262.928 秒で Done、試験全体は 342.95 秒、終了コード 0。
batching は未変更であり、migration の短縮や QPS 改善率を示す結果ではない。

対象の kv/adapter suite は既存 timeout 2 tests の除外付きで成功し、両 tests の timeout は未変更 baseline でも再現した。
差分 lint と通常 Jepsen harness の 349 tests は成功した。
全体 Go suite と独立した Redis scaffold の gate は未完了なので、包括的な完了判定は保留する。
詳細、試行間の fixture 差分、source/binary hash、元ログは [実装検証](../evidence/performance_20261119_implementation/README.md) に保存した。
追試用 VM は結果回収後に停止した。

## 改善の優先順位

### P0：比較できる測定基盤

固定 commit と専用の測定時間帯を確保し、benchmark 間の重複実行を避ける。
API workload を 64 B、1 KiB、16 KiB、同時接続 1/8/32/128、read/write 100/0、95/5、50/50、0/100 に広げる。
uniform、Zipf、単一 hot key、同一 group の複数 hot ranges を分ける。
測定開始前の warmup、一定量のキー投入、compaction 安定化を設ける。
容量評価では dataset を cache より大きくし、既定の Sync を固定する。
まず 3 回、最終比較は同条件で 5 回以上実施する。

同一 group、別 group 同一 leader node、別 group 別 leader node の比較を加える。
CPU、heap、mutex/block profile、GC、Raft commit/apply latency、Pebble WAL sync、compaction、lease hit/fallback を同時に観測する。
別ホストの load generator と 3 台の Linux server で RTT と disk を含む基準を作る。
この基準がない段階では「split で何倍高速化」と判断しない。

### P0：移動中の失敗と回復時間

最初に、負荷中の migration が規定時間内に Done へ進むか、Done 後にエラーが収束するかを確認する。
初回診断では Darwin と Linux の両方で公開 write error が出たが、転送の認証設定を欠いていたため原因別に再検証する。
次は source/target の子 range 別、leader への直接 ingress と forwarding 別に write を分け、回復を妨げる条件を特定する。
固定 commit の短い公開 API 回帰でも移動子への write が失敗したため、この回復確認を allocation 最適化より先に行う。
成功 QPS の改善より先に、fence 以降の write error と phase ごとの停止時間を評価する。
予備実行では closed-loop の write が高速に失敗すると次の要求を直ちに送るため、エラー負荷が膨らむ。
固定 offered rate と retry backoff 付き workload を別ケースとして加え、失敗要求の増幅と migration 自体の進行費用を分離する。
`Unknown` を retryable status に変える場合も、outcome unknown の write を無条件に再実行できるとは扱わず、冪等性と結果照会の契約を先に決める。
移動後にデータと所有権が整合することを確認してから、migration の同時実行数や copy batch を増やす。
既存の [手動 acceptance](../../internal/server/split_migration_acceptance_test.go) と [自動 acceptance](../../internal/server/autosplit_cross_group_e2e_test.go) は、移動前の write と移動後の read を確認する構成である。
移動後の write、write の結果照会、再 split 後の write を別に確認する必要がある。

### P1：進捗のある migration で tick 待ちを繰り返さない

[export plan](../../distribution/migrator.go) は user family と 46 個の内部 family を作り、drain-only の lock family を除外するため、export 対象は 46 family になる。
RawKV だけを使う小さな workload でも、この計画自体は全 family を含む。
[copy](../../adapter/split_job_runner.go) は一つの bracket が Done になるとその呼出しを終了し、[preflight](../../adapter/split_job_transfer_preflight.go) は一回に一ページだけを進める。
[runner](../../adapter/distribution_server.go) は進捗の有無によらず通常の 1 秒 tick を待つ。
各 family が一ページで済んでも、連続する 46 回の処理には tick 間の待ちが約 45 回入る。
Linux でも Backfill が約 47 秒、Fence が約 50 秒、DeltaCopy が約 47 秒だったことと整合する。
これは payload のコピー時間とは別の、control loop の待ちの改善候補である。

同一 phase 内で永続化済み cursor や completion witness が進んだ場合は、短い time/bytes/step budget の中で処理を継続する案を最初に評価する。
budget を使い切った場合はいったん scheduler に返し、no-progress、依存先待ち、エラー時には backoff を使う。
各段階の所有権、catalog version、configuration と transfer admission の再検証は維持する。
空の family を証拠なしに省略したり、30 秒の read fence grace を短くしたりしない。

提案する受入条件は、小さな range で family ごとに 1 秒待つ挙動を除き、一 pass の進行費用を 5 秒以内にすること。
foreground の p99 と成功率、一回の占有上限、停止と再起動、重複 import、cursor の durable replay を併せて検証する。
5 秒には budget 後の再スケジュール待ちと checkpoint 保存も含め、preflight、Backfill、DeltaCopy を別々に測る。
4 page ごとに通常の 1 秒 tick を待つ方式では約 11 秒以上かかるため、進捗時の再選択間隔も測定する。
Done 全体の時間は安全待ちと実 I/O を含めて別に報告する。
まず P0 の write 回復の成否を確定し、その結果とこの最適化を混ぜずに比較する。

#### 1件の split 内部をまとめる範囲

ユーザーの指定により、ここで検討するのは一つの split の内部処理である。
複数 range の split を一括開始する API や、複数 job の同時実行は今回の対象に含めない。

| 案 | まとめる単位 | 判断 |
| --- | --- | --- |
| 同じ tick 内で逐次処理 | 同一 phase の複数 family と page | 最初に採用候補として比較する。既存 RPC、receipt、checkpoint の契約を維持できる |
| 進捗保存の集約 | target が durable ack した複数 cursor | 二段階目。RPC と Raft/catalog 更新の費用が残る場合に検討する |
| export/import RPC の batch 化 | 同一 job、同一 phase の複数 family | 三段階目。protocol と peer capability の変更を伴うため、tick 待ちを除いた後に費用対効果を判定する |

同じ tick で処理しても、各 page の import、durable ack、cursor の保存を既存の順序で行う。
先行 page が完了したら job を読み直し、次の page の admission を再検証する。
この方式なら全 family を並列に投げずに tick 間の待ちを取り除ける。
CPU、bytes、step の budget と foreground の p99 を測って、継続処理の上限を決める。

進捗保存を集約する場合は、target の import receipt が先に durable になっていることを条件にする。
現行 [import RPC](../../proto/internal.proto) は job、bracket、batch sequence ごとの cursor を返す。
[duplicate ack の処理](../../adapter/split_job_runner.go) は、再送時に export の切れ目が変わることを想定し、target が保持する cursor を採用する。
一括保存でも、未確認の page を完了扱いにしたり、再送した page の Done を duplicate ack だけから推定したりしない。
target commit 後かつ catalog checkpoint 前で停止しても、既存 receipt から重複なく再開できることを検証する。

RPC を batch 化する場合は、family ごとの ack と失敗を返し、全体の bytes と in-flight memory を制限する。
既存の 1 MiB/page を family 数だけ無制限に積み上げない。
mixed-version peer に部分的に処理されないよう、全 peer の capability を admission 前に確認する。
source と target は別 Raft group なので、両者の更新と catalog を一つのローカル Raft entry にまとめる設計にはしない。

preflight の完了、source write fence と pending lock の drain、delta copy の完了、cutover、promotion と read drain は順序を保つ。
これらの境界を越えて一括成功を返すことは今回の batch 化に含めない。
同一 phase の正常系だけでなく、途中の leader 交代、job の CAS 不一致、timeout、重複 ack、停止直後の再開、oversized page、空 family を受入試験に含める。

### P1：書込みの永続化費用を分解する

既定 Sync のまま、batch 内の mutation 数 1/8/32/128 と複数 proposal の apply を比較する。
Raft WAL sync、FSM WAL sync、applyMu 待ち、proposal 待ちを分け、batching がどこで効くか確認する。
NoSync は既存の opt-in 比較として測り、永続性の確認前に既定値を変えない。
crash/restart、snapshot/restore、Raft log truncate と再適用、acknowledged write の回収を検証する。
CPU profile で request clone が目立つ場合は、ownership 境界を保って大きな value の重複 copy を減らす。

TSO fanout benchmark は模擬 RTT であり、現状の API の bottleneck を証明しない。
batch の既定値は既に 256 なので、単純な拡大を先行しない。
centralized TSO を有効にした実クラスタで refill 待ち、fanout と p99 を確認し、必要な場合だけ prefetch 等を検討する。
根拠は [TSO flags](../../internal/tsowiring/tso.go) と [fanout benchmark](../../kv/tso_fanout_benchmark_test.go)。

### P1：partition の point admission を選択範囲に比例させる

現在は [range proof 選択](../../kv/migration_apply_range_proofs.go) が全 partition の proof と identity を作ってから交差範囲を絞る。
point access でも allocation が partition 数に比例する理由の一つである。
apply 時に immutable な区間 index を作り、通常の disjoint frontier は二分探索と交差範囲の列挙で選ぶ設計を検討する。
serving view 全体の clone を要求しない限定 API も併せて設計する。
重なった legacy 証明の保守的な選択、unknown barrier、catalog/configuration の一致、差替えられた receipt の拒否を維持する。

evidence index は既に存在するため、単に「検証結果を cache する」だけの変更は避ける。
[evidence index](../../store/migration_partition_evidence_index.go) は選択した partition と active member の digest を都度検証する。
profile で JSON/hash/clone が残ることを確認してから、外部から変更できない handle を境界に導入できるか評価する。
可変な構造体の検証を省略する変更にはしない。

提案する受入条件は、512 partitions の point selection を allocation 90% 減、時間 50% 減とし、16 partitions の性能を 5% 超悪化させないこと。
これは今後の目標であり、達成済みの改善率ではない。
microbenchmark と serving-view 取得を含む benchmark の両方で確認し、API の p99 が悪化しないことも確認する。

### P2：migration と再 split の tail latency

job の各 phase と foreground error code を時系列で記録し、copy、fence、cutover、promotion、read drain、cleanup の費用を分ける。
P1 の control loop 改善後に、serial voter probe、catalog 保存回数、実データ量に比例する費用を profile で切り分ける。
bounded な voter probe 並列化、コピーの bytes/s と outstanding bytes 制限を候補にする。
read fence grace や voter/configuration barrier は安全条件なので、総所要時間を縮めるために削らない。
目標は同じ offered load で migration 中の成功 QPS を baseline の 80% 以上、p99 を 2 倍以内に保つこととし、cutover の retryable error は code 別に件数と時間幅を報告する。
この目標は P0 の安定した基準取得後に見直す。

### P2：staged visibility の sparse scan

既存の 1 MiB scan budget は無制限走査を防ぐが、疎な候補に対して不要な history の decode が残る。
[candidate 解決](../../kv/shard_store.go) で exact-key probe と bounded export の切替を比較する。
memory と Pebble、履歴数 1/16/128、tombstone、TTL、staged/live 両方の候補を含める。
point admission の改善と混ぜず、一変更ずつ比較する。

## split の追加検証と完了条件

same-group split の直前と直後、cross-group の全 phase、移動後の再 split を負荷中に試す。
route 数 16/128/512/4096、移動量 1 MiB/100 MiB/1 GiB、更新継続、scan、cross-group transaction を組み合わせる。
source/target leader の交代、遅い follower、RPC timeout、runner 再起動、import 重複、catalog watch 遅延を注入する。
最新値を識別できる workload と history checker で lost write、stale read、二重所有、timestamp floor 違反を確認する。

変更の完了条件は、対象の unit/integration、`go test ./...`、lint、通常 Jepsen と Redis Jepsen の成功、および同条件の baseline 比較の保存とする。
性能改善は一つの bottleneck ごとに分け、正しさと durable recovery を維持した差分だけ採用する。
計画レビュー後、P0 の RawKV 経路バージョン付与を実装し、検証を進めている。
preflight の batch 化を実装し、同条件の 3 組比較を保存した。
負荷下の p99 と 5 秒目標には未達の項目がある。
copy の batch 化と設定既定値の変更はまだ行っていない。

## preflight の bounded 逐次処理

計画の次の実装単位として、通常 runner 内の preflight を最大 16 pages、受信 payload 4 MiB、次ページ開始判断まで 200 ms の予算で連続処理する。
ページごとの checkpoint 永続化と次ページの再 admission を維持し、完了 witness の保存時にその tick を返す。
200 ms は処理中の RPC や保存を cancel する hard timeout ではない。
単独の phase 呼出し、copy と import ACK の保存単位は変えない。
同一 Linux VM、同じ P0 修正・TLS・Forward auth・Sync・leader 配置・負荷で baseline と candidate を交互に 3 組測る。
正確な差分、測定 source、再現コマンドと結果は [preflight 比較証跡](../evidence/performance_20261119_preflight/README.md) に保存する。

### 同条件の Linux 比較結果

P0 修正、TLS、Forward auth、既定 Sync、同じ 256 keys / 1 KiB value、source node 0 / target node 1 の配置を固定し、同じ workload を交互に 3 組測定した。
split Done の中央値は 260.438 → 206.170 秒（20.8% 短縮）、preflight は 55.957 → 13.147 秒（76.5% 短縮）だった。
Done 後は全 6 run、計 18,000 件の書き込みと固有値の読み返しが成功した。

ただし preflight 中の read/write ops/s はそれぞれ -10.1% / -11.9%、成功 RPC の p99 は read が 2.488 → 3.362 ms、write が 18.320 → 21.230 ms だった。
この負荷下の 5 秒目標は未達で、前景性能との trade-off が残る。
移動中のエラーも残っており、処理量と通常 p99 の全面的な改善、可用性向上、包括的 gate の成功を主張しない。
移動前後の read-only/write-only/mixed、範囲、エラー数、測定限界は [詳細結果](../evidence/performance_20261119_preflight/results.md) と [判断](../evidence/performance_20261119_preflight/README.md) に記録した。
対象の split suite、race、差分 lint、通常 Jepsen harness 349 件は成功した。
standalone Redis の namespace 欠落による gate は未完了。
VM 150 は証跡回収後に停止した。

## 5秒目標に向けた追加実装（2026-11-20）

単一splitのpreflightを最大8ページ・受信4 MiB・次ページ開始判断200 msの予算で処理し、最後に1回のOCCでcheckpointを保存する。
chunk/scanは各512 KiB、進捗時の継続timerは100 ms、exportは任意のmax_pages=1で不要な先読みを抑える。
ページ間のleader・capability・namespace・所有権とguardの全member検証は維持した。
検証を開始時と終了時だけに集約する案は、途中の能力低下に対する保護動作を飛ばすため採用しない。

namespace応答にinstalled capabilityを載せ、既存guardの一致はreadbackし、preflight入口の重複ownership probeを1巡だけ省く。
namespaceの永続versionと構成indexは1つのrecordから読み、応答中に構成が変わったACKを拒否する。
旧版互換で省略を許すのはpartition fieldだけで、guard未対応は初回から拒否する。
旧版fallbackでは明示capability後にnamespace ACKを取り直し、初回に報告された能力の消失・未知versionを全memberで照合してからactivationする。
Astraが指摘した競合と混在時の反例はred/green回帰で確認し、修正後の対象suite・race・lintを実行した。

比較元を前回の16ページ・1秒tickに固定し、最終修正後の同じバイナリでfull比較3組（AB/BA/AB）を実行する。
各試行でpreflightの時間上下限、全区間のp99・成功ops/s・エラー数、Done後5経路×600件の固有値のwrite/readbackを保存する。
途中の予備試行や互換性追修前のprefixは最終候補の統計に混ぜない。
実装、入力SHA、レビュー、回帰と結果は[再比較の証跡](../evidence/performance_20261120_preflight_fair/README.md)にまとめる。

### 追加実装の比較結果

同じ最終バイナリのfull比較3組が完了した。
preflightの観測上限は候補の全3回で5秒以内（3.300〜3.649秒）、中央値は13.030→3.541秒だった。
split Doneの中央値は204.458→191.911秒、移動後の全18,000件のwrite/readbackが成功した。
preflight中のwrite p99は20.306→20.280 msでほぼ維持できたが、read p99は3.092→3.343 ms（8.1%増）で、成功ops/sもread 8.9%減・write 5.4%減だった。
5秒目標はこの条件で達成したが、前景性能の正式採用ゲートは未評価で、悪化がすべて解消したわけではない。
次の改善はpreflight区間のprofileを取り、storeのvisibility lockとtimestamp取得の待ちを切り分けてから、公開順序・OCC・crash/restoreの回帰を含む別の差分として進める。
