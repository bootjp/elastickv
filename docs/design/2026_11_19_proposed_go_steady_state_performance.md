# 現行 Go 実装の平常時の読み書き性能改善

Status: In progress
Author: bootjp
Date: 2026-11-19

## 目的と測定基準

Raft group の構成とキーの所有範囲を固定し、通常の読み書きに必要な CPU と、クライアントが成功応答を受けるまでの p99 を下げる。
最初の変更候補は point read の割り当て削減と、同一 group に同時到着した write の永続化効率である。
どちらも gRPC の応答、読み取りの一貫性、既定の同期耐久性をそろえて比較する。

既存の Linux 診断の初期区間では、1 KiB、8 workers の RawKV で次の結果が得られた。
Go 1.27.3、固定 SHA `5ccfbc74a`、単一 VM 内で 3 ノードを同一プロセスに配置した短時間測定である。[測定ログと実行条件](../evidence/2026_11_19_steady_state_linux_baseline.md)

| 負荷 | 成功 ops/s | 成功要求の p99 | 成功 / エラー |
| --- | ---: | ---: | ---: |
| read-only | 28,968 | 0.599 ms | 57,939 / 0 |
| write-only | 308.9 | 37.012 ms | 625 / 0 |

同じ Linux 環境での Pebble に対する 64 B の単発 apply は、同期ありで中央値 2.755 ms/op、同期なしで 3.538 µs/op だった。
単発 apply の中央値と RPC 全体の p99 は測定箇所も分位点も異なるため、この値から書き込み遅延の内訳は算出できない。
設計確認時の `origin/main`（`afe0a9f9d`）では読み取り要求の寿命管理も更新されている。実装を始める際は、その時点の SHA で定常負荷専用の基準を取り直す。[store 測定ログ](../evidence/2026_11_19_steady_state_linux_baseline.md)・[読み取り要求の実装](https://github.com/bootjp/elastickv/blob/afe0a9f9dc1542b4a039a6a0392776a41370e1ef/kv/read_request.go)

## 読み取りで値長に比例する割り当てを調べる

`RawGet` は、既定の出力レベルが Warn の場合にも、成功経路で key と value を文字列に変換して `InfoContext` に渡す。
Pebble も読み出した値をデコードしてコピーするため、ログ引数の費用は実装間の差から求める。
64 B、1 KiB、16 KiB の各値長で、同じキー列、スナップショット時刻、Warn 設定の成功 hit を基準版と候補版に与える。
候補版の変更は `logger.Enabled(ctx, slog.LevelInfo)` の内側でログ引数を作ることに限る。
各値長で bytes/op と CPU 時間/op の差を求め、割り当てプロファイルは別実行で採ってハンドラと Pebble の割り当て元を照合する。
Info 有効時のログ、空値と欠損の区別、KeyViz の観測回数を回帰試験で確認する。[gRPC ハンドラ](../../adapter/grpc.go)

最新読み、時刻指定の履歴読み、順方向と逆方向の scan を別々に計測する。
leader 直結と follower 経由に分け、lease の成立、ReadIndex への切り替え、snapshot pin の保持時間を記録する。
複数 group にまたがる読み取りでは、read watermark を更新した回数と quorum 待ちを含める。
同じ snapshot で遅延書き込みを挟んだ再読、compaction 中の読み取り、キャンセル後の pin 解放を採用条件とする。[read watermark の既存試験](../../kv/read_watermark_advance_test.go)

## 書き込みの同期回数と待ち時間を減らす

要求受付、Raft 提案、WAL 保存、quorum commit、FSM apply、Pebble batch 完了、応答を同じ操作に対応づけ、キュー待ちと I/O の重なりを確認する。
Raft は一度の Ready で複数 entry を保存できる。group ごとの entries/保存、同期回数/成功 write、batch の bytes、最古要求の待ち時間を測る。
Raft WAL と Pebble のどちらに同期が集中しているかを確認してから、一方ずつ変更する。[Raft の保存処理](../../internal/raftengine/etcd/wal_store.go)・[Pebble の apply](../../store/lsm_store.go)

最初の書き込み候補は、同一 group の `commitRaw` が提案前にまとめる Raw 要求の件数と bytes、先頭要求の待ち時間である。
現行経路はキューの最初の flush で 500 µs 待ち、原則として最大 64 要求を取り出す。
計測では提案経路別に呼び出し数を記録する。
要求ごとの提案コンテキストを保持する `commitRawOwned` が対象負荷を処理する場合、このキューの調整をその負荷の改善候補から外す。[集約と提案](../../kv/transaction.go)

FSM は一つの Raft entry の Raw 要求を順に検証し、対応する store では成功した更新と applied index を同じ batch に書く。
同一キーに「成功、直前の書き込みとの OCC 競合、成功」を並べ、各結果とその順序、各時刻の値、commit 後の適用通知を逐次実行と照合する。
store の読み出しや batch commit が失敗した場合は entry 全体の更新と通知を破棄し、その index を越える前に適用を停止する。
提案前の集約では、commit 済みの entry 間の適用境界を維持する。
連続する三つの entry の中央に障害を注入し、先頭の応答と永続値、中央と末尾の成功応答および通知の件数がゼロであること、再起動時の durable applied index と再適用後の値を検査する。[FSM の試験](../../kv/fsm_batch_entry_test.go)、[完了済みの先頭を検証する試験](../../internal/raftengine/etcd/engine_storage_pipeline_failure_test.go)

WAL 保存を変更する候補では、適用対象のユーザー write がなくても投票などの `HardState` 更新だけで保存を開始する。
同期経路では snapshot があれば先に保存し、次に entries と `HardState` を保存する。
必要な同期が完了してから、それに依存する peer 応答、適用、`Advance` に進む。
試験では投票だけの `Ready` を入力して WAL 再オープン後の term と vote を検査する。
保存処理と同期処理それぞれについて停止中と失敗時を検査し、保存完了に依存する応答と進行を観測する。
WAL の `Save` が内部で同期する経路ではその完了を、明示的に `Sync` を発行する経路ではその完了を待つ。[WAL の試験](../../internal/raftengine/etcd/wal_store_test.go)、[明示的な非同期経路の故障試験](../../internal/raftengine/etcd/engine_storage_pipeline_failure_test.go)
同期前後のプロセス停止、leader 交代、snapshot 復旧後に、成功応答を返した更新が回収できることを確認する。[既存 store benchmark](../../store/lsm_store_sync_mode_benchmark_test.go)

複数 group にまたがる filesystem 操作は実際の操作ジャーナルから計測する。
`RequireAllParticipants=true` の要求では全参加 group の commit 応答を確認した時点までを計時する。
primary 確定後に secondary の応答を失ったケースは結果不明として記録し、復旧後の効果を操作 ID で照合する。[2PC の結果分類](../../kv/sharded_coordinator.go)・[複数 group の journal 試験](../../internal/filesystem/operation_journal_multigroup_test.go)

## 実装順序と採用条件

1. **基準測定。** Linux サーバー 3 台と別ホストの負荷生成器で、TLS、Sync、データセット、leader 配置を固定する。read-only、write-only、95:5 の混合負荷と scan を、均一なアクセスと hot key、同時実行 1/8/32/128 で比較する。
2. **point read。** ログ引数生成だけを変更し、割り当て削減が成功 1 件あたりの CPU または RPC の p99 に現れるか確認する。
3. **write。** 同期回数とキュー待ちから選んだ 1 箇所で集約上限を比較する。低負荷の p99、高負荷の成功 ops/s、故障後の履歴を確認する。

各負荷の到着率は、基準版だけで測った飽和成功 ops/s の 50% と 80%（整数に切り下げ、最低 1 ops/s）とする。
候補を測る前に実際の ops/s、同時実行上限、キー列の seed、期限、データセット、主指標と対象セルを記録して固定する。
各回は 30 秒のウォームアップ後に 120 秒間送信を予定し、最後に送信済み要求の期限まで応答を回収する。
同時実行上限で発行できない予定要求はキューに入れ、期限までに発行できなければ未送信として数える。
予定要求数を分母として、未送信、成功、確定失敗、結果不明を記録し、deadline 超過は別の属性として残す。
成功要求の遅延は予定送信時刻から応答までを測り、送信遅れも記録する。
混合負荷では read と write の成功率と p50/p95/p99 を別々に算出し、飽和時の成功 ops/s は別実行で測る。
同じ時間窓の全サーバーの CPU 時間と割り当て量を成功数で割る。成功数がゼロの窓は停止時間と件数を報告する。
同一条件で基準版と候補版を対にして 10 組測定し、各組の実行順を交互に反転する。
成功数がゼロ、または操作別の p99 を計算する成功が各回 1,000 件未満なら判定を保留し、両版で同じ測定時間を延長して取り直す。

point read の主指標は均一 read-only、1 KiB、同時実行上限 8、到着率 80% での成功 1 件あたりのサーバー CPU 時間とする。
書き込み候補の主指標は同条件の write-only における RPC p99 とする。
各組で候補／基準の比を算出し、10 個の比の中央値と、小さい方から 2 番目および 9 番目を端点とする区間を報告する（独立な組を仮定した中央値の分布によらない約 97.9% 信頼区間）。
採用には主指標の区間上限が 0.90 以下、対象負荷を含む全セルの操作別 p99 の区間上限が 1.05 以下、飽和時の成功 ops/s の区間下限が 0.95 以上であることを求める。
固定到着率の平常時の比較では両版の各回で成功率 100%、結果不明と未送信が各 0 件を求める。
履歴と耐久性の検査に失敗した場合、または区間が閾値をまたいだ場合は採用を保留する。
これらの暫定条件を基準版の測定時に運用 SLO と照合し、変更があれば候補の測定前に記録する。[独立レビュー](../evidence/2026_11_19_go_steady_state_performance_review.md)
各候補を独立した変更として段階導入し、履歴検査の違反、結果不明率の増加、低負荷時の遅延悪化を監視する。
書き込み経路を切り戻す際は受理済み batch の適用と結果通知を完了させてから従来の集約量に戻す。

## 最初の実装とローカル測定

2026-11-19 に、`RawGet` の成功時に Info が有効な場合だけ key と value を文字列化する候補版を実装した。
Go 1.27.3、darwin/arm64、`GOMAXPROCS=1` で、コミット `5ccfbc74a` と、同コミットにガードだけを加えた候補版を比較した。
両版とも同じベンチマークを組み込み、既定の Warn 設定でローカルのハンドラから Pebble の 64 キーを同じ順に読み、各値長で 10 万回すべての hit を確認した。
各セルにつき実行順を交互に入れ替えて 10 組測った。[ベンチマーク](../../adapter/grpc_raw_get_test.go)、[全試行の測定値](../evidence/2026_11_19_steady_state_raw_get.tsv)

| 値長 | 割り当て B/件（基準 → 候補、中央値） | 割り当て回数/件（基準 → 候補） | プロセス CPU 時間/件（候補／基準、組ごとの比の中央値） |
| --- | ---: | ---: | ---: |
| 64 B | 552 → 376 | 9 → 5 | 0.796 |
| 1 KiB | 2,472 → 1,336 | 9 → 5 | 0.824 |
| 16 KiB | 33,193 → 16,696 | 9 → 5 | 0.616 |

割り当ては Go のベンチマーク区間の値である。
CPU 時間は各値長を独立したプロセスで実行し、そのプロセスの user と system CPU 時間の和を読み取り件数で割った値で、起動、データ投入、終了処理を含む。
同条件で別に採った 16 KiB の割り当てプロファイルでは、`RawGet` 自身に帰属する累積割り当て量（pprof の flat）は基準版で 1.53 GiB、候補版で 10 MiB だった。
候補版では `store.decodeValue` が全割り当て量の約 97% を占めた。
3 台構成の採否判定には、TLS 経由の RPC p99、サーバー CPU、飽和時の成功 ops/s の計測が残る。
