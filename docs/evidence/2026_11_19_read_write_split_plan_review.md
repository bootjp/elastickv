# Cross-group split の書き込み回復と runner 改善の独立レビュー

2026-11-19。対象は `docs/design/2026_11_19_proposed_read_write_split_performance.md`。実装の行番号は測定元の `5ccfbc74a31f6b21aa4d631704ca922a0749c18e` に対応する。追加診断 `internal/server/performance_split_test.go` の SHA-256 は Linux 証跡と一致した（`docs/evidence/performance_20261119/linux_sha256.txt:3`）。

**P0 の経路別回帰試験には直ちに着手できる。P1 は同一 job の preflight を最初の実装単位とし、copy は checkpoint の競合を防いでから進める。** 対象は一つの split の中で family と page を逐次処理する変更に限る。

## 重大度・高　P0 の回復を判定できるキーと経路が足りない

Linux では Done を 230.983 秒で観測し、負荷停止後のログ出力は 231.373 秒だった。Done 後の write-only は 2.009 秒間で 17,087 件中 16,222 エラー（94.94%）、成功 865 件。表示されたエラー例は `Unknown / Operation outcome unknown` で、全件の理由は未採取である。既存自動 acceptance の終了コードは 0、診断は 1。既存試験が Done 後に検査するのは移動済みデータと公開 read である。（`docs/evidence/performance_20261119/api_split_linux.txt:15`、同 `:18`、同 `:23`、`docs/evidence/performance_20261119/linux_exits.json:1`、`internal/server/autosplit_cross_group_e2e_test.go:98`・`:111`）

診断は最初に `m` で分割し、その右 range を `t` で分割・移動する。生成キーは `a-*` と `z-*` で、今回の split で source に残る子 `[m,t)` には送信していない。また各 worker はキー配列を 8 個飛ばしで回り、失敗後すぐ次の要求を出す。速く失敗する側ほど試行回数が増えるため、94.94% を失敗するキーや経路の割合には換算できない。集計の `first_error` は worker 順に選んだ一例で、時系列の初回エラーとも限らない。（`internal/server/performance_split_test.go:57`・`:63`・`:85`・`:101`・`:158`・`:195`）

**最小実装と受入条件**

1. 既存と同じ二段 split を使う場合は `p-*` を残留子、`z-*` を移動子、`a-*` を影響範囲外の対照にする。一段 split で再現するときも、catalog から親と両子の区間・所有 group を検証する。両子それぞれに現在の所有 group の leader 直結と別 node からの転送を用意し、各経路で同じ固定レートを送る。移動子は旧所有 group の leader を転送入口に含める。実際の group・leader・term を記録し、同居 leader を転送の証拠に数えない。
2. Done、各 node の durable catalog 反映、各 ingress の routing engine が実際に返す route/version、経路別初回成功を別々に時刻記録する。既存の `waitForCatalogVersion` は catalog の active を検査する。router は watch 経由で更新されるので、`GetRouteWithVersion` で両子の所有 group も照合する。収束までの上限と送信レートを実験前に固定する。収束待ちのエラー・タイムアウトも報告する。（`internal/server/autosplit_e2e_test.go:196`、`internal/server/bootstrap_e2e_test.go:677`・`:689`、`distribution/engine.go:456`）
3. 初回は無再試行の短い回帰で失敗経路を固定する。最終の回復判定では、障害注入のない収束後の連続 60 秒、4 経路すべてで送信予定数・実送信数・完了数を照合し、各経路の有効な要求がすべて成功することを確認する。成功を返した操作は固有キーと値の ID を使って正規所有者から読み返す。古い route で旧所有者に直接提案する別試験では、拒否と無適用を確認する。

## 重大度・高　公開エラーだけでは修正箇所と再試行可否を決められない

`RawPut` は coordinator のエラーを公開用に変換する。結果不明、route fence、lock、応答喪失などが同じ表示になり得る。公開エラーには通常 `ErrorInfo.domain/reason` があり、回帰試験で集計できる。RawPut の wire request は key/value のみで、値に付けた操作 ID は再送を排除しない。現行の raw retry も結果不明や route timestamp floor を除外している。（`adapter/grpc.go:737`、`adapter/grpc_mutation_error.go:24`・`:42`、`adapter/grpc_mutation_error_test.go:32`、`proto/service.proto:39`、`kv/sharded_coordinator_commit_ts_fence.go:124`）

経路別に成功・確定不成立・結果不明を排他的に数え、deadline は属性として付ける。公開 code・detail に加え、テスト内で同じ要求を ingress の dispatch、forward 先、proposal/apply の決定と対応付ける。原因を特定した経路のみ修正する。固定版では同一 node が両 leader の Darwin 実行でも Done 後に失敗したため、Linux の別 leader 配置だけから forwarding を原因と断定できない。対象ブランチには forward の決定分類を変えた commit（例 `55bbca292`、`8c1b793ca`）もあるため、適用先 SHA で再現を確認する。（`docs/evidence/performance_20261119/metadata.json:60`、`docs/evidence/performance_20261119/api_split_fixed.txt:20`）

応答喪失の注入試験は、提案前に確定拒否したケースと、target で適用済みの成功応答を落とすケースを分ける。後者はクライアントで結果不明、サーバー側で一回適用、復旧後の読み取りで当該 ID が見えることを要求する。一般の deadline 後に一度値が見えなくても、遅延中の提案が確定し得る。最終不成立は、未受理または確定拒否の証拠がある場合だけ計上する。無条件再送や公開 status の変更を P0 の回復手段にしない。

## 重大度・高　copy の checkpoint は同一 phase の古い更新を通す

preflight は export 前の job と catalog 上の current が等しい場合にだけ次を保存する。不一致は無更新でも `nil` を返す。一方、copy は current の phase だけを比較して手元の job 全体を書き戻す。coordinator の OCC は保存直前に読んだ版からの競合を検出する仕組みである。export/import 中に別の runner が同一 phase の進捗や capability 回帰を先に保存した場合、古い runner がその**後**に current を読めば、その更新を巻き戻し得る。これはコード上の競合条件で、Linux の foreground write 失敗との因果関係は未検証である。（`adapter/split_job_transfer_preflight.go:119`、`adapter/split_job_runner.go:1493`・`:1591`、`adapter/distribution_server.go:444`・`:1827`・`:1849`）

copy を拡張する前に、page 開始時の job を独立した期待値として保存し、更新時に照合する。次の page は確定した checkpoint を読み直し、同じ job・phase・処理種別で当該 bracket の seq/cursor/Done が期待通り進んだ場合だけ開始する。空 family の完了は cursor が空でも立証できる。時刻の更新、エラーなし、他 runner の進行だけでは継続しない。古い runner が同一 phase の新しい seq と capability 状態を上書きしない試験を先行させる。

## 重大度・高　1 step の境界を page に置く

preflight は一回の `Recv` で一ページを検査し、全 bracket 完了後に `TransferPreflightDone` を別途保存する。Fence 内では、その次の通常処理が実際の fence を開始できる。copy は同一 stream 内で複数ページを既に import・保存するため、外側の phase 関数の呼び出し数に上限を設けても、消費した page 数の上限にならない。（`adapter/split_job_transfer_preflight.go:40`・`:96`、`adapter/split_job_runner.go:1459`・`:1496`・`:1653`）

1. **preflight を先行する。** 一回の scheduler 選択で一つの job を固定し、同じ preflight の page だけを逐次進める。直前の checkpoint、catalog leader、capability、所有権と構成の witness を各 step で再検証する。完了 witness の保存で処理を返す。Fence の獲得、lock drain、delta copy の開始は次の通常処理に委ねる。pending、無進捗、エラー、leader/構成変更、キャンセルでも継続を止め、既存の失敗・abandon 分類を通す。（`adapter/distribution_server.go:390`、`adapter/split_job_transfer_admission.go:176`、`adapter/split_job_failure.go:27`）
2. **copy は page 境界で上限を確認する。** 受信ページ数、payload bytes、経過時間のいずれかで次の page を開始しない。stream を閉じ、次回は durable cursor から再開できるようにする。空ページも 1 step に数える。1 MiB は chunk の目標であり単一 version が上回り得る。合法な大きい 1 行は一度処理して yield し、転送不能な行は既存の preflight/abandon 判定を保つ。疎な走査の費用は payload と別に計上する。現行の export 応答に scanned bytes は無いため、計測は source 側で行うか、page 数と要求した scan budget を区別して報告する。（`adapter/split_job_runner.go:22`・`:1416`、`adapter/internal.go:827`、`proto/internal.proto:209`）
3. **budget の期限で受領済み ack の保存を打ち切らない。** 上限は次の page を開始する判定に使い、すでに開始した export と import の個別 deadline、および親 context での checkpoint を維持する。重複 ack は target の cursor を採り、再 export の Done を流用しない。複数 page の checkpoint 集約と RPC batch は別途設計する。target が永続化するのは job/bracket の最新 seq と cursor であり、古い seq を再送してもその**最新** cursor が返る。応答に ack 済み seq は含まれない。2 page 以上先行させる案では、その差を復元できる契約とクラッシュ試験が先に必要になる。（`adapter/split_job_copy_timeout_test.go:65`、`adapter/split_job_rpc_persistence_test.go:27`、`adapter/split_job_import_replay_test.go:129`、`store/lsm_migration.go:401`・`:426`、`store/migration_versions.go:438`、`proto/internal.proto:232`）

## 重大度・中　5 秒目標と公平性の測定条件を固定する

export 対象は user と 46 内部 family から drain-only の lock を除いた 46 family。Linux の phase 観測差は Backfill 46.977 秒、Fence 49.994 秒、DeltaCopy 46.991 秒で、1 秒 ticker と整合する。時刻は 100 ms ごとの polling による観測値で、各 family の純粋な待ち時間は未計測である。Linux の診断は同一 VM・同一プロセスの 3 nodes、profile ありの一試行、固定 ingress、成功だけの latency、定数値を使った。成功 QPS の増加から回復や容量向上は判定できない。（`distribution/migrator.go:294`・`:316`・`:635`、`adapter/distribution_server.go:296`・`:370`、`docs/evidence/performance_20261119/api_split_linux.txt:10`、`internal/server/performance_split_test.go:61`・`:205`・`:247`、`docs/evidence/performance_20261119/run_linux_perf.sh:14`）

| 試験 | 採用時に必要な観測 |
| --- | --- |
| 小 range の 1 pass | 同じ 46 family、各 1 page の fixture で、pass の最初の page を処理可能になった時点から最後の durable completion witness までを測る。途中の tick 待ち・再スケジュール・checkpoint を含め、preflight、Backfill、DeltaCopy 別に暫定 5 秒以内。計測対象を変更ごとに明記する。30 秒の read-drain grace と Done 全体は別に測る（`adapter/split_job_runner.go:709`）。 |
| 上限と回復 | fake clock と複数 page の stream で step・bytes・時間を個別に使い切り、次の page を開始しないことを検査する。中断後の再開、target commit 後・catalog 保存前の停止、重複 ack の cursor 差、CAS 不一致、空 family、大きい 1 行、timeout、leader と membership 変更を含める。各停止点で再開後の全値・範囲・進捗と通常完走の結果を照合する。 |
| 他 job への影響 | 現行の live job は JobID 順で最初の runnable を選ぶ。budget 後に同じ selector を再実行しても公平性は保証できない。公平性を受入条件にする場合は二つの既存 job を**逐次**選択し、一方が待ち続けても他方が定めた巡回数内に選ばれることを試す。選択順の変更を伴わない初回実装は、一回の占有上限までを保証範囲とする。（`distribution/split_job_catalog.go:383`・`:921`、`adapter/distribution_server.go:495`） |
| foreground への影響 | P0 通過版を比較元とし、profile 無効、同一 offered rate・配置・Sync・データ量で変更前後を交互に最低 5 組測る。成功率、結果不明件数、予定到着からの p99、飽和時成功 ops/s を経路別に報告し、計画の退行上限 5% と測定の揺れを照合する。 |

例えば上限 4 page で毎回次の 1 秒 tick まで待つと、46 page は少なくとも 12 回の選択にまたがる。I/O が十分速くても約 11 秒かかる。5 秒を目指すなら、進捗があり予算を使い切った job をいったん scheduler に返し、他に実行可能な仕事がなければ次の tick より前に再選択できるかを測る。実際の source は stream の送信後すぐ次ページの走査を始め得るため、runner が受領・保存したページ数と source が先読みしたページ数も区別する。（`adapter/internal.go:799`・`:817`）

実装順は **P0 の短い経路別再現 → 原因を確認した経路の修正と 60 秒回復判定 → preflight の bounded 逐次化 → copy の期待状態照合と page 単位の逐次化** とする。今回のレビューは保存済みの証跡とソースの照合であり、追加の負荷試験は実施していない。

## 追補　P0 実装後の証跡照合

**通常運転へ戻った後の RawKV 書き込みについて、計画で指定した経路別の回復条件を満たした。** 固定 commit の回帰試験では移動子の書き込みが `route cutover pending` で失敗した。route version 補完後の Linux 中間試験では直結 3 経路が各 600/600 件成功し、転送 2 経路はそれぞれ 600 件すべてが結果不明だった。本番相当の Forward 送信認証を fixture に加えた最終試験では、routing version 6、source leader 1、target leader 2 の下で転送を含む 5 経路が各 600/600 件成功した。各操作で成功応答の固有値を所有 group から即時に読み返している。（`docs/evidence/performance_20261119_implementation/route-version-red.log:12`、同ディレクトリの `route_version_only_linux.log:106`・`:110`、`final_linux.log:102`・`:108`、`internal/server/performance_split_recovery_test.go:165`）

最終ログから再集計した予定・送信・成功は各 3,000 件、エラー 0 件、元の終了コードは 0。証跡 manifest の全 17 ファイルに加え、source manifest 上の Go ファイル 6 件とビルド時に保存された Linux binary 本体の SHA-256 が一致した。初回のエラー 16,222 件の原因別内訳は未確定のままである。

**経路収束の計時には未観測区間がある。** Done 後、既存の mixed、read-only、write-only と全キー読み返しを済ませてから、経路別試験が時間を取り始める。`routing_convergence_ms=0` はこの検査開始時に既に全 node が一致していた記録である。「Done から何 ms 後にすべての入口が回復したか」を P1 と比較するときは、Done の検出と同時に route/version と各経路の疎通を観測する。応答喪失注入時の適用回数と、再起動後の値回収も独立の検証項目として残る。（`internal/server/performance_split_test.go:117`・`:127`、`internal/server/performance_split_recovery_test.go:104`）

P1 の変更前に、既存の preflight 試験 3 件を共有 checkout で再実行し、終了コード 0 を確認した。各 family の永続化、fence 開始前の witness、cursor の進行、欠損した進捗の拒否を含む。（`adapter/split_job_transfer_preflight_test.go:74`・`:113`・`:141`）

```sh
go test -p 2 ./adapter -run '^TestTransferPreflight(PersistsEveryFamilyBeforeFence|RequiresExplicitDoneAndProgress|CannotTrustIncompleteVersionedEvidence)$' -count=1 -timeout=45s
```
