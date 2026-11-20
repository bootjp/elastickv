# split preflight の 2 ページ checkpoint 集約案のレビュー

2026-11-20。共有 checkout の `5ccfbc74a` 上にある 1 ページ・25 ms 版と、その版に対する集約案をレビューした。**読み取り専用の preflight に限れば、最大 2 ページの検査結果を 1 回の CAS で確定する実験に進める。** 採用判断には、最後の export から保存までの競合を検出すること、oversized の補償が別の進捗を取り消さないこと、同条件の負荷で 5 秒と前景性能を両方検証することが必要になる。[現在の preflight](../../adapter/split_job_transfer_preflight.go)・[従来の測定条件](performance_20261119_preflight/README.md)

## 保存と中断の条件

1. **期待値をバッチ開始時に固定する。** 永続 job を `expected`、その `CloneSplitJob` を作業用の `next` とする。2 ページ目は `next` の最初の未完了 bracket と cursor から読み、永続 job との比較には最後まで `expected` を使う。各 export で同じ preflight TS、route 境界、family・除外条件、保持履歴の下限 0 を使う。初期 TS と plan は先に永続化する。失敗後は最後に読めた durable job から再開し、応答を失った CAS がすでに適用済みの場合も `AcceptedRows` を二重加算しない。[初期化・export・保存](../../adapter/split_job_transfer_preflight.go#L129)・[深いコピー](../../distribution/split_job_catalog.go#L933)
2. **保存直前の確認と、トランザクションが守る範囲を明示する。** 現行の保存処理は member・ownership・control の witness を再確認し、coordinator の OCC 読み取り集合には job キーだけを渡す。最後の export 中に catalog version だけが変わり、job が同値のままなら、job の CAS は成功し得る。「catalog 変更時にはバッチを捨てる」を満たすには、preflight 専用の保存で同一 read TS の version と live job を照合し、version キーと job キーを OCC 対象に含める。経路を独立更新できる設計なら対象 route も含める。両ページの開始時と保存直前に、leader・capability・namespace と、元の構成 index・全 apply member に結びついた ownership/control を検査する。別 Raft group に対する RPC と catalog CAS の間には時間差がある。構成変更後の fence/DeltaCopy は、次の通常 attempt が現構成で取り直した admission と durable completion を条件にする。[job CAS の read set](../../adapter/distribution_server.go#L1833)・[構成と全 member の再検証](../../adapter/split_job_transfer_admission.go#L198)・[fence の入口](../../adapter/split_job_runner.go#L1663)
3. **エラーに伴う状態遷移にも同じ期待値を適用する。** 現行の oversize 自動 abandon は phase・snapshot TS・fence TS を照合するため、同じ phase で他 runner が進捗や capability を更新しても条件が成立し得る。2 ページ目のエラーで仮進捗を捨てた後、その古い観測で新しい job を `ABANDONING` にしないよう、preflight の補償側でも元の durable job と scan の同一性・有効な witness を確認する。分類は認証済み source が返した所定の `FailedPrecondition` と構造化 detail に限定する。local page-size エラー、一般の `ResourceExhausted`、取消、結果不明は既存の扱いを維持する。既に保存した前バッチの cursor・診断は補償後も残す。[エラー分類](../../adapter/migration_transfer_error.go#L39)・[自動 abandon の入口](../../adapter/split_job_failure.go#L27)・[補償 CAS](../../adapter/split_job_transfer_preflight.go#L219)
4. **予算切れで進捗を失わない。** 1 ページ目が正常で時間・バイト予算が尽きた場合は、権限を再確認してその 1 ページを保存し、通常の選択に戻す。2 ページ目の検査・export が失敗した場合は仮進捗を公開せず、次の attempt に戻す。空 family の Done も 1 ステップに数える。完了ビットを立てる既存のステップは export RPC ゼロなので、実ページ数と区別する。バッチ内で全 bracket が揃っても、保存成功を確認して yield し、同じ attempt で fence・drain・copy を呼ばない。25 ms と受信 payload は次ページ開始の予算であり、in-flight RPC、最終再検証、CAS、source の先読みを含む実占有時間は別に測る。[scheduler](../../adapter/split_job_preflight_schedule.go#L14)・[完了ステップ](../../adapter/split_job_transfer_preflight.go#L160)・[source の送信ループ](../../adapter/internal.go#L799)

「途中で破棄」の対象はローカルに積んだ scan の進捗である。`splitJobCapabilityReady` は capability 回帰を永続化して fail-closed の制御を適用し得るし、admission にも guard を設置する経路がある。その更新が入ったら元の `expected` でバッチを打ち切り、補償や再承認に必要な状態を保存しておく。crash 時の「最大 2 ページ」は **1 回の中断で runner が受領済みの未確定ページ数** を表す。繰り返し中断した場合の累積再走査量やサーバー側の先読み量にはこの上限を適用できない。[capability 回帰処理](../../adapter/distribution_server.go#L424)・[guard 設置経路](../../adapter/split_job_transfer_admission.go#L55)

## 最小の回帰試験

| 注入位置 | 確認する結果 |
| --- | --- |
| 同じ family の連続 2 ページ、別 family への遷移、空の最終 family | source が受け取った各 cursor/TS が正しい。1 ページ目の直後に catalog を別 reader で読むと旧値、確定後は両ページが一度だけ見える。進捗の catalog 書き込みは 1 回。最後の call で fence RPC・import RPC は 0 回。 |
| 1 ページ後の時間・バイト予算切れ、および 2 ページ目での取消・一時エラー・cursor 不進行 | 予算切れは 1 ページを確定。エラー時は直前の durable prefix を保持。進捗のない再試行で短周期のループに入らず、正常化後に同じページから完走する。 |
| 2 ページ間、最終 export 中、最終検証と Dispatch の間の競合 | 同 phase の別 runner の cursor/診断更新、catalog version の単独更新、leader 喪失、構成 index 変更、learner の ACK 欠落を個別に注入する。期限切れの期待値で進捗や Done を保存せず、新しい構成で再承認するまで fence を出さない。競合を入れる位置は従来の「ページごとの保存後」から移す。 |
| ページ 2 の正規 oversized と、似た形の transport/local エラー | 正規 verdict と有効な期待値の場合だけ自動 abandon。途中で同 phase の進捗が先行したケースでは後続 job を保全する。単一ステップ経路の既存テストが要求する「既に durable な正常ページは残る」と、集約経路の「未保存のページ 1 は破棄」を両方検査する。 |
| 2 ページ受領直後・CAS 前の停止、CAS 適用後・応答前の停止 | 新 runner がカタログから再開する。前者ではそのバッチの受領済み最大 2 ページを再検査し、後者では確定済みの cursor から始める。両方で最終 plan・行数・Done が単一ステップ版と一致する。 |

テストの起点は既存の [batch 回帰](../../adapter/split_job_preflight_batch_test.go)、[スケジューラ回帰](../../adapter/split_job_preflight_schedule_test.go)、[historical oversized の実 Raft 試験](../../adapter/migration_quorum_partition_acceptance_test.go#L1013)。投票メンバー再開後に `capability_regressed=true` のまま 360 秒停滞した事例は独立の復旧問題であり、無障害の速度比較からその解消は判断できない。[Jepsen 実行履歴](https://github.com/bootjp/elastickv/actions/runs/44547446968/job/135641719623)

## 5 秒と前景性能の再比較

| VM 150、preflight 区間 | 秒 | read / write p99 (ms) | read / write 成功 ops/s |
| --- | ---: | ---: | ---: |
| P0 比較元、3 回の中央値 | 55.957 | 2.488 / 18.320 | 10,013.8 / 403.1 |
| 前回の preflight 版、3 回の中央値 | 13.147 | 3.362 / 21.230 | 8,999.6 / 355.3 |
| 1 ページ・25 ms、全 member readback・namespace 並列化込み、pilot 1 回 | 4.922 | 3.738 / 23.495 | 7,958.6 / 301.7 |

pilot の read/write エラーはともに 0。前回の preflight 版の中央値に対して p99 はそれぞれ 11.2%・10.7% 増えた。4.922 秒と目標の差は 78 ms で、既存ハーネスの catalog 観測周期 100 ms より小さい。試行数と計時粒度から、5 秒以内の再現性は未確定である。[3 組の結果](performance_20261119_preflight/results.md)・[計時と負荷設定](performance_20261119_preflight/README.md)

ローカルの診断ログ `pilot-debug-progress.log` の記録数は admission 56、advance 55、persist 55。persist は計 0.901 秒、中央値 16.109 ms で、計測点は witness 再検証を含む `persistSplitTransferPreflight` の呼び出し全体にある。ここから純粋な catalog 書き込み時間や、集約で削れる時間は確定しない。比較ではバッチ当たりの実ページ数、1 ページで yield した理由、実 CAS 数、probe 時間、再走査数も記録すると、前景 p99 の変化を説明できる。

集約そのものの効果は **1 ページ・25 ms 版と、同じ版に集約だけを加えた候補** を交互に測る。VM、3 nodes / 2 groups、leader 配置、256 キー × 1 KiB、4 reader / 4 writer の closed-loop、既定 Sync、TLS、profile 無効、新規 data dir、計時区間を揃える。各組の preflight 時間、成功 RPC p99、成功 ops/s、成功・エラー数を両方残し、Done 後の 5 経路 × 600 write と所有者からの読み返しも照合する。5 秒境界の判定には同じバイナリ条件で durable completion を高精度に計時し、過去との比較用に従来の 100 ms 観測も併記する。前回 3 組は比較の基準として残し、既定の「p99 退行 5% 以内」の採用判定を行う場合は、P0 比較元・最低 5 組・予定到着からの latency という元の条件を別に満たす。今回の closed-loop の成功 RPC p99 は、その採用判定を代替しない。[既存の実行順序](performance_20261119_preflight/run-comparison.sh)・[計画の受入条件](2026_11_19_read_write_split_plan_review.md#重大度中5-秒目標と公平性の測定条件を固定する)

## 実装時の回帰検証（VM 再比較前）

2 ページ候補で `TestMigrationQuorumPreparedDrainAndHistoricalPreflight` が失敗した。source-only の実 Raft fixture から `finalizeSplitJobFence` を呼ぶと、初期 checkpoint 保存時の `canContinueSplitPreflight` が存在しない target group の namespace を要求する。元の単一ステップ caller と、両 group を admit 済みの通常 runner の契約を区別する必要がある。単一ステップでも source の全 member/control witness・job/version/route の CAS は維持する。2 ページ目の正規 oversized verdict による補償にも同じ caller の検証範囲を適用する。[既存の実 Raft 回帰](../../adapter/migration_quorum_partition_acceptance_test.go#L1013)

source route だけが最終 export 中に変化するケースには、開始時の descriptor を保管し、保存トランザクションの read TS で全フィールドを比較する修正と回帰を追加した。OCC の route key には実際に読んだ route ID を用いる（関連ファイルは `distribution/catalog.go` を含む）。[保存処理](../../adapter/split_job_preflight_checkpoint.go)・[version を据え置いて route を変更する試験](../../adapter/split_job_preflight_route_test.go)

隔離した同一候補で、単独の phase caller は source group、通常 runner は source / target 両 group の namespace を検証する変更を試した。source-only の実 Raft 回帰が 20.133 秒、終了コード 0 で通った。再現用差分は `/private/tmp/elastickv-checkpoint-ab-20261120/standalone.patch`、ログは同じディレクトリの `standalone.log` にある。1 ページ・25 ms の比較元のソースも同じディレクトリの `one/` に固定した。

## 並行する4ページ候補への適用範囲

依頼の2ページ版はローカルブランチ `feature/preflight-two-page-checkpoints` に固定した。共有checkoutの4ページ候補では、中断1回で再読し得る未確定ページの上限が4になる。4 MiBは次ページを開始する受信payload予算であり、wall timeやsource側の先読みの上限には使えない。2ページ版で通したページ2の競合・中断ケースだけでは、4ページ目に入った競合や最終CAS前の再開は未検証である。

共有checkoutにあるnamespace readbackは、source/targetそれぞれのapply-member集合を取り、learnerも含め、対応protocol・適用構成index・activation ACKを照合し、最後に集合を再読する。正常な「未activate」は通常のactivation手順に戻る。probeは並列に実行するため、別memberのエラーと未activateが同時に返った場合は、先に返ったエラーでfallbackに入る可能性がある。fallback自体も全memberの検証を通過して初めて成功を返す必要がある。[readbackの呼び出し](../../adapter/split_job_namespace.go)・[probeとactivation](../../adapter/migration_namespace_activation.go)・[learner、構成変更の試験](../../adapter/split_job_namespace_readback_test.go)

中間ページでtarget namespaceのprobeを省く案は「各export前に両groupを検証する」という2ページ版の契約を変更する。sourceからのread-only exportを許せるかと、durableなcheckpoint・Done・abandonを書けるかは別の判断になる。target側に変化があった状態でも読み取りを許す案を採る場合、sourceの元の構成・所有権・transfer guardを各ページで確認し、checkpoint・正規oversizedによるabandonの**両方**で元の期待値とfreshな両groupのadmissionを検証する必要がある。各回に最新のtarget構成を取り直すだけでは、バッチ開始時と同じ構成だったことは示せない。構成変更時はバッチを捨てる契約なら、開始時のtarget構成index/集合も保持して照合する。開始時・終了時のACKから、途中の全時点でサービス中だったことまでは言えない。

判断に使う回帰は、ページ2〜4でtarget learnerのACK喪失、target構成変更、ページ4の正規oversizedを注入し、保存前の未復旧時に進捗・Done・ABANDONINGが進まないことを検査する。途中回復して構成が変わった場合の期待結果も契約に合わせて固定する。固定1ページ/2ページの性能比較と4ページ候補の結果はそれぞれの実行ログに対応づける。

共有checkoutの追加案 `ExportRangeVersionsRequest.max_pages` はprotobuf field 13のページ数上限で、未指定と0は従来の全量streamを使う。実装はstoreが返した `Done`・cursorをそのまま送信してから上限で返るので、stream終端だけをfamily完了と見なさず、次のRPCを同じTS・family・排除条件・cursorで再開するcallerに適用できる。旧sourceが未知fieldを無視する混在構成では先読み削減は期待できない。提示された回帰は1/2ページ上限で読み継いだ結果と全量streamの行列、およびページ取得呼び出し数を検査する。前景性能への効果はこの回帰からは判断できず、今回固定した1ページ/2ページのバイナリにはこのfieldを含まない。[送信ループ](../../adapter/internal.go#L799)・[ページ上限の試験](../../adapter/internal_migration_page_limit_test.go)・[固定版の実測結果](performance_20261120_checkpoint_ab/README.md)

## 棄却試行：値を送らないpreflight応答

**要約応答の試作は棄却した。** 最大8ページ、進捗後の待機200 ms、各RPCの `max_pages=1` を揃えたpilotでは、要約応答のほうがpreflight時間と読み書きのp99がいずれも大きかった。以下は各方式の単発の観測値である。

| pilot | preflight時間の上下限（秒） | read p99（ms） | write p99（ms） |
|---|---:|---:|---:|
| 値付き応答 | 4.040–4.060 | 4.177 | 22.094 |
| 要約応答 | 4.458–4.480 | 4.596 | 24.690 |

値付き応答の値は保存済みの生ログ、要約応答の値は実装チャットの実行報告による。共有コードから要約方式を取り下げた。現行の応答は `versions / next_cursor / done` の3フィールドで、先読みの抑制には要求のfield 13 `max_pages` を使う。[値付きpilotの生ログ](performance_20261120_preflight_fair/trials/pilots/pilot-eight.log)・[現行の要求・応答](../../proto/internal.proto#L188)

11月20日02:04 JSTに開始した3組のfull比較は、値付き8ページ版（SHA-256先頭 `d0f562d7`、4 MiB・200 ms予算、進捗後の待機200 ms）と旧13秒版（`177eea00`）が対象。下記は要約を試作した際の設計上の検討事項である。

### 試作時の安全性・互換性レビュー

認証済みsourceが通常のexportと同じ履歴ページを検査し、その結果をpreflight専用の応答として返す。最も重大な誤動作は、copyがこの応答を空のデータページとしてtargetに渡し、行を移さずにcursorやDoneを進めることにある。現在の `copySplitJobStream` は受け取った `Versions` とcursorをそのままimportし、ACK後に進捗を保存する。新sourceは `preflight_only=true` を明示した要求にだけ要約を返す。新copyは要約を受けた時点で、importや進捗変更の前にエラーを返す。旧copyの保護はsource側の要求判定に依存する。[copyの受信・ACK処理](../../adapter/split_job_runner.go#L1460)・[既存のwire形式](../../proto/internal.proto#L188)

**検査対象とバイト数。** 通常の `ExportVersions` が返したページから値を取り除く前に、各versionに `MigrationVersionFitsTransfer`、ページ全体に既存のpayload上限を適用する。現行の関数は `proto.Size` を呼ばず、`len(next_cursor) + Σ(len(key) + len(value))` を計算する。これを `checked_payload_bytes` の定義にし、`checked_versions` は同じページの `len(Versions)` とする。storeの `ExportedBytes` にはversionごとのoverheadが入り、`ScannedBytes` にはfilterで落とした分も入る。これらを代入するとバッチの継続判断が変わる。TS・family・除外条件と値に対するfilterも通常exportと共有する。正規の単一version超過は従来の構造化エラーを返し、ページ全体の超過、通信エラー、要約の検証失敗は自動abandonを許可しない。[サイズ判定とpayloadの算出](../../adapter/internal.go#L839)・[storeのバイト計上](../../store/migration_versions.go#L417)・[sourceエラーの分類](../../adapter/migration_transfer_error.go#L42)

| 受信側の要求と応答 | 進捗を変更する前の扱い |
|---|---|
| preflight要求、要約なしのversion 0 | 通常ページとして値を再検査し、行数とpayloadをローカルで算出する。空familyのDoneと、filter後に0行でcursorだけ進むページも有効。 |
| preflight要求、version 1の要約 | `Versions` が空であること、行数がwire契約の上限内（現行sourceは1,024件）、payloadがcursor長以上かつページ上限以下であることを確認する。0行ならpayloadはcursor長と一致する。未完了なら従来どおり空でないcursorの進行を要求する。 |
| copy要求への要約、未知version、値と要約の混在、version 0に要約専用情報が付いた応答 | ローカルのプロトコルエラー。targetへのimport、当該バッチのcheckpoint・Done・abandonは行わない。要約の存在を表せるサブメッセージ等を使えば、正当な0行と要約の欠落をwire上で区別しやすい。 |

runnerは元ページのpayloadを残り予算から引き、件数を仮の `AcceptedRows` に加える。符号付き整数への変換と累計加算の前に範囲を確認する。1件目の有効な大きいversionは要求した `chunk_bytes` や残り予算を超えて返ることがある。この場合はそのページを検査済みとしてyieldできる。要約のwire長や `chunk_bytes` を元ページのハード上限にすると挙動が変わる。既存のexport TS、実route、全memberのguard、最終CAS、oversized補償時の再検証は共通にする。wireの要約versionと、カタログに保存する検査planのversionは別の意味を持つ。[1件目の扱い](../../store/migration_versions.go#L573)・[preflightの進捗更新](../../adapter/split_job_transfer_preflight.go#L157)

**値の借用。** メモリstoreとPebbleはexport結果にkey/valueのコピーを持ち、adapterのprotobuf変換がさらにcloneする。要約専用経路では、返却済みのstore結果を同期的なサイズ判定に借用し、送信には独立した要約とcursorを使える。Pebble iteratorや復号バッファの寿命をgRPC送信まで延ばす変更は必要ない。削減対象はadapterの追加割り当てと転送量であり、sourceでの履歴走査・復号・filterは引き続き発生する。[メモリstore](../../store/migration_versions.go#L593)・[Pebble](../../store/lsm_migration.go#L321)・[protobuf変換](../../adapter/internal.go#L1516)

**試作時に挙げた回帰試験。** 同じsnapshotに対し値付き応答と要約応答をページごとに照合し、Done・cursor・件数・payload・最終checkpointを比較する。空family、疎な0行ページ、tombstone、複数履歴、閾値ちょうどの行、単一version超過、ページ全体超過を含める。新runner→旧sourceと旧runner→新sourceは、追加fieldが実際に未知になるprotobuf/gRPC境界で検証する。新copyへの要約注入ではimport回数0・target ACKとdurable進捗が不変であること、途中ページの不正要約とCAS応答喪失では再開後の行数が重複しないことを確認する。サイズ超過の履歴versionは既存の実Raft試験と同じ診断を返すことが条件になる。性能比較はページ上限・待機時間・`max_pages=1` を固定し、値付きと要約だけを交互に切り替える。提示された4ページ・8ページのpilotは要約導入前の測定である。[履歴versionの実Raft試験](../../adapter/migration_quorum_partition_acceptance_test.go#L990)

## PebbleのRaft適用中に前景読み取りが待つ箇所

**最初の改善候補は、時刻を指定しない `RawGet` が、進行中のRaft entryのSyncを待たずに直前の公開済み `LastCommitTS` を取得できるようにすること。** 8ページ版の保存済みprofile（`candidate-eight-work.test`、`pilot-eight.mutex`・`pilot-eight.block`、ビルドID `4d429bbd5946331604d3a3e290161ef1cf431c0c`）では、65.57秒の診断全体で次の待ちが記録された。

| 観測した側 | 累積待ち時間 | スタックから分かること |
|---|---:|---|
| mutexを解放した側 | 75.60秒のうち `RWMutex.Unlock` が73.88秒 | `commitRaftEntryScope` 経由が43.65秒、`applyMutationsWithOpts` 経由が28.80秒。両経路とも `mtx` の書き込みロックをPebbleのcommit中も保持する。 |
| 読み取りロックを待った側 | `LastCommitTS` が63.12秒 | うち59.42秒が `RawGet → globalSnapshotTS → ShardStore.LastCommitTS` 経由。ほかに `effectiveMinRetainedTS` が5.80秒、`MigrationServingAdmissionState` が5.62秒。 |

各値は待機したgoroutineの時間の合計で、両側の値には同じ競合が含まれる。43.65秒から1回のSync時間やpreflight中の寄与率は求められない。`ExportVersions` と通常のapplyはどちらも `dbMu.RLock` を取得し、exportは保持期限の確認時に `mtx.RLock` も取得する。今回の計測で前景readの主な待ちとして特定できたのは時刻取得だった。[export](../../store/lsm_migration.go#L12)・[メタデータの読み取り](../../store/lsm_store.go#L1012)・[RawGetの時刻選択とread fence](../../adapter/grpc.go#L225)

### 改善案と維持する契約

1. **公開済み時刻の参照を分ける。** 次のstore変更では、まず読み取り時刻の選択に限り、最後に公開した時刻を独立に参照する案を試す。Raft entryでは `Commit(opts)` 成功後にreadinessの更新を終えてから `maxAdvertisedCommitTS` を反映した時刻を公開し、適用成功を返す。進行中のentryと重なったreadは直前の時刻を選べるが、成功応答の後に開始したreadはそのentryを含む。既存のread fenceと保持期限チェックを通す。時刻の公開をOCCの検査、次の時刻の払い出し、readinessとの整合した参照に流用しない。単発適用、直接書き込み、import・promote、readiness更新、open・restoreも公開元になる。特に `PutAt` は実データを書く前に `alignCommitTS` を呼び、空snapshotのrestoreは時刻を0に戻すため、時刻の各代入にatomic storeを足すだけではこの契約を満たせない。[entryの公開](../../store/raft_entry.go#L229)・[直接書き込み](../../store/lsm_store.go#L2036)・[restore](../../store/lsm_store.go#L3400)
2. **書き込みロック自体の短縮は純粋なデータentryから検討する。** `applyMu` をOCC検査前からcommit・公開完了まで保持し、`dbMu.RLock` で同じDBへのcommitを保証する。時刻を更新し得る全writerが同じ直列化に従うと確認した経路だけで、`mtx` 内で旧時刻を取得し、ロック外でentryのデータ・`metaLastCommitTS`・`metaAppliedIndex` を同一batchで一度だけcommitし、`mtx` を再取得して公開する案が成立する。明示TSの `GetAt`、最新version照会、snapshot、applied index参照がPebble側の新データと旧メタデータを組み合わせて従来の契約を破るなら、この短縮は採用しない。readiness/controlを伴うentryは既存の公開順序を保つ。通常のSync、`forceSync`、entry内の先行書き込みを読めるindexed batchは両案で維持する。[ロック順序](../../store/lsm_store.go#L224)・[entry内のOCC](../../store/raft_entry.go#L112)・[単発適用](../../store/lsm_store.go#L2634)

### 検証条件

- **同時実行と公開順序。** commit直前、Sync待ち、commit成功後の公開直前で適用を停止できる試験を作る。2キーを更新するentryと、時刻なしの `RawGet`、明示TSの同一snapshotでの複数キー読み取り、readiness参照を並行させる。部分適用、新しい時刻での欠落、適用成功応答後の古いreadを検出する。同じキーを同じ開始TSで更新する競合writerでは両方の成功を許さず、同一entryの後続要求からは先行要求の値が見えることを確認する。restore・SST snapshotとの並行実行も `-race` と期限付きで検査する。[entry回帰](../../store/raft_entry_test.go#L42)・[readinessの公開と中止](../../store/raft_entry_readiness_test.go#L11)・[並行OCC](../../store/compact_txn_test.go#L177)
- **耐久性と再開。** commit失敗時に成功応答も新しい時刻の公開も起きないことを検査する。ディスク上のPebbleを使う子プロセスをcommit前とSync成功後・公開前でそれぞれ停止し、再openして値・永続時刻・applied index・対象entryの制御メタデータを照合する。前者は旧状態、後者は新状態が一式そろうこと、同じRaft logを再生しても欠落・二重適用・index後退がないことを確認する。Syncの途中で終了した場合は旧／新いずれか一式を許容する。通常のSyncに加え、設定がNoSyncでも `forceSync` となるentryを対象にする。[applied index回帰](../../store/lsm_store_applied_index_test.go#L78)・[制御メタデータの原子性](../../store/raft_control_metadata_test.go#L23)
- **性能の判定。** 512 KiB・最大8ページ・200 ms待機を固定し、store変更だけを切り替え、同じ4 reader・4 writerで現行版と候補を交互に3組測る。各試行でpreflight時間の観測上限が5秒以内か、read/write別のp99・成功ops/s・エラー数、Done後3,000件のwrite/readbackを記録する。profileは別試行でpreflight区間に絞り、`LastCommitTS` と次に待つ `GetAt`・admissionの待ち、`mtx` の保持時間、Pebble commitの所要時間を別々に測る。待ちの移動をread p99の改善と取り違えない。この比較は将来のstore変更用で、512 KiB版の単発pilotからはロック短縮の効果を判定できない。[既存の時間・成功率の計測定義](performance_20261120_checkpoint_ab/README.md)

## namespaceの能力確認とactivation ACKを1回のprobeで取得する案

512 KiB・最大8ページ・200 ms待機の本比較では、候補のpreflightが1回目は4.140–4.161秒、2回目は5.549–5.571秒だった。2回目はread/writeのp99が4.645/61.365 ms、成功ops/sが6,991.4/226.1、エラーがともに0件。下限も5秒を超えたため同版の追加試行を打ち切った（`final512-progress.log` の `final-2-candidate`）。以下は次の変更案であり、この測定には含まれない。

**削減対象は、既にactiveなgroupの各apply memberに送る2本のRPCのうち、先に送る `OWNERSHIP_CAPABILITY`。** 512 KiB版のsplit callerはこの応答からguardのversion、partitionのversion、適用済み構成indexだけを使う。サーバーは経路によって `MigrationServingState` を読み、ownership generationも検査するが、callerはownershipやこの応答の `Ready` を採否に使わない。提案では成功したprobe応答の既存field 10に `implementedPartitionProtocol()` の結果を載せる。`NAMESPACE_ADMISSION` でguard・partition・namespaceの対応version、期待構成index、namespaceの `Ready` がそろえば1本で済む。field 10は導入済み機能の表示であり、ownership適用、partition receipt、当該jobのguard適用は引き続き各専用probeで検証する。共通装飾は各kindの `Ready` やエラーを変えず、既にpartition能力を設定するkindでの二重評価も避ける。[wire上の各field](../../proto/internal.proto#L412)・[partition能力の判定](../../adapter/internal_partition_protocol.go#L22)・[ownership probe](../../adapter/internal_migration_ownership.go#L35)・[転送時の適用証明](../../adapter/split_job_transfer_admission.go#L179)

| 同じmemberから返ったnamespace応答 | 分岐 |
|---|---|
| guard・partitionは要求versionと一致、namespaceは要求version以上、構成indexが期待値と一致 | `Ready=true` はそのmemberのactivation ACK。`false` は能力確認済み・activation未確認として記録し、他のmemberの検査を続ける。 |
| 上記のうちpartitionだけが0（旧実装の未設定を含む） | 同じmember・同じ非0の期待indexで `OWNERSHIP_CAPABILITY` を取り直し、guard・partition・indexを検証する。旧RPCの `Ready` でnamespace ACKを補わない。旧応答側でACKとindexの対応を確定できない場合は、旧来のcapability→namespaceの順で最後のACKを取り直す。 |
| partitionが未知の非0値、他の必須能力不足、構成不一致、空応答、RPC失敗、期限切れ | この試行を終了する。別memberのACK、過去の応答、activation要求で代用しない。 |

**構成変更との競合。** `probeMigrationNamespace` はengineのindexと永続namespace状態から `Ready` を計算し、共通処理 `bindMigrationProbeConfiguration` が再度engineのindexを読み、応答のindexを上書きする。たとえばpeerが41から42へ追いつく瞬間、41で計算した `Ready=true` に42が付けば、42を期待するcallerは古い証明を受理し得る。統合ACKでは永続状態のversionとindexを同じレコードから読み、そこで検証したindexと要求index、最後に観測したengineのindexの一致を求める。途中で異なった応答は破棄して再取得する。これは最終rosterの再検証だけでは検出できない。[namespaceの判定](../../adapter/internal_migration_namespace.go#L62)・[構成indexの付与](../../adapter/internal.go#L372)・[永続namespaceレコード](../../kv/migration_namespace_protocol.go#L46)

**activationの順序。** sourceとtargetそれぞれの非空のapply-member rosterを取得し、learnerを含む全員の初期probeと必要な旧版fallbackが成功してから、inactiveなgroupに初めてMODE8を送る。`Ready=false` を並列probeの最初のエラーとして返すと、別のmemberの能力不足を見落とすため、能力の成否とactivationの要否を個別に集計する。activeなgroupは提案を省き、inactiveなgroupは従来の構成交渉・MODE8・全員の適用後ACKを通す。最後に両groupのindexと各memberのID・アドレスを再取得して照合する。次の試行では全員を再probeする。[activationと適用後ACK](../../adapter/migration_namespace_activation.go#L79)・[rosterの再検証](../../adapter/split_job_transfer_admission.go#L147)

**採用前の回帰と測定。** 全員が新版かつactiveなら1試行につき各memberのnamespace RPCが1件、capability RPCとMODE8は0件になることを数える。旧版learnerとの混在、旧版の本当の能力不足、`Ready=false` と別memberのエラーの同時発生、sourceがinactiveでtargetの最後のlearnerが失敗する順序を含める。能力未確認時は両groupのMODE8とカタログ進捗更新が0件であることを確認する。実際の `Internal.ProbeMigrationState` でindexを応答生成中に41→42へ切り替え、旧ACKを受理しないことも検査する。activation後のACK喪失、同じindexでのアドレス差し替え、次回probeで能力が消えるケースも停止を検証する。性能は512 KiB版の他の条件を固定し、統合の有無を4 reader・4 writerで交互に比較する。kind別RPC数、preflight時間の観測上限、read/write別のp99・成功ops/s・エラー数を各試行に残す。[全memberのreadback試験](../../adapter/split_job_namespace_readback_test.go#L14)・[activation後のACK試験](../../adapter/migration_namespace_activation_test.go#L171)

## 8ページのpreflightで全memberの再確認を開始時と保存直前に集約する案

**一括CASは維持できる。`splitJobCapabilityReady` までページ間から外す案には、現行の障害時動作が変わる具体例がある。** まずこの検査を各ページの間に残し、sourceのownership/controlと両groupのnamespaceの重複probeを削る範囲で評価する。先のnamespace ACKのindex競合については、共有実装で永続version・indexが1回の `GetAt` から返り、engineの最終indexが違えば期待indexの指定有無によらず `Ready=false` となることを確認した。この修正が結び付けるのは1応答内の状態である。[namespace状態の取得](../../kv/migration_namespace_activation.go#L57)・[応答生成中の構成変更の回帰](../../adapter/internal_namespace_configuration_race_test.go#L28)

**反例になるのは、コピーやfenceを実行しなくても外部クライアントが読み書きする区間。** 例えば、初回Done以前のFence phaseでsourceのトラッカーとpinが既に入り、routeにはまだwrite fenceがない。開始時は全員が対応し、ページ2の直前に1台のcapabilityが落ち、ページ7で同じID・アドレス・構成indexのまま戻る。現行はページ2の前のprobeで低下を観測するとjobに `CapabilityRegressed` を記録し、sourceのread/write fenceとtargetの保護を適用・確認する。提案ではこの間に他の観測者がいなければ、その処理を一度も起動せずcheckpointを保存できる。最終ACKでは、その間に受理された前景操作の可否を遡って変えられない。各ページでのポーリングにも観測間隙はあるが、従来なら検出する履歴が新たに通る。peerが応答を一時的に失っただけなら逆に不要な停止を避けられるため、実際の能力低下と到達性だけの障害は試験で区別する。両rosterの照合はクラスタのcapability gate全体を代替しない。[capability低下時のjob更新](../../adapter/distribution_server.go#L424)・[sourceとtargetの保護](../../adapter/split_job_runner.go#L2134)・[ダウングレード時の契約](../design/2026_06_11_implemented_hotspot_split_milestone2_migration.md#111-rolling-upgrade-compatibility)

**一時的なACK喪失だけでは、走査結果が壊れる反例にならない。** sourceの通常のRaft更新は、同じjob・rangeの既存guardに対しsize guardのversionを弱めず、retention pinはより古い方を保持する。clearでretireしたjobには同じIDでreadinessを再設定できない。したがって「learnerのprobeが失敗したのでpinが外れた」とは推論できない。一方、仮に履歴保護が本当に途切れる経路があるなら固定TSだけでは足りない。例えば `TS=10` のoversized旧版と `TS=20` の小さい新版がある未走査キーで、GCの下限が30へ進むと旧版が消え得る。全履歴exportが保持期限と比較するのは今回の上限TSである。上限100は下限30以上なので、残った小さい版だけでページが成功し得る。この例の前提は履歴の実際の消失であり、通常のguard更新で再現した現象ではない。source再起動・leader交代をまたいでも開始時のpin・size guardが連続すること、retire後は最終ACKが失敗することを検証する。[guard更新のマージ](../../kv/fsm_migration_readiness.go#L95)・[retire済みjobの拒否](../../store/migration_readiness.go#L127)・[compactionのpin](../../kv/compactor.go#L291)・[exportの保持期限判定](../../store/migration_versions.go#L511)

**最終検査は元の証明を読み直す。** ページ間ではctx・leader、開始時のjob・catalog version・route全体、source/targetそれぞれの構成indexと全memberのID・アドレスを照合する。通常保存と認証済みoversizedによる `ABANDONING` の両方で同じ完全な検査を通す。元の証明が失われた場合に、その場でMODE8やguardを再適用して古いページを保存することは許容しない。必要な修復は独立したattemptで行い、最後のdurable cursorから再走査する。catalog CASの対象はjob・version・routeで、他groupの状態とは同一トランザクションにならない。Doneを保存したらyieldし、copy/fenceには次attemptの独立したadmissionを要求する。[保存とoversized補償](../../adapter/split_job_preflight_checkpoint.go#L22)・[guard再適用を含む入口](../../adapter/split_job_transfer_admission.go#L55)・[phase切替](../../adapter/split_job_runner.go#L246)

**回帰試験の判定条件。** ページ2〜7の間で①実際のcapability低下と回復、②target learnerの応答喪失と同一構成での回復、③sourceのretireまたは構成変更を別々に注入する。①は進捗に加え前景リクエストとfail-closed制御の発行を観測する。②はguardと履歴が継続していれば最終全ACK後に一度だけ保存できる。③または最終ACK喪失ではcursor・件数・Done・`ABANDONING` のすべてを据え置く。最終ページが正規oversizedの場合にも同じ競合を入れる。一般のexportエラーは保存直前の検査に到達しないため、能力・構成喪失に起因する一時障害が繰り返されて5回の失敗予算を使い切り、`FAILED` に遷移するケースも確認する。200 msは次ページ開始の予算で、進行中のexportの期限は2秒。障害時の無駄な走査量と保護開始までの時間は実測する。[エラー分類と失敗予算](../../adapter/split_job_failure.go#L27)・[ページ予算とexport期限](../../adapter/split_job_transfer_preflight.go#L68)・[既存のfail-closed回帰](../../adapter/distribution_server_test.go#L3304)

## 最終候補のレビューで残った旧memberのnamespace ACK

**旧memberとの混在時に、別の構成で取得したnamespace ACKを採用できる。** 固定候補 `e1b4de64b834fa6befd6e71a4cbcd7f0ddf36dd0d06fbd9e4b637b7c89ec2044` のcallerは、旧memberがfield 10を返さない場合、最初のnamespace応答から `active` を決め、`OWNERSHIP_CAPABILITY` でguard・partition・構成indexを確認した後、その `active` を返す。旧memberはnamespace markerを読んだ後にengineのindexを再取得し、応答のindexを上書きする。このため、最後の能力確認が正しいindexで成功しても、最初のACKの出所がその構成だったとは言えない。[旧版へのfallback](../../adapter/split_job_namespace.go#L117)・[既存の混在試験](../../adapter/split_job_namespace_readback_test.go#L93)

ローカルのGo試験では、固定候補の `readActiveSplitNamespace` に、旧版 `5ccfbc74a31f` のnamespace判定・能力表示・index付与を組み合わせた。memberが構成9でactiveなmarkerを読んだ後、応答送出前に同じmember集合の構成10をFSMに適用した。期待構成10のcallerは `NAMESPACE_ADMISSION(cfg=10, Ready=true)`、次に `OWNERSHIP_CAPABILITY(cfg=10, Ready=true)` を受け、`active=true` を返した。namespaceの読み取りは最初の1回だけだった。この試験では構成10でもnamespaceはactiveで、確認できた問題は**構成10のmarkerを読んでいない応答が、構成10の適用済みACKとして通ること**である。固定候補のバイナリはSHA-256を照合し、試験には固定版と照合したソースと一時的なGo overlayを使用した。

旧版fallbackでは能力確認に成功した後、同じmember・同じ期待indexでnamespaceを再probeし、その結果でactivationを判定する。再probeのACK欠落・index不一致・RPCエラーはactive扱いしない。混在回帰には、旧応答生成中に9→10へ進むケースに加え、能力確認後のnamespace応答が `Ready=false` の場合に最初の `true` を引き継がないケースを入れる。新版同士の通常経路に追加の往復は不要。新版のindex競合2試験、preflight入口のownership順序試験、既存の旧版fallback試験は、固定版のソースで再実行し4件とも成功した。[新版の競合試験](../../adapter/internal_namespace_configuration_race_test.go#L27)・[ownership順序試験](../../adapter/split_job_transfer_phase_readback_test.go#L12)

### 旧版fallbackの再probeで判明した能力低下

構成を再確認した最後のnamespace応答から `Ready` を採る変更で、先に返った古い `Ready` は破棄される。ただし、再probeの応答が明示したguard・partitionの変化も採否に反映する必要がある。`probeNamespacePeers(..., false)` はnamespace versionと構成indexを検査し、`splitNamespaceResponsesActive` は `Ready` だけを読む。namespaceの `Ready=true` とpartition能力の消失は両立する。[再probeとactive判定](../../adapter/split_job_namespace.go#L108)・[応答で検査する項目](../../adapter/migration_namespace_activation.go#L169)

旧memberと新版memberを1台ずつ含むローカル回帰で再現した。構成は41に固定し、全員の `OWNERSHIP_CAPABILITY` がguard/partitionとも1を返す。最後のnamespace応答で、新版memberだけが `Ready=true` のままpartitionを1から0に落とすと、groupは `active=true, err=nil` となった。未知の非0値2でも同じ結果になった。試験は共有実装の `split_job_namespace.go`（ファイルのSHA-256先頭 `edf42c16fd5b`）を一時overlayで読み、peerの応答を模擬した。検証範囲はgroupの返り値までである。

最初と最後の応答を同じmemberごとに照合し、初回に対応版を報告したfieldが消えた場合、または最後に未知の非0値を報告した場合は、その試行をエラーで終了する。旧memberが両回とも省略したfieldは間に挟んだ明示capability応答で確認できる。全memberの能力を検査してから `Ready` を集約すれば、別memberの `Ready=false` による早期returnで能力低下を見落とさない。回帰ではtarget側の能力低下がsource側のMODE8に先立って検出され、提案数が0件であることも確認する。[両groupの検査とactivationの順序](../../adapter/split_job_namespace.go#L38)

### guardを両方のnamespace応答で0と報告する新版member

guard・partitionの `1→0` と `1→2` を拒否する最終修正について、関連11試験を `-race` 付きで再実行し、すべて成功した。一方、`splitNamespaceCapabilitiesRetained` は初回のfieldが0なら最後の0も受け入れる。新版targetが同じ構成で `NAMESPACE_ADMISSION(guard=0, partition=1)`、`OWNERSHIP_CAPABILITY(guard=1, partition=1)`、`NAMESPACE_ADMISSION(guard=0, partition=1)` と応答すると、guardが使えないという最後の観測を無視してactiveになる。旧memberがいない場合にも起きる。[fallbackへの分岐](../../adapter/split_job_namespace.go#L179)・[最終照合](../../adapter/split_job_namespace.go#L203)

ローカルの追加試験では新版の `Internal.ProbeMigrationState` を使用し、targetのguard提供状態だけを3回のprobe間で切り替えた。対象ソースのSHA-256先頭は `622ff3ab57`。`Ready=true`、partition=1、構成index=9を維持したまま、inactiveなsourceへのMODE8提案を1回観測した。namespaceを導入したコミット `964abaf36` の `ProbeMigrationState` もguard対応時には全応答にfield 9を設定する。初回・最終のnamespace応答ではguardが要求版と一致することを必須にし、旧版互換で省略を許すfieldをpartitionに限れば、このケースを追加RPCなしで拒否できる。回帰の期待値はsourceへのMODE8提案0件である。[能力の付与](../../adapter/internal.go#L362)・[既存の4ケース](../../adapter/split_job_namespace_legacy_capability_test.go#L14)

## 最終レビューの結論

**旧memberとの互換性とnamespace ACKに関するblocking issueは解消した。** 修正後はtargetの最初のnamespace応答が `guard=0` ならその時点で停止する。前節と同じ再現試験で、inactiveなsourceへのMODE8提案は修正前の1件から**0件**になった。[guardの判定](../../adapter/split_job_namespace.go#L179)・[activation前に停止する回帰試験](../../adapter/split_job_namespace_legacy_capability_test.go#L14)

`guard=1, partition=0` を返す旧memberは、同じ構成でpartition能力を明示的に確認し、続くnamespaceの再probeで `Ready=true` を確認すると通過する。この互換性試験と上記の再現試験を含む計12件をローカルで `-race` 付きで実行し、全件が成功した。[旧memberの互換性試験](../../adapter/split_job_namespace_readback_test.go#L93)
