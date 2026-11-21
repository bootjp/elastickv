# オンラインSplitの変更捕捉基盤

状態：RawKV point entryのshadow記録を実装。
対応する提案は[Split中の書き込み継続](2026_11_21_proposed_online_split_writes.md)を参照。
この変更だけでは現行Splitのfenceを短縮せず、Split中の書き込み継続を有効化しない。

## 実装した処理

`store.ApplyMigrationCaptureEntry`は、確定済みtimestampを持つpoint mutation群を、capture recordと同じRaft entry batchで保存する。
実際のFSMのadmissionを残して利用する場合は、`store.CaptureMigrationRaftEntry`と`MigrationCaptureWriter.Request`を使う。
callbackのエラーは、呼び出し元が無視してもentry全体の失敗として保持する。
callback外のpoint更新、prefix更新、lastCommitTSを保持する別種の更新を混ぜた場合は、capture対象として黙認せずentryを破棄する。

effectは、要求内容から推測せず、`stageEncodedMutations`が成功した後の実際のstagingから収集する。
storeが同じentryの再送と判定して省略したmutationはeffectに含めない。
元のMVCC timestamp、削除、絶対有効期限、値を保持し、pointの適用順序でordinalを付ける。
暗号化したヘッダーは、tombstoneとして内容を捨てる場合も認証する。

保存には既存の`RaftControlMetadataStore`を再利用する。
recordは`R/migration-capture/v1/`以下に置き、indexなしのmetadata更新から保護する。
データ、record、applied indexは同じ同期batchに入り、sourceの通常データと同じ保存時暗号化を使う。
snapshotへの保存と復元も既存のnative形式とSST形式を使う。
鍵切り替え後の復元試験は、旧鍵を保持した構成で行う。
active sessionのretentionと鍵GCを結び付ける制御まで実装したという意味ではない。

## 再送と記録の上限

recordのidentityはsource group、source lineage、job、generation、Raft indexからなる。
同じidentityの再送はcommand digestと記録上限を照合し、保存済みrecordだけを返す。
異なる入力でidentityを再利用した場合は拒否する。
既に適用済みのindexに対応するrecordがない場合、その書き込みを再適用してrecordを後付けしない。

初期値はentryごとの論理payload 256KiB、effect 4096件、request 256件とした。
小さな上限を指定することもできる。
effect数、キー長、暗号化前の上限に対応する保存長を先に調べ、圧縮値は認証後に展開長を調べてから展開する。
記録上限を超えても、正常なユーザー書き込みは維持し、そのentryのrecordを`Complete=false`にする。
その場合も、実際に書き込んだ要求を誤ってConflictまたはRejectedと宣言するcallbackは拒否する。

`Complete=true`は、その明示entryのpoint effectが記録されたことだけを表す。
前後のRaft index、未対応のcommand、全transfer family、base snapshot、所有権、連続した転送cursorの証拠にはならない。
このAPIからtargetのactivationやsource journalのGCを許可しない。

## FSMでのshadow記録

`kv.WithMigrationCaptureShadow`で、明示した診断runだけRawKV point entryを記録できる。
通常は無効で、serverのCLIやmigration capabilityには接続していない。
有効時も、既存の所有権検査、write fence、timestamp検査をそのまま通す。
適用通知はデータとrecordがコミットしてから発行する。
recordの保存に失敗した場合は、データも破棄してRaft applyを停止する。

timestamp 0、空キー、transaction、prefix更新、timestamp binding付きの要求など、未対応のentryは通常の経路を使い、recordを作らない。
したがってshadow記録には欠番があり、連続した移行ログとして扱えない。
再送時はreceiptのoutcome数を要求数と照合し、不一致なら破損として停止する。
保存した粗いRejected分類から元のprotocol固有理由を再構成できない場合は、成功へ変換せずUnknownを返す。

このmodeは、上限付きの診断runで基盤を検証するためのものである。
job全体の容量管理、retention、active sessionの永続化、最低runtime versionのactivation gateはまだ接続していない。
旧版ノードを含むproductionで継続captureとして有効化する経路は提供しない。

## 検証するケース

- storeのデータとrecordが同時に保存され、metadata失敗時は両方残らない。
- 同一入力の再送で再適用せず、異なる入力やrecordのない既適用indexを拒否する。
- 同一キーとtimestampのstore内再送を二重記録しない。
- OCC拒否、TTL、削除、後から適用する低いtimestampを区別する。
- quotaによるinvalid化でも通常書き込みを維持する。
- writerのcallback失敗や誤ったoutcomeを無視しても部分適用しない。
- recordのdigest、identity、ordinal、outcome、version、末尾の余剰データを検証する。
- 暗号化したrecordがWALとsnapshotに平文値を残さず、鍵切り替え後も復元できる。
- record保存前、保存後かつcommit前、commit後の子プロセス終了から復旧する。
- native snapshotとSST snapshotで、データと対応recordとapplied indexが同じ時点へ復元される。
- 実際のFSMでsingleとbatchを記録し、fenceを迂回せず、commit前に通知しない。
- 診断無効時とtimestamp 0の既存動作を保持し、不完全な再送receiptをACKしない。

## 未接続の処理

有効化の前提として、stagedからliveへのpromotionとexact-version照会の競合を修正した。
liveを先に照会すると、2回の照会の間に版が移動した場合に、コミット済みの値やtombstoneを両方で見失う。
既存のGetと同じくstagedを先に照会し、見つからなければliveを照会する。
memoryとPebbleの実際のpromotionを照会の間に挟む回帰テストで検証する。
この修正だけでは、shadow記録やオンラインSplitを有効化しない。

次の段階では、最低runtime versionと全apply memberの対応を先に有効化してから、active capture sessionをRaft controlとして導入する。
全更新経路のcoverage、変更のないindexのcoverage、baseの一貫snapshot、連続したtarget cursorも必要になる。
その後にtargetへの継続転送と事前準備、排他的なCommitとAbort、sourceのseal、短時間の要求待機を接続する。
現段階のshadow recordを、その証拠として読み替えない。

## 保存処理単体のbenchmark

`BenchmarkMigrationCaptureEntry`は、1KiBのpoint更新16件を1つの同期entryで保存するinactiveとshadowを比較する。
macOSの開発機で、100 entryを3回ずつ実行した暫定値は次のとおりだった。

| 指標の中央値 | inactive | shadow |
| --- | ---: | ---: |
| entryあたり時間 | 5.166ms | 5.460ms |
| entryあたり割当bytes | 23,314 | 130,848 |
| entryあたり割当回数 | 36 | 157 |

これはSplitやLinuxクラスタのスループットを測った結果ではなく、他の検証と並行した開発機での診断値である。
既存のinactive経路の変更前後を比較した試験でもない。
shadowのJSON記録と値の複製には割当の増加があり、active sessionをproductionで有効化する前に、記録形式とbufferのprofileを確認する必要がある。
正式な性能採用条件を満たしたとは評価しない。
