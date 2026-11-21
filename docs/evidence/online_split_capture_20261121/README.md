# RawKV shadow captureの検証結果

検証した実装commitは`3867cd45fcaee145b244079e0e4372ea229c2786`。
追加したbenchmarkと単一ノードetcd-Raftのテストは、同じ本体コードに対して別途実行した。
本体ファイルのSHA-256は`source.json`へ保存し、commit、作業用worktree、固定した検証用worktreeの一致を確認した。

## 実装の対象

今回実装したのは、[RawKV point entryのshadow記録基盤](../../design/2026_11_21_partial_online_split_capture.md)である。
通常のserverでは無効で、CLIやmigration capabilityに有効化経路を追加していない。
未対応commandには記録がなく、この結果から連続転送cursor、所有権、target activation、Split中の書き込み継続を証明することはできない。
継続転送、active session、runtime version gate、seal/cutoverは未接続である。

## 最終チェック

| 検証 | 結果 |
| --- | --- |
| 固定commitで`GOMAXPROCS=4 go test -p 2 -parallel 4 -timeout=30m ./...` | 終了コード0 |
| 同じcommitで対象storeとKVテストを`-race` | 終了コード0。store 4.011秒、KV 2.211秒 |
| 実際の単一ノードetcd-Raft経由でProposalのindexと記録を照合する追加テストを`-race` | 終了コード0。2.437秒 |
| 全体lint | 0 issues |
| Jepsen主ハーネス | 441 tests、6,179 assertions、失敗0、error 0 |
| 上流Redisサブモジュール単独の`lein test` | 終了コード1。`jepsen.redis` namespaceがない既存のplaceholderテストで失敗 |

Jepsenの結果はハーネスの検証であり、新しい障害注入campaignではない。
上流Redisはrepositoryの指定commit `54ab5c10ace229fc780e43e188ef1b64c3a32ee4`をcheckoutしており、上流のテストを変更していない。

対象テストでは、データと記録の原子的な保存、metadata保存失敗、再送、低いtimestamp、TTL、削除、quota、暗号化ヘッダー、record破損、鍵切り替え後の復元、native/SST snapshotを確認した。
3箇所の子プロセス終了試験は、graceful closeなしのprocess crashからの復旧であり、電源断や全replica喪失を再現したものではない。
実FSMの検証では、通常のadmissionを維持し、通知をcommit後まで待ち、不完全なreceiptや誤った拒否結果を成功として扱わないことを確認した。

## レビューで修正した条件

独立した静的レビューで、実際のstagingに基づくeffect収集、暗号化tombstoneのヘッダー認証、展開前の容量確認、indexなしのreceipt変更拒否を確認した。
追加で、timestamp 0を通常経路へ渡す互換性、再送outcome数の照合、quotaとは独立したstagingの記録も回帰テストへ反映した。
最終レビューでは診断用shadow captureとして残る必須修正は指摘されなかった。
レビュー側は最終テストを独立再実行していない。

## 診断benchmarkと除外した試行

保存処理単体のbenchmarkはmacOS開発機で実行した。
16件の1KiB point更新を1つの同期entryへ入れ、inactiveとshadowを100 entryずつ3回比較した。
中央値は5.166msから5.460ms、割当量は23,314 bytesから130,848 bytesへ増えた。
Linuxクラスタでの性能や、変更前のinactive経路に対する回帰の有無を測った結果ではない。

最初の全体Goテストは、実装中のファイル変更によってAPI定義の異なるビルドを混ぜたため除外した。
その後、固定commitの専用worktreeで全体テストを再実行した結果だけを最終結果とした。
最初のLeiningen起動では以前の`/tmp/lein`がなく、次の試行では未初期化のRedisサブモジュールが不足していた。
最終の主ハーネス結果は、インストール済みLeiningenを使い、指定submoduleを初期化してから取得した。
途中の完全なログは`/private/tmp/elastickv-online-split-design-20261121-artifacts`へ保存している。
