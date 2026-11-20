# preflight PRの回帰とJepsen stress

公開用差分はmainの`6943eff8ab66143ad023629dd238906eddd5a989`へ適用した。
上流のread API・転送エラー境界・MVCC履歴最適化を保持し、別の作業treeの変更は取り込まない。
過去の性能値は`performance_20261120_preflight_fair/`の固定ソースの値で、新しい統合SHAの性能とは区別する。

## 検証範囲

- 最新mainへの統合ビルドとmigration・namespace・preflight・CLIの対象回帰。
- 正規のpinned `jepsen/redis` submoduleを使うJepsenハーネス単体テスト。
- `Jepsen Split Preflight`：TLS・peer認証・3プロセス・2つの3-voter groupで、部分checkpoint観測後にvoterをSIGSTOPし、復旧後に同じscan TS・planでDoneへ進むことと、全ingressからack済み値を確認する。
- `Jepsen Scheduled Stress Test`のfaults-only：9 adapter × partition/kill/pauseの27ケース。各300秒、反復fault、2 cycle以上、6 workers、rate 10、32 keys、max writes/key 1000を指定する。

専用oracleとTLS/job JSONヘルパーは既存PR #1527から再利用した。
同PRが記録した復旧停止は、この実装でも解消済みとは仮定せず、専用試験の合否で評価する。
Actions run・artifact・checkout SHA・checkerの実行結果は、PR descriptionから参照する。
単体テストの成功をfault campaignの成功として扱わない。
