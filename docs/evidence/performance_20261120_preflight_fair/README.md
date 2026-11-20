# 単一splitのpreflight再比較

単一splitのpreflightを5秒以内へ短縮し、前景の読み書き性能への影響を測定する。
比較元は前回の16ページ・200 ms予算・1秒tickの版で、両版ともP0の移動中RawPutのroute version修正を含む。
base commitとP0にpreflight差分を重ね、並行作業のstore通常readやRawGetの別の最適化は比較から除いた。
共有checkoutの全差分を組み合わせた性能を示すものではない。
最終比較の値は[結果](results.md)、全区間の値とエラー数は[CSV](metrics.csv)、入力の検証結果は[audit.json](audit.json)に保存する。

## 最終比較の判断

最終修正後の全6試行が成功し、候補は3/3回でpreflightの観測上限5秒以内だった。
候補の観測上限は3.300〜3.649秒。全試行のpreflight中のread/writeエラーは0件で、移動後の計18,000件のwrite/readbackもすべて成功した。

| 各版3回の中央値 | baseline | candidate | 変化 |
| --- | ---: | ---: | ---: |
| preflight観測上限 (s) | 13.030 | 3.541 | -72.8% |
| split Done (s) | 204.458 | 191.911 | -6.1% |
| read p99_ms | 3.092 | 3.343 | +8.1% |
| read ops_s | 9236.600 | 8414.100 | -8.9% |
| write p99_ms | 20.306 | 20.280 | -0.1% |
| write ops_s | 365.600 | 345.700 | -5.4% |

5秒目標はこの条件で達成した。write p99の中央値はほぼ維持できたが、read p99は8.1%増、成功ops/sはreadが8.9%、writeが5.4%減で、前景性能の悪化は残る。
移動後検証の最大lagも25〜681 msとばらつく。各試行の値と移動前後の性能はresults.md、全区間の成功数・エラー数はmetrics.csvに残した。
正式採用ゲートは未評価で、前景性能の全面的な改善を主張しない。
次に取り組む場合はpreflight区間に絞ったprofileでvisibility lockとread timestamp取得の待ちを確認し、Astraレビューの公開順序・OCC・crash/restore条件を満たすstore側の改善を別差分で検証する。

## 固定条件

VM 150、8 vCPU、8 GiB、Ubuntu 24.04、local-lvm、1つのGoプロセス内の3 Raft nodes / 2 groupsを共通にする。
256キー、1 KiB value、4 reader / 4 writerのclosed-loop、既定Sync、TLS、production Forward token、毎回新規data dirを使う。
source leaderはnode 0、target leaderはnode 1。
読み書き先はgRPCのRawKV adapter（RawGet / RawPut）である。
先に同一groupのsplitを行い、その後でgroup間splitを測定する。
移動前後のread-only / write-only / mixed windowは各2秒。

最終比較は、Doneと移動後検証まで進める3組（full、AB/BA/AB）を集計する。
互換性の追修前に採取したpreflight完了で止まる3組（prefix）は`trials/merged-prefix/`へ分け、最終候補の統計に混ぜない。
fullでは各版5経路×600件の固有値の書込みと正規所有者からの読み返しを確認する。
[再現用runner](run-final.sh)は各版で同じ環境変数を使い、既定Syncの上書きを外す。
実行時は[初期runner](run-initial.sh)がprefixとfullの1組目、[続行runner](run-more-full.sh)がfullの2・3組目を担当した。
prefix完了後に旧版fallbackの追修が入り、fullの候補はレビュー済みバイナリの準備完了まで待機した。
その待機はテスト開始前で、Done・preflight所要時間には含めない。
fullでは全3組を同じ修正後バイナリへ揃え、SHAと切替時刻を`final-environment.txt`と実行中バイナリの照合記録に残す。

## 実装

候補は最大8ページを仮進捗として保持し、最後に一度だけcheckpointをOCCで確定する。
RPCのchunkとscanは各512 KiB、次ページ開始の判断予算は受信payload 4 MiBと200 ms、進捗後の継続timerは100 ms。
継続timerは再実行のヒントであり、通常の1秒tickが先に来る場合もある。
進行中のRPC、最終検証、CASを200 msで打ち切ることはしない。
中断した1バッチで再走査する未確定の受領ページは最大8ページで、繰り返す中断の累積量を制限する値ではない。

ページ間のleader・capability・namespace・source所有権とguardの全member確認、および元のjob / catalog version / source route全体の照合を維持する。
最終CASは同じread TSのjobとcatalogを検証し、job・version・実際のroute keyをread setに含める。
Done保存後はyieldし、同じattemptからfenceへ進まない。
exportエラーでは仮進捗を捨て、認証済みoversizedによるabandonも元の証明とcheckpointを再検証する。

不要な処理は次の範囲で減らした。

- exportに任意の`max_pages=1`を指定し、受信しない次ページのsource側の先読みを止める。0は従来のstreamを維持する。旧sourceでは未知fieldが無視されるため、性能効果には更新済みsourceが必要。
- namespace応答からinstalled capabilityとdurable activationを取得し、別のcapability RPCを省く。古い応答には明示的なcapability probeへ戻り、未対応memberを拒否する。
- namespaceのversionとconfiguration indexを1つのdurable recordから読む。応答中にengineの構成が変わったらReady=falseとし、古いACKへ新しいindexを付け替えない。
- 旧版fallbackでは能力確認後のACKを取り直し、初回に報告されたguard・partition能力の消失と未知versionを全memberで検出してからactivationする。
- preflightの入口で同じownership proofを二重に全memberへ問い合わせていた1巡を省く。残す全member ACKとroster再照合はcontrol確認・適用より前に完了する。

[Astraのレビュー](../2026_11_20_preflight_checkpoint_review.md)に従い、ページ間のcapability検査を開始・終了だけに集約する案は採用しなかった。
既存guardを読み返して一致を確認できるときは冗長なproposalを省くが、attemptをまたぐACKのcacheは持たない。
SyncとRaft適用時の原子性を保つstoreのロックは変更していない。

## 検証と再現

`metadata.json`にbase commit、両バイナリのSHA-256、設定と入力ファイルのSHA-256を保存する。
`measured_sources.tar.gz`のcommonは両版共通のP0・測定harness、baselineとcandidateはそれぞれの置換ファイル。
base commitの別々のcheckoutへcommonと各版のファイルを上書きし、Go 1.27.3、`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o baseline.test ./internal/server`で構築する（候補は出力名をcandidate.testにする）。
両バイナリとrunnerを同じディレクトリへ置き、`bash run-final.sh`を実行する。
全6試行の終了コードとモード別の確認条件を満たしたログを回収後、`python3 analyze.py`、`python3 render.py`、`python3 audit.py`で再集計する。

`test_sources.tar.gz`に追加・更新した回帰を保存する。
namespace構成競合、入口の重複probe、旧版ACKの取り直し、混在時の能力低下と初回guard不足のred/green、対象のsplit / migration / namespace / route回帰、race、lint、通常Jepsenハーネス単体テストのログを同じフォルダに保存する。
通常Jepsenは349 tests / 1,400 assertionsが成功した。
Redis専用ハーネスのnamespace不足と、比較元から続く全Go suiteのadapter timeoutは、この変更で解消したという証拠がない。
ハーネス単体テストをJepsen障害注入campaignの成功と解釈しない。

## 失敗・棄却した試行

| 証跡 | 判断 |
| --- | --- |
| `trials/60ms/` | 候補の観測上限5.010秒、5.160秒で目標未達。実行済み分を完走後に追加比較を停止 |
| `trials/four-unlimited/` | 4ページ・80 msは5秒以内だったが、full比較のread/write p99が悪化 |
| `trials/eight-1MiB/` | 8ページ・200 msは4.151秒、p99悪化のため追加比較を停止 |
| `trials/512-separate-probes/` | 512 KiB・200 ms継続、候補2回目が5.571秒、write p99が61.365 ms。2組まで完走し3組目を停止 |
| `trials/summary-prototype/` | 値を送らない応答はpilotで時間・p99の改善を確認できず、追加protocolとcopy誤受理リスクを避けるため取り下げ |
| `trials/combined-before-lint/` | 入口probe削減後の初回比較。lintの関数抽出前なので、最終候補と混ぜない |
| `trials/merged-prefix/` | 統合probeと100 ms継続の予備3組。旧版fallback追修前なので、最終候補の統計に混ぜない |
| `trials/pilots/` | ページ数・timer・scan粒度・namespace probeの予備試行。採用の統計に混ぜない |
| `trials/profile/` | whole-runのmutex/block/CPU診断。preflight限定profileではない |

source走査のロックが主因という仮説はprofileで確認できなかった。
計上された待ちの大部分はRaft適用時のPebble書込みロックだったが、このwhole-run観測だけでpreflight中の因果を断定しない。
`trials/pilots/pilot-checkpoint.log`はビルド失敗後の古いバイナリで起動したため比較から除外する。
停止用launcherの終了コード99とrunner停止は製品テストの失敗と区別する。

Astra側の固定1ページ対2ページの3組比較は[別の証跡](../performance_20261120_checkpoint_ab/)に保存し、最終候補との比較に混ぜない。

## 測定上の限界

5秒判定には10 ms周期で観測したdurable completionの上限を使う。
負荷集計は従来の100 ms周期で、p99は成功RPCの応答時間である。
preflight時間が異なるため、成功件数やquietな時間の割合も異なる。
成功ops/s、エラー数、検査version数、移動前後の性能も併記する。
closed-loopのため、処理が遅くなると要求到着率も下がる。
3組のfullは、予定到着からの遅延・5組以上・障害注入を含む正式な前景性能の採用ゲートを代替しない。

## VMの停止

全6試行と証跡回収後、ProxmoxのVM 150を`qm shutdown 150 --timeout 60`で正常停止した。
`qm status 150`の最終結果は`status: stopped`で、[停止記録](vm-final-status.txt)に保存した。
VMは削除していない。

## PRへの統合時点

このフォルダの性能値は`5ccfbc74a31f6b21aa4d631704ca922a0749c18e`に測定差分を加えた固定ソースの値である。
公開用の差分は最新mainへ適用しており、上流の読み取りAPI・転送エラー境界の変更も保持している。
この新しい統合SHAで同じ性能比較を取り直したという主張はしない。
新しいcheckoutで過去の測定証跡だけを監査するときは`python3 audit.py --measured-only`を使う。
このモードも測定ソースarchive・テストarchive・測定バイナリSHA・全6試行と18,000件の検証を照合するが、現在の作業treeのソース一致は要求せず、不一致ファイルを監査結果へ記録する。
フラグなしでは従来どおり作業treeのソース一致も必須とする。
PRの新しいSHAに対する回帰・Jepsen stressは、PRとActionsの記録を別に参照する。
