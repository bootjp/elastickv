# 単一 split の preflight を連続処理する変更の比較

測定対象は `5ccfbc74a31f6b21aa4d631704ca922a0749c18e` と前回の P0 RawKV 経路バージョン修正。
candidate は、この baseline に [preflight.patch](preflight.patch) の 2 ファイルだけを加える。
共有 checkout の別作業の store / RawGet 最適化は含めない。
実際にビルドした source は [measured_sources.tar.gz](measured_sources.tar.gz)、source / binary の SHA-256 と条件は [metadata.json](metadata.json)。

## 処理の上限と保存境界

通常 runner は 1 件の split の同じ phase で、最大 16 ページを逐次処理する。
次のページを始める判断には、受信 payload 4 MiB、経過時間 200 ms の継続予算も使う。
200 ms は実行中 RPC と checkpoint の保存を打ち切る hard timeout ではない。
1 ページ単位の既存 attempt timeout と page size 検査は維持する。
空 family もページ数に数え、no-progress と依存先待ちは継続しない。

各ページを永続化し、次のページの前に exact checkpoint、catalog version、leader、capability、namespace admission、transfer witness を再検証する。
同じ phase であっても他 runner / 管理者が更新した job を上書きしない。
完了 witness を保存した tick で返り、fence / lock drain / copy は次の通常 attempt に委ねる。
単独の phase caller は従来どおり 1 step で戻る。
copy、import ACK、checkpoint 保存の集約、複数 job / 複数 split の同時実行は変更しない。
selector は最初の runnable job を選ぶままであり、job 間の公平性を保証する変更ではない。

## 同条件の比較方法

既存の専用 VM 150、pve16intel、Ubuntu 24.04、8 vCPU / 8 GiB、local-lvm を使う。
3 nodes / 2 groups は同一 Linux process 内に置く。
両版を同じ Go toolchain、CGO 無効でクロスビルドし、`GOMAXPROCS=8`、既定 Sync、TLS と本番の Forward token 設定で実行する。
profile は両版とも無効。
新しい data directory / cluster を毎回作り、source leader を node 0、target leader を node 1 に固定してからキーを投入する。

256 keys / 1 KiB value を使い、`m` で same-group split、その右子を `t` で cross-group split する。
前後の read-only / write-only / mixed はそれぞれ 2 秒、migration 全期間は 4 readers / 4 writers の closed-loop。
Done 後には全 routing engine の収束、5 経路それぞれの 600 write と正規所有者からの固有値読み返しを確認する。
全体試験は baseline → candidate、candidate → baseline、baseline → candidate の 3 組を重複させず実行する。
再現用 runner は [run-comparison.sh](run-comparison.sh)。

migration phase は 100 ms ごとの catalog 観測を使う。
`Fence` で preflight 未完了の区間を `migration-preflight` として分離し、RPC 開始時に観測していた phase に結果を帰属させる。
phase 境界には poll 間隔と in-flight RPC のずれがある。
p99 は成功 RPC だけの値なので、必ず成功数 / エラー数と併記する。
失敗後も worker は次の操作へ進むため、速く失敗する key / 経路が試行数に占める割合は大きくなる。
エラー率を、失敗する key の割合や実時間の停止割合に換算しない。
`first_error` は worker 順に選んだ一例であり、時系列の最初を意味しない。

容量保証、線形化可能性、lost update、crash recovery の実験ではない。
定数値による移動中の負荷と、Done 後の固有値照合の検査範囲を分ける。
移動中の総集計は phase の滞在比率が両版で変わるため、その QPS / p99 の差だけを同一 phase の改善として解釈しない。
同じ worker 数と操作規則でも終了時刻が異なるため、成功した write の総量と delta copy の対象履歴量は同じにならない。
split 全体の差はこの負荷下での総合効果であり、全量を scheduler 待ち時間の削減だけに帰属させない。
前後の 2 秒窓は短い診断であり、小さい差や長期的な p99 の非劣化を結論づけない。

## コードの回帰検証

- baseline に synctest を使う実行経路の回帰を追加すると、仮想時計上の 5 秒の完了条件を超えて失敗した（`preflight-red.log`）。
- candidate のページ数・time/bytes 予算・途中 checkpoint 保存と再開・job/leader/configuration 変更・既存 quorum/preflight 回帰は成功（`preflight-green.log`）。
- split 関連の既存 Go suite は成功（`split-suite.log`）。
- 新しい preflight batch tests の race 検査は成功（`preflight-race.log`）。
- 対象差分の lint は 0 issues（`lint.log`）。
- 通常 Jepsen harness unit tests は Java 21 で 349 tests / 1,400 assertions、失敗・エラーとも 0（`jepsen.log`）。実クラスタへの nemesis campaign の結果ではない。
- 独立した Redis scaffold は既存の `jepsen.redis` namespace 不足で起動失敗（`jepsen-redis.log`）。この gate は未完了。

Go の loopback integration tests と通常 Jepsen は sandbox の bind 制限を外した許可済み再実行で検証した。
全体 `go test ./...` の成功は主張しない。前回の既存 adapter timeout と standalone Redis の未完了 gate は分けて記録する。

## 結果と判断

全 3 組、計 6 run が終了コード 0 で成功した。
[各組の結果・中央値・範囲・成功/エラー数](results.md)、[機械可読集計](summary.json)、[phase 別 CSV](metrics.csv) を保存した。
所要時間は baseline → candidate の中央値で、split Done は 260.438 → 206.170 秒（20.8% 短縮）、preflight は 55.957 → 13.147 秒（76.5% 短縮）だった。
各組の Done 短縮率は 18.2–22.0%。

ただし preflight 中の read は 10,013.8 → 8,999.6 ops/s（-10.1%）、write は 403.1 → 355.3 ops/s（-11.9%）。
成功 read の p99 は 2.488 → 3.362 ms（+35.1%）、write は 18.320 → 21.230 ms（+15.9%）で、前景負荷の throughput と p99 の trade-off が出た。
preflight 中のエラーは両版とも 0 だったが、負荷下で 5 秒以内という目標は未達。
「split 時間を短縮した」という局所的な結果であり、「読み書きも高速化した」「採用条件をすべて満たした」とは判断しない。

移動後 read-only の中央値は 21,038.1 → 21,119.8 ops/s、write-only は 131.4 → 544.4 ops/s だった。
ただし baseline の write-only は 123.7–549.3 ops/s、candidate は 511.4–583.2 ops/s と大きく変動した。
mixed write も大きく変動しており、各 2 秒の窓と移動中の履歴量の違いから、通常の書込み経路が約 4 倍高速化したとは主張しない。

全 6 run で移動前・same-group 後・Done 後の通常 phase はエラー 0。
Done 後の固定レート確認は計 18,000 write と正規所有者からの固有値照合がすべて成功した。
一方、cross-group 移動中のエラーは両版で残り、総 read error は baseline 16,911 / candidate 23,489、write error は 1,165,355 / 1,147,040 件だった。
期間と phase 構成が異なる closed-loop の試行数なので、この差を可用性改善や障害率の比較に換算しない。
結果不明 mutation の自動再送や、source fence の迂回は行っていない。

次に評価するのは、1 page 当たりの checkpoint と再 admission の費用、および foreground を圧迫しない再スケジュール間隔。
既存の安全検査と保存境界を維持したまま、5 秒目標と p99 の両方を改善できるかを別の差分で確認する。
copy の集約は今回の対象に含めない。
実プロセスの crash/restart、包括的な nemesis campaign、全体 Go suite と standalone Redis の未完了 gate を成功扱いにはしない。

全ログを回収後、VM 150 を通常 shutdown し、[最終状態](vm-final-state.log) が `status: stopped` であることを確認した。
再集計は `python3 analyze.py`、`python3 render.py`、証跡の完全性検査は `python3 audit.py` で行える。
