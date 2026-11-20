# 4 ページ preflight の再比較

## 比較条件

前回の 16 ページ、200 ms、通常 1 秒 tick の版を比較元とし、P0 の route version 修正と計測ハーネスを共通にする。
候補は最大 4 ページ、受信 payload 4 MiB、次ページ開始まで 100 ms を上限に読み取り専用の preflight を進め、最後に一度 CAS でチェックポイントを保存する。
永続化済みの未完了進捗が増えた場合だけ 80 ms 後の再実行を許し、それ以外は通常の tick を待つ。
時間予算は進行中の RPC や最後の検証、保存の所要時間を打ち切る期限ではない。

VM 150（8 vCPU、8 GiB、Ubuntu 24.04、local-lvm）上で、3 Raft nodes と 2 groups を同一プロセスで動かす。
source leader は node 0、target leader は node 1 に固定する。
256 キー、1 KiB 値、既定 Sync、TLS、production Forward token、GOMAXPROCS=8、新規 data dir を各実行で共通にする。
前景負荷は 4 reader と 4 writer の closed-loop で、RPC の書き込み先は gRPC の RawKV adapter（RawPut）である。

比較元と候補を 3 組、AB、BA、AB の順序で直列に動かす。
`ELASTICKV_PERF_PREFLIGHT_ONLY` は解除し、split の Done、残存値、移行後の 5 経路それぞれ 600 件のユニーク write と所有者からの読み返しまで検証する。
各バイナリ、ソース、環境の識別情報は `metadata.json`、`measured_sources.tar.gz`、`final-environment.txt` に保管する。
共有 checkout の他の性能変更は隔離した比較元には含めない。

## 計時と性能の解釈

両版で同じ 10 ms 周期の観測を加え、durable completion の時刻を挟む下限と上限を記録する。
phase の負荷集計は従来の 100 ms 周期のままとし、計時の値と区別する。
5 秒以内の判定には計時上限が 5000 ms 未満であることを使う。

p99 は成功 RPC の応答時間であり、各区間の成功数、エラー数、成功 ops/s を合わせて読む。
closed-loop のため、応答が遅い区間では要求到着率も下がる。
2 秒の before/after window や短い preflight の QPS を、一般的な処理能力の保証として扱わない。
この実験は無障害の診断であり、予定到着からの遅延、クラッシュ復旧、障害注入を含む採用ゲートは別途必要である。

## 棄却した試行

2 ページ、60 ms の最初の full 実行は 4.990〜5.010 秒で 5 秒境界をまたいだ。
次の実行は 5.140〜5.160 秒となり、最初の組でも read/write p99 は比較元の 3.061/20.224 ms から 4.051/23.324 ms へ増加した。
この試行を `trials/60ms/` に保存し、4 ページ版の集計には含めない。
進行中だった 2 回目の候補実行まで完走させ、次の比較元を起動する位置で制御用ランチャーを終了コード 99 にして停止した。
その 99 は製品テスト失敗の測定値ではない。

4 ページ、80 ms の preflight-only pilot は 3.627〜3.650 秒、read/write p99 は 3.142/20.701 ms、同区間のエラーは 0 件だった。
pilot は split の Done と移行後 3000 件を検証しないため、本比較の成功回数には加算しない。

## 再現

同じ Go 版で隔離ソースに P0 と共通ハーネスを置き、比較元または候補の明示したソースを重ねて Linux amd64 の `go test -c ./internal/server` を作成する。
生成物の SHA256 を確認して VM に転送し、`run-final.sh` を実行する。
`analyze.py` は全組のログ、終了コード、Done、5 経路の回復結果を照合して集計する。

本比較の結果と検証、VM 停止の記録は完了後に追記する。
