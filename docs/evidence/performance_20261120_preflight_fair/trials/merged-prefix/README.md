# 旧版互換性追修前のprefix比較

最終候補の統計には含めない。全試行はpreflight完了witnessで停止し、Done・移動後回復は未検証。

| 指標の中央値 | baseline | candidate |
| --- | ---: | ---: |
| upper_ms | 12150.036 | 3290.731 |
| read_ops_s | 10203.500 | 9482.600 |
| write_ops_s | 409.800 | 386.100 |
| read_p99_ms | 2.250 | 2.866 |
| write_p99_ms | 17.218 | 19.511 |

candidateは3/3回で観測上限5秒以内。read/write p99の中央値はどちらも比較元より大きく、初回だけの改善から前景性能の改善は主張できない。
前景preflightエラーは全6試行でread/writeとも0件。負荷はclosed-loopで、版ごとに測定時間が異なる。
入力とSHAはmetadata.jsonとmeasured_sources.tar.gz、環境はprefix-environment.txt、全試行の値はmetrics.csvに保存する。
