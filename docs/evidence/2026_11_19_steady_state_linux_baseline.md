# Linux における平常時の参考測定

コミット `5ccfbc74a31f6b21aa4d631704ca922a0749c18e`、Go go1.27.3。
Ubuntu 24.04 (linux/amd64)、8 vCPU、8 GiB の共有ホスト上の VM に、3 ノードと 2 group を同一プロセスで配置した。
TLS で leader に接続し、既定の同期設定で 256 キー、値長 1 KiB を用いた。
read-only と write-only は各 8 workers、混合負荷は読み書き各 4 workers で、各区間は約 2 秒の closed loop である。

## キーの所有範囲を変更する前の RawKV 測定

元ログ `api_split_linux.txt` の初期区間を抜粋した。

```text
performance_split_test.go:55: PERF topology nodes=3 groups=2 source_leader=2 target_leader=1 fsm_sync_mode=""
performance_split_test.go:75: PERF phase=before-read write=false elapsed_s=2.000 success=57939 errors=0 ops_s=28967.8 p50_ms=0.260 p99_ms=0.599 first_error=<nil>
performance_split_test.go:75: PERF phase=before-write write=true elapsed_s=2.023 success=625 errors=0 ops_s=308.9 p50_ms=25.762 p99_ms=37.012 first_error=<nil>
performance_split_test.go:75: PERF phase=before-mixed write=false elapsed_s=2.018 success=16149 errors=0 ops_s=8001.2 p50_ms=0.270 p99_ms=4.297 first_error=<nil>
performance_split_test.go:75: PERF phase=before-mixed write=true elapsed_s=2.018 success=560 errors=0 ops_s=277.5 p50_ms=13.787 p99_ms=24.143 first_error=<nil>
```

## Pebble の単発 apply

元ログ `store_sync_linux.txt` の `BenchmarkApplyMutationsRaft_SyncMode`（64 B、GOMAXPROCS=8）の各回を示す。

| 同期 | 反復数 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Sync | 567 | 2466414 | 370 | 8 |
| Sync | 468 | 2906573 | 384 | 8 |
| Sync | 441 | 2755029 | 388 | 8 |
| NoSync | 301914 | 3538 | 361 | 9 |
| NoSync | 316376 | 3392 | 359 | 9 |
| NoSync | 299638 | 3612 | 361 | 9 |
