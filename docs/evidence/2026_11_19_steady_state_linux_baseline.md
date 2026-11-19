# Linux steady-state baseline measurements

Commit `5ccfbc74a31f6b21aa4d631704ca922a0749c18e`, Go 1.27.3. Three nodes serving two Raft groups ran in one process on a shared host's Ubuntu 24.04 VM (linux/amd64, 8 vCPUs, 8 GiB RAM). Clients connected to the leader over TLS using the default synchronous durability settings. The dataset contained 256 keys with 1 KiB values.

The read-only and write-only workloads each used eight workers. The mixed workload used four readers and four writers. Each phase ran closed-loop for approximately two seconds.

## RawKV before the key ownership change

The following lines are the initial phases from `api_split_linux.txt`.

```text
performance_split_test.go:55: PERF topology nodes=3 groups=2 source_leader=2 target_leader=1 fsm_sync_mode=""
performance_split_test.go:75: PERF phase=before-read write=false elapsed_s=2.000 success=57939 errors=0 ops_s=28967.8 p50_ms=0.260 p99_ms=0.599 first_error=<nil>
performance_split_test.go:75: PERF phase=before-write write=true elapsed_s=2.023 success=625 errors=0 ops_s=308.9 p50_ms=25.762 p99_ms=37.012 first_error=<nil>
performance_split_test.go:75: PERF phase=before-mixed write=false elapsed_s=2.018 success=16149 errors=0 ops_s=8001.2 p50_ms=0.270 p99_ms=4.297 first_error=<nil>
performance_split_test.go:75: PERF phase=before-mixed write=true elapsed_s=2.018 success=560 errors=0 ops_s=277.5 p50_ms=13.787 p99_ms=24.143 first_error=<nil>
```

## Single-update Pebble apply

Each row is one run of `BenchmarkApplyMutationsRaft_SyncMode` from `store_sync_linux.txt`, with 64 B values and `GOMAXPROCS=8`.

| Sync mode | Iterations | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Sync | 567 | 2466414 | 370 | 8 |
| Sync | 468 | 2906573 | 384 | 8 |
| Sync | 441 | 2755029 | 388 | 8 |
| NoSync | 301914 | 3538 | 361 | 9 |
| NoSync | 316376 | 3392 | 359 | 9 |
| NoSync | 299638 | 3612 | 361 | 9 |
