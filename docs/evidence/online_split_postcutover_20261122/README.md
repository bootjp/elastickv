# Post-cutover latency diagnosis and isolated rerun, 2026-11-22

The remaining transient slowdown was reproduced by starting cold firmware discovery on the benchmark VM. The correction isolates the **measurement host**; measured production code remains `c664c80bb954a462b145846b602f6e13849b0cbe` (also the production code in `49c6a52ee`). No new storage, durability, read-publication or mutation-retry change is made here.

## Cause and controlled reproduction

The original exclusive candidate's slowdown was at 2026-11-21 18:23:51–55 UTC, after Split Done at 18:23:30.458 UTC. The guest journal records `fwupd-refresh.service` and its firmware-discovery daemon starting at 18:23:51 and becoming ready at 18:23:55. The earlier claim of benchmark-process exclusivity did not exclude operating-system maintenance. The original trace remains intact in `../online_split_latency_exclusive_20261122/`.

Two initial fresh trials did not reproduce the large tail; healthy traces alone did not establish a cause. A later diagnostic used Pebble event callbacks, syscall timing, process/disk statistics and a slow-RPC-triggered profile. A controlled trial then evicted **only firmware-daemon files** using `posix_fadvise(DONTNEED)` after stopping that daemon, without evicting database files or changing persistence. At 300 offered ops/s after Done, `systemctl start fwupd-refresh.service` at 19:41:04.116 UTC reproduced the symptom; it finished at 19:41:07.399 UTC. It refreshes metadata, not firmware.

During this controlled probe:

- The last ten seconds of the 300 ops/s stage had read p99 **290.213 ms**, write p99 **747.521 ms**, and successful rates **138.9 / 140.6 ops/s**. The same diagnostic gate fails on the original trace (352.361 / 879.726 ms) and on this reproduction.
- Maximum primary read/write RPC durations were **935.800 / 1,029.948 ms**. Three-node WAL syscall timing reached **428.135 ms** for Raft and **356.324 ms** for Pebble.
- Guest disk reads rose from zero to 14,804 KiB/s; flush await rose from approximately 0.7 ms to 33.73 ms, with disk utilization reaching 96.4%. Stack captures show Raft owners inside `WAL.Save → Fdatasync`, explaining why lease/read progress also stalls.
- No Pebble flush/compaction/stall event occurred in the probe interval. The recorded GC pause was 0.41 + 0.015 ms, far shorter than the observed RPCs. These observations distinguish the reproduced I/O interference from a Go stop-the-world pause or a migration cleanup lock.
- All **17,517** diagnostic ACKs passed **52,551/52,551** exact-value observations through the three ingresses. The five post-Done paths also passed 600 writes/readbacks each.

A 30-second, single-stage **Split=none** control also reproduced the delay. In equal five-second windows before/during the cold probe, read p99 rose **9.157 → 405.914 ms**, write p99 **17.616 → 717.199 ms**, and successful read/write rates fell **150.0/149.8 → 112.2/111.8 ops/s**. All 4,309 ACKs passed 12,927/12,927 exact observations. This demonstrates that the reproduced stall does not require Split, source capture or target cleanup.

The diagnostic is excluded from performance comparisons: it intentionally injects OS activity, enables instrumentation and uses only the first two ramp stages. The larger instrumented exploration also passed its 69,572-ACK / 208,716-observation audit; its throughput is not comparison evidence.

## Correction and reproduction

`ELASTICKV_LOAD_EXCLUSIVE_HOST=1 scripts/run-split-load-ramp.sh /outside/repo/results` now obtains a cooperating-process host lock and temporarily runtime-masks `fwupd-refresh.timer`, `fwupd-refresh.service` and `fwupd.service`. This covers the entire run and ACK audit. The wrapper refuses already-running service maintenance, preserves existing masks, restores active timers, and unwinds partial failures and interrupted child processes before releasing isolation. Six focused tests and full lint pass. Runtime-only masks require no persistent configuration change. Prebuilt A/B controllers can instead be wrapped as one command with `scripts/isolate-load-host.py`; that is how this rerun was performed.

The dedicated VM reservation excludes other benchmark clients on VM 150. The physical storage is still shared with other VMs. This correction prevents this known background probe; it does not guarantee an absence of all host/storage jitter. Reboot or manual recovery is needed after an uncatchable kill; recovery uses only masks recorded as added by this run.

## Frozen comparison

Both runs use exactly the same server and client bytes as the prior comparison, so the rerun does not conflate a fresh server rebuild with the isolation change. Baseline: `8c2282837`; candidate: `c664c80bb`. Binary hashes, configuration, stage samples, errors, drops, windows and audit counts are in `comparison.json` and `manifest.json`. Both server binaries and the identical trace driver were built with Go 1.27.3.

VM 150 has 8 vCPUs / 8 GiB, three real etcd-Raft nodes and two replication groups sharing one server process/disk, a separate client, TLS/Forward authentication and Sync persistence. Each fresh-store run offers **100/300/500/1000 ops/s × 90 s**, concurrency 32, 50% reads, 256 seed keys, unique 1 KiB writes and 2 s RPC deadlines, with immediate ACK readback. Profiles, syscall tracing and diagnostic logging are disabled in both formal arms. Baseline then candidate run under one isolation scope.

Primary-RPC completion timestamps define windows. p99 includes successful primary RPCs only, excluding immediate verification and final audit. Drops remain separate. Recounting checks unique trace IDs, stage attempts/success/error maps/p99, planned = admitted + drops, ACK cardinality, expected audit attempts and successful audit observations.

### Split at the same offered rate

The common window covers the same number of whole seconds after each cross-group request start while the 300 ops/s stage is active.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Window seconds | 65.000 | 65.000 |
| read p99 (ms, success) | 16.732 | 14.388 |
| read successful ops/s | 148.277 | 148.200 |
| read errors | Unknown: 6 | 0 |
| write p99 (ms, success) | 35.382 | 38.034 |
| write successful ops/s | 148.292 | 148.200 |
| write errors | Unavailable: 1 | Unavailable: 4 |

### After Done at the same offered rate

This window starts at each observed Done and uses the largest common whole-second duration still inside the 300 ops/s stage. Cleanup time is therefore excluded from both arms.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Window seconds | 8.000 | 8.000 |
| read p99 (ms, success) | 61.728 | 6.827 |
| read successful ops/s | 149.625 | 150.000 |
| read errors | 0 | 0 |
| write p99 (ms, success) | 133.167 | 12.160 |
| write successful ops/s | 147.875 | 149.875 |
| write errors | 0 | 0 |

### Fixed ten-second tail after route cutover

This is the last ten seconds of the 300 ops/s stage, fixed to its offered schedule. Both routes have cut over before it begins. Baseline includes Cleanup: **True**; candidate includes Cleanup: **False**. This distinction is retained rather than presenting a Cleanup-overlapping window as post-Done.

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Window seconds | 10.000 | 10.000 |
| read p99 (ms, success) | 59.896 | 7.062 |
| read successful ops/s | 149.700 | 150.000 |
| read errors | 0 | 0 |
| write p99 (ms, success) | 132.176 | 12.196 |
| write successful ops/s | 148.600 | 150.000 |
| write errors | 0 | 0 |

## Full workload and ACK audit

| Metric | Baseline | Candidate |
| --- | ---: | ---: |
| Split Done (s) | 76.551 | 65.122 |
| Preflight upper bound (s) | 1.611 | 1.530 |
| Full-run successful reads | 50,695 | 51,249 |
| Full-run read errors | Unknown: 6 | 0 |
| Full-run successful writes | 50,699 | 51,348 |
| Full-run write errors | Unavailable: 1 | Unavailable: 4 |
| Capacity drops | 62,810 | 61,986 |
| Scheduler drops | 6,789 | 6,413 |
| Distinct ACKs | 50,699 | 51,348 |
| Exact observations across three ingresses | 152,097/152,097 | 154,044/154,044 |

All ACKs were read back after Done and route convergence. Five additional post-Done paths each completed 600/600 writes/readbacks with zero errors per arm. Fixture exits are zero; degraded-client exits due to drops or RPC errors are retained separately. This is one pair, not a significance claim, a general throughput guarantee, or proof about unacknowledged outcomes. Full-stage p99 and drops remain available even where selected windows look better. No claim attributes all historical DeadlineExceeded/Unknown responses to this OS probe; their prior outcome/retry fixes and limitations remain in the earlier report.

## Evidence

`compare.py` recomputes comparison metrics from the immutable original logs and compressed CSVs. `check_tail.py` is a diagnostic RED/GREEN symptom gate, not a product SLA or normal CI threshold. The diagnostic patch, syscall probe, controller and service journals are retained here; full raw traces, stack/CPU profiles, disk/process statistics and hashes are preserved at the durable artifact path in `manifest.json`. The formal controller and wrapper were not edited during execution. Host unit restoration, raw hash verification, workload exit and VM shutdown are checked before releasing the VM.
