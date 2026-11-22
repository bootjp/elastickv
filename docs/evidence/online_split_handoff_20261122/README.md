# Online Split handoff and write latency

Measured fixed runtime: `5226ac75744e3575daccee23690a36e44178e30f`.
Previous published runtime: `a9f318f16` / production `c664c80bb`.
Executed binaries and raw output identities are in [manifest.json](manifest.json).

## Correction

A proven old-source pre-dispatch fence rejection can arrive after promotion
has removed the target's staged descriptor and admission guard. The following
hint lookup followed the bare target and missed the source's still-armed
tracker, immediately returning Unavailable. Look across local source groups
for a matching armed tracker on this error path. Every retry still repeats
ordinary routing and authorization; Unknown, transport, joined and partial
outcomes remain terminal. Waiter count/bytes/deadlines are unchanged.

The public RawPut/real-FSM regression fails before this correction at both
descriptor removal and target-guard retirement. It then passes, persists the
value, and confirms that source retirement removes the hint.

Two additional review findings are fixed: unrelated prefix deletes share the
same indexed atomic captured batch after logical-footprint and actual-key
checks; an absolute child path supports invoking the load script from scripts.
All eight review threads are resolved, with regressions.

## Final ABBA measurement

VM150 is reserved exclusively: 8 vCPU/8 GiB, three real etcd-Raft nodes/two
groups sharing one server process/disk, TLS/Forward authentication, Sync
persistence, fresh stores, Go1.27.3,
GOMAXPROCS8, profiles and debug overlays off. The separate trace driver is
identical in every arm. Rates are 100/300/500/1000 offered ops/s ×90s,
concurrency32, 50% reads, 256 immutable seeded keys, unique 1-KiB writes,
2s RPC deadlines, immediate ACK verification and no ambiguous-mutation retry.

A1/B1 are `final/baseline` and `final/candidate`; B2/A2 are
`replica/candidate` and `replica/baseline`. All use the same fixed binary per
variant. A cooperative host lock and identical firmware-service masks cover
both isolation scopes and all audits. Scope one also contains a separate
Split=none diagnostic before A1. The physical disk remains shared.

The predefined primary rule uses the first 60s after observed cross-group
request start, shortened equally to whole seconds if any Done precedes 60s.
The resulting common window is **55s at 300 offered ops/s**.
Count primary RPC completions in that window. p99 uses successful RPCs and
excludes verification/audit. Phase polling is 100ms. Phase composition varies;
full phase, full-stage and after-Done observations remain in the JSON.

| Trial | Read p99 ms | Write p99 ms | Read successful ops/s | Write successful ops/s | RPC errors |
| --- | ---: | ---: | ---: | ---: | --- |
| A1 | 43.949 | 80.519 | 149.836 | 149.764 | `{"read":{},"write":{"Unavailable":3}}` |
| B1 | 14.307 | 27.997 | 150.000 | 150.018 | `{"read":{},"write":{}}` |
| B2 | 12.747 | 26.079 | 149.982 | 149.982 | `{"read":{},"write":{}}` |
| A2 | 10.988 | 20.346 | 150.000 | 149.982 | `{"read":{},"write":{"Unavailable":2}}` |

The complete 300 ops/s stage and complete workload audit:

| Trial | Stage read p99 ms | Stage write p99 ms | Split Done s | Preflight upper s | ACKs | Exact reads, 3 ingresses | Capacity/scheduler drops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 33.291 | 81.074 | 66.314 | 1.540 | 49,229 | 147,687 | 65,685/6,793 |
| B1 | 15.981 | 33.787 | 78.616 | 1.610 | 48,879 | 146,637 | 66,653/6,566 |
| B2 | 13.838 | 36.165 | 63.077 | 1.440 | 48,678 | 146,034 | 67,026/6,614 |
| A2 | 12.054 | 25.615 | 55.848 | 1.280 | 50,759 | 152,277 | 62,992/6,399 |

Both fixed trials have zero primary read/write RPC errors across all four
stages, including zero Unavailable, DeadlineExceeded and Unknown. Every one
of **97,557 fixed ACKs** passed all-three-ingress exact-value audit:
**292,671/292,671 observations**. Each trial separately passes
five post-Done paths ×600 write/readbacks (6,000/6,000 across fixed trials).
All four fixtures exit0. Client degraded exits and high-load offered drops
remain visible and are not counted as successful API operations.

Across the complete four-rate workload (using actual stage durations),
read/write successful ops/s are138.978/138.770 baseline versus
135.442/135.404 fixed. Capacity drops rise128,677→133,679; scheduler drops
are13,192→13,180 across two runs per variant. Successful high-load throughput
has not improved. These mixed results are not hidden behind zero RPC errors.

This is two trials per variant, not statistical significance or a general
latency/availability guarantee. The second pair worsens both read and write p99. Per-trial p99 and throughput vary; a consistent performance improvement remains unverified. Split
completion time and total successes also vary with captured data and I/O;
the correction does not establish a uniformly faster complete Split.

## Write-latency diagnosis and rejected change

The earlier profiled diagnosis found only 11 of 97 writes above 35ms overlapping
measured migration maintenance. Non-maintenance group1 scoped WAL commits overlapped
48 (including ordinary data/catalog work). Another diagnostic moved the largest stall to capture startup, showing
that the late-Cleanup burst was not fixed in time.

A separate, isolated **Split=none** control at the same100/300 ops/s prefix
reproduces 300-stage read/write p99 **33.155/82.220ms**, without RPC errors.
At that rate, kernel-measured Raft fdatasync reaches **109.819ms**, and FSM
fdatasync reaches **123.361ms**. 131 of269 primary writes above 50ms overlap
a >50ms sync observation. This overlap is cooccurrence, not individual RPC
causal attribution. BPF and iostat are diagnostic-only; this trial is excluded
from formal performance comparisons. Its17,891 ACKs pass53,673 exact reads.

Consequently a long write tail does not require Split. Ordinary synchronous
I/O and different phase composition limit interpretation of a small p99
change; this follow-up does not claim to eliminate all write-tail latency.
Guest online-discard was investigated, but QEMU uses discard=ignore; no mount,
disk durability, timestamp guard or NoSync change was made.

A smaller256-version/512-KiB maintenance-page experiment worsened write p99
in both matched comparisons (55s common windows):87.695→157.663ms and
45.557→75.325ms. It was withdrawn. The final runtime keeps the original
1,024-version/4-MiB budgets. [All four experimental results](rejected-maintenance-experiment.json)
remain separate and are not pooled with final runtime results.

## Validation and preservation

Final full native short Go suite passes; full store race passes262.420s.
Focused handoff/prefix races pass; all seven host-isolation/path tests and
full commit-hook lint pass. Root Jepsen harness passes442tests/6,184assertions;
this is not a new live Jepsen fault campaign. The unmodified upstream Redis
unit suite still fails on its missing namespace. [Details](validation.json),
[five self-review passes](self-review.json).

An attempted control was rejected before workload launch because restoring
the timer had started fwupd. The controller then also stopped before launch.
Those admission failures are preserved. After natural daemon exit, control
and A1/B1 ran under one isolation scope; B2/A2 waited for restoration and idle
services before acquiring the next scope. No running scripts/binaries were
modified. The earlier failed native diagnostic and superseded variants are
excluded from Linux performance conclusions.

Selected VM files match their original SHA-256 values, including decompressed
RPC CSV hashes. Runtime masks are restored, workloads/profilers have exited,
and VM150 is stopped. Reservation release is recorded in the manifest.

Raw archive: `/Users/bootjp/.codex/visualizations/2026/11/19/01a1f333-53dc-7602-92f8-e3df6c957124/vm150-handoff-20261122/raw-evidence.zip`

SHA-256: `feee6ba6f8fc51d40bfdb87f4c59b4507a0bf4b4cf382caf2a5f81b11ba949ad`

## Reproduction

Check out the fixed SHA and baseline production SHA above in separate trees.
Build Linux server tests with Go1.27.3, CGO_ENABLED=0, GOOS=linux,
GOARCH=amd64 and `go test -c -p 2 ./internal/server`. The archive includes
the exact trace-driver source overlay; preserve the same driver for all arms.
Rebuilt binary hashes may differ with build paths/VCS metadata: record them.
Use fresh result directories and regenerate expected.json with those hashes.

Use [measure.py](measure.py) with the checked protocol and production binaries
inside `scripts/isolate-load-host.py OUTPUT -- python3 measure.py` on the
dedicated Linux host. The complete original orchestration, reverse-order
queue, environment, stages, RPC trace and audit are in the raw archive.
To replay the archived final four trials, extract the ZIP, then run:

```bash
ln -s ../vm-raw/final/baseline combined/A1
ln -s ../vm-raw/final/candidate combined/B1
ln -s ../vm-raw/replica/candidate combined/B2
ln -s ../vm-raw/replica/baseline combined/A2
python3 combined/compare.py
```

The saved reverse-order queue pins measured hashes and paths; adapt those to
new builds before using it for another experiment.

[compare.py](compare.py) independently checks distinct IDs, every stage count,
error and successful p99, offered/admitted/drop equality, nonzero ACKs and
ACK×3=audit attempts=success, all five recovery paths and the common windows.
