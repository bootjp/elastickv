# Centralized TSO Operations

This runbook covers runtime mode changes, production signals, and the rollout
gates for the dedicated group-0 timestamp oracle. The implementation design is
in `docs/design/2026_04_16_implemented_centralized_tso.md`.

## Runtime configuration

Start every node with the same atomically replaceable mode file:

```text
--tsoModeFile=/etc/elastickv/tso-mode
--tsoModeReloadInterval=5s
--tsoBatchSize=256
```

The file contains exactly one mode: `legacy`, `shadow`, `cutover`, or
`phase-d`. `--tsoModeFile` cannot be combined with `--tsoEnabled`,
`--tsoShadowEnabled`, or `--tsoPhaseDEnabled`; those startup-only flags remain
for backward compatibility. Runtime mode reload requires the dedicated group 0.

Replace the file atomically so a poll cannot observe a truncated value:

```sh
printf '%s\n' shadow > /etc/elastickv/tso-mode.tmp
mv /etc/elastickv/tso-mode.tmp /etc/elastickv/tso-mode
```

An unreadable file, unknown value, backward transition, or skipped phase is
rejected without changing the live allocator. Runtime transitions must be
adjacent and one-way:

```text
legacy -> shadow -> cutover -> phase-d
```

The durable group-0 markers override stale local configuration. Once cutover
or Phase D is committed, a node cannot return to legacy or shadow issuance even
if its mode file moves backward. An allocation already in flight during a
reload may finish under the preceding process mode, but cutover-side requests
still use group 0 and the durable markers never move backward.

## Rollout gates

1. Deploy the binary and group 0 while every mode file remains `legacy`.
2. Change every node to `shadow`. Confirm all nodes expose
   `elastickv_tso_mode{mode="shadow"} == 1`.
3. Hold shadow mode until `legacy_overlap` remains zero for a full 15-minute
   window, allocation errors remain below 1 percent, and allocation p99 remains
   below 50 ms.
4. Change nodes to `cutover`. The first production reservation commits the
   one-way cutover marker. A node still in shadow observes that marker and
   returns the dedicated TSO value.
5. Confirm every binary supports Phase D and every node is on the dedicated
   path. Then change nodes to `phase-d`. The first Phase-D reservation commits
   its floor marker before returning a new window.

After the cutover marker commits, rollback means restoring service around the
dedicated TSO path; it never means returning to legacy issuance. Phase D has the
same one-way rule and additionally keeps data-group HLC renewal retired.

## Metrics and alerts

| Signal | Meaning | Gate or threshold |
|---|---|---|
| `elastickv_tso_request_duration_seconds` | Local/remote reserve and validation attempt latency, split by outcome | Warning at reserve p99 > 50 ms for 10m; critical at > 200 ms for 5m |
| `elastickv_tso_shadow_comparisons_total` | Accepted candidates, overlap discards, cutover bypasses, and errors | `legacy_overlap` must be zero for 15m before cutover |
| `elastickv_tso_shadow_divergence_timestamps` | Absolute candidate-to-floor distance for accepted and discarded shadow comparisons | Investigate sustained growth before cutover |
| `elastickv_tso_mode` | One-hot process-local mode | More than one mode for 15m warns about a stalled rollout |
| `elastickv_tso_mode_reload_total` | Applied and rejected reloads plus file read/parse failures | Any failure in 10m warns |
| `elastickv_tso_durable_state` | Applied `cutover` and `phase_d` markers | Phase D without cutover is critical |

The checked rules are in `monitoring/prometheus/rules/tso-alerts.yml`. Allocation
errors warn above 1 percent for 10 minutes and become critical above 10 percent
for 5 minutes. The expressions require at least 0.1 reserve attempts/second so
an idle cluster does not alert on an empty denominator.

## Write-fanout benchmark

Run the modeled remote-TSO benchmark with:

```sh
go test ./kv -run '^$' -bench '^BenchmarkTSOWriteFanout$' -benchmem -benchtime=1s -count=3
```

The harness uses 16 concurrent write coordinators sharing a `BatchAllocator`.
Each operation stamps 1, 3, or 8 fan-out legs, and each refill pays a modeled
1 ms remote-leader plus Raft-commit delay. The following medians were measured
on Go 1.26.5, darwin/arm64, Apple M1 Max:

| Fan-out | Batch | Wall time/op | p99 | Refills/op | Timestamp writes/s |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1.33 ms | 1,249 ms | 1.000 | 753 |
| 1 | 64 | 24.81 us | 1.323 ms | 0.01563 | 40,309 |
| 1 | 256 | 8.92 us | 0.000292 ms | 0.00391 | 112,081 |
| 3 | 1 | 5.70 ms | 1,233 ms | 3.000 | 526 |
| 3 | 64 | 105.35 us | 3.212 ms | 0.04690 | 28,478 |
| 3 | 256 | 17.30 us | 1.272 ms | 0.01172 | 173,453 |
| 8 | 1 | 10.43 ms | 1,104 ms | 8.000 | 767 |
| 8 | 64 | 157.49 us | 1.316 ms | 0.1251 | 50,796 |
| 8 | 256 | 39.43 us | 1.302 ms | 0.03127 | 202,870 |

Batch size 1 is intentionally retained as the unamortized baseline and shows
severe waiter tails under contention. The production default of 256 keeps the
modeled fan-out p99 below 4 ms in this harness, including the refill-contention
tail at fan-out 8, and below the 50 ms warning threshold. These are controlled
local measurements, not a substitute for observing the production histograms
during rollout.

## Intentional non-goals

This closure does not automate cluster-wide phase orchestration, choose a
dedicated subset of TSO members, or change the HLC ceiling formula. Operators
still advance the shared mode file deployment-by-deployment, and the existing
group membership and clock-floor decisions remain independent future work.
