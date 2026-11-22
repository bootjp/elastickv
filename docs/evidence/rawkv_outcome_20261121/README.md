# RawKV migration write outcomes (2026-11-21)

## Cause and correction

The public gRPC RawKV adapter projected migration fences as `Unknown` even when
the single RawPut had been rejected before data proposal. In the instrumented
baseline, error classes contained 4,163 fence errors, 40 cutover errors, 68
Forward outcome-unknown errors, and three context cancellations: 4,274 failed
writes in total (4,263 public Unknown and 11 DeadlineExceeded). Only the first
stack per class was captured: the sampled fence stack shows the coordinator's
pre-routing check and the cutover sample shows admission rejection. These
samples do not establish the originating call site of every counted error.
The diagnostic is not a performance comparison.

A second race remained after correcting admission classification: migration
metadata could change after the advisory preproposal check but before Raft
apply. The FSM rejected the entry before any data mutation, but the RawKV
transaction layer discarded its nonzero commit index. A diagnostic of that
intermediate implementation found two remaining Unknown writes. The local
cutover error's captured stack establishes a pre-data FSM rejection. The other
was a Forward INTERRUPTED / OUTCOME_UNKNOWN verdict; its remote originating
call site was not established by that diagnostic. No ownership or durability
guard was removed to fix the observed local race.

The correction retains scoped evidence through the coordinator, transaction
layer, Forward protocol, and public gRPC projection:

| Evidence for the complete logical mutation | Public code / ErrorInfo reason |
| --- | --- |
| First single RawKV dispatch rejected before data submission | Unavailable / NOT_ADMITTED |
| First single RawKV dispatch has an indexed, origin-verified pre-data apply rejection | Aborted / PROVEN_NONCOMMIT |
| Lost reply, timeout, ambiguous earlier attempt, mixed/secondary error, insufficient scope or index | Unknown / INDETERMINATE |

A request-level witness cannot certify a whole multi-request or multi-group
mutation. Transaction requests and subsequent attempts do not acquire the new
RawKV proof. Error branches and hidden secondary errors invalidate proof before
Forward status decoding, so selecting one gRPC status cannot erase a transport
failure. Mixed conflict/transport failures are not retried. A validated
pre-submission fence reply is a healthy peer response and does not open the
leader-wide circuit breaker.

Migration fences still intentionally reject writes. This change establishes
their outcome; it does not promise uninterrupted write availability. Real
uncertainty remains Unknown and the load driver does not retry failed mutations.

## Method

The formal comparison uses the same dedicated Linux VM150 on pve16intel:
Ubuntu 24.04, Linux 6.8 x86_64, eight vCPUs, 8 GiB RAM. Three real etcd-Raft
nodes and two groups run in one server test process, with a separate load
driver, TLS, Forward authentication, GOMAXPROCS=8, default synchronous FSM
persistence, and profiles disabled. The workload offers 500 logical operations
per second for 300 seconds, 50% reads, 32 concurrent operations, 256 immutable
seed keys, 1 KiB values, two-second per-RPC deadlines, immediate write readback,
and no mutation retries. Fresh stores are used for each trial.

Order is A1 (baseline), B1 (fixed), B2 (fixed), A2 (baseline). An additional Forward mixed-error safety check was added before the formal
sequence. A preliminary baseline lost its completion record when its active
shell script was edited to pause candidate startup; that entire sequence was
excluded and restarted in a fresh directory with frozen scripts and binaries.
No candidate from an earlier source version is included.

The first candidate measurement (`scope-excluded-B1`) is also excluded: the
full Go suite exposed that an overly broad shared mixed-error classifier
changed existing SQS joined-fence behavior. The strict branch check was scoped
to the public RawKV projection, the three affected SQS test groups passed with
the race detector, and both B trials use the final binary. The accepted A1
baseline precedes this excluded intermediate run; every accepted trial still
starts with fresh stores, and the accepted order remains A1/B1/B2/A2.

After Split Done and all-node routing convergence, every acknowledged write is
read through all three ingress nodes. Five additional paths each execute 600
writes with readback after Done. Capacity and scheduler drops are retained in
the denominator. A passing correctness test does not imply clean availability.
Preflight is the fixture's sampled upper bound and its migrated version count
must be considered when comparing time. Two trials per variant are descriptive,
not a significance estimate.

The original diagnostic, exploratory-v3, and remaining-error diagnostic are
excluded from formal timing. One diagnostic launch failed before test startup
because transfer had not completed (exit 126, Text file busy); it was rerun
only after verifying the complete binary hash. Formal runners record hashes
before launch; all four recorded hashes were checked against `source.json`
during aggregation. The final candidate transfer was additionally checked
against its expected hash before being released for execution. Large binaries
and diagnostic overlays remain in the local artifact
directory `/private/tmp/elastickv-outcome-20261121-artifacts` and the preserved
guest directory `/home/bootjp/outcome-20261121`.

## Validation and results

Formal results, source and binary hashes, per-trial logs and JSON summaries
are recorded alongside this file. `scripts/analyze-load-ramp.py` regenerates
a summary from each `*-run.log`; it retains all errors and drops.

| Metric | Baseline A1 / A2 | Fixed B1 / B2 |
| --- | --- | --- |
| Write Unknown | 4,246 / 4,284 | 0 / 0 |
| Write Unavailable / Aborted | 0 / 0 | Unavailable 4,260 + Aborted 1 / Unavailable 4,265 + Aborted 2 |
| Mean successful reads/s | 194.069 | 194.355 (+0.15%) |
| Mean successful writes/s | 180.331 | 180.982 (+0.36%) |
| Mean of per-trial read p99, ms | 83.001 | 82.771 |
| Mean of per-trial write p99, ms | 182.321 | 181.853 |
| Preflight upper bound, ms | 3,462.292 / 3,481.465 | 3,050.091 / 3,460.136 |
| Preflight versions | 4,761 / 4,694 | 5,658 / 4,693 |
| Split Done, ms | 190,265 / 189,666 | 204,308 / 189,690 |

These p99 values include all attempted RPCs and are averages of two per-trial
quantiles, not a pooled p99. Successful-only quantiles and scheduled logical
latencies are retained in each JSON summary. Throughput and p99 are effectively
unchanged in this small comparison; no statistically significant improvement
is asserted. Mean Split Done is 3.70% longer in the fixed runs (196.999 s versus
189.966 s), with the slower B1 also processing more preflight versions. This
does not establish a causal explanation or a Split latency improvement.

All four trials exited 0 and verified all 216,881 acknowledged writes through
three ingress nodes, without missing or mismatched values. Fixed-run ACKs total
108,634, giving 325,902 audit observations. Every trial also passed all five
post-Done paths (3,000 writes/readbacks per trial). Both fixed runs completed
preflight within five seconds at this load; no bound is established for other
workloads.

The formal load driver records `status.Code(err)`, not `ErrorInfo.reason`.
Thus the observed result is zero write Unknown and 4,261 / 4,267 write
Unavailable-or-Aborted responses. The NOT_ADMITTED/PROVEN_NONCOMMIT mapping is
an API contract verified by focused regression tests, not a per-error reason
measurement in these four trials. Failed writes are not included in the ACK
audit, so that audit does not independently prove their absence from storage.

Availability is **not clean**: fixed runs still fail 4,261 / 4,267 writes,
have 166 / 175 read Unknown errors during migration, and drop 32,207 / 33,429
operations at the concurrency limit plus 108 / 92 scheduler drops. The baseline
also has read errors and drops; see [the full table](comparison.md). Changing
write outcome reporting does not eliminate these availability failures or
justify replaying genuinely indeterminate operations.

Regression evidence includes a real coordinator rejecting a point mutation
without calling Commit, a real FSM guard racing the advisory check and refusing
before writing data, preservation of that refusal with commit index 9 through
the transaction layer, Forward phase/request-scope checks, index-zero and
multi-request negative cases, mixed conflict/transport failures making exactly
one Commit call, and five valid fence replies followed by a successful request
for an unrelated key without opening the peer breaker. Public error messages
and ErrorInfo are fixed and do not expose internal keys or stack traces.

Independent static review identified the Forward mixed-status decoding hole;
its regression test failed before the fix (0.612 s) and passed with the race
detector afterward. Review of the scope correction also noted an existing SQS
hidden-secondary-error issue in the baseline. This RawKV change does not alter
that separate SQS receive path.

Final validation:

- `GOMAXPROCS=3 go test -p 2 -parallel 2 -timeout=30m ./...`: exit 0 on
  final source. Adapter, server, filesystem, KV and store packages passed;
  cached results are explicitly visible in the log.
- Focused KV proof/guard/Forward tests and adapter regression tests passed
  with `-race`; the final SQS/public/Forward regression selection passed in
  9.071 s, and mixed Forward verdict tests passed in 2.197 s.
- Mandatory commit-hook full repository lint: zero issues on each source fix.
- Jepsen repository `lein test`: 441 tests, 6,179 assertions, zero failures or
  errors. This is harness validation, not a new fault-injection campaign.
- The earlier full-suite failure is preserved separately: three SQS groups
  exposed the scope regression and passed after correction; two unchanged
  filesystem process-kill tests passed three isolated reruns each and then the
  final full suite. The original failure is not relabeled as a pass.

After collecting and validating the four complete trial logs, binary hashes,
exit codes and audit counts, VM150 was gracefully shut down. Proxmox reported
`status: stopped`; guest data and binaries are preserved for follow-up.
