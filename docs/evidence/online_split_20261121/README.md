# Default online RawKV Split validation

The implementation selects the online journal for new cross-group jobs on
persistent plaintext Pebble stores. Same-group splits remain catalog operations.
This directory records local verification separately from Linux cluster load.

## Comparison protocol

Baseline: `5f6ef4e20` (the preceding diagnostic capture implementation, inactive
for normal Split jobs). Candidate: the source manifest recorded with each run.
Both use Go 1.27.3, `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`, eight VM vCPUs,
8 GiB RAM, three real etcd-Raft nodes/two groups in one process, a separate
RawKV load process, TLS and Forward authentication, synchronous persistence,
GOMAXPROCS=8, fresh stores and profiles disabled. Offered rates are
100/300/500/1000 logical operations/s, 90 seconds each, 50% reads,
32 concurrent requests, 1 KiB values, two-second RPC deadlines, immediate
write readback and no load-driver mutation retries. Split starts in stage 1.
Every acknowledged write is audited through all three ingress nodes after
catalog convergence. Five post-Done ingress paths additionally write/read back.

One earlier baseline with 60-second stages ended load before Split finished;
its run is exploratory and excluded from the formal comparison. A passing
ACK audit is a correctness observation, not proof that every offered operation
succeeded. Capacity/scheduler drops and all error categories remain in results.
One VM does not demonstrate physical scale-out or a fault-injection campaign.

## Review passes

1. Data loss: source mutation, effects, sequence and applied index share a
   synced indexed batch. Unsupported data writes persist monotone invalidation
   before ordinary execution. Target versions/receipt/sequence commit together;
   Ready closes base and journal import. Quota exhaustion preserves user data.
2. Distributed failures: immutable source seals survive native membership
   changes; fresh current apply-member probes and indexed target reattestation
   replace old evidence. Capture-start nonce is durable and rediscoverable after
   response loss. Learner admission checks online capability. Root CAS and
   existing ownership/read-drain rules remain authoritative.
3. Performance: capture and transfer have explicit frame/byte/time budgets.
   Metadata is checked once per committed batch; the per-request sealed check
   runs only after the source fence is armed. Handoff retry runs only on the
   error path and bounds count, bytes and waiting time. No unknown write retries.
4. Consistency: transfer replays actual MVCC postimages, including low and
   repeated timestamps. Sequence gaps and changed duplicate receipts fail.
   Ambiguous legacy collection keys invalidate both possible moving owners.
   Final cutover checks the exact immutable seal on current source/target members.
5. Coverage: regressions exercise source/target configuration changes, a
   remaining sealed-tail frame, snapshot reattestation, unsupported writes,
   ambiguous keys, malformed persistent metadata, target freshness/locks,
   handoff cancellation/deadlines, continuous writers and lost initial ACK.

## Limits

Only RawKV point entries are captured online. Intersecting transactions and
collection operations abandon migration while preserving ordinary writes.
The target must be a fresh dedicated group. Encrypted stores use the existing
path. Final handoff still uses creation/read fences; bounded request waiting
does not promise unlimited availability under overload or partitions.
