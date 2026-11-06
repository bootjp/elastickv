# FIFO Retroactive Delay and Sequence-Authoritative Heads

Status: implemented in this branch; independent review, publication CI and
main merge remain gates. This record describes the protocol and its observed
local evidence, not a completed crash campaign or deployment. It addresses
issue #1365, independently of the observation-boundary work.

## Contract

[SQS delay queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html)
defines FIFO queue-delay changes as retroactive and Standard changes as
non-retroactive. Queue delay controls initial delivery; it does not replace
an already received message's visibility timeout. For a never-received FIFO
message the new deadline is its original send timestamp plus the new delay,
not the attribute-update timestamp plus the delay. A nonempty queue accepts
delay changes. Undeleted earlier sequences cannot be overtaken in a group,
including when eligible timestamps differ through redrive or clock movement.

## Decisions

A FIFO DelaySeconds update publishes a durable metadata fence and a bounded
byage cursor. Its configuration request drains the job before returning
success. A canceled or failed request leaves the fence and cursor durable;
the existing leader reaper advances one page per queue per tick, and another
SetQueueAttributes request can finish the job synchronously. No receive call
repairs or scans the entire queue. Unknown transaction outcomes propagate as
errors, not successful empty receives or blind retries.

Each page reads up to 16 existing byage entries in one partition at one
leader-issued read timestamp. It updates at most approximately 1 MiB of
accounted message/index mutation bytes. This is not a strict transaction-size
ceiling: the first accepted stored record is allowed to exceed the budget so
it cannot permanently stall the cursor. If it does, no second record is added.
Thus the accounted message/index bytes are bounded by
`max(1 MiB, first_record_mutation_cost)`; the metadata/cursor mutation and
transaction framing are additional. Existing request/body/store limits bound
an accepted record; this patch does not tighten those validation policies.
The overshoot regression sends allowed-maximum bodies with attributes that
fit the public JSON request limit, then verifies base64-expanded storage costs
exactly one record per oversized page and that the next page completes.
Data reads, metadata and generation participate in OCC. The record/index
mutations and cursor advancement commit in the same transaction. A successful
page cannot be skipped after a crash; a failed page cannot advance its cursor.

For each never-received record the page replaces available_at, visible_at,
and the old visibility key with the recomputed deadline. Received records'
body, available_at, visible_at, receive count and receipt state are unchanged.
All surviving FIFO records also get a sequence membership entry. Standard
records are never rewritten by this mechanism.

The membership key is the existing dispatched group-lock key followed by
`0x00 || big-endian-u64(sequence) || encoded(message-id)`, with the message ID
as its value. Encoded group segments cannot contain zero, so the child prefix
cannot alias another group's lock or members. The existing group family gives
these keys the same partition routing, migration, physical backup and
generation cleanup as the lock; no additional global routing family is added.

Only the last page publishes fifo_order_indexed and removes the fence. Before
then SendMessage, FIFO batch entries, ReceiveMessage and redrive INTO that
queue fail with retryable ServiceUnavailable. Stale pre-fence requests lose
their metadata OCC check. Valid receipt-based delete/visibility changes remain
possible and conflict safely with any page touching their data. Delete,
expiry and redrive remove membership even during initial backfill.

After activation, a new send or redrive adds membership in its existing OCC
transaction. An unlocked receive scans exactly one membership entry for its
group and loads that head, rather than choosing by visibility time. A future
head blocks only that group; another group's visible messages may progress.
The existing persisted lock still controls redelivery and prevents concurrent
receivers from acquiring different members. Missing or inconsistent indexed
heads fail closed rather than reverting to visibility-order selection.

## Existing Queues and Recovery

Unchanged queue/message magic and existing byage/data/vis keys remain readable.
There is no assumption that pre-upgrade messages already have membership.
On the first FIFO DelaySeconds update, including a same-value update, the
fenced job builds membership for every surviving record, including inflight
records, before publishing the indexed flag. Queues not yet activated keep
their existing receive implementation. A later delay change uses the same
bounded reconciliation with membership writes remaining idempotent.

Purge changes generation and cancels the old cursor atomically. Delete removes
metadata; the existing tombstone reaper cleans both locks and child membership
keys in the old generation. A stale worker cannot overwrite a purge, deletion,
recreation, concurrent attributes update, receive or receipt-based deletion.
Real Pebble close/reopen tests exercise persistence rather than retaining a
process-local progress map. Physical snapshots retain the same protocol state.
Logical export/import is a separate tool with existing visibility-reset
semantics and is not a transparent physical recovery path.

Activation cannot undo receipts already returned before the fence. A group
owner that was already inflight remains its owner until delete, expiry or
redrive; the patch does not relabel a historically inverted trace as valid.

## Partition Owner Recovery

A real multi-group acceptance test exposed two existing routing gaps that
prevented this protocol from recovering or delivering correctly. Queue
metadata lives on group 1 while partition data/indexes live on group 2.
After the primary COMMIT reply is lost, its durable decision remains in
group 2, but the generic transaction-record read normalized the embedded SQS
key to the global catalog route. Native clean reopen therefore left the
secondary cursor locked, despite the primary record being durable. The
uninterrupted control passed and interrupted recovery failed before correction.

ShardStore now resolves the embedded raw partition key before normalization
for transaction metadata point reads. Commit, rollback, lock, intent, meta and
success-marker families follow that owner, including explicit-group/follower
reads. An unresolved or unavailable owner is an error, not evidence of record
absence. Explicit wrong groups are rejected and existing staged visibility /
readiness checks stay in force. Expired-primary abort selects the same owner.
Ordinary unwrapped ShardStore point reads keep their prior byte-range contract;
the existing primary-evidence route already used the partition resolver.

Once the decision lookup was fixed, stronger receipt assertions exposed a
second gap: deletion returned a no-op 200 after looking for the message in
the global group, leaving the real partition row intact. The SQS adapter now
routes every direct point read through a shared owner-aware getter. Data,
dedup and group-lock reads use explicit partition groups; queue metadata,
generation and sequence retain global routing. Candidate reads that already
carry their group remain explicit. Unknown partition coverage cannot turn
into a successful missing-record/no-op response.

The caller audit covers all ten direct SQS point-read sites (including
visibility changes, reaper and admin peek), transaction status/read-time
resolution and expiry-abort callers. Missing-owner errors remain conservatively
pending where abort confirmation cannot be proved. No guessed abort decision
or new success classification is introduced.

Native tests use two actual Raft engines and Pebble stores, not a local
coordinator replacing 2PC. The hook drops only the reply after a real primary
COMMIT: 16 of 18 records are durably rewritten while the secondary cursor
remains prepared. Both engines/stores close and reopen, then real read-time
resolution must find the persisted decision, publish that page's cursor,
complete the remaining backlog and deliver A before B with actual receipt
deletion. The interrupted and uninterrupted cases pass three race repetitions
after both fixes (14.056s). Initial parent red was 4.431s; the intermediate
false-200 deletion regression was red at 3.069s. These are clean-reopen/lost-
reply checks, not process-kill/power-loss proof or a composed follower-hop plus
staged-migration campaign. Expiry-abort owner selection has a local-FSM unit
control; ambiguous native abort recovery is not claimed.

## Rollout and Cost

Actual Raft membership, not the nonempty entries in an address map, determines
the required peer inventory. Missing/failed/empty membership is rejected.
Only a proven one-member configuration can omit peer mappings; multi-member
configurations must map every Raft endpoint, including learners, to an SQS
endpoint. The union across local groups is used on a sharded coordinator.
Membership is reread after polling and must match its pre-poll snapshot.
HT-FIFO partition routing must also cover every partition before activation.

Every required SQS peer must advertise fifo-retroactive-delay-v1 before a FIFO
delay update can install the fence. An unavailable, malformed or older peer
rejects the update before metadata changes. Capability polling uses the
existing bounded concurrent health poller. Downgrading a cluster after indexed
queues have been activated is not supported: older writers would omit
membership entries. Membership changes and binary downgrades must be quiesced
during activation, and newly admitted members must run supporting binaries.
The health check is not a replicated software-version admission protocol and
does not make arbitrary concurrent old-node admission or downgrade safe.

The tradeoff is a temporary queue-wide Send/Receive outage during a delay
change, with work proportional to backlog only on configuration/recovery.
Normal receives add one bounded group lookup, not an unbounded backlog scan.
Background-only recovery progresses at the existing 30-second reaper cadence;
operators can retry SetQueueAttributes to finish a large interrupted job sooner.
No larger visibility timeout or relaxed ordering assertion is part of the fix.

## Validation Gates

The archived native diagnostic was rerun unchanged first: both legacy and
four-partition FIFO reproduced B-before-A in all three repetitions. Its
original fixture remains outside this patch. The maintained regression asserts
the rewritten persistent deadline, delivery of A (not merely absence of B),
sequence precedence and positive/unchanged-delay controls.

Native Raft/API tests cover delay increases and decreases, legacy/HT/perQueue,
Standard non-retroactivity, unchanged inflight state, cross-group progress and
delete/unlock ordering. Local OCC and real Pebble tests cover bounded paging,
byte limits, stale send/receive/redrive fences, durable continuation and purge.

Completed local gates (implementation based on fc33fa494):

```sh
go test ./adapter -run 'SQS|FIFO|Fifo' -count=1
go test -race ./adapter -run '^(TestSQSFIFO(Delay|RetroactiveDelayNative|Indexed|OrderPrefix)|TestSQSStandardDelay|TestSQSSelectedHead)' -count=3 -timeout=5m
go test -race ./kv -run '^TestCoordinateRaftMembers' -count=3
golangci-lint run ./... --allow-parallel-runners --concurrency=4 --timeout=5m
cd jepsen && LEIN_OFFLINE=true lein test
```

The complete SQS/FIFO selection passed in 27.192 seconds and the final combined
adapter race selection passed three repetitions in 21.402 seconds. Membership
race tests passed three repetitions, and repository-wide lint reported zero issues.
The Jepsen unit suite passed 331 tests / 1206 assertions, zero failures/errors,
using the pinned Redis submodule 54ab5c10ace229fc780e43e188ef1b64c3a32ee4.
The new FIFO regressions, native one/three-node cases, real Pebble close/reopen,
encrypted Pebble reopen, and existing selected-head regressions also pass
targeted repeated race gates. This is local regression evidence, not a proof
of arbitrary crashes, membership changes, or long-running VM nemesis histories.

An unfiltered `go test ./...` was attempted without skips or a larger timeout.
It did not pass: adapter exhausted the default package timeout at 600.769
seconds; the other packages, including root, kv, store, distribution, backup
and the etcd engine, passed. No timeout or assertion was relaxed to turn this
into a successful result. The separate pinned `jepsen/redis` `lein test` also
does not pass: its test requires the absent jepsen.redis namespace and retains
an upstream FIXME / `(is (= 0 1))` scaffold. Neither out-of-scope test was edited.

### Held Stale Page Coverage

An additional barrier regression builds a page and holds it immediately before
Dispatch, then concurrently purges, deletes/recreates, or receipt-deletes its
queue/message. Legacy and HT layouts all require a real OCC conflict, preserved
current metadata, no stale membership/data resurrection and positive subsequent
delivery. Cleanup joins the actual worker body, including on cancellation.
The six cases pass twenty race repetitions (2.551s).

In an isolated staged-tree copy, changing only the page's `IsTxn` to false
caused all six cases to fail at the required OCC verdict (0.731s). Restoring
the file to byte-identical production source retains the original assertions.
Removing only data read-set registration was a surviving negative control:
current receipt deletion also writes the membership tombstone, independently
causing an OCC write conflict. That control is not claimed as a detected
missing-read-key mutation or as evidence that the read set is unnecessary.

### Expanded Final Local Gates

After the owner repairs, the complete KV package passes
`go test -race ./kv -count=1` with no skip (72.615s), and the complete SQS/FIFO selection passes
under race (30.806s), including the native cross-group tests, dedup replay,
actual owner-row deletion, point-read families and staged-catalog guard.
Repository-wide final lint reports zero issues. Independent re-review found
no validated blocker in the supported startup configuration; migration and
explicit-group controls (0.570s) and the RawGet control (0.704s) pass.

Supporting members and known configured groups remain startup/rollout
requirements. The raw explicit-group store API still treats missing local
groups as not found; dynamic removal of a mapped serving group needs the
separate group-lifecycle protocol, not a claim of supported transparent
removal here. Transaction primary metadata explicitly distinguishes unavailable
owners from absent records. No full unfiltered local Go or arbitrary crash
campaign is claimed. Final-push review/security/CI are still required.
An export of the exact staged source, without unpublished files, also passes
the complete SQS/FIFO race selection (26.865s), new KV owner/membership checks
(1.820s) and full repository lint (zero issues).

## Review Lenses

Data loss: cursor progress, data/visibility replacement and membership writes
commit together; read failure and unknown-outcome regressions preserve a
recoverable state. Concurrent deletion participates in data and metadata OCC.

Concurrency/distributed failures: stale send, receive and both redrive sides
are fenced; configuration writers preserve unrelated attributes on retry.
Real membership coverage, version checks and their operational limitations
are explicit rather than inferred from an empty mapping.

Performance: receive adds a one-entry group-index lookup; it never drains a
reindex job. Pages have record and byte budgets, and per-page retries retain
the existing finite retry deadline. Background recovery remains one page per
queue per reaper tick, with the availability cost described above.

Data consistency: timestamps use the existing leader-issued read pipeline;
generation and metadata fence every page. Inflight visibility/receipts remain
unchanged. Normal legacy deletion keeps its previous operation set, while
layout-only reaper cleanup always removes membership for indexed generations.

Test coverage: native delay controls and increases/decreases, Standard
non-retroactivity, literal pre-index SM/1 records, partition-aware reads,
sequence precedence despite redrive timing, receipt deletion, expiry cleanup,
purge/delete/recreate, fault injection and encrypted/durable reopen are covered.

## Changed Paths

All paths below are relative to the repository, on branch
feature/sqs-fifo-retroactive-delay.

```text
adapter/sqs.go
adapter/sqs_catalog.go
adapter/sqs_fifo.go
adapter/sqs_messages.go
adapter/sqs_messages_batch.go
adapter/sqs_purge.go
adapter/sqs_reaper.go
adapter/sqs_redrive.go
adapter/sqs_fifo_delay.go
adapter/sqs_fifo_order_index.go
adapter/sqs_fifo_delay_capability_test.go
adapter/sqs_fifo_delay_concurrency_test.go
adapter/sqs_fifo_delay_native_test.go
adapter/sqs_fifo_delay_redrive_test.go
adapter/sqs_fifo_delay_repro_test.go
adapter/sqs_fifo_delay_test.go
adapter/sqs_fifo_delay_upgrade_test.go
adapter/sqs_fifo_delay_stale_page_test.go
adapter/sqs_fifo_delay_multigroup_test.go
adapter/sqs_point_read.go
adapter/sqs_point_read_test.go
adapter/sqs_admin_peek.go
kv/coordinator_membership.go
kv/coordinator_membership_test.go
kv/shard_store.go
kv/shard_store_partition_txn.go
kv/shard_store_partition_txn_test.go
docs/design/2026_10_30_proposed_sqs_fifo_retroactive_delay.md
```
