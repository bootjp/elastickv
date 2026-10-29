# SQS FIFO: deliver same-millisecond messages in send order

Status: implemented (milestone 1)

Review follow-up: receive-progress correction implemented and locally verified;
merge still requires latest-push review and CI.

## Problem

FIFO messages in one message group must be delivered in the order they
were sent. The receive path walks the visibility index in key order and
installs the group lock on the first visible message of a group that has
no lock. The visibility key is

```
!sqs|msg|vis|<queue>|<gen>|<visibleAtMillis>|<segment(messageID)>
```

(and the partitioned equivalent with the partition after the queue).
`messageID` is 16 random bytes, so two messages of one group that share
`visibleAtMillis` are ordered by a coin flip. When the later message sorts
first, it takes the group lock and is delivered before the head.

The test-suite cleanup audit
(`2026_10_16_partial_test_suite_cleanup.md`, Follow-ups) found this from
two flaky tests: `TestSQSServer_FifoGroupLockHoldsAcrossVisibilityExpiry`
and `TestSQSServer_FifoSequenceNumberMonotonic` fail roughly 7/30 and 2/30
runs without `-race`, because both send two messages back to back. A
ten-message `SendMessageBatch` in one group is received out of order in
about half of the runs.

## Why only the tie is wrong

- Every SQS request is proxied to the leader (`proxyToLeader`), so one
  node's clock stamps `AvailableAtMillis` for every send.
- FIFO sends are serialized: each one reads and rewrites the per-queue
  sequence key under OCC, so send N+1 starts after send N commits and
  observes a later or equal wall-clock millisecond.
- FIFO queues reject per-message `DelaySeconds`, so every message of a
  queue gets the same delay offset.

Within one leader term the visibility index therefore already orders a
group by send order except inside a single millisecond. A redelivered
head gets a later `visibleAtMillis`, but it keeps the group lock, so the
messages that now sort ahead of it are skipped as before.

## Design

Break the tie on the receive side, where the sequence number is already known.
For a free group lock, `resolveFifoCandidate` scans the candidate's entire
millisecond/partition at the receive snapshot and resolves the minimum sequence
of that group. The selected visibility key, ID, decoded record and data key
travel together into the existing OCC rotation. The admitted outer page can
therefore attempt that head directly even if it sorts after another outer page.
Skipping successors and hoping that a later receive reaches the head is not
sufficient under the soft scan budget.

- No key, value or wire format changes. Stored messages are handled the
  same as new ones, so there is no rollout window.
- A candidate that already owns the lock (`fifoLockOwn`) or finds another
  message's lock (`fifoLockSkip`) does not scan.
- The scan reads at the receive's snapshot. A send that commits after it
  gets a higher sequence number, so it cannot be an earlier send.

## Decisions

- **Receive-side check instead of an ordered MessageId.** A
  sequence-prefixed MessageId was the first idea, but the visibility key
  stores the MessageId through `encodeSQSSegment`, whose base64 encoding
  does not preserve byte order, so the prefix would not order the keys.
- **Not adding the sequence number to the visibility key.** Exact, but it
  changes the key layout of every FIFO queue and needs a migration of
  in-flight keys, the backup encoder and the routing prefixes.
- **Not clamping `AvailableAtMillis` to increase per queue.** It would
  also cover clock steps, but needs a new per-queue key (or a new value
  format for the sequence key, which `internal/backup` decodes), and it
  delays other groups after a `DelaySeconds` decrease.
- **Not sorting each scanned page by sequence number.** It fixes only ties
  that fall inside one scan page and changes the cross-group order.

## Cost

One paginated scan of a single millisecond/partition plus one record read per
entry when resolving a free group head. The 16-entry page size does not bound
the whole bucket or enforce hard request latency. A partition/snapshot-local
attempted-group set avoids repeatedly resolving/rotating that group across
outer pages. Owned locks do not scan; standard queues do not use this state.

## Limits

A wall-clock step backwards on the leader, or a leader change onto a node
whose clock is behind, can still give a later send an earlier
`visibleAtMillis` than an earlier send. That needs per-queue clamping of
the send timestamp and is out of scope here.

## Milestones

1. Same-millisecond head resolution on group-lock acquisition, preserving
   receive progress across outer-page budgets, with the tests below.

## Test plan

- `TestResolveFifoCandidate_SameMillisecondKeepsSendOrder` (new,
  deterministic): two sends of one group share a millisecond and the later
  one has the MessageId that sorts first. The later candidate must resolve
  the earlier record and its keys; the earlier candidate, another group
  and an isolated send must retain their own identities.
- `TestSQSServer_FifoBatchDeliversInSendOrder` (new): ten entries of one
  group in one `SendMessageBatch` are received and deleted one by one in
  send order. It failed 14/30 runs before the fix.
- The two flaky tests above stop failing: 50 repetitions each.

## Receive Progress Follow-Up

The initially implemented skip is insufficient when the earliest send
sorts beyond the first receive page. The outer receive checks its 100 ms work
budget between pages and restarts at the beginning on every call. Repeating
expensive tie scans can therefore return empty forever without reaching the
deliverable head, even when no new message arrives. Retaining send order must
not turn a live queue into this fixed point.

The correction replaces skip-and-restart with snapshot-scoped head resolution:

1. Keep the original candidate's missing-record, retention and redrive handling
   before group-lock processing. Standard queues remain unchanged.
2. For a held or owned group lock, retain the existing lock classification;
   do not run the free-lock tie scan or replace an owned redelivery head.
3. For a free lock, scan the candidate's entire visible-at millisecond in its
   partition at the same read timestamp. Select the minimum sequence of that
   same group. Ignore missing records and stale visibility entries whose record
   now names another millisecond. Keep the real selected visibility key,
   message ID, partition, group route, decoded record and data key together.
   Page-local sorting is not sufficient.
4. If another head was selected, apply the existing retention/redrive handling
   to that selected record too, before attempting delivery. Never rotate the
   successor's data under the selected head's key or vice versa.
5. Attempt the selected head directly in the current admitted page using the
   existing OCC rotation, lock and receipt path. Reaching it must not require
   another outer page or a later request. A full bucket scan can exceed the
   soft work budget, as before; this is not a hard request-latency guarantee.
6. Retain a request-local attempted-group set across outer pages of one
   partition, not a process-global cursor or cached mutable message record.
   After a free/owned head is selected, avoid repeating that group's resolution
   and rotation within the snapshot. A successful rotation changes the OCC
   keys; a write conflict or transaction lock permits a conservative short
   receive, but does not prove that those keys changed or that a competing
   transaction cannot abort. The next receive must retry from fresh state. Original
   candidates still pass their retention/redrive handling first. Held/foreign
   lock skips do not mark a group attempted, so its actual owner remains
   reachable. Start fresh on the next receive, partition or read timestamp.

Operational dispatch errors still propagate, and unknown outcomes are not
turned into successful/empty receipts. Only existing expected races may skip.
No durable schema, wire encoding, global cache, wall-clock ordering rule or
cross-group ordering guarantee is added.

Regression gates:

- Drive the actual partition receive with a live same-group head sorted after
  its first page and the soft budget expiring during that admitted page. A
  deadline-bound store barrier makes the current repeated-empty failure
  deterministic; a head-first positive control must remain deliverable.
- After correction, the selected head's ID/body/key/receipt and persisted
  rotation agree; no successor is rotated early. Repeat with pagination and
  stale entries, other groups, owned/foreign locks, and partition isolation.
- Cover selected-head retention expiry/redrive, missing records, expected OCC
  races and operational errors. Audit all callers of the changed classifier
  and delivery helpers, run focused repeated race checks and unfiltered lint,
  then request a new latest-push review and CI before merge.

The attempted-group optimization depends on one read snapshot and partition;
it is not reusable after a dispatch retry obtains a new read timestamp. Its
state is local to the receive attempt and cannot authorize a rotation itself.

### Error precedence and caller audit

Error injection also reproduced an empty-success conversion when an unknown
outcome wrapped a write conflict, or a route fence wrapped a transaction lock.
`isIgnorableSQSReceiveRace` now excludes unknown outcomes and route fences
before testing expected races. Its three consumers are receive rotation,
inline retention expiry and receive-triggered DLQ transfer. Their callers were
audited through standard/FIFO delivery and original/selected-head cleanup.
The shared DynamoDB predicate and background reaper were not changed.

### Local validation and five review passes

The original actual-partition regression failed on legacy and partitioned
queues with three consecutive empty receives, one outer page and two tie scans
per attempt; head-first controls committed the correct rotation. The correction
passes the expanded 4/33-message, legacy/partitioned, first/last-head cases,
including the minimum beyond multiple tie pages (ten race repetitions, 10.574s).
Separate tests verify owned head reachability, visibility-zero suppression,
fresh retry after contention, persisted expiry/DLQ cleanup and joined-error
propagation without successor mutation. The final SQS-related race suite passed
in 17.197s; unfiltered repository lint reported zero issues.

The local MVCC-only head-discovery benchmark (Apple M1 Max, Darwin/arm64) measured
2.68/26.7/255 microseconds and 4,823/41,932/326,451 bytes per operation for
4/33/256 messages. This is neither a Raft/network throughput result nor a hard
latency guarantee. Only retained selected keys are cloned.

1. Data loss: selected data, indices, lock and receipt stay bound; expiry/DLQ
   cleanup is one existing OCC operation. Unknown/fenced outcomes remain errors.
2. Concurrency/distributed failures: snapshot-local suppression is discarded on
   a fresh receive. Foreign-lock skips do not hide the reachable owned head;
   expected contention permits a short receive without claiming another commit.
3. Performance: group attempts are shared across outer pages; bucket occupancy
   still determines scan cost. No process-global cursor or mutable-record cache.
4. Consistency: whole-bucket minimum, stale/missing record filtering, partition,
   generation, selected-body identity and existing snapshot/OCC semantics hold.
5. Coverage: actual runtime-red precedes the fix; direct, persisted-state and
   error-injection checks pass. Exact latest-push review and all CI remain merge
   requirements, not inferred from the source PR or local focused tests.
