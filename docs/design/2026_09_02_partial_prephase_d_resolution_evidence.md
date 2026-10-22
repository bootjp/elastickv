# Closing the pre-Phase-D resolution carve-out

Status: Partial
Author: bootjp
Date: 2026-09-02

Related: [2026_08_29_partial_tso_batch_slot_claims.md](2026_08_29_partial_tso_batch_slot_claims.md)
(the separate TSO slot-claim question).

## 1. The finding

`ValidateForwardedTxnCommitTimestamp` (`kv/tso.go`) exempts a forwarded
`COMMIT`/`ABORT` whose commit timestamp predates the Phase-D floor, provided the
same request's start timestamp also predates it:

```go
if !resolution || startTS == 0 {
    return err
}
startErr := ValidateDurablePersistenceTimestamp(ctx, alloc, startTS, label)
if startErr != nil && errors.Is(startErr, ErrTSOTimestampPrePhaseD) {
    return nil
}
```

Both halves of that proof — `resolution` and `startTS` — arrive in the request
being validated. A direct `Internal.Forward` caller can therefore:

1. `PREPARE` at an arbitrary pre-Phase-D `startTS`, which
   `ValidateForwardedTxnStartTimestamp` admits by design, creating an intent at
   that timestamp; then
2. `COMMIT` at another pre-Phase-D timestamp, which the carve-out accepts
   because the request labels itself a resolution and carries a pre-D start.

The result is a write inserted retroactively below timestamps that have already
been read, which breaks snapshot repeatability. Nothing in either step consults
durable state to confirm a primary ever recorded that commit timestamp.

## 2. What the finding gets right, and what it overstates

**Right:** the phase flag proves nothing. The exemption's entire evidence is
attacker-supplied.

**Historical context:** the original finding was against the Phase-D validation
proposal, before `kv/tso.go` shipped. At that time the main branch accepted
forwarded timestamps without these checks. The general validation and bounded
creation window have since shipped; the remaining local-primary evidence check
is described below.

Also worth stating precisely: the invariant `ValidateDurablePersistenceTimestamp`
exists to protect — "never persist at a timestamp group 0 has not issued yet" —
is *not* violated here. Pre-D timestamps sit below the floor and group 0 only
ever issues above it, so no collision with a future issuance is possible. The
damage is the distinct one the finding names in its last sentence: retroactive
insertion below existing read timestamps.

## 3. Why the carve-out cannot simply be deleted

A cross-shard transaction that began before the Phase-D marker applied can still
hold unresolved intents afterwards. Resolving them replays the commit timestamp
the primary recorded (`LockResolver.resolveExpiredLock` →
`applyTxnResolution`), and on a follower that replay travels through
`Internal.Forward`. Rejecting it strands the transaction with its secondary keys
locked. The rollout does not drain transactions before activating Phase D, so
the exemption has to exist in some form.

Note also that `ABORT` resolutions synthesise their timestamp
(`abortTSFrom(lock.StartTS, commitTS)`); it was never recorded by a primary. Any
"must match a durable record" rule therefore cannot cover the abort path
unchanged.

## 4. Options

### 4.1 Verify the primary's durable commit record (the finding's suggestion)

For a `COMMIT` resolution, require that `txnCommitKey(primaryKey, startTS)`
exists and equals the claimed `commitTS`. This is exactly the evidence
`resolveExpiredLock` itself reads, via `primaryTxnRecordedStatus` →
`txnCommitTS`, so the legitimate path already satisfies it by construction.

Costs and open problems:

- One extra durable read on the forwarded-write path per resolution.
- The primary may live on another shard group. `ShardStore.GetAt` routes by key,
  but the background resolver already has a "primary shard is not locally ready"
  case where it declines to answer. A leader that does not host the primary has
  no safe verdict: rejecting breaks legitimate cross-shard legacy resolution.
- Does not cover `ABORT`, per §3.

### 4.2 Time-bound the PREPARE half instead

The attacker's leverage comes from step 1, not step 2: they must be able to
*create* a fresh pre-D intent after Phase D is active. A transaction genuinely
in flight across the marker can only stay preparable for its lock TTL
(`defaultTxnLockTTLms = 30s`, `maxTxnLockTTLms = 24h`); past that the lock is
expired and resolvable. So `ValidateForwardedTxnStartTimestamp` can admit a
pre-D start only while

```
tso_issued_physical_ms < phaseD_activation_time + maxTxnLockTTLms + grace
```

where `phaseD_activation_time` is the physical half of `PhaseDFloor()`, and
`tso_issued_physical_ms` comes from a fresh timestamp issued by the same
durable TSO group. After
the window closes, no new pre-D intent can be created, and the §3 commit
carve-out is then only ever usable against intents that genuinely predate the
marker — which is what it was written for. The commit carve-out stays open
indefinitely, as correctness requires.

Costs and open problems:

- Each pre-D PREPARE needs a fresh TSO timestamp for the admission check.
  The durable TSO value cannot regress with the receiver's wall clock.
- Picking `grace` is an operational knob, and getting it wrong strands
  legitimate long-TTL transactions.
- It does not stop an attacker who acts inside the window.

### 4.3 Both

§4.2 closes the fabrication route in steady state; §4.1 closes it during the
window for the same-group case, where the primary record is locally readable,
while §4.2 bounds new intent creation when the primary is remote.

## 4a. Decision (2026-09-15)

**§4.3**, with §4.1 where the primary record is readable and §4.2's bound
on PREPARE everywhere else. Answers to §5 in order:

1. §4.3. §4.1 alone has no verdict when the primary is remote, and §4.2 alone
   never consults durable evidence even when it is sitting there.
2. A leader that cannot read the primary permits legacy resolution rather
   than refusing or issuing a second RPC. The window applies to PREPARE, so
   no new pre-D intent can be created after it closes; existing cross-shard
   intents can still resolve.
3. `grace` is one hour on top of `maxTxnLockTTLms`. It absorbs clock skew
   between nodes and the spread of a rolling Phase-D activation, both of which
   can make a legitimately in-flight transaction look older than it is. Too
   small strands long-TTL transactions; too large only extends a window that is
   already bounded, so it errs long.
4. `ABORT` remains admissible after the creation window closes. Its timestamp
   is synthesised by `abortTSFrom`, so no record can support it, and requiring
   one would refuse legitimate abort resolution.

The admission bound uses a fresh timestamp from the durable TSO group, not
the receiver's wall clock. Its physical component is monotonic across local
clock rollback, so the closed window cannot reopen on the receiver.

### What has shipped

§4.2 is wired for PREPARE: `ValidateForwardedTxnStartTimestamp` admits a
pre-Phase-D start only inside the window, which removes step 1 of the attack.
The production allocator wrappers expose `PhaseDFloor()`; a Phase-D allocator
without a usable floor refuses new pre-D PREPAREs. COMMIT and ABORT bypass the
creation window so they can resolve existing intents indefinitely.

§4.1 is wired through `WithInternalPrimaryCommitEvidence` in the production
Internal service. `ValidateForwardedTxnCommitTimestamp` invokes the evidence
check only when both timestamps enter its legacy resolution carve-out.
`Internal.Forward` refuses a COMMIT before proposing it if the locally readable
primary record is missing or disagrees, except for the first-primary case
below. ABORT and valid post-D commits do not
perform the additional evidence read.

Evidence is bounded per forwarded batch: duplicate primary keys are read once,
at most 64 distinct primary keys are admitted (checked before any evidence I/O),
and the whole evidence phase shares one five-second budget, respecting any
earlier caller deadline. The batch reader shares one applied read fence per
local owning group rather than starting a new quorum round trip per metadata
entry. A route-version change makes that cached owner's verdict unknown instead
of refencing repeatedly. This prevents repeated metadata in a large Forward
request from multiplying quorum reads or resetting a five-second timeout.

`ShardStore.PrimaryCommitEvidence` provides the separate readability signal.
It uses the primary's physical owner, including partition-map routing, requires
a local active/non-staged store and an applied linearizable-read fence, and reads directly
without proxying to another primary. It discards the verdict if catalog version,
partition owner, or leadership changes while reading. Unavailable, remote,
follower, or migration-staged owners return an unknown verdict rather than a
missing record; actual storage or decoding failures return errors.

The older `primaryTxnRecordedStatus` helper is not this signal: its `done`
return means a transaction decision was found, not that an absent record was
read authoritatively. Reusing it would conflate missing and unavailable records.

### First primary COMMIT and the remaining proof gap

The first primary COMMIT creates `txnCommitKey` itself; demanding that record
already exist would abort a valid transaction whose PREPARE finished before
Phase-D activation but whose primary COMMIT is forwarded afterward. This path
is admitted only when the request actually commits its primary key and this
node finds a matching durable primary lock and intent for the same startTS.
An unprepared primary, secondary resolution without a decision, or a mismatched
existing decision is still rejected. The FSM retains its existing commit versus
rollback and intent checks.

A prepared primary alone proves an in-flight transaction, not issuance of its claimed
commitTS. An untrusted caller can still fabricate a fresh primary PREPARE and
first COMMIT inside the bounded creation window. Closing that remaining gap
needs a durable chosen-commit proof or a revised admission protocol. The
extension accepted below replaces that incomplete first-primary exception. In particular,
silently allocating a replacement timestamp after PREPARE is not a safe shortcut:
prepared values may already embed the original commit timestamp.

Regression coverage includes missing/matching/mismatched decisions, unknown
and follower owners, partition routing and ownership changes, ABORT/post-D
bypass, every primary in a batch, proposal rejection, corrupt records, and
commit-record persistence across Pebble reopen, first-primary admission, and
waiting for a committed decision's read index to apply. Full distributed cutover and
leader-change acceptance have not been demonstrated by these focused tests;
the document remains Partial until that acceptance evidence is available.

## 4b. Durable PREPARE evidence and activation gate (2026-10-23)

The accepted extension persists the selected commit timestamp with PREPARE
and waits for older, unproven PREPAREs to resolve before activating the stronger
protocol. It does not retimestamp an already prepared value or force an
otherwise resolvable transaction to abort merely to finish the rollout.

### Evidence Format and Rollout

`TxnMeta.PrepareCommitTS` uses a new V3 metadata format. V1 and V2 encodings
remain unchanged; in particular, a historical V1 PREPARE's `CommitTS` is not
retroactively reinterpreted as evidence when replaying its log. The chosen
timestamp is saved atomically with each primary or secondary lock and intent.
An idempotent PREPARE cannot add, remove, or change this proof, and a COMMIT
cannot substitute a different timestamp.

Before V3 can mutate a data FSM, an authenticated protocol negotiation checks
every voter and learner in every configured group, including group zero. The
check also requires each existing member to have applied dedicated-TSO cutover;
an outdated mode file cannot justify continued HLC issuance after that marker.
Membership admission checks the same protocol before adding another member.
A fresh learner may join before applying cutover, but a cutover cluster requires
that it configure the dedicated timestamp runtime. Its catch-up must precede
public serving. Old binaries cannot be admitted after this protocol is required.
Dedicated-runtime capability requires both a non-nil group-zero state and an
allocator actually installed in the serving coordinator. Merely hosting the TSO
FSM in legacy warm-up is not readiness, even if it later applies cutover.

The prepare-evidence RPC checks the configured forwarded-control credential at
the handler boundary, including capability-only probes. An absent credential
disables this RPC even when Admin is disabled or explicitly insecure; disabling
Admin authentication does not make peer inspection public. The current runtime
uses its existing leader-read/admin control credential, not the S3 blob token or
an unmerged migration-controller credential. Every per-group registration must
provide that credential, and peer callers already attach it through their group.
Each data-group handler permits at most one authoritative inspection at a time;
additional requests return a retryable resource-exhausted result before a quorum
read or scan. The applied read fence and all scan pages share one server-owned
five-second deadline, shortened by an earlier caller deadline. Capability-only
negotiation performs no scan and does not wait behind an inspection. Exhaustion,
cancellation and scan errors never attest that old locks have drained; callers
retry activation rather than using a partial result.

The replicated enable marker then requires proof for every new PREPARE in its
data group. An existing unproven lock may retry with its original identity and
may COMMIT or ABORT, but a retry cannot manufacture proof retroactively.
Primary and secondary unproven locks, including staged migration locks, must
all drain. Checking only primaries would not justify the remote-primary unknown
verdict retained by the compatibility classifier. New migration imports cannot
introduce unproven locks after the enable marker.

Before creating proof, both the local coordinator and forwarded ingress ensure
that the chosen timestamp is covered by the dedicated TSO's durable reservation
range. Before Phase D, reserving and discarding a timestamp above the chosen
value can extend that range without modifying the already stamped values.
After Phase D, ordinary strict timestamp validation applies. This is not a claim
that every numeric slot in a reserved batch was individually issued; the separate
slot-claim design remains responsible for that stronger property.

### Activation Order

1. Commit cutover, negotiate support and applied cutover with all members, and
   enable required PREPARE evidence on all data groups.
2. Wait for all old unproven locks to resolve. Return a retryable pending result
   between attempts instead of holding the allocator mutex while waiting for
   COMMIT or ABORT traffic that needs it.
3. Replicate a creation seal on every data group. It rejects raw, one-phase, and
   PREPARE creation at FSM apply, while existing COMMIT and ABORT remain usable.
   This closes the late-apply race: an old raw timestamp greater than a sampled
   floor must not land between that sample and activation.
4. Read fresh authoritative committed floors and compute the final boundary B.
   Persist a pending-D marker with that fixed boundary in group zero. Retries,
   snapshots, restarts, and leadership changes preserve B. Ordinary timestamp
   issuance, including cached batch slots, stops while this marker is pending.
5. Apply the permanent timestamp floor B to every data group, verify the drain
   through applied leader fences, and commit Phase D at B in group zero.
6. Replicate the idempotent creation release on every data group, retaining the
   permanent floor. Only then return new allocations. Release failures must be
   retried even if the group-zero Phase-D marker is already active.

After release, every creation request also carries the released admission floor
as its epoch. Both forwarded ingress and the local transaction-manager queue
discard any incoming epoch, strictly validate the unchanged persistence timestamp
against group zero, and then stamp the current epoch. The FSM rejects an old
epoch even when its timestamp exceeds B. A temporary seal alone would not stop
a request validated before cutover but delayed until after release. Requests
queued before the seal must retain their original epoch, not observe a later
retry's mutation of a shared request object.

The cutover timestamp floor is separate from the backup-pin floor. Raw and
one-phase creation must persist above B. A new proof-bearing PREPARE may keep
an older applied-read snapshot as its startTS, but its chosen commit timestamp
must validate above B and its admission epoch must match. Existing applied-read
vouchers remain usable even after the old creation-grace window expires. A real
backup pin retains its original startTS fence; cutover does not weaken it.

The enable, seal, and release records belong to the local group, not its user
ranges. User mutations cannot forge them, and range migration does not export
or promote them as application data. The seal is a deterministic refusal, not
an operational error that halts a raw-batch replica.
Their exact keys live under the already-reserved `!txn|meta|prepare_evidence/`
namespace. The earlier draft's `!txn|backup|prepare_evidence_*` names were valid
user keys before this change; their values are never decoded as protocol state,
overwritten by activation, or excluded from migration. No data rewrite or backup
floor wire-format change is needed. Migration excludes the exact peer-local
keys even from the transaction-metadata family bracket.
The earlier layout was unreleased and was not deployed to production. There is
no content-based fallback: genuine prototype protocol rows cannot be
distinguished from valid legacy user rows, even when their bytes match exactly.
A known deployment of that unreleased layout must stop and use an explicitly
validated offline recovery or rebuild; silently reinterpreting or dropping its
active state during an in-place upgrade is not supported.

### Already Active Clusters

Snapshots from an older implementation can already have Phase D active but no
PREPARE-proof protocol marker. Such a cluster must bootstrap the stronger
protocol without changing its existing Phase-D floor or stranding old locks.
An authoritative primary may keep the old first-COMMIT exception only until its
creation seal or release is durable. Required-proof admission has already stopped
new unproven PREPAREs before that drain finishes. Once sealed or released, a
first primary COMMIT requires the durable chosen timestamp to match exactly.

### Acceptance Status

The existing PR includes a real nine-engine, three-group gRPC acceptance test
covering forwarded cutover, leadership transfer, release failures, and late
creation. Separate joint-recovery tests restore group-zero pending or active
snapshots together with partially released data groups, keep B fixed, and block
issuance until every release completes. Pebble snapshot/reopen tests cover both
V1 and V3 PREPARE followed by repeated ABORT and rejection of a later COMMIT.

Restored pre-upgrade staging is checked again at promotion: a peer-local
control row refuses the whole chunk without moving ordinary rows, advancing
the migration checkpoint, or halting replication. The staged input is retained
for explicit recovery rather than silently deleted. Actual storage failures
retain their existing halt classification.

These tests do not establish a full process-level rolling upgrade or production
stability. The document remains Partial until that deployment acceptance is
available; code presence and in-process tests alone do not establish it.

## 5. Open questions for review

1. Is the acceptable answer §4.2 alone (cheap, no cross-group read, bounded by a
   clock), §4.1 alone (durable evidence, but no verdict when the primary is
   remote), or §4.3?
2. If §4.1: what should a leader that cannot read the primary do — admit, refuse,
   or forward the check to the primary's group leader (a second RPC on the write
   path)?
3. If §4.2: what `grace` value, and should the window be observable so operators
   can tell whether it is still open?
4. Should the `ABORT` path get its own rule, given its timestamp is synthesised
   rather than recorded?

Settled in §4a and extended by §4b. Remaining acceptance work must demonstrate
closure without rejecting valid cutover transactions. Acceptance must exercise
the real forwarded path across Phase-D activation and leader changes, including
a remote-primary resolution after the creation window has closed.
