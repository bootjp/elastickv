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

A prepared primary proves an in-flight transaction, not issuance of its claimed
commitTS. An untrusted caller can still fabricate a fresh primary PREPARE and
first COMMIT inside the bounded creation window. Closing that remaining gap
needs a durable chosen-commit proof or a revised admission protocol; this PR
does not claim the full retroactive-write carve-out is closed. In particular,
silently allocating a replacement timestamp after PREPARE is not a safe shortcut:
prepared values may already embed the original commit timestamp.

Regression coverage includes missing/matching/mismatched decisions, unknown
and follower owners, partition routing and ownership changes, ABORT/post-D
bypass, every primary in a batch, proposal rejection, corrupt records, and
commit-record persistence across Pebble reopen, first-primary admission, and
waiting for a committed decision's read index to apply. Full distributed cutover and
leader-change acceptance have not been demonstrated by these focused tests;
the document remains Partial until that acceptance evidence is available.

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

Settled in §4a, subject to the first-primary proof gap above. Remaining work
must close that gap without rejecting valid cutover transactions. Acceptance must exercise
the real forwarded path across Phase-D activation and leader changes, including
a remote-primary resolution after the creation window has closed.
