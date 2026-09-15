# Closing the pre-Phase-D resolution carve-out

Status: Partial
Author: bootjp
Date: 2026-09-02

Related: [2026_08_29_proposed_tso_batch_slot_claims.md](2026_08_29_proposed_tso_batch_slot_claims.md)
(the other open TSO admission question on the same PR).

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

**Overstated:** the summary reads as though this PR opens a hole. It does not.
`kv/tso.go` does not exist on `origin/main`; `Internal.Forward` on main accepts
*any* forwarded start and commit timestamp with no validation at all. This PR
closes the general case and leaves one narrow window open. The window is a
regression only against the PR's own stated invariant, not against shipped
behaviour.

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
wall_now < phaseD_activation_time + maxTxnLockTTLms + grace
```

where `phaseD_activation_time` is the physical half of `PhaseDFloor()`. After
the window closes, no new pre-D intent can be created, and the §3 commit
carve-out is then only ever usable against intents that genuinely predate the
marker — which is what it was written for. The commit carve-out stays open
indefinitely, as correctness requires.

Costs and open problems:

- The bound is a wall-clock comparison, which `CLAUDE.md` restricts to
  diagnostics. It would be an *admission* bound rather than an ordering
  decision, but it is still a clock read on a validation path and needs to be
  argued explicitly, not assumed acceptable.
- Picking `grace` is an operational knob, and getting it wrong strands
  legitimate long-TTL transactions.
- It does not stop an attacker who acts inside the window.

### 4.3 Both

§4.2 closes the fabrication route in steady state; §4.1 closes it during the
window for the same-group case, where the primary record is locally readable,
and falls back to §4.2's bound when it is not.

## 4a. Decision (2026-09-15)

**§4.3**, with §4.1 where the primary record is readable and §4.2's bound
everywhere else. Answers to §5 in order:

1. §4.3. §4.1 alone has no verdict when the primary is remote, and §4.2 alone
   never consults durable evidence even when it is sitting there.
2. A leader that cannot read the primary falls back to the window rather than
   refusing or issuing a second RPC. Refusing breaks legitimate cross-shard
   legacy resolution, and a second RPC puts a round trip on the write path to
   close a window that is already bounded.
3. `grace` is one hour on top of `maxTxnLockTTLms`. It absorbs clock skew
   between nodes and the spread of a rolling Phase-D activation, both of which
   can make a legitimately in-flight transaction look older than it is. Too
   small strands long-TTL transactions; too large only extends a window that is
   already bounded, so it errs long.
4. `ABORT` is always a window decision, even when the primary is local. Its
   timestamp is synthesised by `abortTSFrom`, so no record can support it, and
   requiring one would refuse every legitimate abort resolution.

On the wall-clock objection raised in §4.2: the bound is an **admission**
decision about whether a request may create state, not an ordering decision
about where that state sorts. No visibility, OCC or MVCC comparison consults
it, and a clock that is wrong only widens or narrows who may create a pre-D
intent — it can never place a write at the wrong point in the timestamp order.
Skew costs availability for legacy resolution, never correctness.

### What has shipped

§4.2 is wired: `ValidateForwardedTxnStartTimestamp` admits a pre-Phase-D start
only inside the window, which removes step 1 of the attack — the ability to
create a fresh pre-D intent after the marker. An allocator that cannot report
the floor leaves the window open, because a missing signal must not turn a
narrowing into a refusal.

§4.1's decision is implemented and tested as `ClassifyPrePhaseDCommit`, but is
**not yet wired**. It needs a signal the commit-validation path does not
currently have: whether this node can definitively read the primary's record,
as distinct from simply not finding one. `ShardStore.primaryTxnRecordedStatus`
already draws that distinction with its `done` return, and exposing it to the
forwarded-commit path is the remaining work. Wiring it without that signal
would make a remote primary indistinguishable from a missing record and refuse
exactly the cross-shard resolutions §4.1 is careful to preserve.

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

Settled in §4a. The carve-out is now bounded by §4.2; §4.1 remains to be wired
behind a local-primary readability signal.
