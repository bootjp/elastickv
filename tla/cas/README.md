# Single-key MVCC CAS safety

The two [FSM regressions](../../docs/evidence/2026_11_20_single_key_mvcc_cas_review.md#final-pre-pr-review-on-main-2026-11-21) concern one direct byte-range key. Two replicas applying the same CAS at the same index disagreed when one watcher lagged a same-owner split. A bounded cleanup could remove the new head and let a second CAS consume the original token again.

## Implementation correspondence

| Model | ElasticKV |
| --- | --- |
| Shared log, indexed apply, durable image, replay | The log is committed by Raft. `applyCASEntry` and `applyVersionIdentityCleanup` stage the result and applied index together. `StageRaftEntryProgress` skips already durable entries. A local apply error aborts the scope and halts at that index. [CAS FSM](../../kv/fsm_cas.go), [cleanup FSM](../../kv/fsm_migration_cleanup.go), [entry progress](../../store/raft_entry_cas.go) |
| Replicated catalog and independent watcher | Public routing selects a candidate group. The frame binds the request to that group and an admitted catalog version. `checkAppliedVersionCASRoute` reads the group's own catalog inside indexed apply and checks the version floor and authority. A same-owner split preserves the write right. A real fence or ownership loss records an indexed route rejection. A missing authoritative catalog or an unmet version floor prevents progress on an injected frame. [CAS routing](../../kv/version_cas_route.go), [CAS FSM](../../kv/fsm_cas.go) |
| Activation, temporary readiness, permanent coverage | The replicated migration version-identity marker turns on the cleanup guard. A cleanup affecting a direct key requires durable evidence covering its entire effective interval. Retirement preserves legacy certificates and partition transitions preserve or expand the covered union. Temporary readiness excludes the key from CAS while armed. Only permanent coverage authorizes protected cleanup of a direct key. The model projects permanent coverage of the effective interval to a boolean for its single key. [Activation and coverage](../../kv/migration_version_identity.go), [CAS routing](../../kv/version_cas_route.go) |
| Exact comparison, versions, bounded cleanup | `compareAndStageCAS` checks the commit watermark, compares the live head with the expected version, and stages the replacement. The model orders rows newest first and selects up to the page limit at or below the ceiling after a cursor. Coverage is checked before scanning, independently of page contents. Legacy cleanup can run before activation. [CAS FSM](../../kv/fsm_cas.go), [cleanup FSM](../../kv/fsm_migration_cleanup.go), [cleanup store](../../store/migration_cleanup.go) |

## Claims and bounds

- **Lean 4.** `applied_prefix` proves by induction that a replica's durable image comes from folding the first `applied` entries. `replica_agreement` gives equality of all rows and values, catalog and protection state, and modeled per-entry verdicts at equal applied indices, regardless of watcher progress, restart or replay. `reachable_safe` establishes duplicate-free consumption and proves that a previously consumed token is strictly below the head while the range lacks permanent coverage. `consumed_token_cannot_apply_again` also covers the permanently excluded case, where physical cleanup may expose an older row. The proofs quantify over arbitrary finite committed logs, natural-number versions and cleanup page sizes.
- **TLC.** The configuration explores every commit, apply and watcher schedule for six bounded workloads, with two voters, one key, initial version 1, catalog versions 1–4 and at most eight entries per workload. It permits one leader change and one recorded replay per voter. It checks progress bounds, equal-index agreement, single consumption, no token revival while uncovered, row and version order, and the expected verdicts and halt preconditions. The workloads cover a same-owner split, a real fence, a real ownership loss, readiness versus permanent coverage, pre-activation cleanup with a cursor, stale tokens, reused write timestamps and an injected CAS lacking the required catalog authority or version.
- **Negative controls.** The watcher gap fails `EqualAppliedIndexAgrees` after the same CAS advances both applied indices. Catching up and replaying that index would skip the entry. The cleanup gap fails `SingleConsumption` after a one-row page deletes the new head and the old token succeeds again. Lean contains executable examples of both defects and the modeled fixes.

The positive agreement proofs assume a common committed prefix and recoverable atomic persistence of each durable image and index in one cluster history. Matching imports and snapshot installs must preserve that history, including the commit watermark, activation marker and durable coverage. The models treat lock-free, live, non-expiring direct keys with valid CAS start timestamps and a pre-existing commit timestamp fence. Other keys and leader timestamp allocation are abstracted into the replicated watermark. Raft, Pebble, interval coverage calculation, WAL recovery, administrative rewind, TTL, locks, and mixed-key transactions sit outside the model. The consumed-token and verdict histories are proof state. The Go store derives the guarantee from exact comparison, increasing versions and protected cleanup.

## Run

From the repository root, run

```sh
make lean-check
make tla-cas-check
```

`make lean-check` uses the version in [lean/lean-toolchain](../../lean/lean-toolchain) when invoked through Elan. Set `LEAN` to use that version's installed binary directly. The TLC targets use the checksum-pinned jar from `make tla-tools`. `make tla-check` includes the CAS suite and the other repository models. A separate TLC scenario includes independent compaction and checks only the visible head at equal indices. It reproduces the outstanding covered-range cleanup failure, also captured by the final Lean example. The expected-counterexample gate for `VisibleHeadAgrees` records the open defect. A fix must turn this scenario into a passing invariant and retain the old policy as a negative control.

The CAS target fails if the protected model fails, a negative control passes, or a negative control reports a different invariant or tool failure.
