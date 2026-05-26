# Stage 7a-2 — storage-layer registration enforcement (complete write-path coverage)

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-26 |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 writer registry), [`2026_05_26_proposed_7a_process_start_registration.md`](2026_05_26_proposed_7a_process_start_registration.md) (7a coordinator-layer gate) |
| Builds on | 7a (coordinator-layer first-write barrier), 6D-6c-1 (`encryption.StateCache`) |

## 0. Why this slice exists

7a's coordinator-layer barrier (`ShardedCoordinator.Dispatch` +
`leaseRefreshingTxn.Commit`) gates the client-write and lock-resolution
paths. But codex P1 #3 on PR #839 showed it is **structurally
incomplete**: internal callers that write to the store **directly**
bypass the coordinator entirely. The confirmed case:

```
distribution.CatalogStore.Save → applySaveMutations
  → store.ApplyMutations(...)        // the DIRECT, non-Raft local commit path
    → encryptForKey → nonceFactory.Next()
```

`store.ApplyMutations` is the direct local-write path (catalog
bootstrap, admin snapshots, migrations); it is **not** the Raft FSM
apply path (`ApplyMutationsRaft`). It still flows through
`encryptForKey`, so when the §7.1 envelope is active it emits a §4.1
nonce — and it does so without consulting 7a's coordinator barrier, so
a self-originated catalog write can emit a nonce before this node's
writer registration commits.

The single chokepoint where **every** encrypted write emits its nonce
is `store.encryptForKey`. 7a-2 enforces registration there, giving
complete coverage of all self-originated write paths.

## 1. The hard constraint: direct path vs FSM-apply path

`encryptForKey` is reached from **two** classes of caller, which must
be treated differently:

| Path | Entry | Origin | 7a-2 action |
|---|---|---|---|
| **Direct** | `store.ApplyMutations` | self-originated local write (catalog `Save`, admin snapshot, migration) | **fail-closed**: refuse to emit an encrypted nonce before registration |
| **FSM-apply** | `store.ApplyMutationsRaft` | a committed Raft entry being applied on this node (could be any node's write) | **NOT gated**: §4.1 explicitly allows apply during the pre-registration window ("FSM apply may run on leader-proposed entries … decrypted using sidecar DEKs") |

**Why the FSM-apply path must NOT fail-close.** `ApplyMutationsRaft`
runs inside the deterministic FSM apply loop on every node. If it
returned an error for "not registered," the apply loop would HaltApply
— and since the registration entry itself is ordered in the same Raft
log, a storage entry ordered *before* the registration entry would
halt the loop permanently (it can never reach and apply its own
registration). It would also diverge a not-yet-registered follower
from registered peers. The §4.1 contract deliberately permits apply
during the window precisely because the danger is **self-originated**
writes, not replicated apply. 7a's coordinator barrier already gates
self-originated writes that go through the coordinator; 7a-2 closes the
remaining self-originated path (direct `ApplyMutations`), and leaves
the replicated FSM-apply path alone.

This is the key insight that makes "enforce at `encryptForKey`"
tractable: the enforcement is **per-call-path**, gated on the direct
path only, not a blanket check.

## 2. Proposed design

### 2.1 A per-DEK `registered` signal on the StateCache

Add to `encryption.StateCache` a `registeredStorageDEKID atomic.Uint32`
(0 = none) with `MarkRegistered(dekID uint32)` and a `Registered()`
predicate:

```go
func (c *StateCache) Registered() bool {
    id := c.activeStorageDEKID.Load()
    return id != 0 && c.registeredStorageDEKID.Load() == id
}
```

**Per-DEK, not a single bool (gemini medium).** Tracking the registered
DEK *id* rather than a bool gives lock-free per-DEK gating that
composes with 7b: a `rotate-dek` re-points `activeStorageDEKID` to the
new id, and `Registered()` automatically evaluates false (the new id
≠ `registeredStorageDEKID`) until the post-rotation registration marks
the new id — no reset logic, no mutex, just two atomic loads on the
hot path (`activeStorageDEKID` and `registeredStorageDEKID`).

**Mark at BOTH barrier-close sites (claude P1).** `runWriterRegistration`
closes `barrier` at two places — the verify-before-propose path (a
prior attempt already committed) and the propose-success path. 7a-2
refactors both into a single `releaseBarrier(barrier, cache, dekID)`
helper that `close()`s the channel **and** calls
`cache.MarkRegistered(dekID)` atomically, so the
"timed-out-but-committed" verify path cannot leave `registered` false
(which would have the direct-path gate reject writes even after
confirmed registration). The single-active-storage-DEK model means
`registeredStorageDEKID` is one `atomic.Uint32` — no map / `sync.Map`
needed (only one storage DEK is active at a time; 7b's rotate-dek
re-points the active id and re-registers, which `Registered()`'s
equality check handles for free).

Rationale for reusing the StateCache: the storage layer already reads
`StorageEnvelopeActive` / `ActiveStorageKeyID` from it via closures
(6D-6c-2), so a `Registered()` closure threads in the same way without
coupling `store` to the registration machinery.

### 2.2 Store wiring

A new `WithStorageRegistrationGate(registered func() bool)` PebbleStore
option (parallel to `WithStorageEnvelopeGate`). When wired,
`encryptForKey` on the **direct** path returns a typed
`ErrWriterNotRegistered` when the envelope would encrypt
(`StorageEnvelopeActive && activeKeyID != 0`) but `registered()` is
false. The FSM-apply path passes a flag that skips this check.

**`encryptForKey` signature, not just `applyMutationsWithOpts` (claude
P2).** `encryptForKey` is called from more than `applyMutationsWithOpts`,
so 7a-2 adds the path context to **`encryptForKey`'s own signature**
(Option A): `encryptForKey(pebbleKey, plaintext, expireAt, gateRegistration bool)`.
Verified call-site inventory:

| Call site | Line | Path | `gateRegistration` |
|---|---|---|---|
| `PutAt` | 1031 | direct | `true` |
| `ExpireAt` | 1076 | direct (its own call, **not** via `PutAt`) | `true` |
| `applyMutationsBatch` | 1177 | direct (via `ApplyMutations`) | `true` |
| `applyMutationsBatch` | 1177 | FSM-apply (via `ApplyMutationsRaft`) | `false` |

`PutWithTTLAt` delegates to `PutAt` (so it inherits the gate);
**`ExpireAt` does not** — it calls `encryptForKey` directly at line
1076, so it needs its own `gateRegistration = true` (claude P1
correction; an earlier draft wrongly said `ExpireAt` delegates to
`PutAt`). The shared `applyMutationsBatch` site (line 1177) is reached
from both `ApplyMutations` (direct) and `ApplyMutationsRaft` (FSM), so
the flag threaded through `applyMutationsWithOpts` distinguishes those
two rows. `PutAt` / `ExpireAt` are internal/test surfaces today, not
adapter hot paths, but gating them is correct and costs one bool.

`DeletePrefixAt` / `DeletePrefixAtRaft` write only tombstones
(`encodeValue(nil, true, 0, encStateCleartext)`) and never call
`encryptForKey`, so they are outside the gate's scope with no change
needed (noted to close the audit loop — claude P2).

**Runtime direct-write paths too, not just bootstrap (claude P2).**
`CatalogStore.Save` is also reached at runtime via `SplitRange`, not
only startup `EnsureCatalogSnapshot`. An unregistered node attempting a
`SplitRange` before its barrier closes is correctly fail-closed by the
same gate — the implementation must not special-case only the bootstrap
path.

### 2.3 Startup ordering (the catalog-bootstrap question)

`EnsureCatalogSnapshot` runs at startup and may `Save` via the direct
path. If the envelope is active at startup (post-cutover restart) and
this node is not yet registered, a gated catalog `Save` would fail.
Two sub-cases:

- **Catalog already populated** (the steady-state restart): `Save` is
  a no-op (version unchanged), no mutation, no nonce → ungated.
- **Empty catalog + active envelope** (the flagged edge): the bootstrap
  `Save` would be gated. Resolution + deadlock mitigation below.

**No circular dependency.** Registration proposes a reserved-key 0x03
`OpRegistration` entry through the default group's engine; proposing is
submitting bytes to Raft and does not consult the route catalog, and
the registration *apply* writes a `!encryption|writers|…` registry row
(not a route). So registration can commit independent of catalog
bootstrap — bootstrap-gated-on-registration introduces no cycle.
(Verification item retained in §5 against engine bring-up order.)

**Reconciling with the current startup order (claude P1).** Today
`setupDistributionAndRegistration` (`main_encryption_registration.go`)
runs `setupDistributionCatalog` → `EnsureCatalogSnapshot` → (possible
`Save`) **before** `installProcessStartRegistrationGate`. So the
empty-catalog edge currently runs before any gate exists. 7a-2 resolves
this concretely by **reordering** `setupDistributionAndRegistration`:
install the registration gate (arm the barrier + start the 7a
registration goroutine) **first**, then run `EnsureCatalogSnapshot`.
With the gate armed, the bootstrap's direct encrypted `Save` (only when
the envelope is active AND the catalog is empty) returns
`ErrWriterNotRegistered` and is retried via the shared bounded helper
until the barrier closes — at which point `Save` proceeds. This is safe
because registration has no dependency on the route catalog (above), so
arming-before-bootstrap cannot deadlock.

**Deadlock mitigation (gemini medium).** To prevent a permanent startup
hang if registration never commits:
  - the 7a registration goroutine already retries the propose with
    bounded backoff against the run-context (per-attempt timeout,
    leader re-resolution), so "no leader" resolves as leadership
    settles;
  - the direct-path retry is **bounded by the run-context** (and a
    generous ceiling), not infinite — on shutdown it returns the error
    and the process exits cleanly rather than hanging;
  - a WARN log fires when the bootstrap `Save` has been blocked on
    registration beyond a threshold, so a stuck node is diagnosable
    rather than silently hung.
There is no fail-OPEN fallback: bypassing the gate to let bootstrap
proceed unregistered is the exact hazard 7a-2 closes. A node that
genuinely cannot register (e.g. `ErrNodeIDCollision`) fails to start —
which is the intended fail-closed posture, matching the §9.1 startup
guards.

**Centralized retry helper (gemini medium).** The
`ErrWriterNotRegistered` retry/backoff is implemented once (a shared
`retryUntilRegistered(ctx, fn)` helper) and reused by every direct-path
caller (catalog bootstrap, admin snapshot, migration) rather than
duplicated, for consistent backoff + diagnostics.

## 3. Scope

### In scope (7a-2)
- `StateCache.registered` + `MarkRegistered` / `Registered`; 7a's
  `runWriterRegistration` marks it on barrier close.
- `WithStorageRegistrationGate` PebbleStore option +
  `ErrWriterNotRegistered`; `encryptForKey` direct-path enforcement.
- The direct-vs-raft path flag threaded through `applyMutationsWithOpts`.
- main.go wiring of the `Registered()` closure.
- Tests: direct-path write pre-registration → `ErrWriterNotRegistered`;
  post-registration → encrypts; FSM-apply path never gated; envelope
  inactive / no DEK → ungated; catalog-bootstrap ordering.

### Out of scope
- 7b (post-rotation re-registration), 7c (ConfChange-time registration).
- Re-evaluating whether route-catalog data *should* be encrypted at all
  (it is, incidentally, when the envelope is active; changing that is a
  separate question).

## 4. Self-review checklist (for the implementation PR)
- **Data loss** — gating only refuses to *emit* an encrypted write
  before registration (caller sees a typed error and retries); never
  drops a committed write. FSM-apply path untouched → no apply halt.
- **Concurrency** — registration state is `registeredStorageDEKID
  atomic.Uint32` (§2.1); `Registered()` is lock-free — two atomic
  loads (`activeStorageDEKID`, `registeredStorageDEKID`) when an active
  DEK is present, and the direct-path check is skipped entirely on the
  FSM-apply path (`gateRegistration = false`).
- **Data consistency** — FSM-apply determinism preserved (no per-node
  fail-close on replicated apply); coordinator gate + this direct-path
  gate together cover every self-originated encrypted write.
- **Test coverage** — direct vs FSM-apply path, pre/post registration,
  envelope/DEK off, catalog-bootstrap ordering.

## 5. Verification action items (design decisions are settled)
1. **Sequencing is decided** (§2.3: arm the gate first, then
   `EnsureCatalogSnapshot`, with bounded `retryUntilRegistered` for the
   empty-catalog + active-envelope edge). Remaining **verification**
   before implementation: confirm the `OpRegistration` apply path in
   `kv/fsm.go` makes no route-catalog / `distribution.Engine` call, so
   arm-before-bootstrap has no hidden cycle.
2. **Call-site inventory is settled** (§2.2's 4-row table: `PutAt`
   (1031) and `ExpireAt` (1076) are *separate* direct `encryptForKey`
   callers — `ExpireAt` does **not** delegate to `PutAt`;
   `PutWithTTLAt` does; `applyMutationsBatch` (1177) is reached from
   both the direct and FSM paths; `DeletePrefixAt` writes tombstones
   only and is out of scope). Remaining **verification**: grep-confirm
   no *other* `encryptForKey` caller exists.
(Open question 3 — per-DEK vs single bool — is resolved: §2.1 adopts
the per-DEK `registeredStorageDEKID atomic.Uint32`, lock-free and
composing with 7b.)
